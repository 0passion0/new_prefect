"""用持久化检查点衔接可重试的 Prefect Task。"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypedDict, cast

from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph
from prefect import task
from prefect.cache_policies import NO_CACHE
from pydantic import BaseModel, ConfigDict


class InputModel(BaseModel):
    """拒绝未授权的任务输入字段。"""

    model_config = ConfigDict(extra="forbid", strict=True)


class AInput(InputModel):
    value: int


class BInput(InputModel):
    value: int
    divisor: int


class CInput(InputModel):
    value: float


class RecoveryInput(InputModel):
    value: int
    divisor: int


class GraphState(TypedDict, total=False):
    value: int
    divisor: int
    a_output: int
    b_output: float
    c_output: float
    overrides: dict[str, dict[str, Any]]
    tasks: dict[str, dict[str, Any]]
    status: str


@task(name="recovery-A", cache_policy=NO_CACHE, persist_result=True)
def task_a(value: int) -> int:
    """执行 A。"""
    return value + 1


@task(name="recovery-B", cache_policy=NO_CACHE, persist_result=True)
def task_b(value: int, divisor: int) -> float:
    """执行 B；除数为零用于演示可恢复失败。"""
    return value / divisor


@task(name="recovery-C", cache_policy=NO_CACHE, persist_result=True)
def task_c(value: float) -> float:
    """执行 C。"""
    return value * 2


def task_input(state: GraphState, node: str) -> InputModel:
    """合并原始输入和该节点明确允许的输入修改。"""
    defaults: dict[str, tuple[type[InputModel], Callable[[], dict[str, Any]]]] = {
        "A": (AInput, lambda: {"value": state["value"]}),
        "B": (
            BInput,
            lambda: {"value": state["a_output"], "divisor": state["divisor"]},
        ),
        "C": (CInput, lambda: {"value": state["b_output"]}),
    }
    model, values = defaults[node]
    return model.model_validate(
        {**values(), **state.get("overrides", {}).get(node, {})}
    )


def execute_node(state: GraphState, node: str, prefect_task: Any) -> GraphState:
    """记录真实 Task Run，并只在成功时传递其输出。"""
    inputs = task_input(state, node).model_dump()
    result = prefect_task(**inputs, return_state=True)
    record: dict[str, Any] = {
        "task_run_id": str(result.state_details.task_run_id),
        "state": result.type.value,
        "input": inputs,
        "output": None,
    }
    updated: GraphState = {
        "tasks": {**state.get("tasks", {}), node: record},
        "status": "FAILED",
    }
    if result.is_completed():
        output = result.result()
        record["output"] = output
        updated[f"{node.lower()}_output"] = output  # type: ignore[literal-required]
        updated["status"] = "COMPLETED" if node == "C" else "RUNNING"
    return updated


def build_graph(checkpointer: Any) -> Any:
    """构建固定的 A → B → C 图。"""
    builder = StateGraph(GraphState)
    builder.add_node("A", lambda state: execute_node(state, "A", task_a))
    builder.add_node("B", lambda state: execute_node(state, "B", task_b))
    builder.add_node("C", lambda state: execute_node(state, "C", task_c))
    builder.add_edge(START, "A")
    builder.add_conditional_edges(
        "A", lambda state: END if state["status"] == "FAILED" else "B", ["B", END]
    )
    builder.add_conditional_edges(
        "B", lambda state: END if state["status"] == "FAILED" else "C", ["C", END]
    )
    builder.add_edge("C", END)
    return builder.compile(checkpointer=checkpointer)


def graph_state(path: str, run_id: str) -> dict[str, Any]:
    """读取业务运行最新的持久化检查点。"""
    with SqliteSaver.from_conn_string(path) as saver:
        graph = build_graph(saver)
        snapshot = graph.get_state({"configurable": {"thread_id": run_id}})
        return dict(snapshot.values)


def graph_history(path: str, run_id: str) -> list[dict[str, Any]]:
    """按执行顺序返回不同分支中的真实 Task Run。"""
    with SqliteSaver.from_conn_string(path) as saver:
        graph = build_graph(saver)
        config = {"configurable": {"thread_id": run_id}}
        seen: set[str] = set()
        records = []
        for snapshot in reversed(list(graph.get_state_history(config))):
            for node, record in snapshot.values.get("tasks", {}).items():
                if record["task_run_id"] not in seen:
                    seen.add(record["task_run_id"])
                    records.append({"node": node, **record})
        return records


def retry_checkpoint(
    path: str, run_id: str, task_run_id: str, overrides: dict[str, Any]
) -> tuple[str, str]:
    """查找目标 Task 的前置检查点并校验新输入。"""
    with SqliteSaver.from_conn_string(path) as saver:
        graph = build_graph(saver)
        config = {"configurable": {"thread_id": run_id}}
        for snapshot in graph.get_state_history(config):
            for node, record in snapshot.values.get("tasks", {}).items():
                if record["task_run_id"] != task_run_id or not snapshot.parent_config:
                    continue
                parent = graph.get_state(snapshot.parent_config)
                if parent.next != (node,):
                    continue
                state = cast(GraphState, dict(parent.values))
                state["overrides"] = {
                    **state.get("overrides", {}),
                    node: {**state.get("overrides", {}).get(node, {}), **overrides},
                }
                task_input(state, node)
                return node, parent.config["configurable"]["checkpoint_id"]
    raise ValueError("Task does not belong to this recoverable run")

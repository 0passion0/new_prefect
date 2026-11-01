"""验证任务输入修改、检查点恢复和并发保护。"""

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest
from langgraph.checkpoint.sqlite import SqliteSaver

from prefect_agent_mcp.control import CommandService
from prefect_agent_mcp.recovery_graph import (
    build_graph,
    graph_history,
    retry_checkpoint,
)
from prefect_agent_mcp.recovery_worker import update_status
from prefect_agent_mcp.runtime import Runtime
from prefect_agent_mcp.schemas import Registry
from prefect_agent_mcp.store import Store


class FakeState:
    """模拟 Prefect Task 的完成或失败状态。"""

    def __init__(self, output=None, failed=False):
        self.output = output
        self.failed = failed
        self.type = SimpleNamespace(value="FAILED" if failed else "COMPLETED")
        self.state_details = SimpleNamespace(task_run_id=uuid4())

    def is_completed(self):
        return not self.failed

    def result(self):
        return self.output


def test_replay_failed_and_completed_task_keeps_upstream(tmp_path, monkeypatch):
    """失败或完成后重试 B 时，只重新执行 B 与 C。"""
    from prefect_agent_mcp import recovery_graph

    calls = []

    def a(value, return_state):
        calls.append("A")
        return FakeState(value + 1)

    def b(value, divisor, return_state):
        calls.append("B")
        return FakeState(value / divisor) if divisor else FakeState(failed=True)

    def c(value, return_state):
        calls.append("C")
        return FakeState(value * 2)

    monkeypatch.setattr(recovery_graph, "task_a", a)
    monkeypatch.setattr(recovery_graph, "task_b", b)
    monkeypatch.setattr(recovery_graph, "task_c", c)
    path = str(tmp_path / "checkpoints.db")
    config = {"configurable": {"thread_id": "run"}}
    with SqliteSaver.from_conn_string(path) as saver:
        graph = build_graph(saver)
        failed = graph.invoke({"value": 9, "divisor": 0}, config)
    assert failed["status"] == "FAILED"
    failed_id = failed["tasks"]["B"]["task_run_id"]
    node, checkpoint = retry_checkpoint(path, "run", failed_id, {"divisor": 2})
    with pytest.raises(ValueError):
        retry_checkpoint(path, "run", failed_id, {"unknown": 2})
    with SqliteSaver.from_conn_string(path) as saver:
        graph = build_graph(saver)
        parent = {
            "configurable": {
                "thread_id": "run",
                "checkpoint_id": checkpoint,
                "checkpoint_ns": "",
            }
        }
        fork = graph.update_state(parent, {"overrides": {node: {"divisor": 2}}})
        completed = graph.invoke(None, fork)
    assert completed["c_output"] == 10
    assert calls == ["A", "B", "B", "C"]
    assert completed["tasks"]["A"]["task_run_id"] == failed["tasks"]["A"]["task_run_id"]
    node, checkpoint = retry_checkpoint(
        path, "run", completed["tasks"]["B"]["task_run_id"], {"divisor": 5}
    )
    with SqliteSaver.from_conn_string(path) as saver:
        graph = build_graph(saver)
        parent = {
            "configurable": {
                "thread_id": "run",
                "checkpoint_id": checkpoint,
                "checkpoint_ns": "",
            }
        }
        fork = graph.update_state(parent, {"overrides": {node: {"divisor": 5}}})
        replayed = graph.invoke(None, fork)
    assert replayed["c_output"] == 4
    assert calls == ["A", "B", "B", "C", "B", "C"]
    history = graph_history(path, "run")
    assert len(history) == 6
    assert [record["node"] for record in history] == ["A", "B", "B", "C", "B", "C"]
    assert {record["task_run_id"] for record in history}.issuperset(
        {failed_id, completed["tasks"]["B"]["task_run_id"]}
    )

    with SqliteSaver.from_conn_string(path) as saver:
        graph = build_graph(saver)
        initial = graph.invoke(
            {"value": 9, "divisor": 2}, {"configurable": {"thread_id": "other"}}
        )
    node, checkpoint = retry_checkpoint(
        path, "other", initial["tasks"]["A"]["task_run_id"], {"value": 19}
    )
    with SqliteSaver.from_conn_string(path) as saver:
        graph = build_graph(saver)
        parent = {
            "configurable": {
                "thread_id": "other",
                "checkpoint_id": checkpoint,
                "checkpoint_ns": "",
            }
        }
        fork = graph.update_state(parent, {"overrides": {node: {"value": 19}}})
        changed = graph.invoke(None, fork)
    assert changed["c_output"] == 20
    node, checkpoint = retry_checkpoint(
        path, "other", changed["tasks"]["C"]["task_run_id"], {"value": 7.0}
    )
    with SqliteSaver.from_conn_string(path) as saver:
        graph = build_graph(saver)
        parent = {
            "configurable": {
                "thread_id": "other",
                "checkpoint_id": checkpoint,
                "checkpoint_ns": "",
            }
        }
        fork = graph.update_state(parent, {"overrides": {node: {"value": 7.0}}})
        changed_c = graph.invoke(None, fork)
    assert changed_c["c_output"] == 14
    assert (
        changed_c["tasks"]["A"]["task_run_id"] == changed["tasks"]["A"]["task_run_id"]
    )
    assert (
        changed_c["tasks"]["B"]["task_run_id"] == changed["tasks"]["B"]["task_run_id"]
    )


async def test_start_is_idempotent_and_replay_lock_is_atomic(tmp_path, monkeypatch):
    """重复请求只投递一次，同一业务运行只允许一个恢复任务。"""
    from prefect_agent_mcp import control

    scheduled = []

    async def schedule(**kwargs):
        scheduled.append(kwargs["id"])

    monkeypatch.setattr(control.execute_recovery, "create_run", schedule)
    store = Store(str(tmp_path / "monitor.db"))
    await store.initialize()
    runtime = Runtime(store, Registry(), writable=True)
    monkeypatch.setattr(runtime, "client", lambda: None)
    service = CommandService(runtime)
    first = await service.execute(
        "start_recovery", "example", {"value": 9, "divisor": 0}, "key"
    )
    duplicate = await service.execute(
        "start_recovery", "example", {"value": 9, "divisor": 0}, "key"
    )
    assert first.ok and duplicate.data == first.data and len(scheduled) == 1
    run_id = first.data["run_id"]
    assert update_status(str(store.path), run_id, str(scheduled[0]), "FAILED")
    claims = await asyncio.gather(
        store.claim_recovery(run_id, str(uuid4())),
        store.claim_recovery(run_id, str(uuid4())),
    )
    assert sorted(claims) == [False, True]
    await store.engine.dispose()


async def test_unknown_submission_keeps_ids_and_is_not_repeated(tmp_path, monkeypatch):
    """投递结果未知时保留标识，重复请求不再次投递。"""
    from prefect_agent_mcp import control

    calls = []

    async def schedule(**kwargs):
        calls.append(kwargs["id"])
        raise TimeoutError("submission timed out")

    monkeypatch.setattr(control.execute_recovery, "create_run", schedule)
    store = Store(str(tmp_path / "monitor.db"))
    await store.initialize()
    runtime = Runtime(store, Registry(), writable=True)
    monkeypatch.setattr(runtime, "client", lambda: None)
    service = CommandService(runtime)
    arguments = ("start_recovery", "example", {"value": 9, "divisor": 0}, "key")
    first = await service.execute(*arguments)
    duplicate = await service.execute(*arguments)
    assert first.error == duplicate.error == "OUTCOME_UNKNOWN"
    assert first.data == duplicate.data and len(calls) == 1
    assert (await store.recovery(first.data["run_id"]))["execution_task_id"] == str(
        calls[0]
    )
    await store.engine.dispose()

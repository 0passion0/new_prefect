"""由 Prefect Task Worker 执行和恢复固定工作流。"""

import sqlite3
from datetime import UTC, datetime
from typing import Any

from langgraph.checkpoint.sqlite import SqliteSaver
from prefect import task
from prefect.cache_policies import INPUTS

from .recovery_graph import build_graph


def update_status(path: str, run_id: str, task_id: str, status: str) -> bool:
    """仅允许当前执行任务更新业务运行状态。"""
    # shortcut: 进程异常退出后状态需人工核对，需要自动恢复时引入运行租约。
    with sqlite3.connect(path, timeout=5) as connection:
        cursor = connection.execute(
            "UPDATE recovery_runs SET status=?, updated_at=? WHERE id=? AND execution_task_id=?",
            (status, datetime.now(UTC).isoformat(), run_id, task_id),
        )
        return cursor.rowcount == 1


@task(name="recoverable-graph", cache_policy=INPUTS, retries=0, persist_result=True)
def execute_recovery(
    run_id: str,
    task_id: str,
    monitor_path: str,
    checkpoint_path: str,
    parameters: dict[str, Any],
    node: str = "",
    checkpoint_id: str = "",
) -> None:
    """执行首次运行或从指定检查点恢复。"""
    if not update_status(monitor_path, run_id, task_id, "RUNNING"):
        return
    try:
        with SqliteSaver.from_conn_string(checkpoint_path) as saver:
            graph = build_graph(saver)
            config = {"configurable": {"thread_id": run_id}}
            if checkpoint_id:
                config["configurable"]["checkpoint_id"] = checkpoint_id
                config["configurable"]["checkpoint_ns"] = ""
                snapshot = graph.get_state(config)
                overrides = dict(snapshot.values.get("overrides", {}))
                overrides[node] = {**overrides.get(node, {}), **parameters}
                fork = graph.update_state(config, {"overrides": overrides})
                result = graph.invoke(None, fork)
            else:
                result = graph.invoke(
                    {**parameters, "overrides": {}, "tasks": {}}, config
                )
        update_status(monitor_path, run_id, task_id, result["status"])
    except Exception:
        update_status(monitor_path, run_id, task_id, "FAILED")
        raise

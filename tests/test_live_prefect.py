"""针对真实 Prefect 服务和部署的可选集成测试。"""

import asyncio
import os
from uuid import UUID, uuid4

import aiosqlite
import pytest
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from mcp import Client

from prefect_agent_mcp import server
from prefect_agent_mcp.runtime import Runtime
from prefect_agent_mcp.schemas import Registry, Workflow
from prefect_agent_mcp.store import Store


@pytest.mark.skipif(
    not os.getenv("PREFECT_MCP_TEST_DEPLOYMENT_ID"),
    reason="requires a running Prefect server and example deployment",
)
async def test_live_prefect_run_through_mcp(tmp_path, monkeypatch):
    """通过 MCP 执行真实工作流并验证任务、快照和检查点。"""
    deployment_id = UUID(os.environ["PREFECT_MCP_TEST_DEPLOYMENT_ID"])
    path = tmp_path / "monitor.db"
    runtime = Runtime(
        Store(str(path)),
        Registry(
            workflows=[Workflow(workflow_id="document", deployment_id=deployment_id)]
        ),
        writable=True,
    )
    monkeypatch.setattr(server, "runtime", runtime)
    key = str(uuid4())
    async with Client(server.mcp, raise_exceptions=True) as client:
        description = await client.call_tool(
            "describe_workflow", {"workflow_id": "document"}
        )
        assert description.structured_content["ok"], description.structured_content
        arguments = {
            "workflow_id": "document",
            "parameters": {
                "document": "A small document for a real flow.",
                "max_words": 3,
            },
            "idempotency_key": key,
        }
        invalid = await client.call_tool(
            "start_flow",
            {
                **arguments,
                "parameters": {"unknown": "value"},
                "idempotency_key": str(uuid4()),
            },
        )
        assert invalid.structured_content["error"] == "INVALID_INPUT"
        started = await client.call_tool("start_flow", arguments)
        assert started.structured_content["ok"], started.structured_content
        run_id = started.structured_content["data"]["flow_run_id"]
        duplicate = await client.call_tool("start_flow", arguments)
        assert duplicate.structured_content["data"]["flow_run_id"] == run_id
        conflict = await client.call_tool(
            "start_flow", {**arguments, "parameters": {"document": "different"}}
        )
        assert conflict.structured_content["error"] == "IDEMPOTENCY_CONFLICT"
        for _ in range(90):
            result = await client.call_tool("get_flow_run", {"flow_run_id": run_id})
            assert result.structured_content["ok"], result.structured_content
            state = result.structured_content["data"]["state"]
            if state in {"COMPLETED", "FAILED", "CRASHED", "CANCELLED"}:
                break
            await asyncio.sleep(1)
        assert state == "COMPLETED", result.structured_content
        for _ in range(20):
            tasks = await client.call_tool("list_task_runs", {"flow_run_id": run_id})
            assert tasks.structured_content["ok"], tasks.structured_content
            items = tasks.structured_content["data"]["items"]
            if len(items) == 5:
                break
            await asyncio.sleep(1)
        assert len(items) == 5
        analyzed = next(
            item for item in items if item["name"].startswith("agent_analyze")
        )
    graph_path = os.getenv("PREFECT_MCP_LANGGRAPH_DB", "langgraph-checkpoints.db")
    thread_id = f"{run_id}:{analyzed['id']}:{analyzed['run_count']}"
    async with aiosqlite.connect(graph_path) as connection:
        serializer = JsonPlusSerializer(
            pickle_fallback=False, allowed_json_modules=[], allowed_msgpack_modules=[]
        )
        saver = AsyncSqliteSaver(connection, serde=serializer)
        checkpoint = await saver.aget_tuple({"configurable": {"thread_id": thread_id}})
        assert checkpoint is not None
    restored = Store(str(path))
    saved = await restored.get("flow", run_id)
    assert saved and saved[0]["state"] == "COMPLETED"
    await restored.engine.dispose()

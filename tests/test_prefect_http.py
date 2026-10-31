"""验证 Prefect 与 MCP 共用同一个 HTTP 服务。"""

import asyncio
import os
from uuid import uuid4

import httpx
import httpx2
import pytest
from mcp import Client
from mcp.client.streamable_http import streamable_http_client


@pytest.mark.skipif(
    not os.getenv("PREFECT_MCP_TEST_URL"),
    reason="需要已启动的 Prefect 与 MCP 集成服务",
)
async def test_same_prefect_server_serves_mcp():
    """从同一地址访问 Prefect 和 MCP，并执行已注册工作流。"""
    base = os.environ["PREFECT_MCP_TEST_URL"].rstrip("/")
    user = os.environ["PREFECT_MCP_TEST_USER"]
    password = os.environ["PREFECT_MCP_TEST_PASSWORD"]
    async with httpx.AsyncClient() as raw:
        health = await raw.get(f"{base}/api/health")
        denied = await raw.post(f"{base}/api/mcp/", json={})
        version = await raw.get(f"{base}/api/admin/version", auth=(user, password))
    assert health.status_code == 200
    assert denied.status_code == 401
    assert version.status_code == 200

    async with httpx2.AsyncClient(auth=(user, password)) as authenticated:
        transport = streamable_http_client(
            f"{base}/api/mcp/", http_client=authenticated
        )
        async with Client(transport) as client:
            workflows = await client.call_tool("list_workflows", {})
            assert workflows.structured_content["data"][0]["workflow_id"] == "document"
            description = await client.call_tool(
                "describe_workflow", {"workflow_id": "document"}
            )
            assert description.structured_content["ok"]
            arguments = {
                "workflow_id": "document",
                "parameters": {
                    "document": "One Prefect server handles the workflow.",
                    "max_words": 3,
                },
                "idempotency_key": str(uuid4()),
            }
            started = await client.call_tool("start_flow", arguments)
            assert started.structured_content["ok"], started.structured_content
            run_id = started.structured_content["data"]["flow_run_id"]
            duplicate = await client.call_tool("start_flow", arguments)
            assert duplicate.structured_content["data"]["flow_run_id"] == run_id
            for _ in range(90):
                flow = await client.call_tool("get_flow_run", {"flow_run_id": run_id})
                assert flow.structured_content["ok"], flow.structured_content
                state = flow.structured_content["data"]["state"]
                if state in {"COMPLETED", "FAILED", "CRASHED", "CANCELLED"}:
                    break
                await asyncio.sleep(1)
            assert state == "COMPLETED", flow.structured_content
            for _ in range(20):
                tasks = await client.call_tool(
                    "list_task_runs", {"flow_run_id": run_id}
                )
                assert tasks.structured_content["ok"]
                if len(tasks.structured_content["data"]["items"]) == 5:
                    break
                await asyncio.sleep(1)
            assert len(tasks.structured_content["data"]["items"]) == 5

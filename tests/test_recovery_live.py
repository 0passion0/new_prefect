"""验证 MCP、Prefect Task Worker 和检查点共同完成真实恢复。"""

import asyncio
import os
from uuid import UUID, uuid4

import httpx2
import pytest
from mcp import Client
from mcp.client.streamable_http import streamable_http_client
from prefect import get_client


@pytest.mark.skipif(
    not os.getenv("PREFECT_MCP_TEST_URL"), reason="需要运行 Prefect 和恢复 Task Worker"
)
async def test_recover_prefect_tasks_over_http():
    """保留 A 的真实 Task Run，重试 B 并继续 C。"""
    base = os.environ["PREFECT_MCP_TEST_URL"].rstrip("/")
    auth = (
        os.environ["PREFECT_MCP_TEST_USER"],
        os.environ["PREFECT_MCP_TEST_PASSWORD"],
    )
    async with (
        httpx2.AsyncClient(auth=auth) as http,
        Client(streamable_http_client(f"{base}/api/mcp/", http_client=http)) as client,
    ):

        async def call(name, arguments):
            response = await client.call_tool(name, arguments)
            return response.structured_content

        async def wait(run_id, status):
            for _ in range(90):
                result = await call("get_recoverable_run", {"run_id": run_id})
                assert result["ok"], result
                if result["data"]["status"] in {"FAILED", "COMPLETED"}:
                    assert result["data"]["status"] == status, result
                    return result["data"]
                await asyncio.sleep(1)
            pytest.fail("等待 Task Worker 超时")

        arguments = {"value": 9, "divisor": 0, "idempotency_key": str(uuid4())}
        first = await call("start_recoverable_workflow", arguments)
        assert first["ok"], first
        assert (await call("start_recoverable_workflow", arguments))["data"] == first[
            "data"
        ]
        run_id = first["data"]["run_id"]
        failed = await wait(run_id, "FAILED")
        tasks = failed["checkpoint"]["tasks"]
        a_id, failed_b_id = tasks["A"]["task_run_id"], tasks["B"]["task_run_id"]
        assert tasks["B"]["state"] == "FAILED" and "C" not in tasks
        invalid = await call(
            "retry_task",
            {
                "run_id": run_id,
                "task_run_id": failed_b_id,
                "parameters": {"divisor": "bad"},
                "idempotency_key": str(uuid4()),
            },
        )
        assert invalid["error"] == "INVALID_INPUT"
        arguments = {
            "run_id": run_id,
            "task_run_id": failed_b_id,
            "parameters": {"divisor": 2},
            "idempotency_key": str(uuid4()),
        }
        first_retry = await call("retry_task", arguments)
        assert first_retry["ok"], first_retry
        assert (await call("retry_task", arguments))["data"] == first_retry["data"]
        completed = await wait(run_id, "COMPLETED")
        tasks = completed["checkpoint"]["tasks"]
        assert tasks["A"]["task_run_id"] == a_id
        assert tasks["B"]["task_run_id"] != failed_b_id and tasks["C"]["output"] == 10
        b_id, c_id = tasks["B"]["task_run_id"], tasks["C"]["task_run_id"]
        replay = await call(
            "retry_task",
            {
                "run_id": run_id,
                "task_run_id": b_id,
                "parameters": {"divisor": 5},
                "idempotency_key": str(uuid4()),
            },
        )
        assert replay["ok"], replay
        final = await wait(run_id, "COMPLETED")
        tasks = final["checkpoint"]["tasks"]
        assert tasks["A"]["task_run_id"] == a_id
        assert tasks["B"]["task_run_id"] != b_id and tasks["C"]["task_run_id"] != c_id
        assert tasks["C"]["output"] == 4
        history = await call("get_recoverable_history", {"run_id": run_id})
        assert history["ok"] and len(history["data"]) == 6
        async with get_client() as prefect:
            for item in tasks.values():
                run = await prefect.read_task_run(UUID(item["task_run_id"]))
                assert run.state.is_completed() and run.flow_run_id is None

        started = await call(
            "start_recoverable_workflow",
            {"value": 9, "divisor": 2, "idempotency_key": str(uuid4())},
        )
        assert started["ok"], started
        run_id = started["data"]["run_id"]
        tasks = (await wait(run_id, "COMPLETED"))["checkpoint"]["tasks"]
        original_ids = {name: item["task_run_id"] for name, item in tasks.items()}
        replay_a = await call(
            "retry_task",
            {
                "run_id": run_id,
                "task_run_id": original_ids["A"],
                "parameters": {"value": 19},
                "idempotency_key": str(uuid4()),
            },
        )
        assert replay_a["ok"], replay_a
        tasks = (await wait(run_id, "COMPLETED"))["checkpoint"]["tasks"]
        assert all(tasks[name]["task_run_id"] != original_ids[name] for name in tasks)
        assert tasks["C"]["output"] == 20
        a_id, b_id = tasks["A"]["task_run_id"], tasks["B"]["task_run_id"]
        replay_c = await call(
            "retry_task",
            {
                "run_id": run_id,
                "task_run_id": tasks["C"]["task_run_id"],
                "parameters": {"value": 7.0},
                "idempotency_key": str(uuid4()),
            },
        )
        assert replay_c["ok"], replay_c
        tasks = (await wait(run_id, "COMPLETED"))["checkpoint"]["tasks"]
        assert tasks["A"]["task_run_id"] == a_id
        assert tasks["B"]["task_run_id"] == b_id
        assert tasks["C"]["output"] == 14

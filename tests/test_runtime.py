"""验证状态持久化和 MCP 安全边界。"""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from mcp import Client
from sqlalchemy.exc import SQLAlchemyError

from prefect_agent_mcp import app as app_module
from prefect_agent_mcp import runtime as runtime_module
from prefect_agent_mcp import server
from prefect_agent_mcp.control import CommandService, FlowCommand
from prefect_agent_mcp.runtime import Runtime
from prefect_agent_mcp.schemas import Registry, Result, Workflow, redact
from prefect_agent_mcp.store import Store


async def test_snapshots_survive_restart_and_deduplicate(tmp_path):
    """验证状态去重和重启后读取。"""
    path = tmp_path / "monitor.db"
    store = Store(str(path))
    await store.initialize()
    await store.save("flow", "f1", "f1", {"state": "RUNNING"})
    await store.save("flow", "f1", "f1", {"state": "RUNNING"})
    await store.save("flow", "f1", "f1", {"state": "COMPLETED"})
    await store.engine.dispose()
    restored = Store(str(path))
    await restored.initialize()
    snapshot = await restored.get("flow", "f1")
    assert snapshot is not None and snapshot[0]["state"] == "COMPLETED"
    assert len(await restored.timeline("f1", 0, 20)) == 2
    assert len(await restored.timeline("f1", 1, 20)) == 1
    await restored.engine.dispose()


async def test_writes_disabled_before_any_prefect_access(tmp_path):
    """验证只读模式在访问 Prefect 前拒绝写操作。"""
    runtime = Runtime(Store(str(tmp_path / "monitor.db")), Registry(), writable=False)
    result = await CommandService(runtime).execute("start", "missing", {}, "request-1")
    assert result.error == "FORBIDDEN"
    await runtime.store.engine.dispose()


def test_redaction_and_allowlist():
    """验证敏感字段脱敏和部署白名单。"""
    assert redact({"api_key": "secret", "nested": {"password": "abc"}}) == {
        "api_key": "[REDACTED]",
        "nested": {"password": "[REDACTED]"},
    }
    deployment_id = uuid4()
    runtime = Runtime(
        Store(":memory:"),
        Registry(
            workflows=[Workflow(workflow_id="example", deployment_id=deployment_id)]
        ),
    )
    assert runtime.authorized(deployment_id)
    assert not runtime.authorized(uuid4())


async def test_mcp_tools_and_resources_use_real_sdk(tmp_path, monkeypatch):
    """使用真实 MCP SDK 验证工具和资源。"""
    runtime = Runtime(Store(str(tmp_path / "mcp.db")), Registry())
    monkeypatch.setattr(server, "runtime", runtime)
    async with Client(server.mcp, raise_exceptions=True) as client:
        result = await client.call_tool("list_workflows", {})
        assert result.structured_content["ok"] is True
        assert result.structured_content["data"] == []
        forbidden = await client.call_tool(
            "start_flow", {"workflow_id": "x", "parameters": {}, "idempotency_key": "a"}
        )
        assert forbidden.structured_content["error"] == "FORBIDDEN"
        resource = await client.read_resource("prefect-monitor://workflows")
        assert '"ok":true' in resource.contents[0].text


async def test_mcp_subscription_follows_committed_snapshot(tmp_path, monkeypatch):
    """验证快照提交后才能收到订阅通知。"""
    deployment_id, flow_id = uuid4(), uuid4()
    observed = SimpleNamespace(
        id=flow_id,
        deployment_id=deployment_id,
        name="test",
        state_type=SimpleNamespace(value="RUNNING"),
        run_count=1,
        start_time=None,
        end_time=None,
    )

    class FakeClient:
        """提供通知测试所需的最小 Prefect 客户端。"""

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def read_flow_run(self, _run_id):
            return observed

        async def read_flow_runs(self, **kwargs):
            return []

    runtime = Runtime(
        Store(str(tmp_path / "notify.db")),
        Registry(
            workflows=[Workflow(workflow_id="example", deployment_id=deployment_id)]
        ),
        notify=server.notify_resource,
    )
    monkeypatch.setattr(server, "runtime", runtime)
    monkeypatch.setattr(runtime_module, "get_client", FakeClient)
    uri = f"prefect-monitor://flows/{flow_id}"
    async with (
        Client(server.mcp, raise_exceptions=True) as client,
        client.listen(resource_subscriptions=[uri]) as subscription,
    ):
        result = await runtime.flow(flow_id)
        assert result.ok
        event = await asyncio.wait_for(anext(subscription), timeout=5)
        assert str(event.uri) == uri
        snapshot = await runtime.store.get("flow", str(flow_id))
        assert snapshot and snapshot[0]["state"] == "RUNNING"


async def test_offline_flow_returns_last_authorized_snapshot(tmp_path, monkeypatch):
    """验证断线时返回带过期标记的授权快照。"""
    deployment_id, flow_id = uuid4(), uuid4()
    store = Store(str(tmp_path / "offline.db"))
    await store.initialize()
    await store.save(
        "flow",
        str(flow_id),
        str(flow_id),
        {"id": str(flow_id), "deployment_id": str(deployment_id), "state": "RUNNING"},
    )
    runtime = Runtime(
        store,
        Registry(
            workflows=[Workflow(workflow_id="example", deployment_id=deployment_id)]
        ),
    )

    class OfflineClient:
        """模拟 Prefect 连接失败。"""

        async def __aenter__(self):
            raise httpx.ConnectError("offline")

        async def __aexit__(self, *args):
            return False

    monkeypatch.setattr(runtime_module, "get_client", OfflineClient)
    result = await runtime.flow(flow_id)
    assert result.ok and result.stale and result.last_synced_at
    assert result.data["state"] == "RUNNING"
    await store.engine.dispose()


async def test_command_subclass_uses_shared_idempotency(tmp_path, monkeypatch):
    """不同命令子类共享授权和幂等流程。"""

    class ExampleCommand(FlowCommand):
        """记录调用次数的测试命令。"""

        calls = 0

        async def validate(self):
            return None

        async def perform(self, key):
            ExampleCommand.calls += 1
            return Result(ok=True, data={"key": key})

    store = Store(str(tmp_path / "commands.db"))
    await store.initialize()
    runtime = Runtime(store, Registry(), writable=True)
    monkeypatch.setattr(CommandService, "commands", {"example": ExampleCommand})
    service = CommandService(runtime)
    first = await service.execute("example", "flow", {"value": 1}, "key-1")
    second = await service.execute("example", "flow", {"value": 1}, "key-1")
    conflict = await service.execute("example", "flow", {"value": 2}, "key-1")
    assert first.ok and second.data == first.data and ExampleCommand.calls == 1
    assert conflict.error == "IDEMPOTENCY_CONFLICT"

    async def unavailable(*args):
        raise SQLAlchemyError("内部数据库信息")

    monkeypatch.setattr(store, "reserve_command", unavailable)
    failed = await service.execute("example", "flow", {}, "key-2")
    assert failed.error == "STORAGE_UNAVAILABLE" and ExampleCommand.calls == 1
    await store.engine.dispose()


async def test_mcp_boundary_hides_unexpected_errors(tmp_path, monkeypatch):
    """异常在 MCP 边界转换为结构化错误，内部信息不会返回。"""
    runtime = Runtime(Store(str(tmp_path / "boundary.db")), Registry())
    monkeypatch.setattr(server, "runtime", runtime)

    async def failed_query(*args, **kwargs):
        raise RuntimeError("private internal information")

    monkeypatch.setattr(runtime, "flows", failed_query)
    async with Client(server.mcp, raise_exceptions=True) as client:
        result = await client.call_tool("list_flow_runs", {"workflow_id": "example"})
        assert result.structured_content["error"] == "INTERNAL_ERROR"
        assert "private internal information" not in str(result.structured_content)


async def test_cancel_command_does_not_repeat_an_unknown_outcome(tmp_path, monkeypatch):
    """取消请求发出后即使结果保存失败，也不会静默重复执行。"""
    flow_id = uuid4()
    calls = []
    store = Store(str(tmp_path / "cancel.db"))
    await store.initialize()
    runtime = Runtime(store, Registry(), writable=True)

    async def authorized(_flow_id):
        return Result(ok=True, data={"id": str(flow_id)})

    class FakeClient:
        """记录取消请求并返回 Prefect 接受状态。"""

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def set_flow_run_state(self, run_id, state):
            calls.append((run_id, state.type.value))
            return SimpleNamespace(status=SimpleNamespace(value="ACCEPT"))

    async def fail_to_save(*args):
        raise RuntimeError("数据库暂时不可用")

    monkeypatch.setattr(runtime, "flow", authorized)
    monkeypatch.setattr(runtime, "client", FakeClient)
    monkeypatch.setattr(store, "complete_command", fail_to_save)
    service = CommandService(runtime)
    first = await service.execute("cancel", str(flow_id), {}, "cancel-1")
    second = await service.execute("cancel", str(flow_id), {}, "cancel-1")
    assert first.error == second.error == "OUTCOME_UNKNOWN"
    assert calls == [(flow_id, "CANCELLING")]
    await store.engine.dispose()


async def test_invalid_tool_result_and_resource_error_are_isolated(
    tmp_path, monkeypatch
):
    """工具返回值经过校验，资源内部故障也不会泄漏给客户端。"""
    runtime = Runtime(Store(str(tmp_path / "resource.db")), Registry())
    monkeypatch.setattr(server, "runtime", runtime)

    async def bad_description(_workflow_id):
        return Result(ok=True, data=object())

    async def bad_resource(*args):
        raise RuntimeError("内部敏感信息")

    monkeypatch.setattr(runtime, "describe", bad_description)
    monkeypatch.setattr(server, "_run_resource", bad_resource)
    async with Client(server.mcp, raise_exceptions=True) as client:
        tool = await client.call_tool("describe_workflow", {"workflow_id": "example"})
        assert tool.structured_content["error"] == "INTERNAL_ERROR"
        resource = await client.read_resource(f"prefect-monitor://flows/{uuid4()}")
        result = json.loads(resource.contents[0].text)
        assert result["error"] == "INTERNAL_ERROR"
        assert "内部敏感信息" not in str(result)


def test_http_writes_require_prefect_auth(monkeypatch):
    """开启 HTTP 写操作前必须配置 Prefect 的身份认证。"""
    monkeypatch.setattr(app_module, "runtime", SimpleNamespace(writable=True))
    monkeypatch.setattr(
        app_module,
        "PREFECT_SERVER_API_AUTH_STRING",
        SimpleNamespace(value=lambda: None),
    )
    with pytest.raises(RuntimeError, match="身份认证"):
        app_module.install_prefect_mcp(FastAPI())


async def test_install_reuses_existing_app_and_lifespan(monkeypatch):
    """在已有应用中挂载一次，并按顺序启动和关闭原有生命周期。"""
    lifecycle = []

    @asynccontextmanager
    async def host_lifespan(_app):
        lifecycle.append("prefect_start")
        yield
        lifecycle.append("prefect_stop")

    @asynccontextmanager
    async def mcp_lifespan():
        lifecycle.append("mcp_start")
        yield
        lifecycle.append("mcp_stop")

    host = FastAPI(lifespan=host_lifespan)
    api = FastAPI()
    host.api_app = api
    host.mount("/api", api)
    fake_mcp = SimpleNamespace(
        streamable_http_app=lambda **_kwargs: FastAPI(),
        session_manager=SimpleNamespace(run=mcp_lifespan),
    )
    runtime = SimpleNamespace(writable=False, client_factory=None)
    monkeypatch.setattr(app_module, "runtime", runtime)
    monkeypatch.setattr(app_module, "mcp", fake_mcp)
    monkeypatch.setattr(
        app_module,
        "PREFECT_SERVER_API_AUTH_STRING",
        SimpleNamespace(value=lambda: None),
    )
    app_module.install_prefect_mcp(host)
    app_module.install_prefect_mcp(host)
    assert sum(route.path == "/mcp" for route in api.routes) == 1
    async with host.router.lifespan_context(host):
        assert lifecycle == ["prefect_start", "mcp_start"]
    assert lifecycle == ["prefect_start", "mcp_start", "mcp_stop", "prefect_stop"]
    assert runtime.client_factory is not None

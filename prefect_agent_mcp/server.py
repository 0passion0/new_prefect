"""挂载到 Prefect FastAPI 的 MCP 工具与资源。"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from functools import wraps
from typing import Any
from uuid import UUID

from mcp.server import MCPServer
from mcp.server.subscriptions import InMemorySubscriptionBus
from mcp.shared.subscriptions import ResourceUpdated

from .control import CommandService
from .monitoring import Monitor
from .runtime import Runtime
from .schemas import Result

bus = InMemorySubscriptionBus()
runtime = Runtime.from_environment()


async def notify_resource(uri: str) -> None:
    """将已提交的资源变化发布到 MCP 订阅总线。

    :param uri: 发生变化的资源地址。
    """
    await bus.publish(ResourceUpdated(uri=uri))


runtime.notify = notify_resource


@asynccontextmanager
async def lifespan(_server: MCPServer) -> AsyncIterator[None]:
    """初始化并关闭监控数据库和后台任务。

    :param _server: MCP 服务实例。
    :return: 生命周期上下文。
    """
    await runtime.store.initialize()
    monitor_task = asyncio.create_task(Monitor(runtime).run())
    try:
        yield
    finally:
        monitor_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await monitor_task
        await runtime.store.engine.dispose()


mcp = MCPServer("prefect-agent-mcp", lifespan=lifespan, subscriptions=bus)


def safe_tool[**P](
    function: Callable[P, Awaitable[Result]],
) -> Callable[P, Awaitable[Result]]:
    """在 MCP 接口边界转换未预期异常，避免向调用方暴露内部信息。

    :param function: 工具处理函数。
    :return: 返回统一结果的工具处理函数。
    """

    @wraps(function)
    async def guarded(*args: P.args, **kwargs: P.kwargs) -> Result:
        """执行工具并隔离内部错误。"""
        try:
            result = await function(*args, **kwargs)
            return Result.model_validate(result.model_dump(mode="json"))
        except Exception:  # noqa: BLE001 - 对外接口不应暴露内部异常或敏感数据
            logging.getLogger(__name__).error("MCP 工具执行失败：%s", function.__name__)
            return Result(ok=False, error="INTERNAL_ERROR", message="工具执行失败")

    return guarded


@mcp.tool()
@safe_tool
async def list_workflows() -> Result:
    """列出操作员授权的工作流。

    :return: 已注册的工作流列表。
    """
    return Result(
        ok=True,
        data=[item.model_dump(mode="json") for item in runtime.registry.workflows],
    )


@mcp.tool()
@safe_tool
async def describe_workflow(workflow_id: str) -> Result:
    """查询工作流定义及已知能力。

    :param workflow_id: 已注册的工作流标识。
    :return: 工作流描述、参数 Schema 和支持情况。
    """
    return await runtime.describe(workflow_id)


@mcp.tool()
@safe_tool
async def list_flow_runs(workflow_id: str, limit: int = 50, offset: int = 0) -> Result:
    """分页查询工作流的近期运行。

    :param workflow_id: 已注册的工作流标识。
    :param limit: 每页最多返回的记录数。
    :param offset: 分页偏移量。
    :return: 当前页的运行列表和分页信息。
    """
    if not 1 <= limit <= 100 or offset < 0:
        return Result(
            ok=False,
            error="INVALID_ARGUMENT",
            message="limit must be 1..100 and offset nonnegative",
        )
    return await runtime.flows(workflow_id, limit, offset)


@mcp.tool()
@safe_tool
async def get_flow_run(flow_run_id: UUID) -> Result:
    """查询运行状态，服务不可用时返回已有快照。

    :param flow_run_id: 工作流运行标识。
    :return: 实时状态或带过期标记的已保存状态。
    """
    return await runtime.flow(flow_run_id)


@mcp.tool()
@safe_tool
async def list_task_runs(flow_run_id: UUID, limit: int = 50, offset: int = 0) -> Result:
    """分页查询已授权运行的任务。

    :param flow_run_id: 工作流运行标识。
    :param limit: 每页最多返回的记录数。
    :param offset: 分页偏移量。
    :return: 当前页的任务列表和分页信息。
    """
    if not 1 <= limit <= 100 or offset < 0:
        return Result(
            ok=False,
            error="INVALID_ARGUMENT",
            message="limit must be 1..100 and offset nonnegative",
        )
    return await runtime.tasks(flow_run_id, limit, offset)


@mcp.tool()
@safe_tool
async def get_run_timeline(
    flow_run_id: UUID, after: int = 0, limit: int = 50
) -> Result:
    """按顺序读取已持久化的状态观察记录。

    :param flow_run_id: 工作流运行标识。
    :param after: 事件游标，不包含该序号。
    :param limit: 每页最多返回的记录数。
    :return: 事件列表、下一页游标及同步状态。
    """
    if after < 0 or not 1 <= limit <= 100:
        return Result(
            ok=False, error="INVALID_ARGUMENT", message="Invalid cursor or page size"
        )
    parent = await runtime.flow(flow_run_id)
    if not parent.ok:
        return parent
    rows = await runtime.store.timeline(str(flow_run_id), after, limit)
    return Result(
        ok=True,
        data={"items": rows, "next_cursor": rows[-1]["seq"] if rows else after},
        stale=parent.stale,
        last_synced_at=parent.last_synced_at,
    )


@mcp.tool()
@safe_tool
async def start_flow(
    workflow_id: str, parameters: dict[str, Any], idempotency_key: str
) -> Result:
    """使用经过校验的参数启动授权部署。

    :param workflow_id: 已注册的工作流标识。
    :param parameters: 部署运行参数。
    :param idempotency_key: 本次请求的幂等键。
    :return: 启动后的运行标识或错误信息。
    """
    return await CommandService(runtime).execute(
        "start", workflow_id, parameters, idempotency_key
    )


@mcp.tool()
@safe_tool
async def cancel_flow(flow_run_id: UUID, idempotency_key: str) -> Result:
    """向 Prefect 申请取消已授权的运行。

    :param flow_run_id: 工作流运行标识。
    :param idempotency_key: 本次请求的幂等键。
    :return: Prefect 对取消请求的处理结果。
    """
    return await CommandService(runtime).execute(
        "cancel", str(flow_run_id), {}, idempotency_key
    )


@mcp.tool()
@safe_tool
async def retry_task(flow_run_id: UUID, task_run_id: UUID) -> Result:
    """说明当前不支持独立重试任意任务。

    :param flow_run_id: 工作流运行标识。
    :param task_run_id: 任务运行标识。
    :return: 当前不支持该操作的错误信息。
    """
    return Result(
        ok=False,
        error="UNSUPPORTED",
        message="Independent task replay requires an authorized checkpoint and side-effect policy",
    )


def safe_resource[**P](
    function: Callable[P, Awaitable[str]],
) -> Callable[P, Awaitable[str]]:
    """隔离资源读取和序列化过程中的未预期错误。

    :param function: 资源处理函数。
    :return: 返回统一 JSON 错误的资源处理函数。
    """

    @wraps(function)
    async def guarded(*args: P.args, **kwargs: P.kwargs) -> str:
        """读取资源并屏蔽内部异常详情。"""
        try:
            result = await function(*args, **kwargs)
            return result
        except Exception:  # noqa: BLE001 - 资源边界不暴露内部异常或敏感数据
            logging.getLogger(__name__).error("MCP 资源读取失败：%s", function.__name__)
            return Result(
                ok=False, error="INTERNAL_ERROR", message="资源读取失败"
            ).model_dump_json()

    return guarded


@mcp.resource("prefect-monitor://workflows")
@safe_resource
async def workflows_resource() -> str:
    """以 JSON 返回工作流列表。

    :return: JSON 格式的工作流列表。
    """
    return (await list_workflows()).model_dump_json()


@mcp.resource("prefect-monitor://workflows/{workflow_id}")
@safe_resource
async def workflow_resource(workflow_id: str) -> str:
    """以 JSON 返回工作流描述。

    :param workflow_id: 已注册的工作流标识。
    :return: JSON 格式的工作流描述。
    """
    return (await describe_workflow(workflow_id)).model_dump_json()


async def _run_resource(
    flow_run_id: str, reader: Callable[[UUID], Awaitable[Result]]
) -> str:
    """校验运行标识并将工具结果转换为资源 JSON。

    :param flow_run_id: 工作流运行标识。
    :param reader: 已包装错误处理的查询函数。
    :return: JSON 格式的查询结果。
    """
    try:
        run_id = UUID(flow_run_id)
    except ValueError:
        result = Result(
            ok=False, error="INVALID_ARGUMENT", message="Invalid flow run ID"
        )
    else:
        result = await reader(run_id)
    return result.model_dump_json()


@mcp.resource("prefect-monitor://flows/{flow_run_id}")
@safe_resource
async def flow_resource(flow_run_id: str) -> str:
    """以 JSON 返回工作流运行状态。

    :param flow_run_id: 工作流运行标识。
    :return: JSON 格式的运行状态。
    """
    result = await _run_resource(flow_run_id, get_flow_run)
    return result


@mcp.resource("prefect-monitor://flows/{flow_run_id}/tasks")
@safe_resource
async def tasks_resource(flow_run_id: str) -> str:
    """以 JSON 返回首分页的任务列表。

    :param flow_run_id: 工作流运行标识。
    :return: JSON 格式的任务列表。
    """
    result = await _run_resource(flow_run_id, list_task_runs)
    return result


@mcp.resource("prefect-monitor://flows/{flow_run_id}/timeline")
@safe_resource
async def timeline_resource(flow_run_id: str) -> str:
    """以 JSON 返回首分页的观察记录。

    :param flow_run_id: 工作流运行标识。
    :return: JSON 格式的事件列表。
    """
    result = await _run_resource(flow_run_id, get_run_timeline)
    return result

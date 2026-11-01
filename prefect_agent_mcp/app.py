"""将 MCP 挂载到调用方已有的 Prefect FastAPI 应用。"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from prefect.client.orchestration import PrefectClient
from prefect.settings import PREFECT_SERVER_API_AUTH_STRING

from .server import mcp, runtime


def install_prefect_mcp(app: FastAPI) -> None:
    """在 Prefect 启动前将 MCP 加入其 API 和生命周期。

    :param app: 现有 Prefect Server 创建的 FastAPI 应用。
    """
    if getattr(app.state, "prefect_mcp_installed", False):
        return
    auth_string = PREFECT_SERVER_API_AUTH_STRING.value()
    if runtime.writable and not auth_string:
        raise RuntimeError("启用 MCP 写操作前，必须配置 Prefect API 身份认证")
    api_app: FastAPI = app.api_app  # type: ignore[attr-defined]
    runtime.client_factory = lambda: PrefectClient(
        api_app,
        auth_string=auth_string,
        httpx_settings={"base_url": "http://prefect-internal"},
    )
    api_app.mount("/mcp", mcp.streamable_http_app(streamable_http_path="/"))
    prefect_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(host: FastAPI) -> AsyncIterator[None]:
        """随现有 Prefect 服务启动和关闭 MCP。

        :param host: Prefect FastAPI 应用。
        :return: 生命周期上下文。
        """
        async with prefect_lifespan(host), mcp.session_manager.run():
            yield

    app.router.lifespan_context = lifespan
    app.state.prefect_mcp_installed = True

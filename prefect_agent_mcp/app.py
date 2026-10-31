"""在 Prefect 自身的 FastAPI 应用中挂载 MCP。"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from prefect.client.orchestration import PrefectClient
from prefect.server.api.server import create_app as create_prefect_app
from prefect.settings import (
    PREFECT_SERVER_API_AUTH_STRING,
    PREFECT_SERVER_API_HOST,
    PREFECT_SERVER_API_PORT,
)

from .server import mcp, runtime


def create_app() -> FastAPI:
    """创建包含 MCP 端点的 Prefect 应用。

    :return: 保留 Prefect API、界面和后台服务的 FastAPI 应用。
    """
    auth_string = PREFECT_SERVER_API_AUTH_STRING.value()
    if runtime.writable and not auth_string:
        raise RuntimeError("启用 MCP 写操作前，必须配置 Prefect API 身份认证")
    app = create_prefect_app(final=True)
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
        """依次启动 Prefect 和 MCP，共用同一服务生命周期。

        :param host: Prefect FastAPI 应用。
        :return: 生命周期上下文。
        """
        async with prefect_lifespan(host), mcp.session_manager.run():
            yield

    app.router.lifespan_context = lifespan
    return app


def main() -> None:
    """在 Prefect 配置的地址上启动唯一的 HTTP 服务。"""
    uvicorn.run(
        create_app(),
        host=PREFECT_SERVER_API_HOST.value(),
        port=PREFECT_SERVER_API_PORT.value(),
    )

"""在原生 Prefect Server 启动时自动挂载 MCP。"""

import sys
from functools import wraps
from typing import Any

from prefect.plugins import HookContext, register_hook


@register_hook
def setup_environment(*, ctx: HookContext) -> None:
    """通过 Prefect 插件接入 Server 的应用创建过程。"""
    if sys.argv[1:3] != ["server", "start"]:
        return

    from prefect.server.api import server

    original_create_app = server.create_app

    # shortcut: 依赖 Prefect 3.9 的启动实现，升级 Prefect 时须重新验证。
    @wraps(original_create_app)
    def create_app(*args: Any, **kwargs: Any) -> Any:
        from .app import install_prefect_mcp

        app = original_create_app(*args, **kwargs)
        install_prefect_mcp(app)
        return app

    server.create_app = create_app

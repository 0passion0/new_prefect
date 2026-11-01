"""验证 Prefect 插件只在 Server 启动时安装 MCP。"""

import logging
import sys

from fastapi import FastAPI
from prefect.plugins import HookContext
from prefect.server.api import server

from prefect_agent_mcp import app as app_module
from prefect_agent_mcp import plugin


def test_plugin_mounts_mcp_when_prefect_server_starts(monkeypatch):
    host = FastAPI()
    installed = []
    monkeypatch.setattr(sys, "argv", ["prefect", "server", "start"])
    monkeypatch.setattr(server, "create_app", lambda: host)
    monkeypatch.setattr(app_module, "install_prefect_mcp", installed.append)

    plugin.setup_environment(ctx=HookContext("3.9.0", None, logging.getLogger))

    assert server.create_app() is host
    assert installed == [host]


def test_plugin_leaves_other_prefect_commands_alone(monkeypatch):
    original = server.create_app
    monkeypatch.setattr(sys, "argv", ["prefect", "version"])

    plugin.setup_environment(ctx=HookContext("3.9.0", None, logging.getLogger))

    assert server.create_app is original

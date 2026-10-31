"""使用异步 SQLAlchemy 驱动执行 SQLite 迁移。"""

import asyncio
import os
from pathlib import Path

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config

config = context.config
path = Path(os.getenv("PREFECT_MCP_DB", "prefect-monitor.db")).absolute().as_posix()
config.set_main_option("sqlalchemy.url", f"sqlite+aiosqlite:///{path}")


def migrate(connection):
    """在事务中执行数据库迁移。

    :param connection: 数据库连接。
    """
    context.configure(connection=connection, target_metadata=None)
    with context.begin_transaction():
        context.run_migrations()


async def run():
    """建立异步连接并执行迁移。"""
    engine = async_engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with engine.connect() as connection:
        await connection.run_sync(migrate)
    await engine.dispose()


if context.is_offline_mode():
    raise RuntimeError("Offline migrations are not supported")
asyncio.run(run())

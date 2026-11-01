"""SQLite 快照、观察日志和命令存储。"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .schemas import utcnow


class Store:
    """将监控数据保存在独立于 Prefect 的数据库中。"""

    def __init__(self, path: str) -> None:
        """为本地 SQLite 数据库创建引擎。

        :param path: SQLite 数据库文件路径。
        """
        self.path = Path(path).absolute()
        self.engine: AsyncEngine = create_async_engine(
            f"sqlite+aiosqlite:///{self.path.as_posix()}"
        )

        @event.listens_for(self.engine.sync_engine, "connect")
        def configure(dbapi_connection: Any, _record: Any) -> None:
            """启用 SQLite 并发和完整性配置。"""
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA busy_timeout=5000")
            cursor.close()

    async def initialize(self) -> None:
        """创建初始监控数据表。"""
        async with self.engine.begin() as conn:
            await conn.execute(
                text(
                    "CREATE TABLE IF NOT EXISTS snapshots (kind TEXT NOT NULL, id TEXT NOT NULL, flow_id TEXT, payload TEXT NOT NULL, synced_at TEXT NOT NULL, PRIMARY KEY(kind,id))"
                )
            )
            await conn.execute(
                text(
                    "CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, entity_id TEXT NOT NULL, flow_id TEXT, digest TEXT NOT NULL, payload TEXT NOT NULL, observed_at TEXT NOT NULL)"
                )
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS ix_events_flow ON events(flow_id,seq)")
            )
            await conn.execute(
                text(
                    "CREATE TABLE IF NOT EXISTS commands (idempotency_key TEXT PRIMARY KEY, operation TEXT NOT NULL, request_hash TEXT NOT NULL, result TEXT, created_at TEXT NOT NULL)"
                )
            )
            await conn.execute(
                text(
                    "CREATE TABLE IF NOT EXISTS recovery_runs (id TEXT PRIMARY KEY, status TEXT NOT NULL, execution_task_id TEXT NOT NULL, updated_at TEXT NOT NULL)"
                )
            )

    async def create_recovery(self, run_id: str, task_id: str) -> None:
        """登记新的业务运行和待执行的 Prefect Task。"""
        async with self.engine.begin() as conn:
            await conn.execute(
                text("INSERT INTO recovery_runs VALUES (:id, 'QUEUED', :task, :now)"),
                {"id": run_id, "task": task_id, "now": utcnow().isoformat()},
            )

    async def claim_recovery(self, run_id: str, task_id: str) -> bool:
        """原子锁定已结束的业务运行，避免并发重放。"""
        async with self.engine.begin() as conn:
            result = await conn.execute(
                text(
                    "UPDATE recovery_runs SET status='QUEUED', execution_task_id=:task, updated_at=:now "
                    "WHERE id=:id AND status IN ('COMPLETED', 'FAILED')"
                ),
                {"id": run_id, "task": task_id, "now": utcnow().isoformat()},
            )
            return bool(result.rowcount)

    async def recovery(self, run_id: str) -> dict[str, Any] | None:
        """读取业务运行和当前执行任务的状态。"""
        async with self.engine.connect() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            "SELECT id, status, execution_task_id, updated_at FROM recovery_runs WHERE id=:id"
                        ),
                        {"id": run_id},
                    )
                )
                .mappings()
                .first()
            )
            return dict(row) if row else None

    async def save(
        self, kind: str, entity_id: str, flow_id: str, data: dict[str, Any]
    ) -> bool:
        """在同一事务中更新快照并记录变化。

        :param kind: 记录类型。
        :param entity_id: 实体标识。
        :param flow_id: 所属工作流运行标识。
        :param data: 经过脱敏的数据。
        :return: 观察数据是否发生变化。
        """
        payload = json.dumps(data, sort_keys=True, ensure_ascii=False, default=str)
        digest = hashlib.sha256(payload.encode()).hexdigest()
        now = utcnow().isoformat()
        async with self.engine.begin() as conn:
            row = (
                await conn.execute(
                    text("SELECT payload FROM snapshots WHERE kind=:kind AND id=:id"),
                    {"kind": kind, "id": entity_id},
                )
            ).first()
            changed = row is None or row.payload != payload
            if changed:
                await conn.execute(
                    text(
                        "INSERT INTO events(kind,entity_id,flow_id,digest,payload,observed_at) VALUES (:kind,:id,:flow_id,:digest,:payload,:now)"
                    ),
                    {
                        "kind": kind,
                        "id": entity_id,
                        "flow_id": flow_id,
                        "digest": digest,
                        "payload": payload,
                        "now": now,
                    },
                )
            await conn.execute(
                text(
                    "INSERT INTO snapshots(kind,id,flow_id,payload,synced_at) VALUES (:kind,:id,:flow_id,:payload,:now) ON CONFLICT(kind,id) DO UPDATE SET flow_id=excluded.flow_id,payload=excluded.payload,synced_at=excluded.synced_at"
                ),
                {
                    "kind": kind,
                    "id": entity_id,
                    "flow_id": flow_id,
                    "payload": payload,
                    "now": now,
                },
            )

        return changed

    async def get(self, kind: str, entity_id: str) -> tuple[dict[str, Any], str] | None:
        """读取单条已持久化的快照。

        :param kind: 记录类型。
        :param entity_id: 实体标识。
        :return: 快照及最近同步时间；不存在时为空。
        """
        async with self.engine.connect() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT payload,synced_at FROM snapshots WHERE kind=:kind AND id=:id"
                    ),
                    {"kind": kind, "id": entity_id},
                )
            ).first()
        return (json.loads(row.payload), row.synced_at) if row else None

    async def timeline(
        self, flow_id: str, after: int, limit: int
    ) -> list[dict[str, Any]]:
        """按写入顺序读取状态观察记录。

        :param flow_id: 所属工作流运行标识。
        :param after: 事件游标，不包含该序号。
        :param limit: 每页最多返回的记录数。
        :return: 按顺序排列的观察记录。
        """
        async with self.engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT seq,kind,entity_id,payload,observed_at FROM events WHERE flow_id=:flow_id AND seq>:after ORDER BY seq LIMIT :limit"
                        ),
                        {"flow_id": flow_id, "after": after, "limit": limit},
                    )
                )
                .mappings()
                .all()
            )
        return [{**dict(row), "payload": json.loads(row["payload"])} for row in rows]

    async def reserve_command(
        self, key: str, operation: str, digest: str
    ) -> tuple[str, str | None, bool]:
        """原子登记命令并读取既有的幂等记录。

        :param key: 幂等键。
        :param operation: 命令名称。
        :param digest: 请求摘要。
        :return: 原请求摘要、已保存结果和是否新建。
        """
        async with self.engine.begin() as conn:
            inserted = await conn.execute(
                text(
                    "INSERT INTO commands(idempotency_key,operation,request_hash,created_at) VALUES (:key,:op,:digest,:now) ON CONFLICT(idempotency_key) DO NOTHING"
                ),
                {
                    "key": key,
                    "op": operation,
                    "digest": digest,
                    "now": utcnow().isoformat(),
                },
            )
            row = (
                await conn.execute(
                    text(
                        "SELECT request_hash,result FROM commands WHERE idempotency_key=:key"
                    ),
                    {"key": key},
                )
            ).one()
        return row.request_hash, row.result, bool(inserted.rowcount)

    async def complete_command(self, key: str, result: str) -> None:
        """保存已执行命令的结果。

        :param key: 幂等键。
        :param result: 经过序列化的结果。
        """
        async with self.engine.begin() as conn:
            await conn.execute(
                text("UPDATE commands SET result=:result WHERE idempotency_key=:key"),
                {"key": key, "result": result},
            )

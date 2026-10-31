"""初始化状态观察和命令记录表，迁移版本为 0001。"""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    """创建初始数据表并兼容已有数据库。"""
    op.execute(
        "CREATE TABLE IF NOT EXISTS snapshots (kind TEXT NOT NULL, id TEXT NOT NULL, flow_id TEXT, payload TEXT NOT NULL, synced_at TEXT NOT NULL, PRIMARY KEY(kind,id))"
    )
    op.execute(
        "CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, entity_id TEXT NOT NULL, flow_id TEXT, digest TEXT NOT NULL, payload TEXT NOT NULL, observed_at TEXT NOT NULL)"
    )
    op.execute("CREATE INDEX IF NOT EXISTS ix_events_flow ON events(flow_id,seq)")
    op.execute(
        "CREATE TABLE IF NOT EXISTS commands (idempotency_key TEXT PRIMARY KEY, operation TEXT NOT NULL, request_hash TEXT NOT NULL, result TEXT, created_at TEXT NOT NULL)"
    )


def downgrade():
    """禁止未经备份而回退并删除数据。

    :raises RuntimeError: 回退前必须手动备份和迁移。
    """
    raise RuntimeError("Downgrade is intentionally unsupported")

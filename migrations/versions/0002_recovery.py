"""记录可恢复业务运行的执行状态。"""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        "CREATE TABLE IF NOT EXISTS recovery_runs (id TEXT PRIMARY KEY, status TEXT NOT NULL, execution_task_id TEXT NOT NULL, updated_at TEXT NOT NULL)"
    )


def downgrade():
    raise RuntimeError("Downgrade is intentionally unsupported")

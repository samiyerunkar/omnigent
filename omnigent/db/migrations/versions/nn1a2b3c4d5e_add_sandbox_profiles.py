"""Add workspace-scoped immutable sandbox profile references.

Revision ID: nn1a2b3c4d5e
Revises: b2e4f6a8c013
Create Date: 2026-10-12 00:00:00.000000

Deploy this additive schema before the profile API. Older servers ignore the
table, so application rollback preserves accepted references.
PostgreSQL builds the metadata index concurrently and repairs invalid builds
on retry.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from omnigent.db.db_models import Uuid16

revision: str = "nn1a2b3c4d5e"
down_revision: str | None = "b2e4f6a8c013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_RUNNER_HOST_INDEX = "ix_conversation_metadata_runner_host"
_CONVERSATION_TABLE = "omnigent_conversation_metadata"
_RUNNER_HOST_COLUMNS = ["workspace_id", "runner_id", "host_id", "id"]


def _create_runner_host_index() -> None:
    context = op.get_context()
    if context.dialect.name != "postgresql":
        op.create_index(_RUNNER_HOST_INDEX, _CONVERSATION_TABLE, _RUNNER_HOST_COLUMNS)
        return

    with context.autocommit_block():
        if not context.as_sql:
            # Recover failed builds only on the table resolved by this connection.
            # Offline SQL cannot inspect the catalog for invalid indexes.
            invalid = (
                op.get_bind()
                .execute(
                    sa.text(
                        "SELECT i.indexrelid::regclass::text FROM pg_index i "
                        "JOIN pg_class c ON c.oid = i.indexrelid "
                        "WHERE i.indrelid = to_regclass(:table) AND c.relname = :name "
                        "AND NOT i.indisvalid"
                    ),
                    {"table": _CONVERSATION_TABLE, "name": _RUNNER_HOST_INDEX},
                )
                .scalar()
            )
            if invalid:
                op.execute(f"DROP INDEX CONCURRENTLY {invalid}")
        op.execute(
            f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {_RUNNER_HOST_INDEX} "
            f"ON {_CONVERSATION_TABLE} ({', '.join(_RUNNER_HOST_COLUMNS)})"
        )


def upgrade() -> None:
    # Autocommit must not persist the bindings table before a failed index build.
    _create_runner_host_index()
    op.create_table(
        "sandbox_profile_bindings",
        sa.Column("workspace_id", sa.BigInteger(), primary_key=True),
        sa.Column("session_id", Uuid16(), primary_key=True),
        sa.Column("host_id", Uuid16(), nullable=True),
        sa.Column("snapshot_json", sa.LargeBinary(16383), nullable=False),
        sa.CheckConstraint(
            sa.func.length(sa.cast(sa.column("snapshot_json"), sa.LargeBinary)) <= 16383,
            name="ck_sandbox_profile_bindings_snapshot_bytes",
        ),
    )
    op.create_index(
        "ix_sandbox_profile_bindings_host",
        "sandbox_profile_bindings",
        ["workspace_id", "host_id", "session_id"],
    )


def downgrade() -> None:
    op.drop_index(_RUNNER_HOST_INDEX, table_name=_CONVERSATION_TABLE)
    op.drop_table("sandbox_profile_bindings")

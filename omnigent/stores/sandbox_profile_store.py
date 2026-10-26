"""Bounded SQL persistence for accepted provider profile references."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import Engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from omnigent.db.db_models import (
    SqlSandboxProfileBinding,
    current_workspace_id,
    uuid_to_bytes,
)
from omnigent.db.utils import make_named_managed_session_maker, run_write_transaction

MAX_PROFILE_BYTES = 16383


def encode_snapshot(value: dict[str, Any]) -> bytes:
    """Bound the serialized bytes, including all metadata and JSON overhead."""
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    if len(payload) > MAX_PROFILE_BYTES:
        raise ValueError("Sandbox profile must be smaller than 16 KB")
    return payload


class SandboxProfileStore:
    """Reuse the host store's engine and workspace context for profile data."""

    def __init__(self, engine: Engine) -> None:
        self._session = make_named_managed_session_maker(
            engine, query_name_prefix="omnigent.sandbox_profile_store", immediate=True
        )

    def bind_session(self, session_id: str, snapshot: dict[str, Any]) -> None:
        encoded = encode_snapshot(snapshot)

        def accept(session: Session) -> None:
            row = session.get(SqlSandboxProfileBinding, (current_workspace_id(), session_id))
            if row is not None:
                if row.snapshot_json != encoded:
                    raise ValueError("A session's sandbox profile cannot be changed")
                return
            session.add(
                SqlSandboxProfileBinding(
                    workspace_id=current_workspace_id(),
                    session_id=session_id,
                    host_id=None,
                    snapshot_json=encoded,
                )
            )

        try:
            run_write_transaction(self._session, "accept_session_profile", accept)
        except IntegrityError:
            # A concurrent first bind can win the insert on another replica.
            with self._session("read_concurrent_profile_binding") as session:
                row = session.get(SqlSandboxProfileBinding, (current_workspace_id(), session_id))
                if row is None:
                    raise
                if row.snapshot_json != encoded:
                    raise ValueError("A session's sandbox profile cannot be changed") from None

    def get_session_snapshot(self, session_id: str) -> dict[str, Any] | None:
        with self._session("read_session_profile") as session:
            row = session.get(SqlSandboxProfileBinding, (current_workspace_id(), session_id))
            return json.loads(row.snapshot_json) if row is not None else None

    def bind_host(self, session_id: str, host_id: str) -> None:
        host_id = uuid_to_bytes(host_id).hex()

        def bind(session: Session) -> None:
            row = session.get(
                SqlSandboxProfileBinding,
                (current_workspace_id(), session_id),
                with_for_update=True,
            )
            if row is not None:
                if row.host_id is not None and row.host_id != host_id:
                    raise ValueError("A profiled session cannot be moved to a different host")
                row.host_id = host_id

        run_write_transaction(self._session, "record_profile_host", bind)

    def clear_host_binding(self, session_id: str, host_id: str) -> None:
        """Clear only the expected host affinity, preserving the accepted profile."""
        host_id = uuid_to_bytes(host_id).hex()

        def clear(session: Session) -> None:
            row = session.get(
                SqlSandboxProfileBinding,
                (current_workspace_id(), session_id),
                with_for_update=True,
            )
            if row is not None and row.host_id == host_id:
                row.host_id = None

        run_write_transaction(self._session, "clear_profile_host", clear)

    def get_session_host_id(self, session_id: str) -> str | None:
        with self._session("read_session_profile_host") as session:
            return session.scalar(
                select(SqlSandboxProfileBinding.host_id).where(
                    SqlSandboxProfileBinding.workspace_id == current_workspace_id(),
                    SqlSandboxProfileBinding.session_id == session_id,
                )
            )

    def get_host_snapshot(self, host_id: str) -> dict[str, Any] | None:
        with self._session("read_host_profile") as session:
            row = session.scalar(
                select(SqlSandboxProfileBinding)
                .where(
                    SqlSandboxProfileBinding.workspace_id == current_workspace_id(),
                    SqlSandboxProfileBinding.host_id == host_id,
                )
                .order_by(SqlSandboxProfileBinding.session_id)
                .limit(1)
            )
            return json.loads(row.snapshot_json) if row is not None else None

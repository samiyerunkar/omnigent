"""Profile snapshots stay workspace scoped and bounded in SQL and serialized bytes."""

import json

import pytest
from sqlalchemy import event

from omnigent.db.db_models import workspace_scope
from omnigent.db.utils import get_or_create_engine
from omnigent.stores.sandbox_profile_store import SandboxProfileStore, encode_snapshot


def test_get_host_snapshot_is_workspace_scoped_and_bounded(db_uri: str) -> None:
    engine = get_or_create_engine(db_uri)
    store = SandboxProfileStore(engine)
    host_id, other_host_id = "a" * 32, "b" * 32
    for workspace_id, session_number, bound_host, profile_id in (
        (42, 0, host_id, "other-workspace"),
        (41, 1, other_host_id, "other-host"),
        (41, 3, host_id, "later-session"),
        (41, 2, host_id, "first-session"),
    ):
        with workspace_scope(workspace_id):
            session_id = f"{session_number:032x}"
            store.bind_session(session_id, {"id": profile_id})
            store.bind_host(session_id, bound_host)
    queries = []

    def capture(_connection, _cursor, statement, parameters, _context, _executemany):
        if statement.lstrip().upper().startswith("SELECT"):
            queries.append((statement, parameters))

    event.listen(engine, "before_cursor_execute", capture)
    try:
        with workspace_scope(41):
            assert store.get_host_snapshot(host_id) == {"id": "first-session"}
    finally:
        event.remove(engine, "before_cursor_execute", capture)

    assert len(queries) == 1
    statement, parameters = queries[0]
    assert "LIMIT" in statement.upper()
    with engine.connect() as connection:
        # Multiple matches must be bounded in SQL, before scalar() consumes a row.
        assert len(connection.exec_driver_sql(statement, parameters).all()) == 1
        if engine.dialect.name == "sqlite":
            plan = str(
                connection.exec_driver_sql("EXPLAIN QUERY PLAN " + statement, parameters).all()
            )
            assert "ix_sandbox_profile_bindings_host" in plan, plan
            assert "workspace_id=? AND host_id=?" in plan, plan
            assert "SCAN" not in plan, plan
            assert "TEMP B-TREE" not in plan, plan

    with workspace_scope(41):
        assert store.get_host_snapshot(other_host_id) == {"id": "other-host"}
        assert store.get_host_snapshot("c" * 32) is None
    with workspace_scope(42):
        assert store.get_host_snapshot(host_id) == {"id": "other-workspace"}
        assert store.get_host_snapshot(other_host_id) is None
    with workspace_scope(43):
        assert store.get_host_snapshot(host_id) is None


@pytest.mark.parametrize(
    "prefix", ["plain", "模型🚀", 'a"b\\c\n'], ids=["ascii", "non-ascii", "json-escaping"]
)
def test_encode_snapshot_bounds_serialized_bytes(prefix: str) -> None:
    snapshot = {"provider": "company", "id": "build", "revision": prefix}
    overhead = len(json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode())
    snapshot["revision"] += "a" * (16383 - overhead)

    encoded = encode_snapshot(snapshot)
    assert isinstance(encoded, bytes)
    assert len(encoded) == 16383
    assert json.loads(encoded) == snapshot

    snapshot["revision"] += "a"
    assert len(json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode()) == 16384
    with pytest.raises(ValueError, match="smaller than 16 KB"):
        encode_snapshot(snapshot)

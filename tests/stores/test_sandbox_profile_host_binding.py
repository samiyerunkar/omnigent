"""Failed launches release only their own mutable profile affinity."""

import uuid

from omnigent.db.db_models import workspace_scope
from omnigent.db.utils import get_or_create_engine
from omnigent.stores.sandbox_profile_store import SandboxProfileStore


def test_clear_host_binding_preserves_snapshot_and_other_affinities(db_uri: str) -> None:
    store = SandboxProfileStore(get_or_create_engine(db_uri))
    session_id = uuid.uuid4().hex
    host_id, replacement_id = uuid.uuid4(), uuid.uuid4().hex
    snapshot = {"provider": "company", "id": "build", "revision": "immutable"}
    for workspace_id in (41, 42):
        with workspace_scope(workspace_id):
            store.bind_session(session_id, snapshot)
            store.bind_host(session_id, host_id.hex)

    with workspace_scope(41):
        store.clear_host_binding(session_id, replacement_id)
        store.clear_host_binding(uuid.uuid4().hex, host_id.hex)
        assert store.get_session_host_id(session_id) == host_id.hex
        store.clear_host_binding(session_id, str(host_id))
        assert store.get_session_host_id(session_id) is None
        assert store.get_session_snapshot(session_id) == snapshot
        assert store.get_host_snapshot(host_id.hex) is None
        store.clear_host_binding(session_id, host_id.hex)
        store.bind_host(session_id, replacement_id)
        store.clear_host_binding(session_id, host_id.hex)
        assert store.get_session_host_id(session_id) == replacement_id
        assert store.get_session_snapshot(session_id) == snapshot
        assert store.get_host_snapshot(replacement_id) == snapshot

    with workspace_scope(42):
        assert store.get_session_host_id(session_id) == host_id.hex
        assert store.get_session_snapshot(session_id) == snapshot
        assert store.get_host_snapshot(host_id.hex) == snapshot

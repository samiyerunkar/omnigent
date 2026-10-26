"""Conditional host rollback cannot detach a newer session binding."""

import uuid

from omnigent.stores.conversation_store.sqlalchemy_store import SqlAlchemyConversationStore


def test_clear_host_binding_requires_the_expected_host(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    old_host_id, current_host_id = uuid.uuid4(), uuid.uuid4()
    session = conversation_store.create_conversation()
    conversation_store.set_host_id(session.id, old_host_id.hex, "/old")
    conversation_store.set_host_id(
        session.id, current_host_id.hex, "/current", git_branch="current-branch"
    )
    conversation_store.replace_runner_id(session.id, "current-runner")
    current = conversation_store.get_conversation(session.id)
    assert current is not None

    unchanged = conversation_store.clear_host_binding(session.id, expected_host_id=old_host_id.hex)
    assert unchanged == current
    assert conversation_store.get_conversation(session.id) == current

    cleared = conversation_store.clear_host_binding(
        session.id, expected_host_id=str(current_host_id)
    )
    assert cleared.host_id is None and cleared.workspace is None
    assert cleared.git_branch is None and cleared.runner_id is None
    assert cleared.updated_at == current.updated_at
    assert conversation_store.get_conversation(session.id) == cleared

"""Provider catalogs remain scoped, read-only, and authoritative at admission."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field, replace
from typing import ClassVar

import pytest
import pytest_asyncio
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from httpx import ASGITransport, AsyncClient
from sqlalchemy.orm import Session

from omnigent.db.db_models import SqlSandboxProfileBinding, workspace_scope
from omnigent.errors import OmnigentError
from omnigent.onboarding.sandboxes.base import OwnerCredentialMissingError
from omnigent.onboarding.sandboxes.profiles import (
    ProviderSandboxProfile,
    SandboxProfileAccessDenied,
    SandboxProfileChanged,
    SandboxProfileContext,
)
from omnigent.server.auth import UnifiedAuthProvider
from omnigent.server.managed_hosts import ManagedSandboxConfig, ManagedSandboxDeployment
from omnigent.server.routes.sandbox_profiles import create_sandbox_profiles_router
from omnigent.server.sandbox_profiles import SandboxProfileCatalog
from omnigent.stores.host_store import HostStore
from tests.server.helpers import FakeSandboxLauncher

_ALICE = "alice@example.test"
_BOB = "bob@example.test"
_SECRET = "private-provider-token=do-not-expose"
_BUILD = ProviderSandboxProfile(id="build", name="Build", revision="release/2026.10+1")
_RESEARCH = ProviderSandboxProfile(id="research", name="Research", revision="revision-two")
_WRITE = {"name": "Local override", "provider": "company", "config": {}}


@dataclass
class _Profiles:
    allowed: dict[tuple[str, int], list[ProviderSandboxProfile]] = field(
        default_factory=lambda: {
            (_ALICE, 41): [_BUILD],
            (_BOB, 41): [_RESEARCH],
            (_ALICE, 42): [_RESEARCH],
        }
    )
    listings: list[SandboxProfileContext] = field(default_factory=list)
    selections: list[tuple[SandboxProfileContext, ProviderSandboxProfile]] = field(
        default_factory=list
    )
    list_error: Exception | None = None
    select_error: Exception | None = None


class _Provider(FakeSandboxLauncher):
    provider: ClassVar[str] = "company"
    manages_profiles: ClassVar[bool] = True

    def __init__(self, profiles: _Profiles) -> None:
        super().__init__()
        self.profiles = profiles

    def list_profiles(self, context: SandboxProfileContext) -> list[ProviderSandboxProfile]:
        self.profiles.listings.append(context)
        if self.profiles.list_error is not None:
            raise self.profiles.list_error
        return self.profiles.allowed.get((context.user_id, context.workspace_id), [])

    def select_profile(
        self, context: SandboxProfileContext, profile: ProviderSandboxProfile
    ) -> None:
        self.profiles.selections.append((context, profile))
        if self.profiles.select_error is not None:
            raise self.profiles.select_error
        current = next(
            (
                row
                for row in self.profiles.allowed.get((context.user_id, context.workspace_id), [])
                if row.id == profile.id
            ),
            None,
        )
        if current is None:
            raise SandboxProfileAccessDenied(_SECRET)
        if current.revision != profile.revision:
            raise SandboxProfileChanged(_SECRET)


@pytest.fixture
def profiles() -> _Profiles:
    return _Profiles()


@pytest.fixture
def catalog(db_uri: str, profiles: _Profiles) -> SandboxProfileCatalog:
    hosts = HostStore(db_uri)
    deployment = ManagedSandboxDeployment.single(
        ManagedSandboxConfig(
            server_url="https://managed.example.test",
            token_ttl_s=100,
            provider="company",
            launcher_factory=lambda: _Provider(profiles),
        )
    )
    return SandboxProfileCatalog(hosts._engine, deployment)


@pytest_asyncio.fixture
async def client(catalog: SandboxProfileCatalog) -> AsyncIterator[AsyncClient]:
    app = FastAPI()
    app.state.sandbox_profiles = catalog
    app.include_router(
        create_sandbox_profiles_router(
            auth_provider=UnifiedAuthProvider(source="header", local_single_user=False),
        ),
        prefix="/v1",
    )

    @app.exception_handler(OmnigentError)
    async def error_handler(request, exc):
        return JSONResponse(status_code=exc.http_status, content={"detail": str(exc)})

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


def test_discovery_is_per_user_and_workspace_without_local_persistence(
    catalog: SandboxProfileCatalog, profiles: _Profiles
) -> None:
    assert catalog.managed_providers() == ["company"]
    for user_id, workspace_id, expected in (
        (_ALICE, 41, _BUILD),
        (_BOB, 41, _RESEARCH),
        (_ALICE, 42, _RESEARCH),
        (_ALICE, 41, _BUILD),
    ):
        with workspace_scope(workspace_id):
            rows, cursor = catalog.list_profiles(user_id=user_id)
            assert cursor is None
            assert len(rows) == 1
            assert rows[0]["id"] == expected.id
            assert rows[0]["revision"] == expected.revision
            assert set(rows[0]) == {"id", "name", "description", "revision", "provider"}
            assert "user_id" not in rows[0] and "workspace_id" not in rows[0]
            assert profiles.listings[-1] == SandboxProfileContext(
                user_id=user_id, workspace_id=workspace_id
            )


def test_empty_catalog_is_distinct_from_provider_outage(
    catalog: SandboxProfileCatalog, profiles: _Profiles
) -> None:
    with workspace_scope(41):
        assert catalog.list_profiles(user_id="outsider@example.test") == ([], None)
        profiles.list_error = RuntimeError(_SECRET)
        with pytest.raises(HTTPException) as failed:
            catalog.list_profiles(user_id=_ALICE)
    assert failed.value.status_code == 503
    assert _SECRET not in str(failed.value.detail)
    assert catalog.managed_providers() == ["company"]


@pytest.mark.parametrize("provider", [None, "", " "])
def test_managed_profiles_reject_unnamed_embedding_configuration(
    catalog: SandboxProfileCatalog, profiles: _Profiles, provider: str | None
) -> None:
    deployment = catalog._deployment
    assert deployment is not None
    catalog._deployment = ManagedSandboxDeployment.single(
        replace(deployment.default, provider=provider)
    )
    with workspace_scope(41):
        for operation in (
            catalog.managed_providers,
            lambda: catalog.list_profiles(user_id=_ALICE),
            lambda: catalog.require_selection(None, user_id=_ALICE),
            lambda: catalog.resolve("build", user_id=_ALICE),
        ):
            with pytest.raises(HTTPException) as invalid:
                operation()
            assert invalid.value.status_code == 422
            assert "provider name" in invalid.value.detail
    assert profiles.listings == []
    assert profiles.selections == []


def test_unnamed_legacy_provider_keeps_default_launches(catalog: SandboxProfileCatalog) -> None:
    deployment = catalog._deployment
    assert deployment is not None
    catalog._deployment = ManagedSandboxDeployment.single(
        replace(deployment.default, provider=None, launcher_factory=FakeSandboxLauncher)
    )
    assert catalog.managed_providers() == []
    catalog.require_selection(None)


@pytest.mark.parametrize("method", ["list_profiles", "select_profile"])
@pytest.mark.parametrize(
    ("error_type", "status"),
    [
        (SandboxProfileAccessDenied, 403),
        (SandboxProfileChanged, 409),
        (OwnerCredentialMissingError, 409),
        (RuntimeError, 503),
        (TimeoutError, 503),
    ],
)
def test_provider_failures_are_translated_and_sanitized(
    catalog: SandboxProfileCatalog,
    profiles: _Profiles,
    method: str,
    error_type: type[Exception],
    status: int,
) -> None:
    setattr(
        profiles,
        "list_error" if method == "list_profiles" else "select_error",
        error_type(_SECRET),
    )
    with workspace_scope(41), pytest.raises(HTTPException) as failed:
        if method == "list_profiles":
            catalog.list_profiles(user_id=_ALICE)
        else:
            catalog.resolve("build", provider="company", user_id=_ALICE)
    assert failed.value.status_code == status
    assert _SECRET not in str(failed.value.detail)


@pytest.mark.parametrize(
    "rows",
    [
        None,
        [object()],
        [_BUILD, _BUILD],
        [_BUILD, {"id": "secret", "token": _SECRET}],
        [_BUILD.model_copy(update={"id": f"profile-{index}"}) for index in range(1001)],
    ],
    ids=["not-a-list", "malformed-row", "duplicate-id", "invalid-after-valid", "oversized"],
)
def test_malformed_provider_catalog_never_returns_partial_results(
    catalog: SandboxProfileCatalog, monkeypatch: pytest.MonkeyPatch, rows: object
) -> None:
    monkeypatch.setattr(_Provider, "list_profiles", lambda self, context: rows)
    with workspace_scope(41), pytest.raises(HTTPException) as failed:
        catalog.list_profiles(user_id=_ALICE)
    assert failed.value.status_code == 503
    assert _SECRET not in str(failed.value.detail)


def test_selection_requires_identity_and_reauthorizes_the_exact_revision(
    catalog: SandboxProfileCatalog, profiles: _Profiles
) -> None:
    with workspace_scope(41):
        with pytest.raises(HTTPException) as unauthenticated:
            catalog.resolve("build", provider="company")
        assert unauthenticated.value.status_code == 401
        assert profiles.selections == []
        snapshot = catalog.resolve("build", _BUILD.revision, provider="company", user_id=_ALICE)
        assert profiles.selections[-1] == (
            SandboxProfileContext(user_id=_ALICE, workspace_id=41),
            _BUILD,
        )
        assert "config" not in snapshot
        assert snapshot["user_id"] == _ALICE
        assert snapshot["workspace_id"] == 41
        with pytest.raises(HTTPException) as stale:
            catalog.resolve("build", "retired-revision", provider="company", user_id=_ALICE)
        assert stale.value.status_code == 409
        for profile_id in ("build", "guessed"):
            with pytest.raises(HTTPException) as forbidden:
                catalog.resolve(profile_id, provider="company", user_id=_BOB)
            assert forbidden.value.status_code == 403


def test_saved_reference_reauthorizes_after_restart_and_revocation(
    catalog: SandboxProfileCatalog, profiles: _Profiles
) -> None:
    session_id, host_id = uuid.uuid4().hex, uuid.uuid4().hex
    with workspace_scope(41):
        snapshot = catalog.resolve("build", provider="company", user_id=_ALICE)
        catalog.bind_session(session_id, snapshot)
        catalog.bind_host(session_id, host_id)
        restarted = SandboxProfileCatalog(catalog._store._session.engine, catalog._deployment)
        profiles.selections.clear()
        profiles.listings.clear()
        profiles.list_error = RuntimeError("Discovery is unavailable; selection is authoritative")
        assert restarted.validate_snapshot(restarted.get_session_snapshot(session_id)) == snapshot
        assert profiles.selections == [
            (SandboxProfileContext(user_id=_ALICE, workspace_id=41), _BUILD)
        ]
        assert restarted.get_host_snapshot(host_id) == snapshot
        profiles.allowed[(_ALICE, 41)] = []
        with pytest.raises(HTTPException) as denied:
            restarted.validate_snapshot(snapshot)
        assert denied.value.status_code == 403
        assert restarted.get_session_snapshot(session_id) == snapshot
        assert profiles.listings == []


def test_inherited_reference_uses_new_caller_without_mutating_source(
    catalog: SandboxProfileCatalog, profiles: _Profiles
) -> None:
    with workspace_scope(41):
        snapshot = catalog.resolve("build", provider="company", user_id=_ALICE)
        with pytest.raises(HTTPException) as denied:
            catalog.inherit_snapshot(snapshot, _BOB)
        assert denied.value.status_code == 403
        profiles.allowed[(_BOB, 41)] = [_BUILD]
        inherited = catalog.inherit_snapshot(snapshot, _BOB)
        assert inherited["user_id"] == _BOB
        assert inherited["revision"] == snapshot["revision"]
        assert snapshot["user_id"] == _ALICE
        assert profiles.selections[-1] == (
            SandboxProfileContext(user_id=_BOB, workspace_id=41),
            _BUILD,
        )


def test_profile_ids_and_pagination_are_scoped_to_the_provider(
    catalog: SandboxProfileCatalog,
) -> None:
    deployment = catalog._deployment
    assert deployment is not None
    catalog._deployment = replace(
        deployment,
        configs=(*deployment.configs, replace(deployment.default, provider="second-company")),
    )
    with workspace_scope(41):
        first, cursor = catalog.list_profiles(user_id=_ALICE, limit=1)
        assert [(row["provider"], row["id"]) for row in first] == [("company", "build")]
        assert cursor is not None
        second, final_cursor = catalog.list_profiles(user_id=_ALICE, limit=1, cursor=cursor)
        assert [(row["provider"], row["id"]) for row in second] == [("second-company", "build")]
        assert final_cursor is None
        assert catalog.resolve("build", user_id=_ALICE)["provider"] == "company"
        assert (
            catalog.resolve("build", provider="second-company", user_id=_ALICE)["provider"]
            == "second-company"
        )


def test_snapshot_cannot_cross_workspace_boundaries(
    catalog: SandboxProfileCatalog, profiles: _Profiles
) -> None:
    with workspace_scope(41):
        snapshot = catalog.resolve("build", provider="company", user_id=_ALICE)
    profiles.selections.clear()
    with workspace_scope(42), pytest.raises(HTTPException) as denied:
        catalog.validate_snapshot(snapshot)
    assert denied.value.status_code == 403
    assert profiles.selections == []


@pytest.mark.parametrize("allowed", [False, True])
def test_unprofiled_managed_host_cannot_bypass_required_selection(
    catalog: SandboxProfileCatalog, profiles: _Profiles, allowed: bool
) -> None:
    session_id, host_id = uuid.uuid4().hex, uuid.uuid4().hex
    if not allowed:
        profiles.allowed.clear()
    with workspace_scope(41), pytest.raises(HTTPException) as denied:
        catalog.require_session_host(
            session_id, host_id, sandbox_provider="company", user_id=_ALICE
        )
    assert denied.value.status_code == 422
    assert catalog.get_session_snapshot(session_id) is None
    assert catalog.get_host_snapshot(host_id) is None


def test_host_reuse_checks_saved_identity_and_current_access(
    catalog: SandboxProfileCatalog, profiles: _Profiles
) -> None:
    source_id, target_id, host_id = (uuid.uuid4().hex for _ in range(3))
    with workspace_scope(41):
        snapshot = catalog.resolve("build", provider="company", user_id=_ALICE)
        catalog.bind_session(source_id, snapshot)
        catalog.bind_host(source_id, host_id)
        profiles.allowed[(_BOB, 41)] = [_BUILD]
        with pytest.raises(HTTPException) as mismatched_owner:
            catalog.require_session_host(
                target_id, host_id, sandbox_provider="company", user_id=_BOB
            )
        assert mismatched_owner.value.status_code == 403
        assert catalog.get_session_snapshot(target_id) is None
        profiles.allowed[(_ALICE, 41)] = []
        with pytest.raises(HTTPException) as revoked:
            catalog.require_session_host(
                target_id, host_id, sandbox_provider="company", user_id=_ALICE
            )
        assert revoked.value.status_code == 403
        assert catalog.get_session_snapshot(target_id) is None
        profiles.allowed[(_ALICE, 41)] = [_BUILD]
        catalog.require_session_host(
            target_id, host_id, sandbox_provider="company", user_id=_ALICE
        )
        assert catalog.get_session_snapshot(target_id) == snapshot


async def test_api_uses_authenticated_identity_and_hides_execution_identity(
    client: AsyncClient, profiles: _Profiles
) -> None:
    response = await client.get("/v1/sandbox-profiles?user_id=alice@example.test")
    assert response.status_code == 401
    assert profiles.listings == []
    with workspace_scope(41):
        for user_id, expected_id in ((_ALICE, "build"), (_BOB, "research")):
            response = await client.get(
                "/v1/sandbox-profiles?user_id=outsider&workspace_id=42",
                headers={"X-Forwarded-Email": user_id},
            )
            assert response.status_code == 200, response.text
            assert response.headers["Cache-Control"] == "private, no-store"
            body = response.json()
            assert body["managed_providers"] == ["company"]
            assert "can_manage" not in body
            assert [row["id"] for row in body["profiles"]] == [expected_id]
            assert "user_id" not in response.text and "workspace_id" not in response.text
            assert _ALICE not in response.text and _BOB not in response.text
            assert profiles.listings[-1] == SandboxProfileContext(user_id=user_id, workspace_id=41)

        profiles.allowed.clear()
        response = await client.get("/v1/sandbox-profiles", headers={"X-Forwarded-Email": _ALICE})
        assert response.status_code == 200
        assert response.json()["profiles"] == []
        assert response.json()["managed_providers"] == ["company"]
        profiles.list_error = RuntimeError(_SECRET)
        response = await client.get("/v1/sandbox-profiles", headers={"X-Forwarded-Email": _ALICE})
        assert response.status_code == 503
        assert _SECRET not in response.text


@pytest.mark.parametrize("operation", ["create", "edit", "disable"])
async def test_admin_cannot_mutate_provider_catalog(
    client: AsyncClient, catalog: SandboxProfileCatalog, operation: str
) -> None:
    profile_id = "new" if operation == "create" else "build"
    headers = {"X-Forwarded-Email": _ALICE}
    with workspace_scope(41):
        if operation == "disable":
            response = await client.delete(f"/v1/sandbox-profiles/{profile_id}", headers=headers)
        else:
            response = await client.put(
                f"/v1/sandbox-profiles/{profile_id}", json=_WRITE, headers=headers
            )
        assert response.status_code in (404, 405), response.text
        assert (
            catalog.resolve("build", provider="company", user_id=_ALICE)["revision"]
            == _BUILD.revision
        )


def test_catalog_and_bindings_are_workspace_scoped(
    catalog: SandboxProfileCatalog, profiles: _Profiles
) -> None:
    session_id, host_id = uuid.uuid4().hex, uuid.uuid4().hex
    with workspace_scope(41):
        profile = catalog.resolve("build", user_id=_ALICE)
        catalog.bind_session(session_id, profile)
        catalog.bind_host(session_id, host_id)
    with workspace_scope(42):
        assert catalog.list_profiles(user_id="outsider") == ([], None)
        assert catalog.get_session_snapshot(session_id) is None
        assert catalog.get_host_snapshot(host_id) is None
        with pytest.raises(HTTPException, match="403"):
            catalog.resolve("build", user_id="outsider")


def test_host_preflight_does_not_create_or_change_bindings(
    catalog: SandboxProfileCatalog,
    profiles: _Profiles,
) -> None:
    profiles.allowed[(_ALICE, 0)] = [_BUILD]
    profile = catalog.resolve("build", user_id=_ALICE)
    source_id, target_id = uuid.uuid4().hex, uuid.uuid4().hex
    host_id, other_host_id = uuid.uuid4().hex, uuid.uuid4().hex
    catalog.bind_session(source_id, profile)
    catalog.bind_host(source_id, host_id)
    catalog.require_session_host(target_id, host_id, bind=False)
    assert catalog.get_session_snapshot(target_id) is None
    catalog.bind_session(target_id, profile)
    catalog.bind_host(target_id, other_host_id)
    with pytest.raises(HTTPException, match="409"):
        catalog.require_session_host(target_id, host_id, bind=False)
    assert catalog.get_session_snapshot(target_id) == profile
    assert catalog.get_host_snapshot(other_host_id) == profile
    profiles.allowed.clear()
    with pytest.raises(HTTPException, match="403"):
        catalog.require_session_host(target_id, host_id, bind=False)


def test_host_binding_cannot_lose_original_host_profile(
    catalog: SandboxProfileCatalog,
    profiles: _Profiles,
) -> None:
    profiles.allowed[(_ALICE, 0)] = [_BUILD]
    profile = catalog.resolve("build", user_id=_ALICE)
    source_id, target_id = uuid.uuid4().hex, uuid.uuid4().hex
    host_id, other_host_id = uuid.uuid4().hex, uuid.uuid4().hex
    for session_id, bound_host in ((source_id, host_id), (target_id, other_host_id)):
        catalog.bind_session(session_id, profile)
        catalog.bind_host(session_id, bound_host)
        catalog.bind_host(session_id, f"host_{bound_host}")
        catalog.require_session_host(session_id, f"host_{bound_host}", bind=False)
    for bind in (False, True):
        with pytest.raises(HTTPException, match="different host"):
            catalog.require_session_host(source_id, other_host_id, bind=bind)
    with pytest.raises(HTTPException, match="different host"):
        catalog.bind_host(source_id, other_host_id)
    assert catalog.get_host_snapshot(host_id) == profile
    assert catalog.get_host_snapshot(other_host_id) == profile
    profiles.allowed.clear()
    with pytest.raises(HTTPException, match="access denied"):
        catalog.require_session_host(source_id, host_id, bind=False)


def test_unrelated_provider_outage_does_not_block_default_launches(catalog, profiles):
    deployment = catalog._deployment
    assert deployment is not None
    healthy = _Profiles()
    catalog._deployment = replace(
        deployment,
        configs=(
            *deployment.configs,
            replace(
                deployment.default,
                provider="ordinary",
                launcher_factory=FakeSandboxLauncher,
            ),
            replace(
                deployment.default,
                provider="healthy",
                launcher_factory=lambda: _Provider(healthy),
            ),
        ),
    )
    profiles.list_error = RuntimeError(_SECRET)
    assert catalog.list_profiles(provider="ordinary", user_id=_ALICE) == ([], None)
    assert catalog.managed_providers("ordinary") == []
    catalog.require_selection("ordinary", user_id=_ALICE)
    assert profiles.listings == []
    with pytest.raises(HTTPException) as invalid:
        catalog.resolve("build", provider="ordinary", user_id=_ALICE)
    assert invalid.value.status_code == 422
    with workspace_scope(41):
        rows, cursor = catalog.list_profiles(provider="healthy", user_id=_ALICE)
        assert [row["id"] for row in rows] == ["build"]
        assert all(row["provider"] == "healthy" for row in rows)
        assert cursor is None
        assert catalog.managed_providers("healthy") == ["healthy"]
    assert profiles.listings == []


@pytest.mark.parametrize("field", ["config", "users", "credentials"])
def test_saved_references_reject_private_provider_fields(catalog, field):
    with workspace_scope(41):
        snapshot = catalog.resolve("build", user_id=_ALICE)
        with pytest.raises(HTTPException) as invalid:
            catalog.bind_session(uuid.uuid4().hex, {**snapshot, field: _SECRET})
        assert invalid.value.status_code == 422
        assert _SECRET not in str(invalid.value.detail)


def test_session_reference_cannot_use_a_different_execution_identity(catalog, profiles):
    source_id, target_id, host_id = (uuid.uuid4().hex for _ in range(3))
    with workspace_scope(41):
        snapshot = catalog.resolve("build", user_id=_ALICE)
        catalog.bind_session(source_id, snapshot)
        catalog.bind_host(source_id, host_id)
        profiles.allowed[(_BOB, 41)] = [_BUILD]
        catalog.bind_session(target_id, catalog.inherit_snapshot(snapshot, _BOB))
        with pytest.raises(HTTPException) as mismatch:
            catalog.require_session_host(target_id, host_id, user_id=_ALICE)
        assert mismatch.value.status_code == 409
        assert catalog._store.get_session_host_id(target_id) is None


@pytest.mark.parametrize("same_selection", [True, False])
def test_concurrent_first_bind_is_idempotent_or_conflicts(catalog, monkeypatch, same_selection):
    session_id = uuid.uuid4().hex
    with workspace_scope(41):
        winner = catalog.resolve("build", user_id=_ALICE)
        catalog.bind_session(session_id, winner)
        candidate = winner if same_selection else {**winner, "description": "Different selection"}
        original_get = Session.get
        missed = False

        def miss_competing_row(session, entity, *args, **kwargs):
            nonlocal missed
            if entity is SqlSandboxProfileBinding and not missed:
                missed = True
                return None
            return original_get(session, entity, *args, **kwargs)

        # Model the stale first read; the real primary key enforces the insert race.
        monkeypatch.setattr(Session, "get", miss_competing_row)
        if same_selection:
            catalog.bind_session(session_id, candidate)
        else:
            with pytest.raises(HTTPException) as conflict:
                catalog.bind_session(session_id, candidate)
            assert conflict.value.status_code == 409
        assert missed
        assert catalog.get_session_snapshot(session_id) == winner

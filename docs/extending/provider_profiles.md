# Provider-owned sandbox profiles

A sandbox provider can own its profile catalog, permissions, and launch settings.
This lets a customer offer approved environments to specific users and workspaces
without copying their configuration or access rules into Omnigent's profile store.
The provider uses the existing
[`omnigent.sandbox_providers` registration](sandbox_providers.md#creating-a-community-sandbox-provider)
and places its launcher under `omnigent.community.sandbox`.

The runnable [JSON catalog example](../../examples/sandbox-profile-provider/README.md)
subclasses `KubernetesSandboxLauncher`. Its local demo needs neither a cluster
nor a token or identity provider.

## Public contract

Import these types from `omnigent.onboarding.sandboxes.profiles`:

```python
from omnigent.onboarding.sandboxes.profiles import (
    ProviderSandboxProfile,
    SandboxProfileAccessDenied,
    SandboxProfileChanged,
    SandboxProfileContext,
)
```

`ProviderSandboxProfile` is a frozen, strict Pydantic model. Unknown fields and
type coercions are rejected. It contains only public display metadata:

| Field | Contract |
| --- | --- |
| `id` | Stable ID matching `[a-z0-9][a-z0-9_-]{0,63}`; unique within the provider. |
| `name` | Nonempty string, at most 100 characters. |
| `description` | String, at most 500 characters; defaults to `""`. |
| `revision` | Nonempty opaque string, at most 64 characters. Preserve it exactly. |

The revision identifies exact launch settings. Change it when those settings
change, and never reuse it for different settings. A version number or content
hash works; Omnigent does not require hexadecimal revisions. The example hashes
the entire declared `config`, including image and resources.
Object key order and JSON formatting do not affect that hash. Permissions are
checked afresh independently of the revision.

`SandboxProfileContext` is a frozen dataclass with `user_id: str` and
`workspace_id: int`. Omnigent constructs it from authenticated server context.
It is neither a credential nor a client-supplied claim. Resolve it against the
provider's own access policy. Listing and launching managed profiles require
an authenticated user; anonymous and reserved local/public identities are
rejected. A local script may construct contexts explicitly for demonstration.

An OSS server uses workspace **0**. A multi-tenant deployment binds the workspace
from its authenticated request context; it is not a query parameter or a profile
field supplied by the browser. Match the catalog's workspace permissions to that
server context. The example grants workspaces 0 and 1 so both the web demo and
the standalone script work; workspace 2 demonstrates denied access.

On a `SandboxHostLauncher` subclass, set `manages_profiles: ClassVar[bool] = True`
and implement these synchronous methods:

```python
def list_profiles(self, context: SandboxProfileContext) -> list[ProviderSandboxProfile]:
    ...

def select_profile(self, context: SandboxProfileContext, profile: ProviderSandboxProfile) -> None:
    ...
```

`list_profiles` returns only profiles currently available to that user in that
workspace. Return an empty list for a caller with no permitted choices. Keep
configuration, permission lists, and credentials out of the returned models.

`select_profile` must read current permissions and settings again, authorize
the requested ID and exact revision, and configure **this launcher instance**.
Selection runs before `prepare_for_launch`, `prepare`, `provision`, or `resume`,
including wake and replacement. A prior listing or persisted selection does
not authorize a later launch. Do not create a separate launcher and configure it
while leaving the instance supplied by Omnigent unchanged.

Raise `SandboxProfileAccessDenied` when access has been revoked or the profile
is unavailable to the caller. Raise `SandboxProfileChanged` when the caller
still has access but the requested revision cannot be honored. Both derive
from `SandboxError`; the API returns 403 and 409 respectively. Other provider
failures return a generic 503, without exposing private error details. `OwnerCredentialMissingError` returns
an actionable 409 asking the caller to connect their provider account. Never
silently substitute current settings for a saved revision. Providers may keep
old immutable revisions if they can still authorize and honor them; the example
keeps only the current revision and rejects older selections.

Both methods must bound their provider I/O: set explicit connect/read timeouts,
cap response sizes and pagination, and avoid unbounded retries. Neither method
may provision infrastructure or perform launch preflight. The example reads at
most 64 KiB and 100 profiles from a local regular file, rejects pipes, and reads
fresh on every call. Keep that file on local storage; a remote filesystem would
need its own I/O deadline strategy. Constructing the example launcher does not
read the file or initialize Kubernetes clients.

Providers that do not opt in retain `manages_profiles = False`. The base methods
raise `SandboxCapabilityError`. Existing providers keep their deployment defaults.
Omnigent has no profile editor, catalog table, or catalog write API.

## Credentials and lifecycle operations

Providers that act with a user's connected account can declare
`owner_credential_provider` and implement `bind_owner_credential(resolve)`, using
the existing launcher contract. The server binds the deployment's corresponding
per-user resolver before discovery or selection. Calling that resolver obtains
the current credential in the caller's workspace; it may refresh tokens and must
run in a worker thread. Profile context remains identity metadata, with no token
fields. A fork selects using the fork owner's credential, and saved-session
selection uses its recorded owner. There is no fallback to a server credential
when the user's connection is absent.

A provider using customer-configured service credentials can leave
`owner_credential_provider` as `None` and enforce its own authorization using
the trusted context. This does not add a new connection flow: the built-in
server currently wires an owner resolver for configured Databricks connections;
embedding deployments can supply their own `owner_credentials` mapping.

Stop, delete, and status operations must work from stable provider configuration
and `sandbox_id`, without requiring a profile to remain listed or selectable.
Revoked permissions, deleted profiles, and catalog outages must not prevent
cleanup. Keep resource-location settings such as namespace in stable provider
config or in a durable resource identifier. The example puts namespace in
`CatalogConfig` (default `omnigent-demo`) and forbids it in individual profiles.
Keep that namespace unchanged while existing sandboxes still refer to it.

## Discovery API

`GET /v1/sandbox-profiles` includes `managed_providers: string[]`. A provider
remains in that list even when the current caller has no authorized profiles,
so clients can require a selection and explain the empty catalog.

The picker uses `?provider=<name>` to isolate discovery from unrelated providers.
`limit` accepts 1–100 and `cursor` is opaque. Catalogs contain at most 1,000
unique IDs per provider. Every page rechecks authorization; an outage returns
an error, never a partial catalog. Discovery responses use `private, no-store`.

Rows contain only the provider name and public metadata. For example:

```json
{
  "id": "demo-small",
  "name": "Small development sandbox",
  "description": "Approved development environment",
  "revision": "opaque-version-1",
  "provider": "profile_demo"
}
```

The response does not expose `user_id`, `workspace_id`, private settings, or
permission lists. Those identity fields belong to trusted server state.
Catalog mutation endpoints do not exist, including for administrators.
Saved references include the accepted public name and description as an immutable
display snapshot, alongside provider/profile/revision and execution identity.
They contain no provider configuration or directory membership data.
Change settings at the provider, then refresh the list. New launches must explicitly select a profile from a managed
provider; an empty catalog cannot fall back to unprofiled provisioning.

Native `/clear` and `/fork` keep their existing create → bind replacement →
detach source sequence. After checking current provider authorization, the
replacement adopts the immutable profile and trusted host/workspace in one
transaction. Stale runner writes cannot overwrite that binding. The server
rejects detaching the last directly bound session, changing a profiled session
to another runner, and mismatched or ambiguous host bindings. Hostless side
chats continue to use their source's runner. Scheduled fresh sandboxes without
a saved profile are rejected; the scheduler has no profile picker yet.

Authorization runs when accepting a selection and again immediately before
allocation or reuse. If authorization is revoked after acceptance, the owned
session and any uploaded/private agent remain visible with a failed launch;
no sandbox is allocated. If a fresh profiled host fails during binding or runner
startup, cleanup removes its conversation and profile host associations before
deleting the sandbox, retaining the immutable profile so a retry can launch
again. Shutdown drains pending binding writes; a fully bound host remains
available for startup on its replica. A rejection before acceptance removes
the new fork and its private agent copy without announcing a session.

## Trying the example

Follow the [example README](../../examples/sandbox-profile-provider/README.md)
to install the package, start an isolated backend and Vite frontend, compare
Alice's and Bob's choices, and change settings or revoke access between listing
and selection. It also documents temporary plugin installation for browser E2E
fixtures. The fake launch exercises
provider discovery, authorization, revision checks, and launcher configuration.
It does not establish cluster connectivity, Kubernetes RBAC, scheduling, or
network enforcement. The README also explains how to configure the
same provider for an actual Kubernetes-backed Omnigent server.

This feature requires the server opt-in `OMNIGENT_FEATURES=sandbox_profiles`
and a configured provider that declares `manages_profiles = True`. Existing
providers and servers without the flag keep their default launch behavior.
Flag-off clients never fetch profile catalogs or submit a restored selection.

## Deployment and rollback

1. Apply the [additive binding migration](../sandbox-profiles-schema.md). No
   backfill is needed. Deploy the server and UI while the feature remains off.
2. Install and configure the customer's provider in the server environment.
   Confirm authenticated user/workspace mapping, bounded directory calls,
   revision retention, and cleanup behavior against that provider's infrastructure.
3. Enable `sandbox_profiles` in `OMNIGENT_FEATURES`, preserving other overrides.
   Test an allowed user, a denied user, a stale revision, and wake after revocation
   before wider rollout. Ordinary providers still launch with deployment defaults.

For rollback, stop creating new profiled sessions and drain their running
sandboxes before disabling the flag or rolling back the application. Preserve
binding rows and provider revisions for later recovery. A flag-off server does
not enforce profile authorization; disabling the flag is not an access-revocation
mechanism. Providers that still declare `manages_profiles` reject launches without
selection at the launcher boundary. Keep provider configuration available for
cleanup, and do not downgrade the schema after profiles have been accepted.

Okta/Google Groups integration belongs inside the customer's provider. This
feature does not ship a directory connector or manage identities. Validate that
integration and its infrastructure separately from the local example.

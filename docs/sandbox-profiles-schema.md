# Sandbox profile schema deployment

Migration `nn1a2b3c4d5e`, following `b2e4f6a8c013`, adds one table to the
Omnigent metadata database, plus a runner/host lookup index on conversation
metadata. Existing rows and columns are unchanged.

| Table | Primary key | Purpose |
| --- | --- | --- |
| `sandbox_profile_bindings` | `(workspace_id, session_id)` | Immutable provider/profile revision, public metadata, trusted execution identity, and optional host association. |

The host-binding index includes workspace, host, and session IDs for a bounded,
deterministic lookup. Session and host IDs use the existing binary UUID type.
The JSON reference uses binary storage with a database check constraint limiting
it to 16,383 serialized bytes, including multibyte encodings.
Provider configuration, credentials, and membership lists are never stored here.

The `(workspace_id, runner_id, host_id, id)` index supports two bounded seeks to
identify a runner's host and detect ambiguous host ownership. It skips hostless
children and repeated bindings without hydrating conversations. The existing
runner/session index remains necessary for pagination ordered by session ID.
On PostgreSQL the new index is built concurrently before creating the bindings
table. Retries remove an invalid partial index before rebuilding it, so an
interrupted deployment can resume without blocking session writes.

Deploy this additive migration before enabling `sandbox_profiles`. Older servers
ignore the table. With the flag off, new servers do not read or write it. No
backfill or production data cleanup is required.

Retain the table and its data on application rollback. A migration downgrade
removes accepted references; use it only before feature use or after preserving
the data separately. Drain profiled workloads before disabling authorization or
rolling back to a server that does not support it.

The schema PR contains this migration, its ORM declaration, this deployment note,
and dialect/additivity tests. Provider authorization ships in the next PR.

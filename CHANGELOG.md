# Changelog

This file tracks notable changes to the workspace tag stream
(`launchapp-dev/animus-protocol`). Per-crate Cargo.toml versions are the
source of truth for individual crate bumps. Tags map roughly to
"workspace cuts" — a tag may bump multiple crates at once.

## v0.5.10 — queue precise-wake (2026-06-14)

### Added

`animus-queue-protocol` 0.3.1 -> 0.3.2 (additive, backward compatible):

- `METHOD_QUEUE_NEXT_DEADLINE = "queue/next_deadline"` + `QueueNextDeadlineResponse
  { next_run_at: Option<String> }` — reports the earliest future `run_at`
  across pending deferred entries so the daemon can sleep until exactly that
  instant (precise wake) instead of relying on its heartbeat. `None` when the
  queue holds no future-dated entries.

## v0.5.9 — deferred queue dispatch (2026-06-13)

### Added

`animus-queue-protocol` 0.3.0 -> 0.3.1 (additive, backward compatible):

- `QueueEnqueueRequest.run_at: Option<String>` — RFC 3339 earliest-dispatch
  time. When set and in the future, the entry is enqueued deferred: it
  stays `pending` but is excluded from `queue/lease` until the instant
  passes. `None` preserves dispatch-ASAP behavior.
- `QueueEnqueueRequest.expire_after_secs: Option<u64>` — grace window after
  `run_at`; a still-pending deferred entry past `run_at + expire_after_secs`
  is dropped on sweep instead of dispatched late. `None` = never expire.
- `QueueEnqueueResponse.warning: Option<String>` — non-fatal advisory. Set
  (most commonly) when another entry already exists for the same subject;
  the duplicate is still enqueued (deferred enqueues are never deduped) and
  the caller decides whether to drop it.
- `QueueEntry.run_at` / `QueueEntry.expire_after_secs` — surfaced on
  list/lease so callers can distinguish scheduled-for-later entries.
- `QueueStats.deferred: usize` — subset of `pending` not yet leasable.

All new fields use serde defaults / `skip_serializing_if`, so older
clients and stored payloads round-trip unchanged.

## v0.5.7 — restore `subject/delete` + plugin-runtime subject helpers (2026-06-07)

### Restored

Reverts the regression introduced by `aed9f42` ("v0.1.14: sync ... from
animus-cli"), which dropped a number of load-bearing surfaces on
`animus-subject-protocol`. Downstream Rust subject plugins were still
pinned to `v0.1.13` because of the regression; v0.5.7 makes the canonical
tag forward-compatible again.

`animus-subject-protocol` 0.1.14 -> 0.1.15:

- `METHOD_SUBJECT_DELETE = "subject/delete"` wire constant.
- `BackendError::Unsupported(String)` variant + JSON-RPC mapping to
  `METHOD_NOT_SUPPORTED` (-32001) with `{"category": "unsupported"}`.
- `SubjectBackend::delete` trait method with default impl returning
  `Unsupported`, so existing implementors compile unchanged.
- `DeleteSubjectRequest { id: SubjectId }` and
  `DeleteSubjectResponse { ok: bool }`.
- `Subject::native_status: Option<String>`,
  `Subject::status_metadata: Value`, `Subject::attachments: Vec<SubjectAttachment>`.
- `SubjectAttachment { id, kind, uri, title, mime_type, metadata }`.
- `StatusDispatchHint { native_status, maps_to, dispatch_label, description }`.
- `SubjectSchema::native_status_values: Vec<String>`,
  `SubjectSchema::status_dispatch_hints: Vec<StatusDispatchHint>`.
- `SubjectFilter::native_status`, `dispatch_label`, `has_attachment_kind`
  fields.
- `SubjectChangedEvent::previous_native_status`,
  `previous_dispatch_label` fields.
- `ChangeKind::DispatchLabelChanged`, `::AttachmentAdded`, `::AttachmentRemoved`
  variants.

`animus-plugin-runtime` 0.2.0 -> 0.2.1:

- `subject_backend_main(info, backend)` — drop-in for the v0.1.13
  entrypoint of the same name. Wires the five non-streaming subject
  verbs (`list`, `get`, `update`, `delete`, `schema`) onto a generic
  `Plugin` shell and runs the stdio loop. Reads `backend.schema().kinds`
  once at startup and registers both the canonical `subject/<verb>` and
  the kind-prefixed `<kind>/<verb>` aliases for every declared kind,
  matching the dispatcher shape the daemon's `SubjectRouter` produces
  in production. Forwards `health/check` to `backend.health()` via the
  new `Plugin::on_health` hook so backends correctly report upstream
  outages instead of always reporting healthy. The streaming
  `subject/watch` subscription is NOT registered — the generic
  `Plugin` shell does not yet model per-subscription notification
  streams. Backends that need watch should drive the `Plugin` builder
  directly and register a custom subscription handler.
- `subject_backend_main_with_capabilities(info, backend, extra)` —
  parity with v0.1.13.
- `subject_backend_main_with_kinds(info, backend, kinds)` — registers
  the kind-prefixed `<kind>/<verb>` aliases for an explicit kinds list
  (use when the backend declares more kinds than `schema().kinds` would
  return).
- `subject_plugin(info, backend)` / `subject_plugin_with_kind_aliases` —
  builder-style alternatives for plugins that need to keep customizing
  the `Plugin` before `.run().await`.
- Kind-prefixed `<kind>/list` invocations inject the kind into
  `SubjectFilter.kind` before calling `backend.list`, so a single
  backend serving multiple kinds can distinguish `task/list` from
  `issue/list` even when the caller sends an empty filter.
- `Plugin::advertised_methods()` and `Plugin::has_method_handler()`
  read-only accessors so tests can verify the manifest shape without
  driving the stdio loop.
- `Plugin::on_health(hook)` builder method registers a backend-specific
  `health/check` hook. When set, the shell awaits the hook and returns
  the backend's `HealthCheckResult` (or an `RpcError` from the hook).
  Unset plugins continue to report `HealthStatus::Healthy` as before.

### Added

- `SubjectSchema::supports_delete: bool` — mirror of `supports_create`,
  defaults to `false` for back-compat. Backends that override
  `SubjectBackend::delete` should set this to `true`.
- The `supports_create` doc comment is updated. The "reserved for
  v0.4.x" text is removed. The new wording documents the actual
  semantics: the field declares whether the plugin honors
  `<kind>/create` verb invocations. The protocol-canonical
  `subject/create` verb is **not** wired in any first-party plugin or
  daemon path today; it remains a candidate for a future revision but
  v0.5.7 does not introduce a new wire surface for it.
- JSON Schema export bin (`animus-subject-protocol-export-schema`) now
  emits artifacts for the restored `SubjectAttachment`,
  `StatusDispatchHint`, `DeleteSubjectRequest`, `DeleteSubjectResponse`
  types alongside the existing entries.

### Kept from v0.1.14

- The `schemars::JsonSchema` derives on every public message type.
- The `export_schema` bin that dumps per-type JSON Schema artifacts.

### Why this matters

The v0.1.14 "sync from animus-cli" merge replaced the upstream protocol
crate with a snapshot of the in-tree ao-cli copy, which had been
incrementally pruned of subject extensions that downstream plugins
depended on. The regression silently broke any plugin author who tried
to upgrade past v0.1.13 and forced `launchapp-dev/animus-subject-default`
v0.1.3 to stay pinned to v0.1.13.

v0.5.7 makes the upstream protocol forward-compatible with v0.1.13
again. Downstream Rust subject plugins can now bump their pin to v0.5.7
in a single edit, pick up `subject/delete`, and stop being trapped on
v0.1.13.

### Future-proofing the protocol crate

If you find yourself authoring an "_sync from animus-cli_" commit in
this repo, read this file first. Sync the **direction** is from
protocol-out (this repo) to ao-cli-in, not the other way around.

## v0.5.6 — `animus-agent-runner-protocol` deprecation (2026-06-04)

`animus-agent-runner-protocol` 0.1.0 -> 0.1.1: marks the crate
deprecated. The agent-runner sidecar was removed from ao-cli in v0.5.3;
no first-party agent_runner plugin will ship. Plugin authors should
target `animus-provider-protocol` / `animus-session-backend` instead.

## v0.5.0 -> v0.5.5

- Four new plugin-kind protocol crates: `animus-workflow-runner-protocol`,
  `animus-queue-protocol`, `animus-durable-store-protocol`,
  `animus-memory-store-protocol`.
- `animus-plugin-runtime` v0.2.0: generic `Plugin` shell + `register_method!`
  macro replaces the kind-specific `*_backend_main` helpers.
- `animus-notifier-protocol` v0.1.0: notifier plugin-kind wire types.
- `animus-queue-protocol` v0.3.0: `exclude_subjects` on `QueueLeaseRequest`.

## v0.1.0 -> v0.1.14

See `git log` on this repository. v0.1.x marks the original protocol
extraction era; v0.5.x is the protocol-stabilization era.

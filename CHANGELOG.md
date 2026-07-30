# Changelog

This file tracks notable changes to the workspace tag stream
(`launchapp-dev/animus-protocol`). Per-crate Cargo.toml versions are the
source of truth for individual crate bumps. Tags map roughly to
"workspace cuts" — a tag may bump multiple crates at once.

## Unreleased

### Added

`animus-execution-protocol` 0.1.0 introduces
`animus.execution-fence.v1`: one durable envelope binds workflow and subject
generations, queue entry/owner/lease generation and expiry, and an exact
repository base/head-ref reservation. `animus-queue-protocol` 0.4.0 adds an
additive `queue/v2/*` surface for idempotent generation allocation, pending-only
leasing, typed collision results, CAS renewal, explicit expired-lease recovery,
fenced completion, and fenced return-to-pending. Recovery rotates the lease
owner/generation while preserving the workflow id/generation; ordinary leasing
cannot clone expired assigned work. Workflow runner 0.4.0 and environment 0.2.0
carry and echo the fence, publication receipts validate against it, and the
control protocol exposes typed fleet capacity/reservation/recovery state for
Portal and MCP consumers. See `docs/execution-fence.md`.

`animus-config-protocol` 0.2.0 and `animus-workflow-runner-protocol` 0.3.0 add
the v1 single-owner workflow publication contract and proof-carrying receipt.
Workflows explicitly select a runner or phase owner and a fail-safe cleanup
policy; semantic validation rejects missing, repeated, unknown, manual, or
opaque command owners. Runner results carry a versioned receipt fenced by
workflow and qualified-subject generations with commit/tree, observed remote,
recovery-ref, PR, issuer, and timestamp proof. Legacy configs remain readable
but publication is disabled and exposed through an explicit migration
diagnostic—workflow or phase names are never treated as publication intent.
Committed JSON Schemas and the cross-version compatibility contract are
documented in `docs/workflow-publication-contract.md`.

`animus-config-protocol` 0.1.2: optional, typed
`AgentProfile.application_chat_controls` policy with bounded control enums and
configured references. Profile omission remains backward-compatible; overlay
omission inherits, explicit `null` revokes, and a concrete object replaces.
Unknown fields, duplicate or oversized lists, and invalid configured references
fail during canonical deserialization.

`animus-plugin-protocol`: additive
`conversation_operation_fenced_append_v1` capability and
`ConversationAppendMessageRequest.operation_fence`. Shared backends validate
the exact operation lease and active conversation reservation atomically with
assistant-message insertion, preventing a provider result from a reclaimed or
terminalized runtime from becoming canonical.

`animus-subject-protocol` 0.2.0 adds a non-downgradable authenticated subject
surface:

- Required `SubjectRequestContext` carrying a typed `Actor` plus optional
  request, correlation, and idempotency identifiers.
- Typed v2 list/get/create/update/status/delete request shapes and distinct
  `subject/v2/<verb>` / `<kind>/v2/<verb>` method names.
- `ActorScopedSubjectBackend`, separate from the legacy global
  `SubjectBackend`, so a v2 request can never silently fall through to v1.
- JSON Schema exports and compatibility tests for the new wire types.

`animus-plugin-protocol`: shared multi-host chat operation authority via the
additive `conversation/operation_*` reserve, load, renew, execution-bind,
release, user-accept, and terminalize RPCs. The authenticated tenant/actor plus
repository, conversation, and caller key form the durable partition. Opaque
leases fence every mutation by token and expiry; reclaims rotate authority,
terminal receipts are immutable, and replay/load never expose lease tokens.

`animus-plugin-protocol`: `ConversationScope.tenant_id`, an optional-on-wire,
1..=128-character opaque server-selected workspace/tenant partition key carried
by every conversation-store request. Shared backends include it in every
conversation/message key and validate it against the authenticated transport
actor, failing closed unless an operator explicitly pins a legacy tenant.
Conversation creation stamps owner from that authenticated call context, and
ordinary metadata saves cannot transfer or clear ownership.

`animus-plugin-protocol`: optional
`ConversationMeta.active_operation_id` on the canonical load/save-meta path.
The field durably identifies which keyed chat operation owns a revision
reservation so that operation can recover after a crash. It is absent by
default for backward compatibility, constrained to 1..=128 ASCII alphanumeric
or `._:-` characters, writable only through `conversation/save_meta`, and
intentionally omitted from create requests and list summaries.

`animus-plugin-protocol` (0.1.18): `PluginManifest.supports_mcp: Option<bool>`
— a first-class, plugin-DECLARED capability field the kernel reads instead of
hardcoding per-tool MCP behavior in a name table (REQUIREMENT-039 / TASK-277).

- `PROTOCOL_VERSION` bumped `1.1.0` -> `1.2.0` (backward-compatible minor: the
  field is optional and `#[serde(default, skip_serializing_if = "Option::is_none")]`).
- Back-compat: absent = undeclared; the kernel keeps its historical default
  (provider plugins are MCP-capable). Only an explicit `false` opts a provider out.
- `animus-plugin-runtime` (0.2.2): `Plugin::supports_mcp(bool)` builder so a Rust
  plugin author can declare the flag. The provider runtime (`provider_main`) emits
  `None` for now — auto-mapping from `ProviderCapabilities.mcp` is deferred because
  that flag defaults `false` and would regress providers relying on the default.
- This is the proof-of-pattern field for the wider REQUIREMENT-039 cleanup
  (launch template, permission-mode flag, reasoning-effort, default model, ...).

## v0.1.21 — config_source write-back (`config/write`)

### Added

`animus-config-protocol`: a write-back path so config sources can persist a
kernel-validated canonical model.

- `METHOD_CONFIG_WRITE` (`config/write`) — optional, gated on the new
  `CAPABILITY_CONFIG_WRITE` (`"config_write"`) manifest capability. The kernel
  ships the entire validated `ConfigModel`; the plugin persists it. Coarse
  full-model write only — no granular per-entity wire methods. Sources that
  cannot persist (e.g. the YAML source) omit the capability; if they receive
  `config/write` anyway they MUST respond `METHOD_NOT_SUPPORTED`.
- `ConfigWriteRequest { project_root, repo_scope, config }` and
  `ConfigWriteResponse { cache_token }` wire types + exported JSON schemas.

## v0.1.19 — protocol + config-protocol move into animus-protocol

### Added

Moved the kernel `protocol` crate and `animus-config-protocol` out of
`launchapp-dev/animus-cli` into this repo, per the architecture rule that no
protocol/wire-type crates live inside the CLI. They now build as workspace
members alongside the other `animus-*-protocol` crates:

- `protocol` 0.1.0 — kernel wire types (orchestrator enums, `PhaseCapabilities`,
  `PhaseRoutingConfig`, `McpRuntimeConfig`, `hook_policy`, model routing,
  repository scope, sync config, error classification). Sibling dep on
  `animus-subject-protocol` is now a path dep within this workspace.
- `animus-config-protocol` 0.1.0 — `config_source` plugin wire types
  (`ConfigModel` envelope + the canonical YAML parser + `WorkflowConfig`
  types). Path-deps `protocol`.

ao-cli and out-of-tree plugins (config-yaml, workflow-runner) git-dep both by
this tag instead of pinning the CLI repo rev.

## v0.5.13 — subject/unwatch verb

### Added

`animus-subject-protocol` 0.1.16 (additive):

- Added [`METHOD_SUBJECT_UNWATCH`] (`"subject/unwatch"`) and the
  `SubjectUnwatchRequest { watch_id: String }` request type. The daemon
  issues this when it drops a `subject/watch` subscription so the backend
  can cancel the backing `watch()` task instead of leaking it until plugin
  shutdown. The `watch_id` correlates with the JSON-RPC request id used in
  the originating `subject/watch` call. Best-effort — backends that do not
  track per-watch tasks may treat it as a no-op. The schema export binary
  now emits `SubjectUnwatchRequest.json`.

## v0.5.12 — restore transport_backend_main entrypoints (regression fix)

### Fixed

`animus-plugin-runtime` 0.2.1 (additive, restores dropped public API):

- Restored `transport_backend_main` and
  `transport_backend_main_with_capabilities`, the stdio-loop entrypoints every
  transport plugin (`animus-transport-http`, `animus-transport-graphql`,
  `animus-web-ui` wrapper) calls from `main.rs`. They were accidentally dropped
  in commit `aed9f42` ("v0.1.14: sync ... from animus-cli") when the crate was
  refactored to its provider-focused shape, which broke transport-plugin
  compilation against the current protocol. The restored code lives in a new
  `src/transport.rs` module (coexisting with `plugin.rs` / `subject.rs`) and is
  adapted to the current `animus-plugin-protocol` wire types
  (`PluginCapabilities.projections`, `InitializeResult.kind_capabilities`,
  `PluginManifest.env_required` / `notification_buffer_size`).
  `extra_capabilities` is `Vec<String>`. Adds an `animus-transport-protocol`
  path dependency to the crate.

## v0.5.11 — remove orphaned agent-runner-protocol crate (2026-06-14)

### Removed

- `animus-agent-runner-protocol` crate deleted. It was the wire protocol for
  the agent-runner sidecar removed in v0.5.3 (providers now spawn/supervise
  the coding-agent CLIs end to end), was bumped to `v0.1.1 deprecated`, and
  had zero consumers across the entire fleet (ao-cli + all plugin repos).
  Older git tags still contain the crate, so any historical pin is
  unaffected. The `PLUGIN_KIND_AGENT_RUNNER` wire constant on
  `animus-plugin-protocol` is retained (manifest-parse compatibility) with an
  updated doc comment.

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

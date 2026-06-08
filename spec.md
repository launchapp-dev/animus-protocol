# Animus Plugin Protocol Specification

**Status:** `1.0.0` (frozen for the 1.x line)
**Editor:** Launchapp.dev
**Audience:** plugin authors writing Animus plugins in any language.

This document specifies the wire protocol Animus uses to communicate with plugins. It is intentionally language-agnostic. The Rust crates in this repository (`animus-plugin-protocol`, `animus-subject-protocol`, `animus-provider-protocol`, `animus-plugin-runtime`) are one reference implementation; a conformant Python, TypeScript, Go, or Zig plugin is just as compatible.

When the Rust crates and this document disagree, **this document wins.** Bugs in the SDK should be filed against this spec as the source of truth.

## 1. Transport

### 1.1 Wire format

Plugins are spawned as child processes of the Animus daemon. The daemon writes JSON-RPC 2.0 frames to the plugin's **stdin** and reads JSON-RPC 2.0 frames from the plugin's **stdout**. Each frame is a single JSON value terminated by a single newline (`\n`, U+000A).

- Frames MUST NOT contain literal newlines inside string values; senders MUST escape them as `\n`.
- Frames MAY be pretty-printed, but the trailing newline still terminates the frame.
- The plugin's **stderr** is reserved for human-readable diagnostics and is not part of the protocol. Hosts MAY capture stderr to a per-plugin log but MUST NOT attempt to parse it.

### 1.2 Charset

Frames are UTF-8. Senders MUST NOT emit a BOM.

### 1.3 Framing edge cases

- A blank line (only `\n`) is ignored.
- A frame that fails JSON parsing is logged by the recipient and discarded. A recipient MAY respond with a JSON-RPC `parse_error` (-32700) carrying `id: null` if it can extract or invent an id, but MUST NOT close the connection on a single bad frame.
- A frame larger than 16 MiB MAY be rejected with a `parse_error`. Plugins SHOULD chunk large payloads into multiple notifications.

## 2. JSON-RPC envelopes

The protocol uses the JSON-RPC 2.0 specification verbatim. The shapes below are the only frames a conformant plugin or host emits.

### 2.1 Request

```json
{
  "jsonrpc": "2.0",
  "id": 17,
  "method": "subject/list",
  "params": { "filter": { "status": ["ready"] } }
}
```

- `jsonrpc` MUST be `"2.0"`.
- `id` MUST be a string or number. Hosts SHOULD use integers.
- `method` is the method name (see §6, §7).
- `params` MAY be omitted when no parameters are needed.

### 2.2 Notification

```json
{
  "jsonrpc": "2.0",
  "method": "subject/changed",
  "params": { "id": "linear:ENG-123", "subject": { ... } }
}
```

Notifications have no `id` and never receive a response.

### 2.3 Response (success)

```json
{
  "jsonrpc": "2.0",
  "id": 17,
  "result": { ... }
}
```

### 2.4 Response (error)

```json
{
  "jsonrpc": "2.0",
  "id": 17,
  "error": { "code": -32601, "message": "method 'subject/foo' not found" }
}
```

A response MUST have exactly one of `result` or `error`.

## 3. Lifecycle

```
spawn ──▶ initialize (request)
           │
           ▼
       initialized (notification, host→plugin)
           │
           ▼
       request/response loop (+ notifications, either direction)
           │
           ▼
        shutdown (request, host→plugin)
           │
           ▼
        exit (notification, host→plugin)  ──▶ process exit, then close stdin
```

### 3.1 Spawn

The host MAY pass `--manifest` (or `-m`) on the command line. A plugin that recognizes this flag MUST print a single [`PluginManifest`](#82-pluginmanifest) JSON object on stdout and exit 0 — the JSON-RPC loop MUST NOT start in this mode. This is the discovery surface used by `animus plugin install`.

A plugin invoked without `--manifest` starts the JSON-RPC loop immediately. If the plugin detects that stdin is a TTY (i.e. no host is connected), it SHOULD print a one-line usage hint to stderr and exit 2.

### 3.2 `initialize`

The host's first frame is a request with method `"initialize"`.

```json
{
  "jsonrpc": "2.0", "id": 1, "method": "initialize",
  "params": {
    "protocol_version": "1.0.0",
    "host_info": { "name": "animus", "version": "0.4.0" },
    "capabilities": { "streaming": true, "progress": false, "cancellation": true }
  }
}
```

The plugin replies with [`InitializeResult`](#88-initialize):

```json
{
  "jsonrpc": "2.0", "id": 1,
  "result": {
    "protocol_version": "1.0.0",
    "plugin_info": { "name": "animus-subject-linear", "version": "0.1.0", "plugin_kind": "subject_backend" },
    "capabilities": {
      "methods": ["subject/list", "subject/get", "subject/update", "subject/schema", "health/check"],
      "streaming": false,
      "subject_kinds": ["issue"]
    }
  }
}
```

Hosts MUST check `protocol_version` for compatibility before sending any domain request. See §10 for versioning.

### 3.3 `initialized`

The host then sends a notification:

```json
{ "jsonrpc": "2.0", "method": "initialized" }
```

The plugin MUST NOT send domain responses until it has received `initialized`. Hosts MAY send domain requests immediately after `initialized`.

### 3.4 Steady state

Both sides exchange requests, responses, and notifications freely. Requests on either side MUST receive a response within a reasonable time; long-running operations SHOULD stream notifications carrying the original request id, then send a final response.

### 3.5 `shutdown` and `exit`

The host sends a `shutdown` request. The plugin completes in-flight work, flushes state, and replies with an empty object. The host then sends an `exit` notification (or simply closes stdin). On `exit`, the plugin process MUST terminate.

A plugin MAY treat stdin EOF as an implicit `exit`. Hosts SHOULD prefer explicit `exit` but MUST tolerate plugins that exit on EOF.

## 4. Error codes

| Code | Constant | Meaning |
|---|---|---|
| -32700 | `parse_error` | Invalid JSON received. |
| -32600 | `invalid_request` | JSON is not a valid request object. |
| -32601 | `method_not_found` | Method does not exist. |
| -32602 | `invalid_params` | Method parameters were invalid. |
| -32603 | `internal_error` | Unspecified internal error. |
| -32000 | `plugin_not_initialized` | Domain method received before `initialize` completed. |
| -32001 | `method_not_supported` | Method is recognized but not implemented (host should fall back). |
| -32002 | `request_cancelled` | Host cancelled the request via `$/cancelRequest`. |
| -32003 | `timeout` | Request did not complete within the host-imposed timeout. |

Codes outside `-32000..-32099` and `-32700..-32600` are reserved for application-specific errors and MAY be used by plugins for their own categorization. Hosts treat unknown codes as generic failures.

## 5. Plugin kinds

A plugin declares its kind in [`PluginInfo::plugin_kind`](#87-plugininfo) during `initialize`. The kind determines which domain methods the host will call.

| Kind | Methods (in addition to lifecycle) |
|---|---|
| `provider` | `agent/run`, `agent/resume`, `agent/cancel` |
| `subject_backend` | `subject/list`, `subject/get`, `subject/update`, `subject/delete` (optional, v0.1.8+), `subject/watch` (optional), `subject/schema` |
| `trigger_backend` | `trigger/watch`, `trigger/event` (notification), `trigger/ack` (optional), `trigger/schema` |
| `log_storage_backend` | `log_storage/store`, `log_storage/query` (optional), `log_storage/tail` (optional), `log_storage/event` (notification), `log_storage/schema` |
| `transport_backend` | `transport/start`, `transport/shutdown`, `transport/schema` |
| `workflow_runner` (v1.1.0+) | `workflow/execute`, `workflow/run_phase` |
| `queue` (v1.1.0+) | `queue/enqueue`, `queue/list`, `queue/lease`, `queue/stats`, `queue/hold`, `queue/release`, `queue/release_pending`, `queue/drop`, `queue/reorder`, `queue/mark_assigned`, `queue/completion` |
| `durable_store` (v1.1.0+) | `durable/begin_workflow_run`, `durable/begin_step`, `durable/commit_step`, `durable/abandon_step`, `durable/recover_in_flight`, `durable/query_run` |
| `memory_store` (v1.1.0+) | `memory/put`, `memory/get`, `memory/query`, `memory/list_scopes`, `memory/delete_scope` |
| `notifier` (v1.1.0+) | `notifier/notify`, `notifier/flush` (optional), `notifier/schema` |
| `custom` | none predefined; host treats domain methods opaquely and surfaces them via `animus.plugin.call` MCP |

Hosts MUST NOT call domain methods for a kind other than the one declared.

## 6. Shared methods

These methods are available to plugins of every kind.

### 6.1 `health/check`

Request:

```json
{ "jsonrpc": "2.0", "id": 5, "method": "health/check" }
```

Response (`HealthCheckResult`):

```json
{ "jsonrpc": "2.0", "id": 5,
  "result": {
    "status": "healthy",
    "uptime_ms": 124000,
    "memory_usage_bytes": 41943040,
    "last_error": null
  }
}
```

`status` MUST be one of `"healthy"`, `"degraded"`, `"unhealthy"`. The host MAY use this to gate work or initiate restart.

### 6.2 `$/ping`

Liveness probe. The plugin MUST respond with an empty object as quickly as possible. Hosts SHOULD impose a short timeout (≤ 1s) and treat repeated misses as an unresponsive plugin.

### 6.3 `$/cancelRequest` (notification)

```json
{ "jsonrpc": "2.0", "method": "$/cancelRequest", "params": { "id": 17 } }
```

If the plugin advertised `capabilities.cancellation = true`, it SHOULD make a best-effort attempt to cancel the in-flight request with the matching id and respond with error code `-32002`.

### 6.4 `$/progress` (notification)

Optional progress reporting. Carries `params.id` matching the request, plus arbitrary `params.value`. Hosts may surface progress to operators.

## 7. Domain methods

### 7.1 Subject backend methods

#### `subject/list`

Returns ready/filtered subjects.

Params: [`SubjectFilter`](#91-subjectfilter). Result: [`SubjectList`](#92-subjectlist).

#### `subject/get`

Returns a single subject by id.

Params: `{ "id": "linear:ENG-123" }`. Result: `{ "subject": Subject }`.

#### `subject/update`

Applies a [`SubjectPatch`](#94-subjectpatch).

Params: `{ "id": "linear:ENG-123", "patch": { ... } }`. Result: `{ "subject": Subject }` (the refreshed subject).

#### `subject/delete`

Permanently removes a subject. Added in v0.1.8.

Params: `{ "id": "linear:ENG-123" }`. Result: `{ "ok": true }`.

Backends that do not support deletion MUST respond with error code `-32001` (`method_not_supported`). Hosts then fall back to status-only soft-cancel semantics (e.g. transitioning to `cancelled`). The wire dispatch in `animus-plugin-runtime` always accepts `<kind>/delete` and `subject/delete`; backends opt in by overriding `SubjectBackend::delete` on the Rust trait.

#### `subject/watch`

Opens a server-streaming subscription. The response is sent immediately to acknowledge; subsequent change events arrive as `subject/changed` notifications carrying the original request id in `params.id`.

Polling-only backends MUST respond with error code `-32001` (`method_not_supported`). Hosts then fall back to periodic `subject/list` calls.

#### `subject/schema`

Returns a [`SubjectSchema`](#95-subjectschema) capability declaration. SHOULD be cheap (constant or one-shot at startup).

### 7.2 Provider methods

#### `agent/run`

Starts a new agent session. Params: [`AgentRunRequest`](#101-agentrunrequest). Streams `agent/output`, `agent/thinking`, `agent/toolCall`, `agent/toolResult`, `agent/error` notifications. Final response: [`AgentRunResponse`](#102-agentrunresponse).

#### `agent/resume`

Resumes a prior session. Same param shape as `agent/run`, with `session_id` set. Same streaming and response shape.

#### `agent/cancel`

Params: `{ "session_id": "..." }`. Best-effort termination of the session. Result: `{ "session_id": "...", "cancelled": true }`.

### 7.3 Trigger backend methods

Trigger backends are push-driven event sources. The host calls `trigger/watch` once after `initialized` to open the event stream; the plugin acknowledges immediately and then emits `trigger/event` notifications for the life of the connection.

#### `trigger/watch`

Opens the event stream. The response is sent immediately to acknowledge; subsequent events arrive as `trigger/event` notifications. Hosts SHOULD only issue one in-flight `trigger/watch` per plugin connection.

Params: none. Result: `{ "watching": true }`.

If the backend cannot open its upstream (e.g. missing credentials), it MUST return a JSON-RPC error response rather than emit a stream that fails on the first poll.

#### `trigger/event` (notification)

```json
{ "jsonrpc": "2.0", "method": "trigger/event",
  "params": {
    "event_id": "slack:T123/C456/1715701234.000100",
    "trigger_id": "configured-trigger-id",
    "payload": {
      "user": "U1",
      "text": "@animus please review",
      "kind": "slack_mention"
    },
    "subject_id": "linear:ENG-123",
    "subject_kind": "issue",
    "action_hint": "run_workflow"
  }
}
```

`params` IS the [`TriggerEvent`](#111-triggerevent) struct directly — fields are flat on `params`, not nested under an `event` wrapper. See the wire-shape note below.

Stream-level errors that occur after `trigger/watch` has already been ack'd are emitted as a `trigger/event` notification whose `params` carries a JSON-RPC `error` object in place of the `TriggerEvent` fields (i.e. `{ "jsonrpc": "2.0", "method": "trigger/event", "params": { "error": { "code": ..., "message": "..." } } }`). Such an error notification terminates the stream; the plugin SHOULD then exit or wait for `shutdown`.

##### Wire shape note

`trigger/event` notifications use a **flat** `params` object. The `TriggerEvent` fields (`event_id`, `trigger_id`, `subject_id`, `subject_kind`, `action_hint`, `payload`) live directly on `params`. There is **no** outer `{ "id": <watch-id>, "event": { ... } }` envelope — earlier drafts of this spec showed a nested shape; that wrapper was never implemented by the host and MUST NOT be emitted by plugins. The host deserializes `params` straight into [`TriggerEvent`](#111-triggerevent) (see `crates/orchestrator-daemon-runtime/src/schedule/trigger_supervisor.rs` and `crates/animus-plugin-protocol/src/lib.rs::TriggerEvent` in the `animus-cli` repo). Hosts correlate events with their originating `trigger/watch` per-connection: there is only ever one in-flight watch per plugin connection, so an echo id is unnecessary.

#### `trigger/ack`

Acknowledges an event id so the backend does not redeliver it on resume. Backends that don't track delivery state MAY accept any id (no-op) or MAY respond with `-32001` (`method_not_supported`); hosts must tolerate either.

Params: `{ "event_id": "..." }`. The `event_id` is the value the host received in a prior `trigger/event` notification's `params.event_id`, echoed back verbatim. Result: `{ "event_id": "...", "acked": true }`.

#### `trigger/schema`

Returns a [`TriggerSchema`](#112-triggerschema) capability declaration. SHOULD be cheap (constant or one-shot at startup).

### 7.4 Log storage backend methods

Log storage backends persist structured [`LogEntry`](#121-logentry) records emitted by the daemon, plugins, the CLI, and individual workflow runs. The daemon calls `log_storage/store` as it produces events; operators (or other plugins) call `log_storage/query` and `log_storage/tail` to read them back.

Backends declare which read surface they support in [`LogStorageSchema`](#124-logstorageschema). A write-only sink advertises `supports_query = false` / `supports_tail = false` and returns `-32001` (`method_not_supported`) for the corresponding calls.

#### `log_storage/store`

Persists a batch of [`LogEntry`](#121-logentry) records. Backends MAY deduplicate by `LogEntry.id` to keep at-least-once delivery idempotent.

Params:

```json
{
  "entries": [
    {
      "id": "evt-001",
      "ts": "2026-05-17T18:20:34Z",
      "level": "info",
      "source": "plugin",
      "source_name": "animus-subject-linear",
      "target": "plugin.animus-subject-linear.client",
      "message": "fetched 14 issues",
      "fields": { "count": 14 }
    }
  ]
}
```

Result:

```json
{ "stored": 1 }
```

Implementations SHOULD be transactional within a single call — either all entries land or none do — so callers can retry on partial failure without producing duplicates.

#### `log_storage/query`

Non-streaming query for historical log entries. Params: [`LogQuery`](#122-logquery). Result: [`LogQueryResult`](#123-logqueryresult).

Backends honor the filters they advertise in [`LogStorageSchema.supports_filtering`](#124-logstorageschema). Filters the backend cannot evaluate SHOULD be ignored — the daemon applies the remainder in-process.

Backends that cannot read return `-32001` (`method_not_supported`).

#### `log_storage/tail`

Opens a streaming query. The response is sent immediately to acknowledge; subsequent entries arrive as `log_storage/event` notifications carrying the original request id in `params.id`. If [`LogQuery::follow`](#122-logquery) is `true`, the stream stays open and emits new entries as they arrive; otherwise it closes after replaying the matching backlog.

Params: [`LogQuery`](#122-logquery). Result: `{ "tailing": true }`.

If the backend cannot open the tail (e.g. upstream auth failure), it MUST return a JSON-RPC error response rather than emit a stream that fails on the first poll.

#### `log_storage/event` (notification)

```json
{ "jsonrpc": "2.0", "method": "log_storage/event",
  "params": {
    "id": 17,
    "entry": {
      "id": "evt-001",
      "ts": "2026-05-17T18:20:34Z",
      "level": "info",
      "source": "plugin",
      "source_name": "animus-subject-linear",
      "target": "plugin.animus-subject-linear.client",
      "message": "fetched 14 issues",
      "fields": { "count": 14 }
    }
  }
}
```

`params.id` echoes the originating `log_storage/tail` request id so hosts can correlate streams. `params.entry` is a [`LogEntry`](#121-logentry).

Stream-level errors are emitted as `log_storage/event` notifications with an `error` field in place of `entry`; an error notification terminates the tail.

#### `log_storage/schema`

Returns a [`LogStorageSchema`](#124-logstorageschema) capability declaration. SHOULD be cheap (constant or one-shot at startup).

### 7.5 Workflow runner methods (v1.1.0+)

Workflow runners execute Animus workflow YAML by orchestrating phases, evaluating decision contracts, and applying post-success actions. Defined in [`animus-workflow-runner-protocol`](animus-workflow-runner-protocol/src/lib.rs).

#### `workflow/execute`

Drive a full workflow run. Request: `WorkflowExecuteRequest` (subject envelope via `subject_dispatch` OR convenience fields, plus workflow ref, overrides, opaque routing configs). Response: `WorkflowExecuteResult` (terminal `workflow_status`, per-phase results, and the full `phase_events` vector).

Project root is bound at `initialize` time via the `init_extensions.project_binding` extension; it is NOT a per-request field.

#### `workflow/run_phase`

Execute exactly one phase (used by the daemon's per-phase scheduler). Request: `WorkflowPhaseRunRequest`. Response: `WorkflowPhaseRunResult` with `phase_status` of `"completed" | "manual_pending" | "failed"`.

### 7.6 Queue methods (v1.1.0+)

Queue plugins own a per-project priority FIFO of `SubjectDispatch` envelopes. Defined in [`animus-queue-protocol`](animus-queue-protocol/src/lib.rs).

Capacity policy stays in the kernel — the queue plugin just provides ordered access. The daemon polls the queue for items via `queue/lease` (atomic dispatch path) or `queue/list` (read-only inspection) and decides how many to lease per tick based on its own capacity logic.

Methods:

- `queue/enqueue` — append a dispatch to the queue. Idempotent on duplicate dispatches.
- `queue/list` — paginated, filterable read-only view.
- `queue/lease` — atomic dispatch path: claim up to `max` pending entries (optionally tagging them with daemon-supplied `workflow_ids`) and transition them to Assigned in one transaction. Optional `exclude_subjects: Vec<SubjectId>` (queue-protocol v0.3.0+) instructs the plugin to skip over entries whose `subject_dispatch.subject_key()` matches any id in the list; matched entries stay in Pending with no state change. Hosts use this to advance past head-of-line entries whose subjects already have an in-flight workflow without round-tripping through `queue/release_pending`. Omitting the field is identical to v0.2.0 behavior.
- `queue/stats` — fast aggregate counts.
- `queue/hold` / `queue/release` / `queue/drop` / `queue/mark_assigned` / `queue/completion` — entry-id-targeted mutations returning `QueueMutationResponse { changed, not_found }`.
- `queue/reorder` — atomic reorder by entry id list; returns `QueueReorderResponse { reordered_count }`.

Project root bound at `initialize` time.

### 7.7 Durable store methods (v1.1.0+)

Durable stores provide reservation-fenced step persistence so the daemon can recover from crashes without re-executing already-committed side effects. Defined in [`animus-durable-store-protocol`](animus-durable-store-protocol/src/lib.rs).

The contract:

1. Caller issues `durable/begin_workflow_run` to register a fresh phase execution; the plugin returns a monotonically increasing `epoch`.
2. Before each side-effecting step, the caller issues `durable/begin_step`. The plugin checks committed steps first, then live reservations:
   - `step_status: "already_committed"` / `"prior_error"` → caller short-circuits the side effect.
   - `step_status: "in_progress"` → another caller has the reservation; back off until `reservation_expires_at`.
   - `step_status: "new"` → caller proceeds; reservation is held with the supplied `reservation_ttl_secs` (or backend default).
3. After the side effect, the caller issues `durable/commit_step` with the **required** `outcome: "success" | "error"` field (independent of `output`/`error` payload nulls).
4. If the caller abandons before commit (e.g. upstream cancellation), it issues `durable/abandon_step` to release the reservation. Committed errors are NOT cleared — `prior_error` is terminal for that `idempotency_key`.
5. On daemon restart, `durable/recover_in_flight` reports workflows with outstanding reservations or pending state, keyed off the `since_epoch` cursor.
6. `durable/query_run` returns the full committed-step history for a `(run_id, phase_id)` pair.

Project root bound at `initialize` time.

### 7.8 Memory store methods (v1.1.0+)

Memory stores provide persistent semantic memory across runs, agents, and tasks. Defined in [`animus-memory-store-protocol`](animus-memory-store-protocol/src/lib.rs).

Scopes are hierarchical: project-wide (`project_id` only), per-agent (`+ agent_id`), or per-task (`+ task_id`). Plugins map this hierarchy to backend-specific structures.

Methods:

- `memory/put` — store a value under a key. Response includes `indexed_immediately: bool` so callers can tell whether read-after-write semantics apply (Zep is `false`; SQLite-backed is typically `true`).
- `memory/get` — retrieve a value by exact key. Backends that lack native key get (Zep) fall back to a search-based implementation; this is declared via the `native_key_get` capability flag.
- `memory/query` — semantic search within a scope. Clamped to `max_query_top_k` capability.
- `memory/list_scopes` — cursor-paginated list of scopes, optionally filtered by `project_id`.
- `memory/delete_scope` — delete a scope and all its entries.

Project root bound at `initialize` time.

### 7.9 Notifier methods (v1.1.0+)

Notifiers are the outbound counterpart to triggers: a trigger plugin converts an external event into a daemon event; a notifier plugin takes a daemon event record and forwards it to an external system (HTTP webhook, Slack, email, PagerDuty, ...). The daemon publishes daemon events and hands each one to every installed notifier plugin. Defined in [`animus-notifier-protocol`](animus-notifier-protocol/src/lib.rs).

Notifiers are advisory: the daemon does not block on them, and `notifier` is an optional role by default (the daemon only refuses to start without one when an operator explicitly wires that policy). Backends that need to retry MUST persist their own outbox internally.

#### `notifier/notify`

Hand one daemon event record to the plugin. The plugin SHOULD enqueue and best-effort flush. Params: [`NotifierNotifyParams`](#125-notifier-types) (`{ "event": DaemonEventRecord }`). Result: [`NotifierNotifyResult`](#125-notifier-types) — `accepted` reports whether at least one connector took ownership of the event; `delivered` is the count of synchronous deliveries (backends that only enqueue MUST set this to `0`); `lifecycle_events` carries best-effort delivery-lifecycle records the daemon can fan out into `events.jsonl`.

#### `notifier/flush`

Request that the plugin drain any pending deliveries from its internal outbox. Optional: backends without background retry MAY return `-32001` (`method_not_supported`). Params: [`NotifierFlushParams`](#125-notifier-types) (optional `project_root` scoping; omit to flush every project the plugin tracks). Result: [`NotifierFlushResult`](#125-notifier-types) (`lifecycle_events`).

When [`NotifierSchema.supports_flush`](#125-notifier-types) is `true`, the daemon SHOULD call `notifier/flush` on its tick boundary; when `false`, the daemon skips flush.

#### `notifier/schema`

Returns a [`NotifierSchema`](#125-notifier-types) capability declaration (advertised `connector_kinds` plus the `supports_flush` flag). SHOULD be cheap (constant or one-shot at startup).

## 8. Plugin protocol types

### 8.1 `PROTOCOL_VERSION`

A constant string. Current value: `"1.1.0"` (v0.5 release; bumped from `"1.0.0"` additively). Plugins MUST advertise their built-against version in `initialize`. Hosts MUST advertise theirs.

The v1.1.0 changes are all additive:

1. Four new plugin-kind constants: `workflow_runner`, `queue`, `durable_store`, `memory_store`. v1.0.0 plugins continue to work unchanged.
2. New optional field `InitializeParams.init_extensions: HashMap<String, Value>` — opaque per-extension blobs the host may pass on initialize. v0.5 uses this for `project_binding` (the project root the plugin process is bound to for its lifetime). v1.0.0 plugins simply ignore this field.
3. New optional field `InitializeResult.kind_capabilities: HashMap<String, KindCapability>` — typed per-kind capability map. Each entry declares the per-kind protocol crate version the plugin was built against plus a `extra: Value` blob typed by the per-kind protocol crate. v1.0.0 plugins leave this empty.

### 8.2 `PluginManifest`

The `--manifest` output:

```json
{
  "name": "animus-subject-linear",
  "version": "0.1.0",
  "plugin_kind": "subject_backend",
  "description": "Linear subject backend",
  "protocol_version": "1.0.0",
  "capabilities": ["subject/list", "subject/get", "subject/update", "subject/schema", "health/check"]
}
```

### 8.3 `RpcRequest` / `RpcNotification` / `RpcResponse` / `RpcError`

See §2.

### 8.4 `HostInfo`

```json
{ "name": "animus", "version": "0.4.0" }
```

### 8.5 `HostCapabilities`

```json
{ "streaming": true, "progress": false, "cancellation": true }
```

### 8.6 `PluginCapabilities`

```json
{
  "methods": ["subject/list", ...],
  "streaming": false,
  "progress": false,
  "cancellation": false,
  "subject_kinds": ["issue"],
  "mcp_tools": []
}
```

#### 8.6.1 Extra capability strings (v0.1.13)

`methods` is the canonical capability registry: hosts probe it before
issuing a method, and conformance harnesses gate scenarios on the
strings it contains. Most entries are wire methods (`agent/run`,
`subject/list`, …) the plugin promises to implement, but the field also
acts as a **feature flag namespace**: plugins MAY advertise opt-in
strings — prefixed `$harness/`, `$host/`, or `$vendor/` — that describe
host-side capabilities the plugin opts into rather than methods it
serves. Examples:

- `$harness/cancellation-loop-v2` — provider opts in to the testkit's
  concurrent-cancel scenario; the harness asserts the run reply
  surfaces error `-32002` after a mid-flight `agent/cancel`.
- `$harness/oai-style` — provider opts in to the stateless OpenAI
  tool-call scenarios; the harness skips `agent/toolResult` assertions
  these providers cannot honor.

Unknown extras are ignored by hosts that don't recognize them, so this
namespace is forward-compatible. The runtime crate exposes
`*_main_with_capabilities` entrypoints for each backend kind
(`provider`, `subject_backend`, `trigger_backend`,
`log_storage_backend`, `transport_backend`) — pass extra strings
through and they appear in this `methods` array after the
runtime-derived defaults, deduplicated. Plugins built against the older
`*_main` entrypoints behave as if they passed an empty extras vector;
this extension point is purely additive.

### 8.7 `PluginInfo`

```json
{ "name": "animus-subject-linear", "version": "0.1.0", "plugin_kind": "subject_backend", "description": "..." }
```

### 8.8 `InitializeParams` / `InitializeResult`

See §3.2.

### 8.9 `HealthCheckResult`

See §6.1.

## 9. Subject types

### 9.1 `SubjectFilter`

```json
{
  "status": ["ready", "in-progress"],
  "kind": ["task", "issue"],
  "assignee": ["alice", "agent:default"],
  "labels_any": ["backend"],
  "labels_all": ["P1"],
  "updated_since": "2026-05-10T00:00:00Z",
  "cursor": null,
  "limit": 50,
  "native_status": "In Review",
  "dispatch_label": "code-review",
  "has_attachment_kind": "document"
}
```

All fields optional. Combined with AND semantics. `cursor` is opaque to the host.

The trailing three fields were added in v0.1.1 (see §9.7). v0.1.0 hosts/backends omit them; they MUST be tolerated when absent on both sides.

### 9.2 `SubjectList`

```json
{
  "subjects": [Subject, ...],
  "next_cursor": "opaque-string-or-null",
  "fetched_at": "2026-05-13T14:00:00Z"
}
```

### 9.3 `Subject`

```json
{
  "id": "linear:ENG-123",
  "kind": "issue",
  "title": "Implement subject backend protocol",
  "description": "...",
  "status": "ready",
  "priority": 3,
  "assignee": "agent:default",
  "labels": ["backend", "P1"],
  "parent": "linear:ENG-100",
  "children": [],
  "url": "https://linear.app/...",
  "created_at": "2026-05-01T12:00:00Z",
  "updated_at": "2026-05-13T13:55:00Z",
  "custom": { "story_points": 5 },
  "native_status": "In Review",
  "status_metadata": { "state_id": "abc-123", "color": "#FFAA00", "type": "started" },
  "attachments": [
    {
      "id": "doc-7",
      "kind": "document",
      "uri": "linear://issue/ENG-123/doc/spec",
      "title": "Spec",
      "mime_type": "text/markdown",
      "metadata": { "revision": 3 }
    }
  ]
}
```

`status` is one of: `"ready"`, `"in-progress"`, `"blocked"`, `"done"`, `"cancelled"`. Native (per-backend) state names map into these via workflow YAML's `status_map`; the mapping lives in configuration, not in the protocol.

`id` MUST be prefixed with the backend name. The host treats the value as opaque.

The `native_status`, `status_metadata`, and `attachments` fields are v0.1.1 additions (see §9.7). v0.1.0 emitters omit them and v0.1.0 consumers ignore them. v0.1.1 emitters MUST omit them when at their default value (`null` / empty) so the wire output of a default-shape `Subject` stays byte-identical to v0.1.0.

### 9.4 `SubjectPatch`

```json
{
  "status": "in-progress",
  "assignee": null,
  "labels_add": ["wip"],
  "labels_remove": ["ready"],
  "comment": "Workflow run wf-7b8a started",
  "custom": { "pr_url": "https://github.com/.../pull/42" }
}
```

Omitted fields are not modified. `"assignee": null` explicitly clears; omitting the key leaves the assignee untouched. Labels use add/remove sets to avoid lost-write races on the labels list.

### 9.5 `SubjectSchema`

```json
{
  "kinds": ["issue"],
  "status_values": ["ready", "in-progress", "blocked", "done", "cancelled"],
  "supports_watch": false,
  "supports_create": false,
  "supports_pagination": true,
  "native_status_values": ["Backlog", "Todo", "In Review", "Shipped", "Cancelled"],
  "status_dispatch_hints": [
    { "native_status": "In Review",  "maps_to": "in-progress", "dispatch_label": "code-review",   "description": "Awaiting peer review" },
    { "native_status": "Shipped",    "maps_to": "done",        "dispatch_label": "post-ship-qa",  "description": "Deployed to production" }
  ],
  "custom_fields": [
    { "key": "story_points", "type": "number" }
  ]
}
```

`status_dispatch_hints` is a v0.1.1 addition; v0.1.0 hosts ignore the field. Each entry declares how a backend-native status maps into the normalized [`SubjectStatus`] bucket plus an optional `dispatch_label` that workflow YAML can gate phases on. See §9.7.

### 9.6 `SubjectChangedEvent` (notification payload)

```json
{
  "id": "linear:ENG-123",
  "change_kind": "status-changed",
  "subject": { ... },
  "previous_native_status": "Todo",
  "previous_dispatch_label": "triage"
}
```

`change_kind` is one of: `"created"`, `"updated"`, `"status-changed"`, `"deleted"`, `"dispatch-label-changed"`, `"attachment-added"`, `"attachment-removed"`.

The trailing three `change_kind` values and the `previous_native_status` / `previous_dispatch_label` fields are v0.1.1 additions. v0.1.0 consumers will see them as unknown enum variants and SHOULD treat them as `"updated"`. v0.1.1 emitters MUST omit `previous_*` fields when they are `null`.

### 9.7 Flexible status, dispatch labels, and attachments (v0.1.1)

Three orthogonal mechanisms expand the Subject schema beyond the fixed five-variant [`SubjectStatus`] without breaking v0.1.0 consumers:

**1. `Subject.native_status` + `Subject.status_metadata`** — the backend's raw status string and arbitrary backend status payload. `native_status` preserves Linear's `"In Review"` / `"Spec"` / `"Shipped"`, Jira's `"Done — won't ship"`, GitHub Project's `"Cycle 12 / blocked on infra"`, and any other vocabulary a backend exposes. `status_metadata` carries free-form JSON (state id, color, type, position) — workflows may read it via templating (`{{subject.status_metadata.color}}`).

**2. `SubjectSchema.status_dispatch_hints` + `dispatch_label`** — each native status declares the normalized bucket it maps into and an optional workflow `dispatch_label`. Workflow YAML can then say:

```yaml
phases:
  - id: code-review
    triggered_by_dispatch_label: code-review
```

…and any backend (Linear, Jira, GitHub) that advertises `dispatch_label = "code-review"` fires the phase. The label is the contract; the backend's native status is an implementation detail.

**3. `Subject.attachments` + `SubjectAttachment`** — first-class document/url/file/comment-thread attachments. `kind` is opaque to the host; conventional values are `"document"`, `"url"`, `"file"`, `"comment-thread"`. Workflow YAML can gate on attachment presence (`requires_document_attachment: true`) to drive document-aware phases. `SubjectChangedEvent` emits new change kinds (`"attachment-added"`, `"attachment-removed"`) so the daemon can react without polling.

`SubjectFilter` accepts three matching v0.1.1 fields:

- `native_status` — exact-match against `Subject.native_status`.
- `dispatch_label` — exact-match against the backend's resolved dispatch label.
- `has_attachment_kind` — at least one attachment with this `kind` must be present.

All v0.1.1 fields are Optional or default-empty. Emitters MUST omit them at their defaults so wire output remains bit-identical to v0.1.0 for subjects that do not use the new mechanisms.

## 10. Provider types

### 10.1 `AgentRunRequest`

```json
{
  "session_id": null,
  "prompt": "Refactor auth module...",
  "model": "claude-sonnet-4-6",
  "system_prompt": null,
  "cwd": "/path/to/repo",
  "project_root": "/path/to/repo",
  "permission_mode": "acceptEdits",
  "timeout_secs": 600,
  "env": { "FOO": "bar" },
  "mcp_servers": null,
  "tools": null,
  "response_schema": null,
  "runtime_contract": null
}
```

### 10.2 `AgentRunResponse`

```json
{
  "session_id": "abc123",
  "exit_code": 0,
  "output": "Done. Modified 4 files.",
  "metadata": [],
  "tool_calls": [...],
  "tool_results": [...],
  "thinking": ["..."],
  "errors": [],
  "duration_ms": 12500,
  "backend": "claude-code:1.0.0",
  "tokens_used": { "input": 4500, "output": 920, "cached": 0, "cache_writes": 0 },
  "decision_verdict": null
}
```

### 10.3 Streaming notifications

During an `agent/run` or `agent/resume`, the plugin emits any of:

| Method | Payload (`params`) |
|---|---|
| `agent/output` | `{ "session_id": "...", "text": "...", "final": false }` |
| `agent/thinking` | `{ "session_id": "...", "text": "..." }` |
| `agent/toolCall` | `{ "session_id": "...", "name": "...", "arguments": ..., "server": "..." }` |
| `agent/toolResult` | `{ "session_id": "...", "name": "...", "output": "...", "success": true }` |
| `agent/error` | `{ "session_id": "...", "message": "...", "recoverable": false }` |

After streaming, the plugin emits the final `AgentRunResponse` as the response to the original request id.

## 11. Trigger types

### 11.1 `TriggerEvent`

```json
{
  "event_id": "slack:T123/C456/1715701234.000100",
  "trigger_id": "configured-trigger-id",
  "payload": {
    "user": "U1",
    "text": "@animus please review",
    "kind": "slack_mention"
  },
  "subject_id": "linear:ENG-123",
  "subject_kind": "issue",
  "action_hint": "run_workflow"
}
```

This is the flat object emitted on the wire as the `params` of every [`trigger/event`](#triggerevent-notification) notification — there is no `{ id, event }` wrapper. Field-by-field, mirroring `crates/animus-plugin-protocol/src/lib.rs::TriggerEvent` in the `animus-cli` repo:

- `event_id` (string, **required**): unique event id assigned by the plugin. Used by the host to send back [`trigger/ack`](#triggerack). Plugins SHOULD make this stable across restarts when possible so duplicate deliveries can be deduplicated.
- `trigger_id` (string, optional): logical trigger id this event belongs to. Matches the `id` of a `WorkflowTrigger` in the project's workflow YAML. Omitted when the plugin emits free-floating events not bound to a specific configured trigger.
- `subject_id` (string, optional): subject this event refers to (e.g. `"linear:ENG-123"`). When set, the host MAY resolve the subject via its configured subject backend and kick the subject's assigned workflow. MUST be backend-prefixed when present (see §9.3).
- `subject_kind` (string, optional): subject kind for `subject_id` (e.g. `"issue"`, `"task"`). Helps the host route to the correct subject backend without re-parsing the id prefix.
- `action_hint` (string, optional): hint for what the host should do. Wire form is a snake_case string; known values are `"create_task"` and `"run_workflow"`. Unknown values round-trip verbatim so older hosts can still forward events from newer plugins. Plugins MAY omit this and let the host fall back to the trigger config's `workflow_ref`.
- `payload` (object, **required**, defaults to `{}` when absent): trigger-specific event body. The host treats it as opaque and exposes it to workflows via templating (e.g. `{{trigger.payload.user}}`). Plugins SHOULD include any "kind" / event-type signal inside `payload` (e.g. `payload.kind`) rather than as a top-level `TriggerEvent` field; the host does not inspect a top-level `kind`.

Omitted optional fields MUST NOT appear in the wire form (serializers `skip_serializing_if = "Option::is_none"`).

### 11.2 `TriggerSchema`

```json
{
  "kinds": ["slack_mention", "slack_channel_message"],
  "supports_resume": true,
  "supports_dedup": true,
  "supports_ack": true
}
```

- `kinds`: every `kind` value the backend may emit (recorded inside `TriggerEvent.payload`, not as a top-level `TriggerEvent` field — see §11.1). Hosts MAY surface this to workflow authors for autocompletion.
- `supports_resume`: backend honors a delivery cursor across `trigger/watch` reconnects. Backends without resume re-emit only events occurring after `trigger/watch`.
- `supports_dedup`: backend re-emits the same [`TriggerEvent.event_id`](#111-triggerevent) for a re-seen event. Hosts may use this to skip their own dedup table.
- `supports_ack`: backend implements `trigger/ack`. Hosts skip the call when `false`.

## 12. Log storage types

### 12.1 `LogEntry`

```json
{
  "id": "evt-001",
  "ts": "2026-05-17T18:20:34Z",
  "level": "info",
  "source": "plugin",
  "source_name": "animus-subject-linear",
  "target": "plugin.animus-subject-linear.client",
  "message": "fetched 14 issues",
  "fields": { "count": 14, "tenant": "acme" }
}
```

- `id` MUST be stable for a given log record so backends can deduplicate on retry. Backends that cannot produce a natural id SHOULD synthesize one (e.g. `(source, ts, hash(message))`).
- `ts` MUST be the original emission timestamp (RFC 3339, UTC). Backends MUST preserve it rather than overwriting with arrival time.
- `level` is one of `"trace"`, `"debug"`, `"info"`, `"warn"`, `"error"` (lowercase). Follows the [`tracing`](https://docs.rs/tracing) convention.
- `source` is one of `"daemon"`, `"plugin"`, `"cli"`, `"workflow"` (snake_case). The closed set keeps queries cheap.
- `source_name` disambiguates emitters within a `source` (e.g. plugin name, workflow id, CLI command). Omitted when null.
- `target` is a hierarchical `tracing`-style module path, e.g. `"daemon.scheduler.dispatch"`. Backends MAY index this for faster glob matches.
- `message` is the human-readable log line. Multi-line content is allowed.
- `fields` is opaque structured context (request_id, subject_id, workflow_id, ...). Omitted when null.

### 12.2 `LogQuery`

```json
{
  "min_level": "warn",
  "source": "daemon",
  "source_name": "scheduler",
  "target_glob": "daemon.scheduler.*",
  "since": "2026-05-17T00:00:00Z",
  "until": "2026-05-18T00:00:00Z",
  "limit": 100,
  "cursor": "opaque-page-2",
  "follow": false
}
```

All fields except `follow` are optional and combined with AND semantics. `follow` defaults to `false` and is only meaningful for `log_storage/tail`. `cursor` is opaque to the host; pass back a [`LogQueryResult::next_cursor`](#123-logqueryresult) verbatim to fetch the next page.

Backends honor the subset of filters they advertise via [`LogStorageSchema.supports_filtering`](#124-logstorageschema). Unsupported filters are silently ignored on the wire — the daemon evaluates them in-process before surfacing results.

### 12.3 `LogQueryResult`

```json
{
  "entries": [LogEntry, ...],
  "next_cursor": "opaque-string-or-null"
}
```

`entries` is in chronological order (oldest first) unless documented otherwise by the backend. `next_cursor` is `null` (or absent) when the query is exhausted.

### 12.4 `LogStorageSchema`

```json
{
  "supports_query": true,
  "supports_tail": true,
  "supports_dedup": false,
  "supports_filtering": {
    "by_level": true,
    "by_source": true,
    "by_target": true,
    "by_time_range": true,
    "by_glob": false
  },
  "max_query_window": 2592000000,
  "retention_hint": 604800000
}
```

- `supports_query`: backend implements `log_storage/query`. Hosts MUST NOT call it when `false`.
- `supports_tail`: backend implements `log_storage/tail`. Hosts MUST NOT call it when `false`.
- `supports_dedup`: backend deduplicates by [`LogEntry::id`]. Hosts MAY skip their own dedup table when `true`.
- `supports_filtering`: which [`LogQuery`](#122-logquery) filters the backend evaluates server-side. Fields default to `false`; the host applies any unsupported filter in-process.
- `max_query_window`: maximum span (in **milliseconds**) the backend will honor for a single query (e.g. Loki caps at 30 days ≈ `2592000000`). Omitted when the backend declines to advertise a limit.
- `retention_hint`: typical retention period (in **milliseconds**) after which entries are evicted. Surfaced to operators so they understand how far back queries can reach.

Durations are encoded as signed millisecond integers because JSON has no native duration type and `chrono::Duration` does not serialize directly.

### 12.5 Notifier types

Wire shapes for the [notifier methods](#79-notifier-methods-v110) (`notifier/notify`, `notifier/flush`, `notifier/schema`). Defined in [`animus-notifier-protocol`](animus-notifier-protocol/src/lib.rs).

`DaemonEventRecord` — one daemon event record forwarded to notifiers. Mirrors the daemon's native event record so it can be handed over the wire without translation:

```json
{
  "schema": "animus.daemon-event.v1",
  "id": "evt-1",
  "seq": 7,
  "timestamp": "2026-05-31T00:00:00Z",
  "event_type": "workflow_completed",
  "project_root": "/repo",
  "data": { "workflow_id": "wf-1" }
}
```

- `schema` is the schema URI for the event payload. `id` is a globally-unique event id.
- `seq` is the monotonic sequence number the daemon assigned for the run (defaults to `0`).
- `timestamp` is the RFC 3339 timestamp the daemon stamped at emission. `event_type` is the event kind (e.g. `"workflow_completed"`, `"task-state-change"`).
- `project_root` is the optional project root the event is about (omitted when null). `data` is the free-form event payload.

`NotifierNotifyParams` is `{ "event": DaemonEventRecord }`. `NotifierNotifyResult`:

```json
{
  "accepted": true,
  "delivered": 0,
  "lifecycle_events": []
}
```

- `accepted` (required) is `true` iff at least one connector accepted the event for delivery.
- `delivered` is the number of deliveries completed synchronously (defaults to `0`; enqueue-only backends MUST report `0`).
- `lifecycle_events` is a list of `NotifierLifecycleEvent` records (defaults to empty).

`NotifierLifecycleEvent` is the record the daemon mirrors into operator-visible logs:

```json
{
  "event_type": "notification-delivery-enqueued",
  "project_root": "/repo",
  "data": {}
}
```

- `event_type` (required) is the lifecycle label (e.g. `"notification-delivery-enqueued"`, sent / failed / dead-lettered).
- `project_root` is the project root the underlying event belonged to, if known (omitted when null).
- `data` (required) is a free-form payload mirrored verbatim into `DaemonEventRecord.data`.

`NotifierFlushParams` is `{ "project_root": "/repo" | null }` (omit to flush every project). `NotifierFlushResult` is `{ "lifecycle_events": [] }` — same `NotifierLifecycleEvent` shape as the notify result.

`NotifierSchema`:

```json
{
  "connector_kinds": ["webhook", "slack_webhook"],
  "supports_flush": true
}
```

- `connector_kinds` (required) lists the free-form connector kinds the plugin can route to. Workflows may use this to surface which transports are available.
- `supports_flush` (required) declares whether the plugin maintains its own outbox + background retry loop. When `true`, the daemon SHOULD call `notifier/flush` on its tick boundary; when `false`, it skips flush.

## 13. Transport types

A **transport backend** is the sixth plugin kind alongside subject, provider, trigger, log storage, and the in-process control surface. A transport plugin owns an *external* protocol surface — HTTP, GraphQL, gRPC, WebSocket, MQTT — and translates inbound requests on that surface into [control protocol](#14-control-protocol) RPCs against the daemon's Unix socket. They are the controller-as-plugin endgame: the daemon stays a small JSON-RPC core, and every external surface ships as a separate, independently versioned process.

Transport backends have a different lifecycle from the other plugin kinds. The daemon issues `transport/start` with a [`TransportConfig`](#131-transportconfig) once after `initialize`, the plugin returns a [`TransportInfo`](#132-transportinfo) when the listener is bound, and the daemon issues `transport/shutdown` before the plugin process terminates. The wire format is defined by the [`animus-transport-protocol`](animus-transport-protocol/) crate alongside the rest of this workspace.

### 13.1 `TransportConfig`

Sent on `transport/start` to configure the listener.

```json
{
  "control_socket_path": "/Users/op/.animus/scope/control.sock",
  "project_root": "/Users/op/code/animus",
  "bind_addr": "127.0.0.1:8080",
  "config": {
    "cors": {"allowed_origins": ["*"]},
    "auth_token": "redacted"
  }
}
```

- `control_socket_path` is the absolute path to the daemon's [control socket](#142-method-name-conventions). The transport connects to this socket to issue control RPCs on behalf of inbound requests. POSIX hosts only; Windows named-pipe naming is reserved.
- `project_root` is the absolute path to the project the daemon is serving. Transports surface this in metadata responses (e.g. HTTP `/healthz`) and use it to scope filesystem access if they expose static-file routes.
- `bind_addr` is the listener address. Format is transport-specific; HTTP/GraphQL/gRPC use `host:port`. Omit to let the plugin pick its [`TransportSchema::default_port`](#133-transportschema) on `localhost`.
- `config` is a free-form transport-specific JSON blob the daemon does not parse. Omitted on the wire when null.

### 13.2 `TransportInfo`

Returned by `transport/start` once the listener is bound and ready.

```json
{
  "bound_addr": "127.0.0.1:8080",
  "started_at": "2026-05-23T12:00:00Z"
}
```

- `bound_addr` is the *actual* address the listener accepted on (after any `0` port resolution). Daemons MAY surface this verbatim in `daemon/status` output.
- `started_at` is the RFC 3339 (UTC) timestamp at which the listener became ready. Lets dashboards display uptime without the daemon tracking it separately.

### 13.3 `TransportSchema`

Returned by `transport/schema`.

```json
{
  "kinds": ["http", "rest"],
  "supports_streaming": true,
  "supports_websocket": false,
  "default_port": 8080
}
```

- `kinds`: every protocol kind the transport exposes. Convention is a single lowercase token per kind, e.g. `["http", "rest"]`, `["graphql"]`, `["grpc"]`, `["mqtt"]`, `["websocket"]`. Multi-protocol transports list every kind they expose.
- `supports_streaming`: server-streaming responses are supported (HTTP/2 push, gRPC server streaming, GraphQL subscriptions over SSE, ...). Hosts use this to decide whether to route streaming control methods (`daemon/events`, `daemon/logs`, `subject/watch`) through this transport.
- `supports_websocket`: the transport accepts WebSocket upgrades. Distinct from `supports_streaming` because HTTP transports may stream without supporting WebSocket and vice versa.
- `default_port`: the port the transport binds to when [`TransportConfig.bind_addr`](#131-transportconfig) is omitted. Hosts surface this to operators so they know where to point clients. Omitted when the transport refuses to start without an explicit `bind_addr`.

### 13.4 Methods

| Method | Direction | Description |
|---|---|---|
| `transport/start` | host → plugin | Bind the listener with [`TransportConfig`](#131-transportconfig). Returns [`TransportInfo`](#132-transportinfo). |
| `transport/shutdown` | host → plugin | Graceful shutdown. Drain in-flight requests, release the bound address. Safe to call more than once. |
| `transport/schema` | host → plugin | Capability declaration; returns [`TransportSchema`](#133-transportschema). |

Errors map to the JSON-RPC error namespace in §4 with the following `error.data.category` values: `not_supported`, `invalid_request`, `address_in_use`, `permission_denied`, `unavailable`, `other`. Hosts SHOULD branch on `category`, not on `message`.

## 14. Control protocol

The protocol so far in this document covers the *outbound* surface — the way the Animus daemon talks to plugin processes (subject backends, providers, triggers, log storage, transports). The **control protocol** is the *inbound* counterpart — the way a human (CLI), an agent (MCP), or another process (WebAPI, future REST / gRPC clients) asks the daemon to do something.

The wire format for control protocol traffic is defined by the [`animus-control-protocol`](animus-control-protocol/) crate alongside the rest of this workspace.

### 14.1 Wire transport

Control traffic uses the same newline-delimited JSON-RPC 2.0 envelopes defined in §2. The daemon exposes its control surface on:

- A local Unix domain socket at `~/.animus/<repo-scope>/control.sock` (POSIX hosts). Permissions on the socket file (`0600`, owned by the running user) are the v0.1.3 authorization model — anyone with read/write access to the socket can issue any control command. Personal access tokens are reserved for v0.5.x.
- A named pipe at `\\.\pipe\animus-<repo-scope>` on Windows (reserved; not implemented in v0.1.3).

Clients that already share the daemon's address space (e.g. the in-process CLI today) MAY skip the socket and call the [`animus_control_protocol::ControlSurface`](animus-control-protocol/) trait directly. The wire shape is unchanged either way.

### 14.2 Method-name conventions

Method names follow `<group>/<verb>` with a forward slash separator. Groups are: `subject`, `plugin`, `daemon`, `workflow`, `agent`, `queue`, `project`. The full list is defined as `pub const` strings in [`animus_control_protocol::method`](animus-control-protocol/src/method.rs). Examples:

- `subject/list`, `subject/get`, `subject/create`, `subject/update`, `subject/next`, `subject/status`
- `plugin/list`, `plugin/install`, `plugin/uninstall`, `plugin/ping`, `plugin/call`, `plugin/search`, `plugin/browse`, `plugin/update`
- `daemon/status`, `daemon/health`, `daemon/start`, `daemon/stop`, `daemon/restart`, `daemon/agents`
- `workflow/list`, `workflow/get`, `workflow/run`, `workflow/execute`, `workflow/pause`, `workflow/resume`, `workflow/cancel`, `workflow/events` (v0.1.10)
- `agent/run`, `agent/status`, `agent/cancel`
- `queue/list`, `queue/enqueue`, `queue/drop`, `queue/hold`, `queue/release`, `queue/reorder`, `queue/stats`
- `project/init`, `project/setup`, `project/status`

Domain payloads (subject, log entry, ...) are imported from the existing plugin-protocol crates — `Subject`, `SubjectId`, `SubjectFilter`, `SubjectPatch`, `LogEntry`, `LogLevel` — so the control protocol and the plugin protocols share one schema per concept.

### 14.3 Streaming methods

Four control methods open a server-streaming subscription:

- `subject/watch` — emits `subject/changed` notifications (one per subject change), with payload [`SubjectChangedEvent`].
- `daemon/events` — emits `daemon/event` notifications (one per daemon run event), with payload `DaemonRunEvent`.
- `daemon/logs` — emits `daemon/log` notifications (one per log entry), with payload `LogEntry` from `animus-log-storage-protocol`. When the request sets `follow: true` the stream stays open after the historical tail; when `follow: false` the daemon delivers the historical window and then closes the stream.
- `workflow/events` (v0.1.10) — emits `workflow/event` notifications (one per workflow-scoped event such as `phase_started`, `phase_completed`, `workflow_completed`, `workflow_failed`), with payload `WorkflowEvent`. The request optionally filters by `workflow_id` and event `kinds`; both filters combine with AND semantics.

The convention is: a method ending in `/watch` is bound to a specific resource family (subjects in a backend, ...), a method ending in `/events` is the broader daemon-wide stream, and `/logs` carries log entries specifically. Each streaming method MUST be paired with a singular notification (`<group>/changed`, `<group>/event`, `<group>/log`). The request id of the originating streaming request is echoed in `params.id` of every notification on that stream so the client can multiplex multiple subscriptions over one connection.

The wire handshake for any streaming method is:

1. Client sends a normal JSON-RPC request: `{"jsonrpc":"2.0","id":<id>,"method":"<group>/<verb>","params":{...}}`.
2. Server replies with an ack result frame: `{"jsonrpc":"2.0","id":<id>,"result":{"watching":true}}`. The ack body is opaque; clients MUST treat the absence of an error as success.
3. Server then emits zero or more notification frames: `{"jsonrpc":"2.0","method":"<group>/<event>","params":{"id":<id>,"data":<payload>}}`. Notification frames carry no top-level `id`; correlation is via `params.id` echoing the originating request id.
4. The server MAY close the stream by closing the underlying connection, by ceasing to send notifications, or (since v0.1.12) by emitting a single terminal `subscription/closed` notification with `params.id` echoing the originating streaming request id and `params.reason` carrying a short operator-facing close reason. Clients written against v0.1.12+ treat that notification as authoritative end-of-stream — the `Subscription<T>::recv()` returns `None` on the next pull. Pre-v0.1.12 clients (and servers that never emit it) keep working unchanged: socket EOF remains the universal end-of-stream signal.

A client cancels a stream by closing the connection or by issuing a JSON-RPC notification with method `$/cancelRequest` and `params: { id: <request_id> }` (mirrors §6's lifecycle cancellation). Daemons MAY treat `$/cancelRequest` as a best-effort hint; closing the socket is the authoritative cancellation signal.

### 14.4 Error codes

Control surface errors map to JSON-RPC error responses using the same `error_codes` namespace defined in §4. The categorical kind is carried in `error.data.category`:

| Category            | JSON-RPC code | Meaning                                                                  |
|---------------------|---------------|--------------------------------------------------------------------------|
| `not_found`         | -32602        | Resource referenced by the request does not exist                        |
| `invalid_request`   | -32602        | Request was malformed at the domain level                                |
| `permission_denied` | -32600        | Caller lacks permission                                                  |
| `unavailable`       | -32603        | Daemon or a dependency is temporarily unavailable                        |
| `not_supported`     | -32001        | Method exists in the protocol but is not implemented by this daemon yet  |
| `conflict`          | -32600        | Operation would conflict with the current state (e.g. cancelling a done) |
| `internal`          | -32603        | Catch-all internal failure                                               |

The error body matches `animus_control_protocol::ControlError`'s serde representation: `{ "category": "<kind>", "message": "<text>" }`. Clients SHOULD branch on `category`, not on `message`.

### 14.5 Capabilities

The daemon advertises its supported control methods via a `daemon/status` response field `capabilities.methods: [String]`. Clients SHOULD probe capabilities before issuing methods they need; if a method is missing, the daemon either returns `not_supported` (mirrors §6's `method_not_supported` semantics) or rejects the call with `method_not_found` (-32601). Either is acceptable during an incremental v0.4.x rollout.

### 14.6 Auth

v0.1.3 relies on filesystem permissions on the control socket. The socket is created with `0600` and owned by the user running the daemon. Any client that can `connect(2)` to the socket may issue any control method.

Future protocol versions (reserved for v0.5.x) will introduce a personal-access-token bearer scheme negotiated during a `daemon/authenticate` handshake. The token shape, scoping, and revocation are TBD. The capability field above gives the daemon a forward-compatible way to gate `daemon/authenticate` behind a feature flag without breaking v0.1.3 clients.

### 14.7 Client subscription API (v0.1.9, extended in v0.1.10)

`animus-control-protocol` v0.1.9 ships a `Subscription<T>` type that wraps the streaming wire shape from §14.3 for the `client` feature. Transport plugins (graphql, http, future gRPC) use it instead of re-implementing the NDJSON read demultiplexer.

Concurrency model: each `ControlClient` instance owns one `UnixStream`. A background reader task continuously parses inbound frames and demultiplexes them by matching `id` (responses → pending oneshot) or by reading `params.id` (notifications → subscription mpsc). Writes serialize through an `Arc<Mutex<WriteHalf>>`, so a single `ControlClient` MAY host any mix of in-flight unary RPCs and open subscriptions over one socket.

Methods:

- `ControlClient::subject_watch(SubjectWatchRequest) -> Result<Subscription<SubjectChangedEvent>>`
- `ControlClient::daemon_events(DaemonEventsRequest) -> Result<Subscription<DaemonRunEvent>>`
- `ControlClient::daemon_logs_follow(DaemonLogsRequest) -> Result<Subscription<DaemonLogEntry>>` — forces `follow = true` on the request
- `ControlClient::workflow_events(WorkflowEventsRequest) -> Result<Subscription<WorkflowEvent>>` (v0.1.10) — workflow-scoped event stream, see §14.8

`Subscription<T>` exposes:

- `async fn recv(&mut self) -> Option<T>` — pulls the next decoded event, `None` on stream close
- `fn request_id(&self) -> u64` and `fn method(&self) -> &str` — correlation helpers

Cancellation: dropping the `Subscription` closes the local receiver, aborts the per-subscription decoder, sends a best-effort `$/cancelRequest` notification on the same socket, and removes the subscription from the demultiplexer table. Clients that need stricter shutdown semantics SHOULD also drop the owning `ControlClient`, which aborts the reader task and closes the socket.

### 14.8 `workflow/events` (v0.1.10)

`workflow/events` opens a server-streaming subscription scoped to workflow lifecycle events. Notifications use the method `workflow/event` and carry a [`WorkflowEvent`] payload. The handshake and cancellation semantics match §14.3.

Request shape (`WorkflowEventsRequest`):

```jsonc
{
  "workflow_id": "wf-42",                                  // optional — None streams every workflow
  "kinds": ["phase_started", "phase_completed",            // optional — None streams every kind
            "workflow_completed", "workflow_failed"]
}
```

Both filters are optional and combine with AND semantics: an event is delivered when its workflow id matches `workflow_id` (or `workflow_id` is `None`) AND its kind is in `kinds` (or `kinds` is `None`).

Event shape (`WorkflowEvent`):

```jsonc
{
  "workflow_id": "wf-42",
  "kind": "phase_completed",
  "payload": { "phase_id": "implement", "status": "ok" },  // kind-specific JSON; opaque to the protocol
  "occurred_at": "2026-05-23T10:42:00Z"
}
```

The `kind` discriminator is opaque to the protocol; daemons MAY emit additional kinds in any minor release, and clients SHOULD ignore unknown kinds rather than error.

Daemon-side status: as of v0.1.10 the protocol crate ships the client-side wire (constants, types, `ControlClient::workflow_events`). The matching daemon-side emitter lands separately in `animus-cli`; until that ships, `workflow_events` subscribers will hang on `recv` (the daemon never emits `workflow/event` notifications) or receive a `method_not_found` error on subscribe. Clients that need to ship before the daemon catches up SHOULD fall back to `daemon_events` + kind filtering.

## 15. Versioning

The protocol uses semantic versioning. The current version is `1.0.0`.

- **MAJOR** bumps are breaking. A plugin built against `1.x` is **not** compatible with a host advertising `2.x` and the host MUST refuse to load it (or treat it as unhealthy).
- **MINOR** bumps add methods or fields. Plugins built against an older minor version remain compatible; they simply won't advertise the new capabilities.
- **PATCH** bumps are documentation/clarification only.

Compatibility check on `initialize`:

```
host_major == plugin_major  →  compatible
host_major != plugin_major  →  incompatible (host refuses)
```

Plugins SHOULD tolerate hosts on the same major version even when the host's minor is older — i.e. don't require methods you've added in a newer minor.

## 16. Conformance

A plugin is conformant if:

1. It handles `--manifest` as defined in §3.1.
2. It implements the lifecycle (`initialize` → `initialized` → `shutdown` → `exit`) as defined in §3.
3. It implements `health/check` and `$/ping` as defined in §6.
4. It implements every method it advertises in `capabilities.methods`.
5. It returns `-32001` (`method_not_supported`) for optional methods it has chosen not to implement.
6. Its frames are valid JSON-RPC 2.0 per §2.
7. Its `Subject`/`SubjectPatch`/`AgentRunRequest`/`TriggerEvent`/`TriggerSchema`/`LogEntry`/`LogQuery`/`LogStorageSchema` payloads use the field names defined in §9, §10, §11, and §12 exactly (case-sensitive, snake_case unless otherwise specified).

A host is conformant if it speaks the same lifecycle, honors the capabilities a plugin advertises, and never calls a method a plugin did not advertise.

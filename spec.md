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
| `subject_backend` | `subject/list`, `subject/get`, `subject/update`, `subject/watch` (optional), `subject/schema` |
| `trigger_backend` | `trigger/watch`, `trigger/event` (notification), `trigger/ack` (optional), `trigger/schema` |
| `log_storage_backend` | `log_storage/store`, `log_storage/query` (optional), `log_storage/tail` (optional), `log_storage/event` (notification), `log_storage/schema` |
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

Opens the event stream. The response is sent immediately to acknowledge; subsequent events arrive as `trigger/event` notifications carrying the original request id in `params.id`. Hosts SHOULD only issue one in-flight `trigger/watch` per plugin connection.

Params: none. Result: `{ "watching": true }`.

If the backend cannot open its upstream (e.g. missing credentials), it MUST return a JSON-RPC error response rather than emit a stream that fails on the first poll.

#### `trigger/event` (notification)

```json
{ "jsonrpc": "2.0", "method": "trigger/event",
  "params": {
    "id": 17,
    "event": {
      "id": "slack:T123/C456/1715701234.000100",
      "occurred_at": "2026-05-14T18:20:34Z",
      "kind": "slack_mention",
      "payload": { "user": "U1", "text": "@animus please review" },
      "subject_id": "linear:ENG-123",
      "action_hint": "run-workflow:review"
    }
  }
}
```

`params.id` echoes the originating `trigger/watch` request id so hosts can correlate streams. `params.event` is a [`TriggerEvent`](#111-triggerevent).

Stream-level errors are emitted as `trigger/event` notifications with an `error` field in place of `event`; an error notification terminates the stream.

#### `trigger/ack`

Acknowledges an event id so the backend does not redeliver it on resume. Backends that don't track delivery state MAY accept any id (no-op) or MAY respond with `-32001` (`method_not_supported`); hosts must tolerate either.

Params: `{ "event_id": "..." }`. Result: `{ "event_id": "...", "acked": true }`.

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

## 8. Plugin protocol types

### 8.1 `PROTOCOL_VERSION`

A constant string. Current value: `"1.0.0"`. Plugins MUST advertise their built-against version in `initialize`. Hosts MUST advertise theirs.

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
  "id": "slack:T123/C456/1715701234.000100",
  "occurred_at": "2026-05-14T18:20:34Z",
  "kind": "slack_mention",
  "payload": { "user": "U1", "text": "@animus please review" },
  "subject_id": "linear:ENG-123",
  "action_hint": "run-workflow:review"
}
```

- `id` MUST be stable for a given upstream event so hosts can deduplicate on restart. Backends that cannot produce a natural id SHOULD synthesize one (e.g. `kind + occurred_at + payload-hash`).
- `kind` is opaque to the host; workflow YAML matches on it. Convention is `"<backend>_<event>"`.
- `payload` is trigger-specific; the host treats it as opaque and exposes it to workflows via templating (e.g. `{{trigger.payload.user}}`).
- `subject_id` and `action_hint` are optional. `subject_id` MUST be backend-prefixed when present (see §9.3).

### 11.2 `TriggerSchema`

```json
{
  "kinds": ["slack_mention", "slack_channel_message"],
  "supports_resume": true,
  "supports_dedup": true,
  "supports_ack": true
}
```

- `kinds`: every `kind` value the backend may emit. Hosts MAY surface this to workflow authors for autocompletion.
- `supports_resume`: backend honors a delivery cursor across `trigger/watch` reconnects. Backends without resume re-emit only events occurring after `trigger/watch`.
- `supports_dedup`: backend re-emits the same [`TriggerEvent::id`] for a re-seen event. Hosts may use this to skip their own dedup table.
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

## 13. Control protocol

The protocol so far in this document covers the *outbound* surface — the way the Animus daemon talks to plugin processes (subject backends, providers, triggers, log storage). The **control protocol** is the *inbound* counterpart — the way a human (CLI), an agent (MCP), or another process (WebAPI, future REST / gRPC clients) asks the daemon to do something.

The wire format for control protocol traffic is defined by the [`animus-control-protocol`](animus-control-protocol/) crate alongside the rest of this workspace.

### 13.1 Wire transport

Control traffic uses the same newline-delimited JSON-RPC 2.0 envelopes defined in §2. The daemon exposes its control surface on:

- A local Unix domain socket at `~/.animus/<repo-scope>/control.sock` (POSIX hosts). Permissions on the socket file (`0600`, owned by the running user) are the v0.1.3 authorization model — anyone with read/write access to the socket can issue any control command. Personal access tokens are reserved for v0.5.x.
- A named pipe at `\\.\pipe\animus-<repo-scope>` on Windows (reserved; not implemented in v0.1.3).

Clients that already share the daemon's address space (e.g. the in-process CLI today) MAY skip the socket and call the [`animus_control_protocol::ControlSurface`](animus-control-protocol/) trait directly. The wire shape is unchanged either way.

### 13.2 Method-name conventions

Method names follow `<group>/<verb>` with a forward slash separator. Groups are: `subject`, `plugin`, `daemon`, `workflow`, `agent`, `queue`, `project`. The full list is defined as `pub const` strings in [`animus_control_protocol::method`](animus-control-protocol/src/method.rs). Examples:

- `subject/list`, `subject/get`, `subject/create`, `subject/update`, `subject/next`, `subject/status`
- `plugin/list`, `plugin/install`, `plugin/uninstall`, `plugin/ping`, `plugin/call`, `plugin/search`, `plugin/browse`, `plugin/update`
- `daemon/status`, `daemon/health`, `daemon/start`, `daemon/stop`, `daemon/restart`, `daemon/agents`
- `workflow/list`, `workflow/get`, `workflow/run`, `workflow/execute`, `workflow/pause`, `workflow/resume`, `workflow/cancel`
- `agent/run`, `agent/status`, `agent/cancel`
- `queue/list`, `queue/enqueue`, `queue/drop`, `queue/hold`, `queue/release`, `queue/reorder`, `queue/stats`
- `project/init`, `project/setup`, `project/status`

Domain payloads (subject, log entry, ...) are imported from the existing plugin-protocol crates — `Subject`, `SubjectId`, `SubjectFilter`, `SubjectPatch`, `LogEntry`, `LogLevel` — so the control protocol and the plugin protocols share one schema per concept.

### 13.3 Streaming methods

Three control methods open a server-streaming subscription:

- `subject/watch` — emits `subject/changed` notifications (one per subject change), with payload [`SubjectChangedEvent`].
- `daemon/events` — emits `daemon/event` notifications (one per daemon run event), with payload `DaemonRunEvent`.
- `daemon/logs` — emits `daemon/log` notifications (one per log entry), with payload `LogEntry` from `animus-log-storage-protocol`.

The convention is: a method ending in `/watch` is bound to a specific resource family (subjects in a backend, ...), a method ending in `/events` is the broader daemon-wide stream, and `/logs` carries log entries specifically. Each streaming method MUST be paired with a singular notification (`<group>/changed`, `<group>/event`, `<group>/log`). The request id of the originating streaming request is echoed in `params.id` of every notification on that stream so the client can multiplex multiple subscriptions over one connection.

A client cancels a stream by closing the connection or by issuing a JSON-RPC notification with method `$/cancelRequest` and `params: { id: <request_id> }` (mirrors §6's lifecycle cancellation).

### 13.4 Error codes

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

### 13.5 Capabilities

The daemon advertises its supported control methods via a `daemon/status` response field `capabilities.methods: [String]`. Clients SHOULD probe capabilities before issuing methods they need; if a method is missing, the daemon either returns `not_supported` (mirrors §6's `method_not_supported` semantics) or rejects the call with `method_not_found` (-32601). Either is acceptable during an incremental v0.4.x rollout.

### 13.6 Auth

v0.1.3 relies on filesystem permissions on the control socket. The socket is created with `0600` and owned by the user running the daemon. Any client that can `connect(2)` to the socket may issue any control method.

Future protocol versions (reserved for v0.5.x) will introduce a personal-access-token bearer scheme negotiated during a `daemon/authenticate` handshake. The token shape, scoping, and revocation are TBD. The capability field above gives the daemon a forward-compatible way to gate `daemon/authenticate` behind a feature flag without breaking v0.1.3 clients.

## 14. Versioning

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

## 15. Conformance

A plugin is conformant if:

1. It handles `--manifest` as defined in §3.1.
2. It implements the lifecycle (`initialize` → `initialized` → `shutdown` → `exit`) as defined in §3.
3. It implements `health/check` and `$/ping` as defined in §6.
4. It implements every method it advertises in `capabilities.methods`.
5. It returns `-32001` (`method_not_supported`) for optional methods it has chosen not to implement.
6. Its frames are valid JSON-RPC 2.0 per §2.
7. Its `Subject`/`SubjectPatch`/`AgentRunRequest`/`TriggerEvent`/`TriggerSchema`/`LogEntry`/`LogQuery`/`LogStorageSchema` payloads use the field names defined in §9, §10, §11, and §12 exactly (case-sensitive, snake_case unless otherwise specified).

A host is conformant if it speaks the same lifecycle, honors the capabilities a plugin advertises, and never calls a method a plugin did not advertise.

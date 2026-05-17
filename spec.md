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
| `trigger_backend` | reserved for v0.4.x; not stabilized |
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
  "limit": 50
}
```

All fields optional. Combined with AND semantics. `cursor` is opaque to the host.

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
  "custom": { "story_points": 5 }
}
```

`status` is one of: `"ready"`, `"in-progress"`, `"blocked"`, `"done"`, `"cancelled"`. Native (per-backend) state names map into these via workflow YAML's `status_map`; the mapping lives in configuration, not in the protocol.

`id` MUST be prefixed with the backend name. The host treats the value as opaque.

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
  "native_status_values": ["Backlog", "Todo", "In Progress", "Done", "Cancelled"],
  "custom_fields": [
    { "key": "story_points", "type": "number" }
  ]
}
```

### 9.6 `SubjectChangedEvent` (notification payload)

```json
{
  "id": "linear:ENG-123",
  "change_kind": "status-changed",
  "subject": { ... }
}
```

`change_kind` is one of: `"created"`, `"updated"`, `"status-changed"`, `"deleted"`.

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

## 11. Versioning

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

## 12. Conformance

A plugin is conformant if:

1. It handles `--manifest` as defined in §3.1.
2. It implements the lifecycle (`initialize` → `initialized` → `shutdown` → `exit`) as defined in §3.
3. It implements `health/check` and `$/ping` as defined in §6.
4. It implements every method it advertises in `capabilities.methods`.
5. It returns `-32001` (`method_not_supported`) for optional methods it has chosen not to implement.
6. Its frames are valid JSON-RPC 2.0 per §2.
7. Its `Subject`/`SubjectPatch`/`AgentRunRequest` payloads use the field names defined in §9 and §10 exactly (case-sensitive, snake_case unless otherwise specified).

A host is conformant if it speaks the same lifecycle, honors the capabilities a plugin advertises, and never calls a method a plugin did not advertise.

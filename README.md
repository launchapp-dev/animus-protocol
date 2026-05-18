# animus-protocol

**The plugin protocol stack for [Animus](https://github.com/launchapp-dev/animus-cli).** Build a subject backend, an LLM provider, or any custom Animus plugin in Rust — or in any language that can speak newline-delimited JSON-RPC 2.0 over stdio.

## Status

**v0.1.0 scaffold — protocol design + wire types + spec landed; runtime helpers standalone-compilable.** Animus core v0.4.0 ships these crates as workspace members (`crates/animus-{plugin,subject,provider}-protocol/` + `crates/animus-plugin-runtime/` in [`launchapp-dev/animus-cli`](https://github.com/launchapp-dev/animus-cli)), so the design + wire types have been exercised against a real codebase. The standalone repo now compiles cleanly end-to-end. Honest state before the first `cargo publish` to crates.io:

| Crate | Standalone-compilable? | Notes |
|---|---|---|
| `animus-plugin-protocol` | yes | Wire types only; no external Animus deps. |
| `animus-subject-protocol` | yes | Pure trait + schema definitions. |
| `animus-provider-protocol` | yes | Pure trait + schema definitions. |
| `animus-trigger-protocol` | yes | Pure trait + schema definitions for push-driven event sources (Slack, webhooks, file watchers, cron). |
| `animus-log-storage-protocol` | yes | Pure trait + schema definitions for log storage backends (local `events.jsonl` file, Loki, Splunk, ClickHouse). |
| `animus-plugin-runtime` | yes | Slim stdio JSON-RPC loop; exposes `subject_backend_main`, `provider_main`, `trigger_backend_main`, and `log_storage_backend_main`. Provider session helpers (event channels, child-process plumbing) will land in a separate `animus-session-backend` crate. |

The protocol [`spec.md`](./spec.md) is the source of truth for cross-language plugin authors — it can be implemented in Python, TypeScript, Go, or any language that speaks newline-delimited JSON-RPC 2.0 over stdio.

The protocol + subject + provider + runtime crates are usable today via git path/tag dependency from this repo. Plugin authors write `subject_backend_main(info, backend).await` or `provider_main(info, backend).await` from `main` and avoid hand-rolling the wire layer.

## Crates

| Crate | Purpose |
|---|---|
| [`animus-plugin-protocol`](./animus-plugin-protocol) | Wire types every plugin uses: `RpcRequest`, `RpcResponse`, `RpcNotification`, `RpcError`, error codes, `InitializeParams` / `InitializeResult`, `PluginManifest`, `HealthCheckResult`. |
| [`animus-subject-protocol`](./animus-subject-protocol) | `SubjectBackend` trait + normalized `Subject` schema for backends like Linear, Jira, GitHub Issues, Notion, Asana — anything with a system-of-record API. |
| [`animus-provider-protocol`](./animus-provider-protocol) | `ProviderBackend` trait + `AgentRunRequest`/`AgentRunResponse` shapes for LLM provider plugins (Claude, Codex, Gemini, OpenAI-compatible, on-prem). |
| [`animus-trigger-protocol`](./animus-trigger-protocol) | `TriggerBackend` trait + `TriggerEvent`/`TriggerSchema` shapes for push-driven event sources (Slack mentions, generic webhooks, file watchers, cron). |
| [`animus-log-storage-protocol`](./animus-log-storage-protocol) | `LogStorageBackend` trait + `LogEntry`/`LogQuery`/`LogQueryResult`/`LogStorageSchema` shapes for log storage backends (local `events.jsonl` file, Loki, Splunk, ClickHouse). |
| [`animus-plugin-runtime`](./animus-plugin-runtime) | Shared stdio JSON-RPC loop, handshake, `--manifest` mode, notification helpers. Plugin authors call `subject_backend_main(...)` / `provider_main(...)` / `trigger_backend_main(...)` / `log_storage_backend_main(...)` from `main` and avoid hand-rolling the wire layer. |

`animus-plugin-protocol` is the only required dependency for non-Rust plugin authors — and even then only as a reference. Any process that emits the documented JSON over stdio is a compatible Animus plugin.

## Subject backend quickstart (Rust)

Cargo.toml:

```toml
[package]
name = "animus-subject-linear"
version = "0.1.0"
edition = "2021"

[dependencies]
animus-plugin-protocol = "0.1"
animus-subject-protocol = "0.1"
animus-plugin-runtime   = "0.1"
async-trait = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

src/main.rs:

```rust
use animus_plugin_protocol::{PluginInfo, PLUGIN_KIND_SUBJECT_BACKEND};
use animus_plugin_runtime::subject_backend_main;

mod backend;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let info = PluginInfo {
        name: "animus-subject-linear".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        plugin_kind: PLUGIN_KIND_SUBJECT_BACKEND.into(),
        description: Some("Linear subject backend for Animus".into()),
    };
    subject_backend_main(info, backend::LinearBackend::new()).await
}
```

src/backend.rs (sketch):

```rust
use animus_plugin_protocol::{HealthCheckResult, HealthStatus};
use animus_subject_protocol::{
    BackendError, EventStream, StatusDispatchHint, Subject, SubjectAttachment, SubjectBackend,
    SubjectFilter, SubjectId, SubjectList, SubjectPatch, SubjectSchema, SubjectStatus,
};
use async_trait::async_trait;
use chrono::Utc;

pub struct LinearBackend { /* api client, etc. */ }

impl LinearBackend {
    pub fn new() -> Self { Self { /* ... */ } }
}

#[async_trait]
impl SubjectBackend for LinearBackend {
    async fn list(&self, _filter: SubjectFilter) -> Result<SubjectList, BackendError> {
        // Call Linear's API, map to Subject, return.
        // A richer Subject surfaces the native state vocabulary verbatim and
        // attaches any documents the issue carries so workflows can dispatch
        // on `native_status`, `dispatch_label`, or attachment presence.
        let subject = Subject {
            id: SubjectId::new("linear:ENG-123"),
            kind: "issue".into(),
            title: "Implement subject backend protocol".into(),
            description: None,
            status: SubjectStatus::InProgress,
            priority: Some(3),
            assignee: Some("agent:default".into()),
            labels: vec!["backend".into()],
            parent: None,
            children: vec![],
            url: Some("https://linear.app/...".into()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            custom: Default::default(),
            native_status: Some("In Review".into()),
            status_metadata: serde_json::json!({ "state_id": "abc", "color": "#FFAA00" }),
            attachments: vec![SubjectAttachment {
                id: "doc-1".into(),
                kind: "document".into(),
                uri: "linear://issue/ENG-123/doc/spec".into(),
                title: Some("Spec".into()),
                mime_type: Some("text/markdown".into()),
                metadata: serde_json::Value::Null,
            }],
        };
        Ok(SubjectList { subjects: vec![subject], next_cursor: None, fetched_at: Utc::now() })
    }

    async fn get(&self, id: &SubjectId) -> Result<Subject, BackendError> {
        Err(BackendError::NotFound(id.to_string()))
    }

    async fn update(&self, _id: &SubjectId, _patch: SubjectPatch) -> Result<Subject, BackendError> {
        // Translate patch into a Linear mutation; return the refreshed Subject.
        unimplemented!()
    }

    async fn watch(&self) -> Option<EventStream> { None }

    fn schema(&self) -> SubjectSchema {
        SubjectSchema {
            kinds: vec!["issue".into()],
            status_values: vec![
                SubjectStatus::Ready,
                SubjectStatus::InProgress,
                SubjectStatus::Done,
            ],
            supports_watch: false,
            supports_create: false,
            supports_pagination: true,
            native_status_values: vec![
                "Backlog".into(),
                "Todo".into(),
                "In Review".into(),
                "Shipped".into(),
            ],
            status_dispatch_hints: vec![
                StatusDispatchHint {
                    native_status: "In Review".into(),
                    maps_to: SubjectStatus::InProgress,
                    dispatch_label: Some("code-review".into()),
                    description: Some("Awaiting peer review".into()),
                },
                StatusDispatchHint {
                    native_status: "Shipped".into(),
                    maps_to: SubjectStatus::Done,
                    dispatch_label: Some("post-ship-qa".into()),
                    description: None,
                },
            ],
            custom_fields: vec![],
        }
    }

    async fn health(&self) -> Result<HealthCheckResult, BackendError> {
        Ok(HealthCheckResult {
            status: HealthStatus::Healthy,
            uptime_ms: None,
            memory_usage_bytes: None,
            last_error: None,
        })
    }
}
```

The `native_status`, `status_metadata`, `attachments`, and `status_dispatch_hints` fields are v0.1.1 additions. Backends that don't yet surface them can omit them entirely — the wire output stays byte-identical to v0.1.0. Workflow YAML in newer hosts can then gate phases on `dispatch_label` to fire phases like `code-review` regardless of which backend's vocabulary the subject came from. See [`spec.md` §9.7](./spec.md).

Run:

```bash
cargo build --release
animus plugin install ./target/release/animus-subject-linear
```

## Provider quickstart (Rust)

```rust
use animus_plugin_protocol::{PluginInfo, PLUGIN_KIND_PROVIDER};
use animus_plugin_runtime::provider_main;

mod provider;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let info = PluginInfo {
        name: "animus-provider-claude".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        plugin_kind: PLUGIN_KIND_PROVIDER.into(),
        description: Some("Claude Code CLI provider".into()),
    };
    provider_main(info, provider::ClaudeProvider::new()).await
}
```

`provider::ClaudeProvider` implements [`ProviderBackend`](./animus-provider-protocol) — `manifest`, `run_agent`, `resume_agent`, `cancel_agent`, and `health`. The runtime handles `initialize`, `$/ping`, `health/check`, `agent/run`, `agent/resume`, `agent/cancel`, and `shutdown`, and dispatches each call into the trait implementation.

For v0.1.0 each `run_agent` call is request/response: the provider runs the session to completion inside the trait method and returns the aggregated [`AgentRunResponse`](./animus-provider-protocol). A streaming event-emitter API (so the runtime can flush `agent/output` / `agent/thinking` / `agent/toolCall` / `agent/toolResult` / `agent/error` notifications mid-run) will land in a follow-up `animus-session-backend` crate.

## Trigger backend quickstart (Rust)

```rust
use animus_plugin_protocol::{PluginInfo, PLUGIN_KIND_TRIGGER_BACKEND};
use animus_plugin_runtime::trigger_backend_main;

mod backend;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let info = PluginInfo {
        name: "animus-trigger-slack".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        plugin_kind: PLUGIN_KIND_TRIGGER_BACKEND.into(),
        description: Some("Slack trigger backend for Animus".into()),
    };
    trigger_backend_main(info, backend::SlackBackend::new()).await
}
```

src/backend.rs (sketch):

```rust
use animus_plugin_protocol::{HealthCheckResult, HealthStatus};
use animus_trigger_protocol::{
    BackendError, TriggerBackend, TriggerEvent, TriggerSchema, TriggerStream,
};
use async_trait::async_trait;
use chrono::Utc;
use futures_core::stream;

pub struct SlackBackend { /* socket-mode client, etc. */ }

impl SlackBackend {
    pub fn new() -> Self { Self { /* ... */ } }
}

#[async_trait]
impl TriggerBackend for SlackBackend {
    fn schema(&self) -> TriggerSchema {
        TriggerSchema {
            kinds: vec!["slack_mention".into(), "slack_channel_message".into()],
            supports_resume: true,
            supports_dedup: true,
            supports_ack: true,
        }
    }

    async fn watch(&self) -> Result<TriggerStream, BackendError> {
        // In a real backend you'd subscribe to Slack socket-mode here and
        // yield each event as a `TriggerEvent`. This sketch emits one
        // synthetic event and ends.
        let event = TriggerEvent {
            id: "slack:T123/C456/1715701234.000100".into(),
            occurred_at: Utc::now(),
            kind: "slack_mention".into(),
            payload: serde_json::json!({"user": "U1", "text": "@animus please review"}),
            subject_id: None,
            action_hint: Some("run-workflow:review".into()),
        };
        Ok(Box::pin(stream::iter(vec![Ok(event)])))
    }

    async fn ack(&self, _event_id: &str) -> Result<(), BackendError> {
        // Persist the cursor so we don't redeliver after restart.
        Ok(())
    }

    async fn health(&self) -> Result<HealthCheckResult, BackendError> {
        Ok(HealthCheckResult {
            status: HealthStatus::Healthy,
            uptime_ms: None,
            memory_usage_bytes: None,
            last_error: None,
        })
    }
}
```

The runtime calls `watch` once after `initialize`/`initialized`, forwards every event the stream yields as a `trigger/event` notification, and dispatches `trigger/ack` calls back into the trait. Backends that don't track delivery state can rely on the default no-op `ack` implementation.

## Log storage backend quickstart (Rust)

```rust
use animus_log_storage_protocol::PLUGIN_KIND_LOG_STORAGE_BACKEND;
use animus_plugin_protocol::PluginInfo;
use animus_plugin_runtime::log_storage_backend_main;

mod backend;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let info = PluginInfo {
        name: "animus-log-storage-file".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        plugin_kind: PLUGIN_KIND_LOG_STORAGE_BACKEND.into(),
        description: Some("Local events.jsonl log storage".into()),
    };
    log_storage_backend_main(info, backend::FileBackend::new()).await
}
```

src/backend.rs (sketch):

```rust
use animus_log_storage_protocol::{
    BackendError, LogEntry, LogQuery, LogQueryResult, LogStorageBackend, LogStorageSchema,
    LogStream, SupportsFiltering,
};
use animus_plugin_protocol::{HealthCheckResult, HealthStatus};
use async_trait::async_trait;
use futures_core::stream;

pub struct FileBackend { /* file handle, mutex, ... */ }

impl FileBackend {
    pub fn new() -> Self { Self { /* ... */ } }
}

#[async_trait]
impl LogStorageBackend for FileBackend {
    async fn store(&self, _entries: Vec<LogEntry>) -> Result<(), BackendError> {
        // Append each entry as one JSON line to events.jsonl. Dedup by entry.id
        // if the file already contains it (or skip dedup and rely on the
        // host).
        Ok(())
    }

    async fn query(&self, _filter: LogQuery) -> Result<LogQueryResult, BackendError> {
        // Scan events.jsonl, filter in-process, return.
        Ok(LogQueryResult { entries: vec![], next_cursor: None })
    }

    async fn tail(&self, _filter: LogQuery) -> Result<LogStream, BackendError> {
        // Open a follower over the JSONL file (inotify, kqueue, polling, ...)
        // and yield each new entry as it lands.
        Ok(Box::pin(stream::iter(Vec::<Result<LogEntry, BackendError>>::new())))
    }

    fn schema(&self) -> LogStorageSchema {
        LogStorageSchema {
            supports_query: true,
            supports_tail: true,
            supports_dedup: false,
            supports_filtering: SupportsFiltering {
                by_level: true,
                by_source: true,
                by_target: true,
                by_time_range: true,
                by_glob: true,
            },
            max_query_window: None,
            retention_hint: None,
        }
    }

    async fn health(&self) -> Result<HealthCheckResult, BackendError> {
        Ok(HealthCheckResult {
            status: HealthStatus::Healthy,
            uptime_ms: None,
            memory_usage_bytes: None,
            last_error: None,
        })
    }
}
```

The runtime handles `initialize`, `$/ping`, `health/check`, `log_storage/store`, `log_storage/query`, `log_storage/tail` (with `log_storage/event` notification streaming), `log_storage/schema`, and `shutdown`, and dispatches each call into the trait implementation. Write-only sinks set `supports_query = false` / `supports_tail = false` in the schema and return `BackendError::NotSupported` from the corresponding methods — the runtime translates that to `-32001` (`method_not_supported`) on the wire and hosts fall back gracefully.

## Source of truth

- [`spec.md`](./spec.md) is the language-agnostic protocol specification. **A Python or TypeScript plugin that conforms to `spec.md` is a first-class Animus plugin.** The Rust crates in this repo are one reference implementation.
- [`animus-plugin-protocol/src/lib.rs`](./animus-plugin-protocol/src/lib.rs) is the canonical type definitions; the spec mirrors them.

## Design pointers

The plugin protocol exists because of two design constraints:

- **No system-of-record migration.** Most teams will not move work out of Linear / Jira / GitHub Issues. See [Subject Backend Plugins](https://github.com/launchapp-dev/animus-cli/blob/main/docs/architecture/subject-backend-plugins.md).
- **Provider parity.** Claude, Codex, Gemini, and any future LLM CLI should plug into the daemon through the same surface as a custom HTTP provider. See [Naming Contract](https://github.com/launchapp-dev/animus-cli/blob/main/docs/architecture/naming-contract.md) and [Subject Dispatch Daemon](https://github.com/launchapp-dev/animus-cli/blob/main/docs/architecture/subject-dispatch-daemon.md).

## License

MIT. Copyright (c) 2026 Launchapp.dev. See [`LICENSE`](./LICENSE).

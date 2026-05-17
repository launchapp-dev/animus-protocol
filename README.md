# animus-protocol

**The plugin protocol stack for [Animus](https://github.com/launchapp-dev/animus-cli).** Build a subject backend, an LLM provider, or any custom Animus plugin in Rust — or in any language that can speak newline-delimited JSON-RPC 2.0 over stdio.

## Status

**v0.1.0 scaffold — protocol design + wire types + spec landed; runtime helpers standalone-compilable.** Animus core v0.4.0 ships these crates as workspace members (`crates/animus-{plugin,subject,provider}-protocol/` + `crates/animus-plugin-runtime/` in [`launchapp-dev/animus-cli`](https://github.com/launchapp-dev/animus-cli)), so the design + wire types have been exercised against a real codebase. The standalone repo now compiles cleanly end-to-end. Honest state before the first `cargo publish` to crates.io:

| Crate | Standalone-compilable? | Notes |
|---|---|---|
| `animus-plugin-protocol` | yes | Wire types only; no external Animus deps. |
| `animus-subject-protocol` | yes | Pure trait + schema definitions. |
| `animus-provider-protocol` | yes | Pure trait + schema definitions. |
| `animus-plugin-runtime` | yes | Slim stdio JSON-RPC loop; exposes `subject_backend_main` and `provider_main`. Provider session helpers (event channels, child-process plumbing) will land in a separate `animus-session-backend` crate. |

The protocol [`spec.md`](./spec.md) is the source of truth for cross-language plugin authors — it can be implemented in Python, TypeScript, Go, or any language that speaks newline-delimited JSON-RPC 2.0 over stdio.

The protocol + subject + provider + runtime crates are usable today via git path/tag dependency from this repo. Plugin authors write `subject_backend_main(info, backend).await` or `provider_main(info, backend).await` from `main` and avoid hand-rolling the wire layer.

## Crates

| Crate | Purpose |
|---|---|
| [`animus-plugin-protocol`](./animus-plugin-protocol) | Wire types every plugin uses: `RpcRequest`, `RpcResponse`, `RpcNotification`, `RpcError`, error codes, `InitializeParams` / `InitializeResult`, `PluginManifest`, `HealthCheckResult`. |
| [`animus-subject-protocol`](./animus-subject-protocol) | `SubjectBackend` trait + normalized `Subject` schema for backends like Linear, Jira, GitHub Issues, Notion, Asana — anything with a system-of-record API. |
| [`animus-provider-protocol`](./animus-provider-protocol) | `ProviderBackend` trait + `AgentRunRequest`/`AgentRunResponse` shapes for LLM provider plugins (Claude, Codex, Gemini, OpenAI-compatible, on-prem). |
| [`animus-plugin-runtime`](./animus-plugin-runtime) | Shared stdio JSON-RPC loop, handshake, `--manifest` mode, notification helpers. Plugin authors call `subject_backend_main(...)` / `provider_main(...)` from `main` and avoid hand-rolling the wire layer. |

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
    BackendError, EventStream, Subject, SubjectBackend, SubjectFilter, SubjectId,
    SubjectList, SubjectPatch, SubjectSchema, SubjectStatus,
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
        Ok(SubjectList { subjects: vec![], next_cursor: None, fetched_at: Utc::now() })
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
            native_status_values: vec!["Backlog".into(), "Todo".into(), "In Progress".into(), "Done".into()],
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

## Source of truth

- [`spec.md`](./spec.md) is the language-agnostic protocol specification. **A Python or TypeScript plugin that conforms to `spec.md` is a first-class Animus plugin.** The Rust crates in this repo are one reference implementation.
- [`animus-plugin-protocol/src/lib.rs`](./animus-plugin-protocol/src/lib.rs) is the canonical type definitions; the spec mirrors them.

## Design pointers

The plugin protocol exists because of two design constraints:

- **No system-of-record migration.** Most teams will not move work out of Linear / Jira / GitHub Issues. See [Subject Backend Plugins](https://github.com/launchapp-dev/animus-cli/blob/main/docs/architecture/subject-backend-plugins.md).
- **Provider parity.** Claude, Codex, Gemini, and any future LLM CLI should plug into the daemon through the same surface as a custom HTTP provider. See [Naming Contract](https://github.com/launchapp-dev/animus-cli/blob/main/docs/architecture/naming-contract.md) and [Subject Dispatch Daemon](https://github.com/launchapp-dev/animus-cli/blob/main/docs/architecture/subject-dispatch-daemon.md).

## License

MIT. Copyright (c) 2026 Launchapp.dev. See [`LICENSE`](./LICENSE).

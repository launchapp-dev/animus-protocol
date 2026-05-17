# animus-protocol

**The plugin protocol stack for [Animus](https://github.com/launchapp-dev/animus-cli).** Build a subject backend, an LLM provider, or any custom Animus plugin in Rust — or in any language that can speak newline-delimited JSON-RPC 2.0 over stdio.

## Status

**v0.4.0 cut — pending crates.io publish.** The wire shapes are frozen for the 1.x protocol. APIs in the Rust SDK crates may still see small renames before the first `cargo publish`. If you're prototyping today, depend on the git tags rather than the path or registry.

## Crates

| Crate | Purpose |
|---|---|
| [`animus-plugin-protocol`](./animus-plugin-protocol) | Wire types every plugin uses: `RpcRequest`, `RpcResponse`, `RpcNotification`, `RpcError`, error codes, `InitializeParams` / `InitializeResult`, `PluginManifest`, `HealthCheckResult`. |
| [`animus-subject-protocol`](./animus-subject-protocol) | `SubjectBackend` trait + normalized `Subject` schema for backends like Linear, Jira, GitHub Issues, Notion, Asana — anything with a system-of-record API. |
| [`animus-provider-protocol`](./animus-provider-protocol) | `ProviderBackend` trait + `AgentRunRequest`/`AgentRunResponse` shapes for LLM provider plugins (Claude, Codex, Gemini, OpenAI-compatible, on-prem). |
| [`animus-plugin-runtime`](./animus-plugin-runtime) | Shared stdio JSON-RPC loop, handshake, `--manifest` mode, streaming notification helpers. Plugin authors call `run_provider(...)` / `subject_backend_main(...)` from `main` and avoid hand-rolling the wire layer. |

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
use animus_plugin_runtime::subject_backend_main;

mod backend;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    subject_backend_main(backend::LinearBackend::new()).await
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
use animus_plugin_runtime::{run_provider, ProviderInfo, SessionBackendProvider};
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let backend = Arc::new(/* your SessionBackend impl */);
    run_provider(
        ProviderInfo {
            plugin_name: "animus-provider-claude",
            plugin_version: env!("CARGO_PKG_VERSION"),
            description: "Claude Code CLI provider",
            default_tool: "claude",
            default_model: "claude-sonnet-4-6",
        },
        SessionBackendProvider::new(backend),
    ).await
}
```

The runtime handles `initialize`, `$/ping`, `health/check`, `agent/run`, `agent/resume`, `agent/cancel`, `shutdown`, `exit`, and streams `agent/output` / `agent/thinking` / `agent/toolCall` / `agent/toolResult` / `agent/error` notifications back to the host.

## Source of truth

- [`spec.md`](./spec.md) is the language-agnostic protocol specification. **A Python or TypeScript plugin that conforms to `spec.md` is a first-class Animus plugin.** The Rust crates in this repo are one reference implementation.
- [`animus-plugin-protocol/src/lib.rs`](./animus-plugin-protocol/src/lib.rs) is the canonical type definitions; the spec mirrors them.

## Design pointers

The plugin protocol exists because of two design constraints:

- **No system-of-record migration.** Most teams will not move work out of Linear / Jira / GitHub Issues. See [Subject Backend Plugins](https://github.com/launchapp-dev/animus-cli/blob/main/docs/architecture/subject-backend-plugins.md).
- **Provider parity.** Claude, Codex, Gemini, and any future LLM CLI should plug into the daemon through the same surface as a custom HTTP provider. See [Naming Contract](https://github.com/launchapp-dev/animus-cli/blob/main/docs/architecture/naming-contract.md) and [Subject Dispatch Daemon](https://github.com/launchapp-dev/animus-cli/blob/main/docs/architecture/subject-dispatch-daemon.md).

## License

MIT. Copyright (c) 2026 Launchapp.dev. See [`LICENSE`](./LICENSE).

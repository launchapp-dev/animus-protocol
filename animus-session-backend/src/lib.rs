//! Session-management primitives for Animus provider plugins that wrap CLI binaries.
//!
//! This crate is the extracted, dependency-light version of the in-tree
//! `cli-wrapper` crate from the main `animus-cli` workspace. It exists so the
//! four published Animus CLI-wrapping provider plugins
//! (`animus-provider-claude`, `animus-provider-codex`, `animus-provider-gemini`,
//! `animus-provider-opencode`) can depend on a stable, narrow surface without
//! pulling in the entire `animus-cli` core workspace.
//!
//! # What's here
//!
//! - The [`SessionBackend`] trait + associated [`SessionRequest`] /
//!   [`SessionRun`] / [`SessionEvent`] types — the abstraction a provider
//!   plugin's `agent/run` handler routes through.
//! - The [`SubprocessSessionBackend`] — a generic CLI-spawning implementation
//!   of the trait.
//! - Per-CLI native session backends — [`ClaudeSessionBackend`],
//!   [`CodexSessionBackend`], [`GeminiSessionBackend`],
//!   [`OpenCodeSessionBackend`], [`OaiRunnerSessionBackend`] — that drive each
//!   CLI's native JSON streaming format.
//! - [`SessionBackendResolver`] — picks the right backend for a given tool
//!   string and falls back to the subprocess backend for unknown tools.
//! - CLI launch helpers ([`LaunchInvocation`], `ensure_*` argv normalizers,
//!   [`parse_launch_from_runtime_contract`]).
//! - Generic text-event extraction ([`extract_text_from_line`],
//!   [`NormalizedTextEvent`]).
//!
//! # What's NOT here (intentionally)
//!
//! - `PluginSessionBackend` / `discover_provider_plugins` — those live in the
//!   in-tree `cli-wrapper` because they're the daemon-side adapter that
//!   spawns provider plugins. Provider plugins themselves don't need them.
//! - The `CliTester` / `CliRegistry` / `CliValidator` surface — those are
//!   daemon-side tooling for probing installed CLIs.
//! - `orchestrator-*` and `animus-cli` internal types — fully stripped.
//!
//! # Compatibility
//!
//! The trait and event shapes match the in-tree `cli-wrapper` 1-to-1 so a
//! provider plugin can be authored against this crate today and switch back
//! and forth without touching its handler logic.

pub mod cli;
pub mod error;
pub mod parser;
pub mod session;

pub use animus_actor::{Actor, CLAIM_ADMIN};
pub use cli::{
    codex_exec_insert_index_json, ensure_codex_config_override, ensure_codex_config_override_json,
    ensure_flag, ensure_flag_value, ensure_flag_value_json, ensure_machine_json_output,
    is_ai_cli_tool, is_binary_on_path, launch_prompt_insert_index_json, lookup_binary_in_path,
    parse_cli_type, parse_launch_from_runtime_contract, CliType, LaunchInvocation,
};
pub use error::{Error, Result};
pub use parser::{extract_text_from_line, NormalizedTextEvent};
pub use session::{
    ClaudeSessionBackend, CodexSessionBackend, GeminiSessionBackend, OaiRunnerSessionBackend,
    OpenCodeSessionBackend, SessionBackend, SessionBackendInfo, SessionBackendKind,
    SessionBackendResolver, SessionCapabilities, SessionEvent, SessionRequest, SessionRun,
    SessionStability, SubprocessSessionBackend, APPROVALS_PROMPT_PREAMBLE,
    CLAUDE_PERMISSION_PROMPT_TOOL,
};

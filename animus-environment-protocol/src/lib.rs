//! Wire types for the Animus `environment` plugin role (v0.7).
//!
//! An *environment plugin* owns the execution context a provider harness runs
//! inside. The three flagship implementations are a git-worktree environment
//! (local, the default), a container environment (Docker / OCI), and a remote
//! environment (a Railway runner, an SSH host, a cloud sandbox). All three
//! speak the same three-call contract:
//!
//! 1. [`METHOD_ENVIRONMENT_PREPARE`] — materialize the context (check out the
//!    repo set, spin up the container, provision the remote host) and return an
//!    [`EnvironmentHandle`].
//! 2. [`METHOD_ENVIRONMENT_EXEC`] — run a [`HarnessCommand`] inside the prepared
//!    context and return its buffered [`ExecResponse`]. A streaming variant,
//!    [`METHOD_ENVIRONMENT_EXEC_STREAM`], emits incremental
//!    [`NOTIFICATION_ENVIRONMENT_OUTPUT`] notifications for stdout/stderr as they
//!    are produced, then returns the same [`ExecResponse`] as the final reply.
//! 3. [`METHOD_ENVIRONMENT_TEARDOWN`] — dispose of the context (prune the
//!    worktree, stop + remove the container, release the remote host).
//!
//! Like every Animus plugin, environment plugins speak newline-delimited
//! JSON-RPC 2.0 over stdio (see `animus-plugin-protocol`). This crate defines
//! only the language-neutral request/response/notification shapes and the
//! method-name constants; it deliberately does not define a Rust trait or the
//! stdio loop (those live in `animus-plugin-runtime` and can be layered on
//! later).
//!
//! # Exec streaming
//!
//! The exec surface follows the proven server-streaming pattern used by
//! `animus-provider-protocol`'s `agent/run`: a plugin that supports streaming
//! emits [`ExecNotification`]s (wrapped by the runtime into
//! [`NOTIFICATION_ENVIRONMENT_OUTPUT`] JSON-RPC notifications) on the same
//! channel as the eventual [`ExecResponse`] reply. The buffered
//! [`METHOD_ENVIRONMENT_EXEC`] call is the baseline every environment plugin
//! MUST implement; [`METHOD_ENVIRONMENT_EXEC_STREAM`] is the opt-in streaming
//! upgrade, and a plugin that does not implement it responds with
//! [`animus_plugin_protocol::error_codes::METHOD_NOT_SUPPORTED`].
//!
//! Streaming is currently one-directional (plugin → host: stdout/stderr).
//! Buffered stdin is carried up-front on [`ExecRequest::stdin`]. Live,
//! interactive stdin over the lifetime of a streamed exec is deferred — see the
//! TODO on [`ExecRequest::stdin`]; when added it will mirror
//! `animus-provider-protocol`'s host → plugin `agent/respond` request rather
//! than a notification.

#![warn(missing_docs)]

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// =====================================================================
// Method-name constants (the JSON-RPC wire methods)
// =====================================================================

/// `environment/prepare` — materialize an execution context from an
/// [`EnvironmentSpec`] and return an [`EnvironmentHandle`].
pub const METHOD_ENVIRONMENT_PREPARE: &str = "environment/prepare";

/// `environment/exec` — run a [`HarnessCommand`] inside a prepared context and
/// return its buffered [`ExecResponse`]. This is the baseline exec call every
/// environment plugin implements.
pub const METHOD_ENVIRONMENT_EXEC: &str = "environment/exec";

/// `environment/exec_stream` — like [`METHOD_ENVIRONMENT_EXEC`], but the plugin
/// emits incremental [`NOTIFICATION_ENVIRONMENT_OUTPUT`] notifications for
/// stdout/stderr as they are produced and then returns the aggregated
/// [`ExecResponse`] as the final reply. Optional; plugins that do not implement
/// streaming respond with
/// [`animus_plugin_protocol::error_codes::METHOD_NOT_SUPPORTED`].
pub const METHOD_ENVIRONMENT_EXEC_STREAM: &str = "environment/exec_stream";

/// `environment/teardown` — dispose of a prepared context by handle.
pub const METHOD_ENVIRONMENT_TEARDOWN: &str = "environment/teardown";

/// `environment/output` — server-streaming notification carrying an
/// [`ExecNotification`] for an in-flight [`METHOD_ENVIRONMENT_EXEC_STREAM`]
/// call.
pub const NOTIFICATION_ENVIRONMENT_OUTPUT: &str = "environment/output";

// =====================================================================
// Repo set / workspace
// =====================================================================

/// A single repository in an environment's workspace (repo set).
///
/// A multi-repo workspace (see the top-level `workspace:` config) checks out
/// more than one `RepoRef` under [`EnvironmentHandle::workspace_root`], each in
/// its own subdirectory named by [`RepoRef::name`] (or derived from
/// [`RepoRef::url`] when `name` is unset).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RepoRef {
    /// Clone URL or local path for the repository. The originating environment
    /// plugin interprets the value (an `https://`/`git@` remote for the
    /// worktree/container/remote runners, or a local path for a bind-mount).
    pub url: String,

    /// Subdirectory name to check the repo out under, relative to
    /// [`EnvironmentHandle::workspace_root`]. When unset, the plugin derives it
    /// from the last path segment of [`Self::url`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Git ref (branch, tag, or commit) to check out. When unset, the plugin
    /// uses the remote's default branch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_ref: Option<String>,

    /// True when this repo is the primary workspace repo (the one a
    /// single-repo subject maps to, and the default `cwd` for
    /// [`HarnessCommand`]). At most one repo in a set should be primary; when
    /// none is marked, the first entry is primary.
    #[serde(default, skip_serializing_if = "is_false")]
    pub primary: bool,
}

// =====================================================================
// Prepare
// =====================================================================

/// Declarative description of the execution context to materialize.
///
/// [`Self::kind`] names the environment plugin id (e.g. `"worktree"`,
/// `"container"`, `"railway"`) so the kernel can route a `prepare` call to the
/// right plugin. The rest of the spec is intentionally open: [`Self::image`],
/// [`Self::resources`], and [`Self::metadata`] carry plugin-specific knobs that
/// the kernel passes through opaquely.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EnvironmentSpec {
    /// Environment plugin id that should service this spec (the plugin's
    /// declared environment kind, not [`animus_plugin_protocol::PluginKind`]).
    pub kind: String,

    /// The repo set / workspace to materialize. May be empty for a
    /// repo-less environment (e.g. a scratch container).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub repos: Vec<RepoRef>,

    /// Container/VM image reference for image-based environments (Docker tag,
    /// OCI ref, AMI id, ...). Ignored by the worktree environment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    /// Resource requests/limits for the environment (cpu, memory, disk,
    /// timeout, region, ...). Shape is plugin-defined; carried opaquely.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<Value>,

    /// Environment variables to inject into every command run in this context.
    /// Non-secret config only — secrets flow through the kernel's secret store,
    /// not this field.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,

    /// Free-form plugin-specific metadata (labels, base_ref, network mode,
    /// mounts, ...). Carried opaquely by the kernel.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub metadata: Value,
}

/// Request payload for [`METHOD_ENVIRONMENT_PREPARE`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PrepareRequest {
    /// The environment to materialize.
    pub spec: EnvironmentSpec,
}

/// Response payload for [`METHOD_ENVIRONMENT_PREPARE`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PrepareResponse {
    /// Handle to the materialized context, used for subsequent `exec` and
    /// `teardown` calls.
    pub handle: EnvironmentHandle,
}

/// Handle to a prepared execution context.
///
/// The kernel treats [`Self::id`] as opaque and passes the whole handle back on
/// every [`ExecRequest`] / [`TeardownRequest`]; only the originating plugin
/// interprets the id. [`Self::workspace_root`] is the absolute path (on the
/// plugin's side of the world) that command `cwd`s resolve against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EnvironmentHandle {
    /// Opaque, plugin-assigned identifier for this prepared context.
    pub id: String,

    /// Absolute path to the root of the materialized workspace. Command `cwd`s
    /// ([`HarnessCommand::cwd`]) resolve relative to this path; for a
    /// multi-repo workspace each [`RepoRef`] lives in a subdirectory under it.
    pub workspace_root: String,

    /// Free-form plugin-specific metadata about the prepared context
    /// (container id, remote host, allocated ports, ...). Carried opaquely.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub metadata: Value,
}

// =====================================================================
// Exec
// =====================================================================

/// A command to run inside a prepared environment.
///
/// This is the harness invocation the provider layer would otherwise run
/// directly on the host; the environment plugin runs it inside the prepared
/// context instead.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HarnessCommand {
    /// Executable to run (looked up on the environment's `PATH` unless
    /// absolute).
    pub program: String,

    /// Arguments passed to [`Self::program`], not including `argv[0]`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,

    /// Extra environment variables for this command, merged over (and
    /// overriding) [`EnvironmentSpec::env`].
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,

    /// Working directory for the command, relative to
    /// [`EnvironmentHandle::workspace_root`]. When unset, runs in the primary
    /// repo's directory (or the workspace root when there is no primary repo).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
}

/// Request payload for [`METHOD_ENVIRONMENT_EXEC`] and
/// [`METHOD_ENVIRONMENT_EXEC_STREAM`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExecRequest {
    /// The prepared context to run in.
    pub handle: EnvironmentHandle,

    /// The command to run.
    pub command: HarnessCommand,

    /// Bytes to feed to the command's stdin, up front, as a UTF-8 string.
    ///
    // TODO(v0.7): live, interactive stdin over the lifetime of a streamed exec
    // is not yet modeled. When added it will mirror
    // `animus-provider-protocol`'s host → plugin `agent/respond` request
    // (a correlated JSON-RPC call), not a fire-and-forget notification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdin: Option<String>,

    /// Hard wall-clock timeout in seconds. When exceeded the environment kills
    /// the command and returns [`ExecResponse::timed_out`] = true. `None` means
    /// no explicit timeout (the environment may still impose its own).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
}

/// Response payload for [`METHOD_ENVIRONMENT_EXEC`] /
/// [`METHOD_ENVIRONMENT_EXEC_STREAM`].
///
/// For [`METHOD_ENVIRONMENT_EXEC_STREAM`], [`Self::stdout`] / [`Self::stderr`]
/// carry the aggregated output already delivered incrementally via
/// [`ExecNotification`]s; a client that consumed the stream can ignore them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExecResponse {
    /// Process exit code. `None` when the process was terminated by a signal or
    /// killed on timeout without producing an exit code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,

    /// Aggregated stdout captured from the command.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stdout: String,

    /// Aggregated stderr captured from the command.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stderr: String,

    /// True when the command was killed because it exceeded
    /// [`ExecRequest::timeout_secs`].
    #[serde(default, skip_serializing_if = "is_false")]
    pub timed_out: bool,
}

/// Which output stream an [`ExecNotification`] delta belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ExecStream {
    /// Standard output.
    Stdout,
    /// Standard error.
    Stderr,
}

/// A streaming notification an environment plugin emits mid-exec during a
/// [`METHOD_ENVIRONMENT_EXEC_STREAM`] call.
///
/// The runtime wraps these into [`NOTIFICATION_ENVIRONMENT_OUTPUT`] JSON-RPC
/// notifications and forwards them to the host on the same channel as the
/// eventual [`ExecResponse`] reply. This mirrors
/// `animus-provider-protocol`'s `AgentNotification` server-streaming surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ExecNotification {
    /// Incremental stdout/stderr the command has produced. Maps to
    /// [`NOTIFICATION_ENVIRONMENT_OUTPUT`].
    Output {
        /// Handle id of the environment this exec runs in.
        handle_id: String,
        /// Which stream this delta belongs to.
        stream: ExecStream,
        /// The output delta (UTF-8).
        text: String,
    },
}

impl ExecNotification {
    /// Wire-method constant for the JSON-RPC notification this variant maps to.
    pub fn method(&self) -> &'static str {
        match self {
            ExecNotification::Output { .. } => NOTIFICATION_ENVIRONMENT_OUTPUT,
        }
    }

    /// The wire payload for the notification (i.e. its `params`).
    pub fn payload(&self) -> Value {
        match self {
            ExecNotification::Output {
                handle_id,
                stream,
                text,
            } => serde_json::json!({
                "handle_id": handle_id,
                "stream": stream,
                "text": text,
            }),
        }
    }
}

// =====================================================================
// Teardown
// =====================================================================

/// Request payload for [`METHOD_ENVIRONMENT_TEARDOWN`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TeardownRequest {
    /// The prepared context to dispose of.
    pub handle: EnvironmentHandle,
}

/// Response payload for [`METHOD_ENVIRONMENT_TEARDOWN`]. Empty on success; the
/// wire-level error shape is `animus_plugin_protocol::RpcError`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
pub struct TeardownResponse {}

fn is_false(value: &bool) -> bool {
    !*value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn method_constants_match_wire_strings() {
        assert_eq!(METHOD_ENVIRONMENT_PREPARE, "environment/prepare");
        assert_eq!(METHOD_ENVIRONMENT_EXEC, "environment/exec");
        assert_eq!(METHOD_ENVIRONMENT_EXEC_STREAM, "environment/exec_stream");
        assert_eq!(METHOD_ENVIRONMENT_TEARDOWN, "environment/teardown");
        assert_eq!(NOTIFICATION_ENVIRONMENT_OUTPUT, "environment/output");
    }

    #[test]
    fn prepare_round_trips_minimum_fields() {
        let req = PrepareRequest {
            spec: EnvironmentSpec {
                kind: "worktree".to_string(),
                repos: vec![RepoRef {
                    url: "https://example.test/org/repo.git".to_string(),
                    name: None,
                    git_ref: Some("main".to_string()),
                    primary: true,
                }],
                image: None,
                resources: None,
                env: BTreeMap::new(),
                metadata: Value::Null,
            },
        };
        let value = serde_json::to_value(&req).expect("serializes");
        // Empty/None fields are omitted for back-compat.
        let spec = value.get("spec").and_then(|s| s.as_object()).unwrap();
        assert!(spec.get("image").is_none());
        assert!(spec.get("env").is_none());
        assert!(spec.get("metadata").is_none());
        let decoded: PrepareRequest = serde_json::from_value(value).expect("round-trips");
        assert_eq!(decoded, req);
    }

    #[test]
    fn exec_response_omits_empty_output() {
        let resp = ExecResponse {
            exit_code: Some(0),
            stdout: String::new(),
            stderr: String::new(),
            timed_out: false,
        };
        let value = serde_json::to_value(&resp).expect("serializes");
        assert!(value.get("stdout").is_none());
        assert!(value.get("stderr").is_none());
        assert!(value.get("timed_out").is_none());
        assert_eq!(value.get("exit_code"), Some(&serde_json::json!(0)));
    }

    #[test]
    fn exec_notification_maps_to_wire_method_and_payload() {
        let note = ExecNotification::Output {
            handle_id: "env-1".to_string(),
            stream: ExecStream::Stderr,
            text: "boom\n".to_string(),
        };
        assert_eq!(note.method(), NOTIFICATION_ENVIRONMENT_OUTPUT);
        assert_eq!(
            note.payload(),
            serde_json::json!({
                "handle_id": "env-1",
                "stream": "stderr",
                "text": "boom\n",
            })
        );
    }

    #[test]
    fn teardown_response_is_empty_object() {
        let value = serde_json::to_value(TeardownResponse::default()).expect("serializes");
        assert_eq!(value, serde_json::json!({}));
    }
}

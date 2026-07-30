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

use animus_execution_protocol::ExecutionFence;
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

/// `environment/exec_session` — dispatch a SUBJECT to the environment's own
/// animus (REQ-052 remote-animus): the plugin hands the subject to the node's
/// in-container animus, which runs the workflow through its own provider/session
/// layer and streams rich [`NOTIFICATION_ENVIRONMENT_JOURNAL`] events back;
/// the final reply is an [`ExecSessionResponse`]. Optional; plugins that do not
/// run a remote animus respond with
/// [`animus_plugin_protocol::error_codes::METHOD_NOT_SUPPORTED`].
pub const METHOD_ENVIRONMENT_EXEC_SESSION: &str = "environment/exec_session";

/// `environment/teardown` — dispose of a prepared context by handle.
pub const METHOD_ENVIRONMENT_TEARDOWN: &str = "environment/teardown";

/// `environment/list` — list every managed node (instance) the plugin owns,
/// each an [`EnvironmentNode`]. Part of the node-management surface, which lets
/// an operator inspect + reap environment instances uniformly across substrates.
pub const METHOD_ENVIRONMENT_LIST: &str = "environment/list";

/// `environment/get` — describe one managed node by substrate id or name.
pub const METHOD_ENVIRONMENT_GET: &str = "environment/get";

/// `environment/teardown_node` — destroy ONE managed node by substrate id or
/// name. Distinct from [`METHOD_ENVIRONMENT_TEARDOWN`], which disposes a
/// prepared context by its [`EnvironmentHandle`].
pub const METHOD_ENVIRONMENT_TEARDOWN_NODE: &str = "environment/teardown_node";

/// `environment/reap` — destroy orphaned/dead managed nodes (see
/// [`ReapRequest`]). The cleanup that keeps leaked instances from accumulating.
pub const METHOD_ENVIRONMENT_REAP: &str = "environment/reap";

/// `environment/output` — server-streaming notification carrying an
/// [`ExecNotification`] for an in-flight [`METHOD_ENVIRONMENT_EXEC_STREAM`]
/// call.
pub const NOTIFICATION_ENVIRONMENT_OUTPUT: &str = "environment/output";

/// `environment/journal` — server-streaming notification carrying a journal
/// event from an in-flight [`METHOD_ENVIRONMENT_EXEC_SESSION`] call (the node's
/// own workflow journal, forwarded verbatim).
pub const NOTIFICATION_ENVIRONMENT_JOURNAL: &str = "environment/journal";

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

/// Request payload for [`METHOD_ENVIRONMENT_EXEC_SESSION`] — dispatch a subject
/// to the environment's own animus (REQ-052 remote-animus).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExecSessionRequest {
    /// The prepared context (whose in-container animus runs the subject).
    pub handle: EnvironmentHandle,

    /// The subject to dispatch, qualified `kind:id` (e.g. `task:TASK-1`).
    pub subject_id: String,

    /// Workflow to run the subject through. `None` uses the node's default
    /// routing for the subject's kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_ref: Option<String>,

    /// Optional dispatch input forwarded to the node's run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch_input: Option<String>,

    /// The DELEGATING run's workflow id (REQ-052 one-id). When set, the node
    /// MUST execute INTO this already-bootstrapped run (resume-existing) rather
    /// than minting its own, so exactly ONE journal row exists for the dispatch
    /// and the node's transcript lands on the id the portal reads. `None` keeps
    /// the legacy behavior (the node mints its own run id).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,

    /// Exact workflow/subject/queue generation authority delegated to the
    /// remote node. Generation-aware environments MUST validate that
    /// `workflow_id` matches this fence and pass it unchanged to node Animus.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_fence: Option<ExecutionFence>,
}

impl ExecSessionRequest {
    /// Validate the optional/required execution fence before the environment
    /// dispatches to node Animus.
    pub fn validate_execution_fence(&self, required: bool) -> Result<(), String> {
        let Some(execution) = self.execution_fence.as_ref() else {
            return if required {
                Err("remote execution session requires execution_fence".to_string())
            } else {
                Ok(())
            };
        };
        execution.validate()?;
        if self.workflow_id.as_deref() != Some(execution.workflow_id.as_str()) {
            return Err("exec_session workflow_id does not match execution fence".to_string());
        }
        if let Some(subject) = execution.subject.as_ref() {
            let suffix = format!(":{}", self.subject_id);
            if subject.qualified_id != self.subject_id && !subject.qualified_id.ends_with(&suffix) {
                return Err("exec_session subject_id does not match execution fence".to_string());
            }
        }
        Ok(())
    }
}

/// Response payload for [`METHOD_ENVIRONMENT_EXEC_SESSION`]: the node-local run
/// id the dispatch spawned and its terminal status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExecSessionResponse {
    /// The node-local workflow run id, when one was spawned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,

    /// Echo of the generation fence validated by the node. The parent rejects
    /// a terminal response that omits or changes a required fence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_fence: Option<ExecutionFence>,

    /// Terminal status of the node-local run (e.g. `completed`, `failed`,
    /// `escalated`, `cancelled`, or `no-run` when nothing was dispatched).
    pub status: String,
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
    /// One journal event from an in-flight [`METHOD_ENVIRONMENT_EXEC_SESSION`],
    /// forwarded verbatim from the node's own workflow journal. Maps to
    /// [`NOTIFICATION_ENVIRONMENT_JOURNAL`].
    Journal {
        /// Handle id of the environment this session runs in.
        handle_id: String,
        /// The node-local run id this event belongs to.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        workflow_id: Option<String>,
        /// The journal event kind (e.g. `phase_started`, `output_chunk`,
        /// `tool_call`, `run_failed`).
        event_kind: String,
        /// Phase this event belongs to, when phase-scoped.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        phase_id: Option<String>,
        /// Event status discriminator, when present.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<String>,
        /// Event timestamp (RFC 3339).
        ts: String,
        /// The event's full payload, forwarded verbatim.
        payload: Value,
        /// True on the final event of the session (the run reached a terminal
        /// status), signalling the host to settle.
        #[serde(default, skip_serializing_if = "is_false")]
        terminal: bool,
    },
}

impl ExecNotification {
    /// Wire-method constant for the JSON-RPC notification this variant maps to.
    pub fn method(&self) -> &'static str {
        match self {
            ExecNotification::Output { .. } => NOTIFICATION_ENVIRONMENT_OUTPUT,
            ExecNotification::Journal { .. } => NOTIFICATION_ENVIRONMENT_JOURNAL,
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
            ExecNotification::Journal {
                handle_id,
                workflow_id,
                event_kind,
                phase_id,
                status,
                ts,
                payload,
                terminal,
            } => serde_json::json!({
                "handle_id": handle_id,
                "workflow_id": workflow_id,
                "event_kind": event_kind,
                "phase_id": phase_id,
                "status": status,
                "ts": ts,
                "payload": payload,
                "terminal": terminal,
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

// =====================================================================
// Node management (list / get / teardown_node / reap)
// =====================================================================

/// A managed environment instance (a "node") as reported by the node-management
/// surface. Substrate-agnostic: a Railway service, a Docker container, a k8s pod.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EnvironmentNode {
    /// Substrate-native id (railway service id, container id, ...).
    pub id: String,
    /// Human-facing name (e.g. `animus-run-<hash>`).
    pub name: String,
    /// Lifecycle state as the substrate reports it (`SUCCESS`, `FAILED`,
    /// `CRASHED`, `unknown`, ...).
    pub state: String,
    /// The animus run id this node serves, when recoverable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Image / impl ref backing the node, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// Creation timestamp (ISO 8601) when the substrate exposes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    /// True when the node has no live owning run (a reap candidate).
    pub orphan: bool,
}

/// Request payload for [`METHOD_ENVIRONMENT_LIST`] (no parameters).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ListNodesRequest {}

/// Response payload for [`METHOD_ENVIRONMENT_LIST`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ListNodesResponse {
    /// Every known environment node.
    #[serde(default)]
    pub nodes: Vec<EnvironmentNode>,
}

/// Request payload for [`METHOD_ENVIRONMENT_GET`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GetNodeRequest {
    /// Substrate id or name of the node to describe.
    pub id: String,
}

/// Response payload for [`METHOD_ENVIRONMENT_GET`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GetNodeResponse {
    /// The node, or null when no node matched.
    #[serde(default)]
    pub node: Option<EnvironmentNode>,
}

/// Request payload for [`METHOD_ENVIRONMENT_TEARDOWN_NODE`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TeardownNodeRequest {
    /// Substrate id or name of the node to destroy.
    pub id: String,
}

/// Response payload for [`METHOD_ENVIRONMENT_TEARDOWN_NODE`]. Idempotent — an
/// already-gone node yields an empty `deleted`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TeardownNodeResponse {
    /// Substrate ids actually deleted.
    #[serde(default)]
    pub deleted: Vec<String>,
}

/// Request payload for [`METHOD_ENVIRONMENT_REAP`]. With no fields set, reap
/// deletes only dead nodes (always safe — a live node is never dead).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReapRequest {
    /// Also reap non-dead nodes that have no live owning run (needs `force`).
    #[serde(default, skip_serializing_if = "is_false")]
    pub all: bool,
    /// Required alongside `all` — guards a fresh (no-liveness) reaper from
    /// treating every healthy node as an orphan.
    #[serde(default, skip_serializing_if = "is_false")]
    pub force: bool,
    /// Report what WOULD be reaped without deleting anything.
    #[serde(default, skip_serializing_if = "is_false")]
    pub dry_run: bool,
    /// Only reap nodes at least this many seconds old.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub older_than_secs: Option<u64>,
}

/// Response payload for [`METHOD_ENVIRONMENT_REAP`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReapResponse {
    /// Substrate ids deleted (or that WOULD be deleted, when `dry_run`).
    #[serde(default)]
    pub deleted: Vec<String>,
    /// Nodes spared.
    #[serde(default)]
    pub kept: Vec<EnvironmentNode>,
    /// Echoes the request's `dry_run`.
    #[serde(default)]
    pub dry_run: bool,
}

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
        assert_eq!(METHOD_ENVIRONMENT_LIST, "environment/list");
        assert_eq!(METHOD_ENVIRONMENT_GET, "environment/get");
        assert_eq!(
            METHOD_ENVIRONMENT_TEARDOWN_NODE,
            "environment/teardown_node"
        );
        assert_eq!(METHOD_ENVIRONMENT_REAP, "environment/reap");
        assert_eq!(NOTIFICATION_ENVIRONMENT_OUTPUT, "environment/output");
    }

    #[test]
    fn reap_request_omits_default_flags() {
        let value = serde_json::to_value(ReapRequest::default()).expect("serializes");
        assert_eq!(value, serde_json::json!({}));
        let full = ReapRequest {
            all: true,
            force: true,
            dry_run: true,
            older_than_secs: Some(60),
        };
        let decoded: ReapRequest =
            serde_json::from_value(serde_json::to_value(&full).expect("ser")).expect("round-trips");
        assert_eq!(decoded, full);
    }

    #[test]
    fn environment_node_round_trips_and_omits_none() {
        let node = EnvironmentNode {
            id: "svc-1".to_string(),
            name: "animus-run-abc".to_string(),
            state: "FAILED".to_string(),
            run_id: None,
            image: None,
            created_at: None,
            orphan: true,
        };
        let value = serde_json::to_value(&node).expect("serializes");
        assert!(value.get("run_id").is_none());
        assert_eq!(value.get("orphan"), Some(&serde_json::json!(true)));
        let decoded: EnvironmentNode = serde_json::from_value(value).expect("round-trips");
        assert_eq!(decoded, node);
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

    #[test]
    fn exec_session_round_trips_execution_fence() {
        use animus_execution_protocol::{
            ExecutionFence, SubjectGeneration, EXECUTION_FENCE_SCHEMA_ID, EXECUTION_FENCE_VERSION,
        };
        let fence = ExecutionFence {
            schema: EXECUTION_FENCE_SCHEMA_ID.to_string(),
            version: EXECUTION_FENCE_VERSION,
            workflow_id: "workflow-1".to_string(),
            workflow_generation: 1,
            subject: Some(SubjectGeneration {
                qualified_id: "task:TASK-1175".to_string(),
                generation: 3,
            }),
            queue_lease: None,
            repository: None,
        };
        let request = ExecSessionRequest {
            handle: EnvironmentHandle {
                id: "node-1".to_string(),
                workspace_root: "/workspace".to_string(),
                metadata: Value::Null,
            },
            subject_id: "task:TASK-1175".to_string(),
            workflow_ref: Some("coding".to_string()),
            dispatch_input: None,
            workflow_id: Some("workflow-1".to_string()),
            execution_fence: Some(fence.clone()),
        };
        request.validate_execution_fence(true).unwrap();
        let decoded: ExecSessionRequest =
            serde_json::from_value(serde_json::to_value(&request).unwrap()).unwrap();
        assert_eq!(decoded.execution_fence, Some(fence));
    }
}

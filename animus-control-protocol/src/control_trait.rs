//! The [`ControlSurface`] trait — what the daemon implements and what CLI /
//! MCP / WebAPI clients call.
//!
//! Transports never call `ControlSurface` directly across the process
//! boundary — they dispatch JSON-RPC frames whose method names are the
//! constants in [`crate::method`] and whose `params` deserialize into the
//! `*Request` types in [`crate::types`]. The trait is the in-process
//! representation the daemon-side implementation lives behind, and it lets
//! the daemon's own tests drive the surface without spinning up a transport.
//!
//! Each domain method is fallible with [`ControlError`]. Streaming methods
//! return a [`Pin<Box<dyn Stream<Item = T> + Send>>`] just like
//! [`animus_subject_protocol::EventStream`] and the runtime forwards each
//! stream item as a corresponding `*/<event>` notification.

use std::pin::Pin;

use async_trait::async_trait;
use futures_core::Stream;

use animus_subject_protocol::{Subject, SubjectChangedEvent};

use crate::error::ControlError;
use crate::types::{
    AgentCancelRequest, AgentRunRequest, AgentRunResult, AgentStatus, AgentStatusRequest,
    DaemonAgentsResponse, DaemonEventsRequest, DaemonHealthResponse, DaemonLogEntry,
    DaemonLogsRequest, DaemonRunEvent, DaemonStatusResponse, PluginBrowseRequest,
    PluginCallRequest, PluginCallResponse, PluginInfo, PluginInfoRequest, PluginInstallRequest,
    PluginInstallResponse, PluginListRequest, PluginListResponse, PluginPingRequest,
    PluginPingResponse, PluginSearchRequest, PluginSearchResponse, PluginUninstallRequest,
    PluginUpdateRequest, PluginUpdateResponse, ProjectInfo, ProjectInitRequest,
    ProjectSetupRequest, ProjectStatusResponse, QueueDropRequest, QueueEnqueueRequest, QueueEntry,
    QueueHoldRequest, QueueListRequest, QueueListResponse, QueueReleaseRequest,
    QueueReorderRequest, QueueStats, SubjectCreateRequest, SubjectGetRequest, SubjectListRequest,
    SubjectListResponse, SubjectNextRequest, SubjectNextResponse, SubjectStatusRequest,
    SubjectUpdateRequest, SubjectWatchRequest, Unit, WorkflowCancelRequest, WorkflowExecuteRequest,
    WorkflowGetRequest, WorkflowListRequest, WorkflowListResponse, WorkflowPauseRequest,
    WorkflowResumeRequest, WorkflowRun, WorkflowRunRequest, WorkflowRunStart,
};

/// Stream of [`SubjectChangedEvent`] items returned by
/// [`ControlSurface::subject_watch`].
pub type SubjectWatchStream = Pin<Box<dyn Stream<Item = SubjectChangedEvent> + Send>>;

/// Stream of [`DaemonRunEvent`] items returned by
/// [`ControlSurface::daemon_events`].
pub type DaemonEventStream = Pin<Box<dyn Stream<Item = DaemonRunEvent> + Send>>;

/// Stream of [`DaemonLogEntry`] items returned by
/// [`ControlSurface::daemon_logs`].
pub type DaemonLogStream = Pin<Box<dyn Stream<Item = Result<DaemonLogEntry, ControlError>> + Send>>;

/// What the Animus daemon exposes to the CLI / MCP / WebAPI bindings.
///
/// Implementations live in the daemon (or in-process test fakes); transports
/// adapt JSON-RPC frames into trait calls and back. The trait is intentionally
/// flat — one method per protocol verb — so the wire shape and the Rust shape
/// stay in lockstep.
#[async_trait]
pub trait ControlSurface: Send + Sync + 'static {
    // ----- Subject ----------------------------------------------------

    /// List subjects matching the request.
    async fn subject_list(
        &self,
        request: SubjectListRequest,
    ) -> Result<SubjectListResponse, ControlError>;

    /// Fetch a single subject by id.
    async fn subject_get(&self, request: SubjectGetRequest) -> Result<Subject, ControlError>;

    /// Create a new subject in a writable backend.
    async fn subject_create(&self, request: SubjectCreateRequest) -> Result<Subject, ControlError>;

    /// Apply a patch to a subject.
    async fn subject_update(&self, request: SubjectUpdateRequest) -> Result<Subject, ControlError>;

    /// Return the next ready subject for dispatch.
    async fn subject_next(
        &self,
        request: SubjectNextRequest,
    ) -> Result<SubjectNextResponse, ControlError>;

    /// Set the normalized status of a subject.
    async fn subject_status(&self, request: SubjectStatusRequest) -> Result<Subject, ControlError>;

    /// Open a stream of subject change events.
    async fn subject_watch(
        &self,
        request: SubjectWatchRequest,
    ) -> Result<SubjectWatchStream, ControlError>;

    // ----- Plugin -----------------------------------------------------

    /// List installed plugins.
    async fn plugin_list(
        &self,
        request: PluginListRequest,
    ) -> Result<PluginListResponse, ControlError>;

    /// Return detailed info for a single installed plugin.
    async fn plugin_info(&self, request: PluginInfoRequest) -> Result<PluginInfo, ControlError>;

    /// Install a plugin.
    async fn plugin_install(
        &self,
        request: PluginInstallRequest,
    ) -> Result<PluginInstallResponse, ControlError>;

    /// Uninstall a plugin.
    async fn plugin_uninstall(&self, request: PluginUninstallRequest)
        -> Result<Unit, ControlError>;

    /// Lifecycle-ping a plugin.
    async fn plugin_ping(
        &self,
        request: PluginPingRequest,
    ) -> Result<PluginPingResponse, ControlError>;

    /// Opaque pass-through invocation of a plugin domain method.
    async fn plugin_call(
        &self,
        request: PluginCallRequest,
    ) -> Result<PluginCallResponse, ControlError>;

    /// Search the plugin registry.
    async fn plugin_search(
        &self,
        request: PluginSearchRequest,
    ) -> Result<PluginSearchResponse, ControlError>;

    /// Browse plugin registry entries.
    async fn plugin_browse(
        &self,
        request: PluginBrowseRequest,
    ) -> Result<PluginSearchResponse, ControlError>;

    /// Check / apply plugin upgrades.
    async fn plugin_update(
        &self,
        request: PluginUpdateRequest,
    ) -> Result<PluginUpdateResponse, ControlError>;

    // ----- Daemon -----------------------------------------------------

    /// Return process status.
    async fn daemon_status(&self) -> Result<DaemonStatusResponse, ControlError>;

    /// Return per-plugin health.
    async fn daemon_health(&self) -> Result<DaemonHealthResponse, ControlError>;

    /// Start the daemon. No-op if running.
    async fn daemon_start(&self) -> Result<Unit, ControlError>;

    /// Stop the daemon.
    async fn daemon_stop(&self) -> Result<Unit, ControlError>;

    /// Restart the daemon.
    async fn daemon_restart(&self) -> Result<Unit, ControlError>;

    /// List currently active agents.
    async fn daemon_agents(&self) -> Result<DaemonAgentsResponse, ControlError>;

    /// Open a stream of daemon run events.
    async fn daemon_events(
        &self,
        request: DaemonEventsRequest,
    ) -> Result<DaemonEventStream, ControlError>;

    /// Open a streaming log query.
    async fn daemon_logs(
        &self,
        request: DaemonLogsRequest,
    ) -> Result<DaemonLogStream, ControlError>;

    // ----- Workflow ---------------------------------------------------

    /// List workflow runs.
    async fn workflow_list(
        &self,
        request: WorkflowListRequest,
    ) -> Result<WorkflowListResponse, ControlError>;

    /// Return a single workflow run by id.
    async fn workflow_get(&self, request: WorkflowGetRequest) -> Result<WorkflowRun, ControlError>;

    /// Start a new workflow run for a task.
    async fn workflow_run(
        &self,
        request: WorkflowRunRequest,
    ) -> Result<WorkflowRunStart, ControlError>;

    /// Execute an ad-hoc workflow without binding to a task.
    async fn workflow_execute(
        &self,
        request: WorkflowExecuteRequest,
    ) -> Result<WorkflowRunStart, ControlError>;

    /// Pause an in-flight workflow run.
    async fn workflow_pause(&self, request: WorkflowPauseRequest) -> Result<Unit, ControlError>;

    /// Resume a paused workflow run.
    async fn workflow_resume(&self, request: WorkflowResumeRequest) -> Result<Unit, ControlError>;

    /// Cancel a workflow run.
    async fn workflow_cancel(&self, request: WorkflowCancelRequest) -> Result<Unit, ControlError>;

    // ----- Agent ------------------------------------------------------

    /// Start a one-shot agent session.
    async fn agent_run(&self, request: AgentRunRequest) -> Result<AgentRunResult, ControlError>;

    /// Fetch live status for an agent session.
    async fn agent_status(&self, request: AgentStatusRequest) -> Result<AgentStatus, ControlError>;

    /// Cancel an in-flight agent session.
    async fn agent_cancel(&self, request: AgentCancelRequest) -> Result<Unit, ControlError>;

    // ----- Queue ------------------------------------------------------

    /// List queue entries.
    async fn queue_list(
        &self,
        request: QueueListRequest,
    ) -> Result<QueueListResponse, ControlError>;

    /// Enqueue a new dispatch entry.
    async fn queue_enqueue(&self, request: QueueEnqueueRequest)
        -> Result<QueueEntry, ControlError>;

    /// Drop an entry from the queue.
    async fn queue_drop(&self, request: QueueDropRequest) -> Result<Unit, ControlError>;

    /// Hold an entry (mark non-dispatchable).
    async fn queue_hold(&self, request: QueueHoldRequest) -> Result<Unit, ControlError>;

    /// Release a held entry.
    async fn queue_release(&self, request: QueueReleaseRequest) -> Result<Unit, ControlError>;

    /// Reorder a queue entry.
    async fn queue_reorder(&self, request: QueueReorderRequest) -> Result<Unit, ControlError>;

    /// Return queue stats.
    async fn queue_stats(&self) -> Result<QueueStats, ControlError>;

    // ----- Project ----------------------------------------------------

    /// Initialize Animus in the current working directory.
    async fn project_init(&self, request: ProjectInitRequest) -> Result<ProjectInfo, ControlError>;

    /// Finalize project setup.
    async fn project_setup(
        &self,
        request: ProjectSetupRequest,
    ) -> Result<ProjectInfo, ControlError>;

    /// Snapshot project workflow / task / run state.
    async fn project_status(&self) -> Result<ProjectStatusResponse, ControlError>;
}

//! Wire format for the Animus daemon control RPC.
//!
//! This crate defines the surface that CLI / MCP / WebAPI clients use to ask
//! the Animus daemon to do something. It is the *inbound* counterpart to the
//! outbound plugin protocols (`animus-subject-protocol`,
//! `animus-provider-protocol`, `animus-trigger-protocol`,
//! `animus-log-storage-protocol`).
//!
//! See `spec.md` §13 for the full protocol specification, and
//! `docs/architecture/control-protocol.md` in `animus-cli` for the design
//! decisions behind the surface.
//!
//! # Layout
//!
//! - [`method`] — JSON-RPC method-name constants.
//! - [`types`] — request and response shapes for every method.
//! - [`error`] — [`ControlError`] and its JSON-RPC mapping.
//! - [`control_trait`] — the [`ControlSurface`] trait, plus the streaming
//!   type aliases that mirror the runtime's notification channels.
//!
//! # Stability
//!
//! Method names, request/response shapes, and the `ControlSurface` trait are
//! versioned together with the rest of `animus-protocol`. v0.1.3 is the
//! initial release of this crate; future protocol revisions extend
//! shapes with optional fields rather than breaking them.
//!
//! # Plugin authoring vs. control protocol
//!
//! Plugin authors typically depend on `animus-subject-protocol` or
//! `animus-provider-protocol`; they do not need to depend on this crate.
//! This crate is for transport authors (CLI, MCP, WebAPI, future REST /
//! gRPC bindings) and for the daemon-side implementation behind those
//! transports.

#![warn(missing_docs)]

pub mod control_trait;
pub mod error;
pub mod method;
pub mod types;

#[cfg(feature = "client")]
pub mod client;

#[cfg(feature = "client")]
pub use client::ControlClient;

// Re-export the core types at the crate root so downstream code can write
// `animus_control_protocol::ControlSurface` instead of reaching into the
// modules directly.
pub use animus_actor::{Actor, CLAIM_ADMIN};
pub use control_trait::{ControlSurface, DaemonEventStream, DaemonLogStream, SubjectWatchStream};
pub use error::ControlError;
pub use types::{
    AgentCancelRequest, AgentInfo, AgentLifecycle, AgentRunRequest, AgentRunResult, AgentStatus,
    AgentStatusRequest, AgentUsage, DaemonAgentsResponse, DaemonEventsRequest,
    DaemonHealthResponse, DaemonHealthStatus, DaemonLogEntry, DaemonLogsRequest, DaemonRunEvent,
    DaemonStatusResponse, PluginBrowseRequest, PluginCallRequest, PluginCallResponse, PluginHealth,
    PluginInfo, PluginInfoRequest, PluginInstallRequest, PluginInstallResponse, PluginListRequest,
    PluginListResponse, PluginPingRequest, PluginPingResponse, PluginRegistryEntry,
    PluginSearchRequest, PluginSearchResponse, PluginUninstallRequest, PluginUpdateEntry,
    PluginUpdateRequest, PluginUpdateResponse, PluginWarning, ProjectInfo, ProjectInitRequest,
    ProjectSetupRequest, ProjectStatusResponse, QueueDropRequest, QueueEnqueueRequest, QueueEntry,
    QueueEntryStatus, QueueHoldRequest, QueueListRequest, QueueListResponse, QueueReleaseRequest,
    QueueReorderPosition, QueueReorderRequest, QueueStats, SubjectCreateRequest, SubjectGetRequest,
    SubjectListRequest, SubjectListResponse, SubjectNextRequest, SubjectNextResponse,
    SubjectStatusRequest, SubjectUpdateRequest, SubjectWatchRequest, Unit, WorkflowCancelRequest,
    WorkflowExecuteRequest, WorkflowGetRequest, WorkflowListRequest, WorkflowListResponse,
    WorkflowPauseRequest, WorkflowResumeRequest, WorkflowRun, WorkflowRunRequest, WorkflowRunStart,
    WorkflowRunSummary, WorkflowStatus,
};

//! Cross-process [`ControlClient`] for the Animus daemon control socket.
//!
//! [`ControlSurface`](crate::ControlSurface) is the in-process Rust trait the
//! daemon implements. Transport plugins (`animus-transport-http`,
//! `animus-transport-graphql`, future gRPC, future WebSocket) live in a
//! separate process and need to *call* that surface over IPC. Prior to v0.1.8
//! every transport reinvented ~260 LOC of NDJSON JSON-RPC plumbing on top of
//! the Unix control socket. This module consolidates that into one client
//! with one method per [`ControlSurface`](crate::ControlSurface) verb.
//!
//! # Framing
//!
//! NDJSON (newline-delimited JSON), not length-prefixed frames. The control
//! socket is line-oriented to match the rest of the protocol (`stdio` plugins
//! also use NDJSON), so transports can debug it with `nc -U`. Each request is
//! one JSON object terminated by `\n`; each response is one JSON object
//! terminated by `\n`. Multi-line JSON is not supported.
//!
//! # Concurrency model
//!
//! A single `UnixStream` is wrapped in [`tokio::sync::Mutex`] so concurrent
//! callers serialize through the socket. Because every request carries a
//! monotonic id and the server replies in order, the client also tolerates
//! out-of-order replies — it loops reading lines until it sees a response with
//! the matching id, discarding mismatched frames. The current daemon never
//! interleaves responses, so the loop exits on the first read.
//!
//! # Why feature-gated
//!
//! Plugin authors who only consume the wire constants and request/response
//! types do not need [`tokio`] or [`anyhow`]. The `client` feature gates the
//! tokio/Unix-socket transport so the core crate stays tiny for those callers.

#![cfg(feature = "client")]

#[cfg(unix)]
mod imp {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use animus_plugin_protocol::{RpcError, RpcRequest, RpcResponse};
    use anyhow::{anyhow, Context, Result};
    use serde::{de::DeserializeOwned, Serialize};
    use serde_json::Value;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixStream;
    use tokio::sync::Mutex;

    use crate::method;
    use crate::types::{
        AgentCancelRequest, AgentRunRequest, AgentRunResult, AgentStatus, AgentStatusRequest,
        DaemonAgentsResponse, DaemonHealthResponse, DaemonLogEntry, DaemonLogsRequest,
        DaemonStatusResponse, PluginBrowseRequest, PluginCallRequest, PluginCallResponse,
        PluginInfo, PluginInfoRequest, PluginInstallRequest, PluginInstallResponse,
        PluginListRequest, PluginListResponse, PluginPingRequest, PluginPingResponse,
        PluginSearchRequest, PluginSearchResponse, PluginUninstallRequest, PluginUpdateRequest,
        PluginUpdateResponse, ProjectInfo, ProjectInitRequest, ProjectSetupRequest,
        ProjectStatusResponse, QueueDropRequest, QueueEnqueueRequest, QueueEntry, QueueHoldRequest,
        QueueListRequest, QueueListResponse, QueueReleaseRequest, QueueReorderRequest, QueueStats,
        SubjectCreateRequest, SubjectGetRequest, SubjectListRequest, SubjectListResponse,
        SubjectNextRequest, SubjectNextResponse, SubjectStatusRequest, SubjectUpdateRequest, Unit,
        WorkflowCancelRequest, WorkflowExecuteRequest, WorkflowGetRequest, WorkflowListRequest,
        WorkflowListResponse, WorkflowPauseRequest, WorkflowResumeRequest, WorkflowRun,
        WorkflowRunRequest, WorkflowRunStart,
    };
    use animus_subject_protocol::Subject;

    /// Async JSON-RPC client over the Animus daemon control socket.
    ///
    /// Wraps a single [`UnixStream`] in a [`Mutex`] so concurrent callers
    /// serialize through one socket. Spawn multiple instances if you need
    /// real parallelism.
    pub struct ControlClient {
        stream: Mutex<UnixStream>,
        next_id: AtomicU64,
        socket_path: PathBuf,
    }

    impl ControlClient {
        /// Open a new client by connecting to the daemon's control socket.
        pub async fn connect(socket_path: &Path) -> Result<Self> {
            let stream = UnixStream::connect(socket_path)
                .await
                .with_context(|| format!("connect {}", socket_path.display()))?;
            Ok(Self {
                stream: Mutex::new(stream),
                next_id: AtomicU64::new(1),
                socket_path: socket_path.to_path_buf(),
            })
        }

        /// Socket path this client is bound to.
        pub fn socket_path(&self) -> &Path {
            &self.socket_path
        }

        async fn rpc<P: Serialize, R: DeserializeOwned>(
            &self,
            method: &str,
            params: P,
        ) -> Result<R> {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            let params_value = serde_json::to_value(params)
                .with_context(|| format!("serialize params for {method}"))?;
            let request = RpcRequest {
                jsonrpc: "2.0".into(),
                id: Some(Value::from(id)),
                method: method.to_string(),
                params: Some(params_value),
            };
            let mut frame = serde_json::to_string(&request)?;
            frame.push('\n');

            let mut stream = self.stream.lock().await;
            stream
                .write_all(frame.as_bytes())
                .await
                .with_context(|| format!("write {method} to {}", self.socket_path.display()))?;
            stream
                .flush()
                .await
                .with_context(|| format!("flush {method}"))?;

            let mut reader = BufReader::new(&mut *stream);
            let mut line = String::new();
            loop {
                line.clear();
                let bytes = reader
                    .read_line(&mut line)
                    .await
                    .with_context(|| format!("read response for {method}"))?;
                if bytes == 0 {
                    return Err(anyhow!(
                        "control socket closed while awaiting response for {method}"
                    ));
                }
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let response: RpcResponse = serde_json::from_str(trimmed)
                    .with_context(|| format!("decode response for {method}"))?;
                if response.id != Some(Value::from(id)) {
                    continue;
                }
                if let Some(err) = response.error {
                    return Err(rpc_error_to_anyhow(method, err));
                }
                let result = response
                    .result
                    .ok_or_else(|| anyhow!("{method}: response missing result"))?;
                return serde_json::from_value(result)
                    .with_context(|| format!("decode {method} result"));
            }
        }

        async fn rpc_no_params<R: DeserializeOwned>(&self, method: &str) -> Result<R> {
            self.rpc::<Value, R>(method, Value::Null).await
        }

        // ---- Subject ------------------------------------------------------

        /// Call `subject/list`.
        pub async fn subject_list(
            &self,
            request: SubjectListRequest,
        ) -> Result<SubjectListResponse> {
            self.rpc(method::METHOD_SUBJECT_LIST, request).await
        }

        /// Call `subject/get`.
        pub async fn subject_get(&self, request: SubjectGetRequest) -> Result<Subject> {
            self.rpc(method::METHOD_SUBJECT_GET, request).await
        }

        /// Call `subject/create`.
        pub async fn subject_create(&self, request: SubjectCreateRequest) -> Result<Subject> {
            self.rpc(method::METHOD_SUBJECT_CREATE, request).await
        }

        /// Call `subject/update`.
        pub async fn subject_update(&self, request: SubjectUpdateRequest) -> Result<Subject> {
            self.rpc(method::METHOD_SUBJECT_UPDATE, request).await
        }

        /// Call `subject/next`.
        pub async fn subject_next(
            &self,
            request: SubjectNextRequest,
        ) -> Result<SubjectNextResponse> {
            self.rpc(method::METHOD_SUBJECT_NEXT, request).await
        }

        /// Call `subject/status`.
        pub async fn subject_status(&self, request: SubjectStatusRequest) -> Result<Subject> {
            self.rpc(method::METHOD_SUBJECT_STATUS, request).await
        }

        // ---- Daemon -------------------------------------------------------

        /// Call `daemon/status`.
        pub async fn daemon_status(&self) -> Result<DaemonStatusResponse> {
            self.rpc_no_params(method::METHOD_DAEMON_STATUS).await
        }

        /// Call `daemon/health`.
        pub async fn daemon_health(&self) -> Result<DaemonHealthResponse> {
            self.rpc_no_params(method::METHOD_DAEMON_HEALTH).await
        }

        /// Call `daemon/start`.
        pub async fn daemon_start(&self) -> Result<Unit> {
            self.rpc_no_params(method::METHOD_DAEMON_START).await
        }

        /// Call `daemon/stop`.
        pub async fn daemon_stop(&self) -> Result<Unit> {
            self.rpc_no_params(method::METHOD_DAEMON_STOP).await
        }

        /// Call `daemon/restart`.
        pub async fn daemon_restart(&self) -> Result<Unit> {
            self.rpc_no_params(method::METHOD_DAEMON_RESTART).await
        }

        /// Call `daemon/agents`.
        pub async fn daemon_agents(&self) -> Result<DaemonAgentsResponse> {
            self.rpc_no_params(method::METHOD_DAEMON_AGENTS).await
        }

        /// Call `daemon/logs`. The historical-only call: hosts that need a
        /// streaming follow tail should open the JSON-RPC notification surface
        /// directly. The optional `limit` argument is folded into the
        /// request params for clients that want to cap the historical window
        /// without restructuring the request body.
        pub async fn daemon_logs(
            &self,
            request: DaemonLogsRequest,
            limit: usize,
        ) -> Result<Vec<DaemonLogEntry>> {
            let mut params = serde_json::to_value(&request)?;
            if let Some(obj) = params.as_object_mut() {
                obj.insert("limit".into(), Value::from(limit));
            }
            self.rpc(method::METHOD_DAEMON_LOGS, params).await
        }

        // ---- Workflow -----------------------------------------------------

        /// Call `workflow/list`.
        pub async fn workflow_list(
            &self,
            request: WorkflowListRequest,
        ) -> Result<WorkflowListResponse> {
            self.rpc(method::METHOD_WORKFLOW_LIST, request).await
        }

        /// Call `workflow/get`.
        pub async fn workflow_get(&self, request: WorkflowGetRequest) -> Result<WorkflowRun> {
            self.rpc(method::METHOD_WORKFLOW_GET, request).await
        }

        /// Call `workflow/run`.
        pub async fn workflow_run(&self, request: WorkflowRunRequest) -> Result<WorkflowRunStart> {
            self.rpc(method::METHOD_WORKFLOW_RUN, request).await
        }

        /// Call `workflow/execute`.
        pub async fn workflow_execute(
            &self,
            request: WorkflowExecuteRequest,
        ) -> Result<WorkflowRunStart> {
            self.rpc(method::METHOD_WORKFLOW_EXECUTE, request).await
        }

        /// Call `workflow/pause`.
        pub async fn workflow_pause(&self, request: WorkflowPauseRequest) -> Result<Unit> {
            self.rpc(method::METHOD_WORKFLOW_PAUSE, request).await
        }

        /// Call `workflow/resume`.
        pub async fn workflow_resume(&self, request: WorkflowResumeRequest) -> Result<Unit> {
            self.rpc(method::METHOD_WORKFLOW_RESUME, request).await
        }

        /// Call `workflow/cancel`.
        pub async fn workflow_cancel(&self, request: WorkflowCancelRequest) -> Result<Unit> {
            self.rpc(method::METHOD_WORKFLOW_CANCEL, request).await
        }

        // ---- Queue --------------------------------------------------------

        /// Call `queue/list`.
        pub async fn queue_list(&self, request: QueueListRequest) -> Result<QueueListResponse> {
            self.rpc(method::METHOD_QUEUE_LIST, request).await
        }

        /// Call `queue/stats`.
        pub async fn queue_stats(&self) -> Result<QueueStats> {
            self.rpc_no_params(method::METHOD_QUEUE_STATS).await
        }

        /// Call `queue/enqueue`.
        pub async fn queue_enqueue(&self, request: QueueEnqueueRequest) -> Result<QueueEntry> {
            self.rpc(method::METHOD_QUEUE_ENQUEUE, request).await
        }

        /// Call `queue/reorder`.
        pub async fn queue_reorder(&self, request: QueueReorderRequest) -> Result<Unit> {
            self.rpc(method::METHOD_QUEUE_REORDER, request).await
        }

        /// Call `queue/hold`.
        pub async fn queue_hold(&self, request: QueueHoldRequest) -> Result<Unit> {
            self.rpc(method::METHOD_QUEUE_HOLD, request).await
        }

        /// Call `queue/release`.
        pub async fn queue_release(&self, request: QueueReleaseRequest) -> Result<Unit> {
            self.rpc(method::METHOD_QUEUE_RELEASE, request).await
        }

        /// Call `queue/drop`.
        pub async fn queue_drop(&self, request: QueueDropRequest) -> Result<Unit> {
            self.rpc(method::METHOD_QUEUE_DROP, request).await
        }

        // ---- Plugin -------------------------------------------------------

        /// Call `plugin/list`.
        pub async fn plugin_list(&self, request: PluginListRequest) -> Result<PluginListResponse> {
            self.rpc(method::METHOD_PLUGIN_LIST, request).await
        }

        /// Call `plugin/info`.
        pub async fn plugin_info(&self, request: PluginInfoRequest) -> Result<PluginInfo> {
            self.rpc(method::METHOD_PLUGIN_INFO, request).await
        }

        /// Call `plugin/install`.
        pub async fn plugin_install(
            &self,
            request: PluginInstallRequest,
        ) -> Result<PluginInstallResponse> {
            self.rpc(method::METHOD_PLUGIN_INSTALL, request).await
        }

        /// Call `plugin/uninstall`.
        pub async fn plugin_uninstall(&self, request: PluginUninstallRequest) -> Result<Unit> {
            self.rpc(method::METHOD_PLUGIN_UNINSTALL, request).await
        }

        /// Call `plugin/ping`.
        pub async fn plugin_ping(&self, request: PluginPingRequest) -> Result<PluginPingResponse> {
            self.rpc(method::METHOD_PLUGIN_PING, request).await
        }

        /// Call `plugin/call`.
        pub async fn plugin_call(&self, request: PluginCallRequest) -> Result<PluginCallResponse> {
            self.rpc(method::METHOD_PLUGIN_CALL, request).await
        }

        /// Call `plugin/search`.
        pub async fn plugin_search(
            &self,
            request: PluginSearchRequest,
        ) -> Result<PluginSearchResponse> {
            self.rpc(method::METHOD_PLUGIN_SEARCH, request).await
        }

        /// Call `plugin/browse`.
        pub async fn plugin_browse(
            &self,
            request: PluginBrowseRequest,
        ) -> Result<PluginSearchResponse> {
            self.rpc(method::METHOD_PLUGIN_BROWSE, request).await
        }

        /// Call `plugin/update`.
        pub async fn plugin_update(
            &self,
            request: PluginUpdateRequest,
        ) -> Result<PluginUpdateResponse> {
            self.rpc(method::METHOD_PLUGIN_UPDATE, request).await
        }

        // ---- Agent --------------------------------------------------------

        /// Call `agent/run`.
        pub async fn agent_run(&self, request: AgentRunRequest) -> Result<AgentRunResult> {
            self.rpc(method::METHOD_AGENT_RUN, request).await
        }

        /// Call `agent/status`.
        pub async fn agent_status(&self, request: AgentStatusRequest) -> Result<AgentStatus> {
            self.rpc(method::METHOD_AGENT_STATUS, request).await
        }

        /// Call `agent/cancel`.
        pub async fn agent_cancel(&self, request: AgentCancelRequest) -> Result<Unit> {
            self.rpc(method::METHOD_AGENT_CANCEL, request).await
        }

        // ---- Project ------------------------------------------------------

        /// Call `project/init`.
        pub async fn project_init(&self, request: ProjectInitRequest) -> Result<ProjectInfo> {
            self.rpc(method::METHOD_PROJECT_INIT, request).await
        }

        /// Call `project/setup`.
        pub async fn project_setup(&self, request: ProjectSetupRequest) -> Result<ProjectInfo> {
            self.rpc(method::METHOD_PROJECT_SETUP, request).await
        }

        /// Call `project/status`.
        pub async fn project_status(&self) -> Result<ProjectStatusResponse> {
            self.rpc_no_params(method::METHOD_PROJECT_STATUS).await
        }
    }

    fn rpc_error_to_anyhow(method: &str, err: RpcError) -> anyhow::Error {
        anyhow!("{method} failed (code {}): {}", err.code, err.message)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::types::WorkflowStatus;
        use chrono::Utc;
        use tempfile::TempDir;
        use tokio::net::UnixListener;

        /// Spawn a one-shot mock control server on a tempdir Unix socket and
        /// reply once with `result_value`. Returns the socket path so the test
        /// can connect a `ControlClient` to it.
        async fn spawn_mock_once(
            tmp: &TempDir,
            method: &'static str,
            result_value: serde_json::Value,
        ) -> std::path::PathBuf {
            let socket = tmp.path().join("control.sock");
            let listener = UnixListener::bind(&socket).expect("bind socket");
            let socket_path = socket.clone();
            tokio::spawn(async move {
                let (mut conn, _) = listener.accept().await.expect("accept");
                let (read_half, mut write_half) = conn.split();
                let mut reader = BufReader::new(read_half);
                let mut line = String::new();
                reader.read_line(&mut line).await.expect("read request");
                let req: RpcRequest = serde_json::from_str(line.trim()).expect("decode request");
                assert_eq!(req.method, method);
                let resp = RpcResponse {
                    jsonrpc: "2.0".into(),
                    id: req.id,
                    result: Some(result_value),
                    error: None,
                };
                let mut frame = serde_json::to_string(&resp).unwrap();
                frame.push('\n');
                write_half.write_all(frame.as_bytes()).await.unwrap();
                write_half.flush().await.unwrap();
            });
            socket_path
        }

        #[tokio::test]
        async fn round_trip_daemon_status() {
            let tmp = TempDir::new().unwrap();
            let socket = spawn_mock_once(
                &tmp,
                method::METHOD_DAEMON_STATUS,
                serde_json::json!({
                    "running": true,
                    "pid": 4242u32,
                    "uptime_seconds": 90u64,
                    "version": "0.1.8"
                }),
            )
            .await;
            let client = ControlClient::connect(&socket).await.unwrap();
            let status = client.daemon_status().await.unwrap();
            assert!(status.running);
            assert_eq!(status.pid, Some(4242));
            assert_eq!(status.version.as_deref(), Some("0.1.8"));
        }

        #[tokio::test]
        async fn round_trip_workflow_run() {
            let tmp = TempDir::new().unwrap();
            let started = Utc::now();
            let socket = spawn_mock_once(
                &tmp,
                method::METHOD_WORKFLOW_RUN,
                serde_json::json!({
                    "workflow_id": "wf-1",
                    "status": "running",
                    "started_at": started.to_rfc3339()
                }),
            )
            .await;
            let client = ControlClient::connect(&socket).await.unwrap();
            let resp = client
                .workflow_run(WorkflowRunRequest {
                    task_id: "task:T1".into(),
                    definition: Some("default".into()),
                    params: Default::default(),
                })
                .await
                .unwrap();
            assert_eq!(resp.workflow_id, "wf-1");
            assert!(matches!(resp.status, WorkflowStatus::Running));
        }

        #[tokio::test]
        async fn rpc_error_surface() {
            let tmp = TempDir::new().unwrap();
            let socket = tmp.path().join("control.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            let socket_path = socket.clone();
            tokio::spawn(async move {
                let (mut conn, _) = listener.accept().await.unwrap();
                let (read_half, mut write_half) = conn.split();
                let mut reader = BufReader::new(read_half);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let req: RpcRequest = serde_json::from_str(line.trim()).unwrap();
                let resp = RpcResponse {
                    jsonrpc: "2.0".into(),
                    id: req.id,
                    result: None,
                    error: Some(RpcError {
                        code: -32601,
                        message: "no such queue entry".into(),
                        data: None,
                    }),
                };
                let mut frame = serde_json::to_string(&resp).unwrap();
                frame.push('\n');
                write_half.write_all(frame.as_bytes()).await.unwrap();
                write_half.flush().await.unwrap();
            });
            let client = ControlClient::connect(&socket_path).await.unwrap();
            let err = client
                .queue_drop(QueueDropRequest {
                    id: "missing".into(),
                })
                .await
                .unwrap_err();
            let msg = format!("{err}");
            assert!(msg.contains("-32601"), "msg = {msg}");
            assert!(msg.contains("no such queue entry"), "msg = {msg}");
        }
    }
}

#[cfg(unix)]
pub use imp::ControlClient;

#[cfg(not(unix))]
mod stub {
    use std::path::Path;

    /// Stub `ControlClient` for non-Unix targets. The Animus daemon control
    /// surface is currently Unix-socket only; on Windows the daemon will use
    /// a named pipe (`\\.\pipe\animus-<repo-scope>`, reserved for a future
    /// protocol revision). Until that lands, calling [`ControlClient::connect`]
    /// on a non-Unix target returns an error.
    pub struct ControlClient;

    impl ControlClient {
        /// Always errors on non-Unix targets.
        pub async fn connect(_socket_path: &Path) -> anyhow::Result<Self> {
            Err(anyhow::anyhow!(
                "animus-control-protocol ControlClient is Unix-only in v0.1.8; Windows named-pipe support is reserved for a future release"
            ))
        }
    }
}

#[cfg(not(unix))]
pub use stub::ControlClient;

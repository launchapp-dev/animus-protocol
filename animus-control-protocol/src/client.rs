//! Cross-process [`ControlClient`] for the Animus daemon control socket.
//!
//! [`ControlSurface`](crate::ControlSurface) is the in-process Rust trait the
//! daemon implements. Transport plugins (`animus-transport-http`,
//! `animus-transport-graphql`, future gRPC, future WebSocket) live in a
//! separate process and need to *call* that surface over IPC. Prior to v0.1.8
//! every transport reinvented ~260 LOC of NDJSON JSON-RPC plumbing on top of
//! the Unix control socket. v0.1.8 consolidated unary RPCs into one client;
//! v0.1.9 extends that with server-streaming subscriptions
//! ([`Subscription`]) so the graphql / http transports can power
//! `subject/watch`, `daemon/events`, and `daemon/logs --follow` without
//! re-implementing the demultiplexer.
//!
//! # Framing
//!
//! NDJSON (newline-delimited JSON), not length-prefixed frames. The control
//! socket is line-oriented to match the rest of the protocol (`stdio` plugins
//! also use NDJSON), so transports can debug it with `nc -U`. Each request is
//! one JSON object terminated by `\n`; each response or notification is one
//! JSON object terminated by `\n`. Multi-line JSON is not supported.
//!
//! # Concurrency model
//!
//! A single [`UnixStream`] is split into read/write halves. A dedicated
//! background task reads frames, demultiplexes them by either matching `id`
//! (responses → pending oneshot) or by reading `params.id` (notifications →
//! subscription mpsc). Writes go through an [`Arc<Mutex<WriteHalf>>`] so
//! concurrent RPCs and subscription cancellations serialize one frame at a
//! time without blocking the read loop.
//!
//! Why a background reader instead of the v0.1.8 inline read loop: the
//! daemon's streaming methods emit notifications interleaved with unrelated
//! RPC responses on the same socket. An inline loop would either block the
//! socket forever once a subscription is open, or would lose notifications
//! that arrive while a different RPC is pending.
//!
//! # Why feature-gated
//!
//! Plugin authors who only consume the wire constants and request/response
//! types do not need [`tokio`] or [`anyhow`]. The `client` feature gates the
//! tokio/Unix-socket transport so the core crate stays tiny for those callers.

#![cfg(feature = "client")]

#[cfg(unix)]
mod imp {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use animus_plugin_protocol::{RpcError, RpcRequest, RpcResponse};
    use anyhow::{anyhow, Context, Result};
    use serde::{de::DeserializeOwned, Serialize};
    use serde_json::Value;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
    use tokio::net::UnixStream;
    use tokio::sync::{mpsc, oneshot, Mutex};
    use tokio::task::JoinHandle;

    use crate::method;
    use crate::types::{
        AgentCancelRequest, AgentRunRequest, AgentRunResult, AgentStatus, AgentStatusRequest,
        DaemonAgentsResponse, DaemonEventsRequest, DaemonHealthResponse, DaemonLogEntry,
        DaemonLogsRequest, DaemonRunEvent, DaemonStatusResponse, PluginBrowseRequest,
        PluginCallRequest, PluginCallResponse, PluginInfo, PluginInfoRequest, PluginInstallRequest,
        PluginInstallResponse, PluginListRequest, PluginListResponse, PluginPingRequest,
        PluginPingResponse, PluginSearchRequest, PluginSearchResponse, PluginUninstallRequest,
        PluginUpdateRequest, PluginUpdateResponse, ProjectInfo, ProjectInitRequest,
        ProjectSetupRequest, ProjectStatusResponse, QueueDropRequest, QueueEnqueueRequest,
        QueueEntry, QueueHoldRequest, QueueListRequest, QueueListResponse, QueueReleaseRequest,
        QueueReorderRequest, QueueStats, SubjectCreateRequest, SubjectGetRequest,
        SubjectListRequest, SubjectListResponse, SubjectNextRequest, SubjectNextResponse,
        SubjectStatusRequest, SubjectUpdateRequest, SubjectWatchRequest, Unit,
        WorkflowCancelRequest, WorkflowEvent, WorkflowEventsRequest, WorkflowExecuteRequest,
        WorkflowGetRequest, WorkflowListRequest, WorkflowListResponse, WorkflowPauseRequest,
        WorkflowResumeRequest, WorkflowRun, WorkflowRunRequest, WorkflowRunStart,
    };
    use animus_subject_protocol::{Subject, SubjectChangedEvent};

    /// Async JSON-RPC client over the Animus daemon control socket.
    ///
    /// Wraps a single [`UnixStream`] split into read/write halves. A
    /// background task continuously reads frames and demultiplexes them
    /// to pending RPC oneshots or open [`Subscription`] channels.
    pub struct ControlClient {
        write_half: Arc<Mutex<OwnedWriteHalf>>,
        next_id: AtomicU64,
        socket_path: PathBuf,
        pending: Arc<Mutex<HashMap<u64, oneshot::Sender<RpcResponse>>>>,
        subscriptions: Arc<Mutex<HashMap<u64, mpsc::Sender<Value>>>>,
        reader_task: Mutex<Option<JoinHandle<()>>>,
    }

    impl ControlClient {
        /// Open a new client by connecting to the daemon's control socket.
        pub async fn connect(socket_path: &Path) -> Result<Self> {
            let stream = UnixStream::connect(socket_path)
                .await
                .with_context(|| format!("connect {}", socket_path.display()))?;
            let (read_half, write_half) = stream.into_split();

            let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<RpcResponse>>>> =
                Arc::new(Mutex::new(HashMap::new()));
            let subscriptions: Arc<Mutex<HashMap<u64, mpsc::Sender<Value>>>> =
                Arc::new(Mutex::new(HashMap::new()));

            let reader_pending = Arc::clone(&pending);
            let reader_subs = Arc::clone(&subscriptions);
            let reader_task = tokio::spawn(reader_loop(read_half, reader_pending, reader_subs));

            Ok(Self {
                write_half: Arc::new(Mutex::new(write_half)),
                next_id: AtomicU64::new(1),
                socket_path: socket_path.to_path_buf(),
                pending,
                subscriptions,
                reader_task: Mutex::new(Some(reader_task)),
            })
        }

        /// Socket path this client is bound to.
        pub fn socket_path(&self) -> &Path {
            &self.socket_path
        }

        async fn write_frame<T: Serialize>(&self, frame: &T) -> Result<()> {
            let mut bytes = serde_json::to_vec(frame).context("serialize frame")?;
            bytes.push(b'\n');
            let mut guard = self.write_half.lock().await;
            guard.write_all(&bytes).await.context("write frame")?;
            guard.flush().await.context("flush frame")?;
            Ok(())
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

            let (tx, rx) = oneshot::channel();
            {
                let mut guard = self.pending.lock().await;
                guard.insert(id, tx);
            }

            if let Err(err) = self.write_frame(&request).await {
                let mut guard = self.pending.lock().await;
                guard.remove(&id);
                return Err(
                    err.context(format!("write {method} to {}", self.socket_path.display()))
                );
            }

            let response = rx.await.map_err(|_| {
                anyhow!("control socket closed while awaiting response for {method}")
            })?;
            if let Some(err) = response.error {
                return Err(rpc_error_to_anyhow(method, err));
            }
            let result = response
                .result
                .ok_or_else(|| anyhow!("{method}: response missing result"))?;
            serde_json::from_value(result).with_context(|| format!("decode {method} result"))
        }

        async fn rpc_no_params<R: DeserializeOwned>(&self, method: &str) -> Result<R> {
            self.rpc::<Value, R>(method, Value::Null).await
        }

        // Internal helper: open a server-streaming subscription. The daemon
        // returns `{"watching": true}` as the ack; subsequent frames arrive
        // as JSON-RPC notifications with `params.id` echoing the original
        // request id and `params.data` carrying the per-event payload.
        async fn subscribe<P, T>(
            &self,
            method: &str,
            params: P,
            buffer: usize,
        ) -> Result<Subscription<T>>
        where
            P: Serialize,
            T: DeserializeOwned + Send + 'static,
        {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            let params_value = serde_json::to_value(params)
                .with_context(|| format!("serialize params for {method}"))?;
            let request = RpcRequest {
                jsonrpc: "2.0".into(),
                id: Some(Value::from(id)),
                method: method.to_string(),
                params: Some(params_value),
            };

            let (resp_tx, resp_rx) = oneshot::channel();
            let (event_tx, event_rx) = mpsc::channel::<Value>(buffer);
            {
                let mut guard = self.pending.lock().await;
                guard.insert(id, resp_tx);
            }
            {
                let mut guard = self.subscriptions.lock().await;
                guard.insert(id, event_tx);
            }

            if let Err(err) = self.write_frame(&request).await {
                self.pending.lock().await.remove(&id);
                self.subscriptions.lock().await.remove(&id);
                return Err(
                    err.context(format!("write {method} to {}", self.socket_path.display()))
                );
            }

            let response = resp_rx.await.map_err(|_| {
                anyhow!("control socket closed while awaiting subscription ack for {method}")
            })?;
            if let Some(err) = response.error {
                self.subscriptions.lock().await.remove(&id);
                return Err(rpc_error_to_anyhow(method, err));
            }
            // The ack body is `{"watching": true}` (or any other JSON the
            // daemon chooses to send); the client treats it as opaque
            // confirmation and starts pulling notifications.

            let (item_tx, item_rx) = mpsc::channel::<T>(buffer);
            let decode_task = tokio::spawn(decode_loop(event_rx, item_tx));

            Ok(Subscription {
                id,
                method: method.to_string(),
                receiver: item_rx,
                write_half: Arc::clone(&self.write_half),
                pending: Arc::clone(&self.pending),
                subscriptions: Arc::clone(&self.subscriptions),
                decode_task: Some(decode_task),
            })
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

        /// Open a `subject/watch` subscription.
        ///
        /// Returns a [`Subscription`] yielding [`SubjectChangedEvent`] items
        /// for every subject change the daemon observes. Dropping the
        /// subscription cancels the stream by sending a `$/cancelRequest`
        /// notification and closing the local channel.
        pub async fn subject_watch(
            &self,
            request: SubjectWatchRequest,
        ) -> Result<Subscription<SubjectChangedEvent>> {
            self.subscribe(method::METHOD_SUBJECT_WATCH, request, 64)
                .await
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
        /// streaming follow tail should use [`Self::daemon_logs_follow`]
        /// instead. The optional `limit` argument is folded into the
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

        /// Open a `daemon/events` subscription.
        ///
        /// Returns a [`Subscription`] yielding [`DaemonRunEvent`] items as
        /// the daemon emits them.
        pub async fn daemon_events(
            &self,
            request: DaemonEventsRequest,
        ) -> Result<Subscription<DaemonRunEvent>> {
            self.subscribe(method::METHOD_DAEMON_EVENTS, request, 256)
                .await
        }

        /// Open a `daemon/logs` subscription with follow-mode enabled.
        ///
        /// Forces `request.follow = true` so the daemon streams new entries
        /// after the historical tail completes. Use [`Self::daemon_logs`]
        /// for a one-shot historical fetch.
        pub async fn daemon_logs_follow(
            &self,
            mut request: DaemonLogsRequest,
        ) -> Result<Subscription<DaemonLogEntry>> {
            request.follow = true;
            self.subscribe(method::METHOD_DAEMON_LOGS, request, 256)
                .await
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

        /// Open a `workflow/events` subscription (v0.1.10).
        ///
        /// Returns a [`Subscription`] yielding [`WorkflowEvent`] items as the
        /// daemon emits workflow-scoped events. The request optionally filters
        /// by `workflow_id` and/or event `kinds`; see [`WorkflowEventsRequest`].
        ///
        /// NOTE: daemons that have not yet implemented `workflow/events` will
        /// return a `method_not_found` error on subscribe. Clients SHOULD
        /// degrade to `daemon_events` + kind filtering in that case.
        pub async fn workflow_events(
            &self,
            request: WorkflowEventsRequest,
        ) -> Result<Subscription<WorkflowEvent>> {
            self.subscribe(method::METHOD_WORKFLOW_EVENTS, request, 256)
                .await
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

    impl Drop for ControlClient {
        fn drop(&mut self) {
            if let Ok(mut guard) = self.reader_task.try_lock() {
                if let Some(handle) = guard.take() {
                    handle.abort();
                }
            }
        }
    }

    /// Server-streaming subscription handle.
    ///
    /// Yields decoded events via [`Subscription::recv`]. Dropping the handle
    /// best-effort cancels the underlying stream by sending a
    /// `$/cancelRequest` notification with the originating request id; the
    /// daemon also tears the stream down when the socket closes, so the
    /// notification is a forward-compat hint, not a correctness requirement.
    pub struct Subscription<T> {
        id: u64,
        method: String,
        receiver: mpsc::Receiver<T>,
        write_half: Arc<Mutex<OwnedWriteHalf>>,
        pending: Arc<Mutex<HashMap<u64, oneshot::Sender<RpcResponse>>>>,
        subscriptions: Arc<Mutex<HashMap<u64, mpsc::Sender<Value>>>>,
        decode_task: Option<JoinHandle<()>>,
    }

    impl<T> Subscription<T> {
        /// Wait for the next event from the stream.
        ///
        /// Returns `None` when the server closes the stream or the
        /// underlying socket is dropped.
        pub async fn recv(&mut self) -> Option<T> {
            self.receiver.recv().await
        }

        /// Originating request id. Useful for transports that want to
        /// correlate notifications with a higher-level subscription handle.
        pub fn request_id(&self) -> u64 {
            self.id
        }

        /// Method name of the originating subscribe call.
        pub fn method(&self) -> &str {
            &self.method
        }
    }

    impl<T> Drop for Subscription<T> {
        fn drop(&mut self) {
            // Stop accepting decoded events first so the decoder task can
            // exit on its own.
            self.receiver.close();
            if let Some(task) = self.decode_task.take() {
                task.abort();
            }

            // Issue a best-effort cancellation. We use $/cancelRequest per
            // the spec §14.3; the daemon currently relies on connection
            // close to tear streams down but accepts the notification as a
            // forward-compat path.
            let write_half = Arc::clone(&self.write_half);
            let pending = Arc::clone(&self.pending);
            let subs = Arc::clone(&self.subscriptions);
            let id = self.id;
            tokio::spawn(async move {
                let notification = serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": "$/cancelRequest",
                    "params": { "id": id },
                });
                if let Ok(bytes) = serde_json::to_vec(&notification) {
                    let mut framed = bytes;
                    framed.push(b'\n');
                    if let Ok(mut guard) = write_half.try_lock() {
                        let _ = guard.write_all(&framed).await;
                        let _ = guard.flush().await;
                    } else {
                        let mut guard = write_half.lock().await;
                        let _ = guard.write_all(&framed).await;
                        let _ = guard.flush().await;
                    }
                }
                pending.lock().await.remove(&id);
                subs.lock().await.remove(&id);
            });
        }
    }

    async fn reader_loop(
        read_half: OwnedReadHalf,
        pending: Arc<Mutex<HashMap<u64, oneshot::Sender<RpcResponse>>>>,
        subscriptions: Arc<Mutex<HashMap<u64, mpsc::Sender<Value>>>>,
    ) {
        let mut reader = BufReader::new(read_half);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let frame: Value = match serde_json::from_str(trimmed) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let frame_obj = match frame.as_object() {
                Some(o) => o,
                None => continue,
            };

            // Notification frames carry `method` and no `id` at the top
            // level; the originating request id is echoed inside
            // `params.id` per the daemon's wire convention.
            let has_method = frame_obj.contains_key("method");
            let top_id = frame_obj.get("id");

            if has_method && (top_id.is_none() || top_id == Some(&Value::Null)) {
                let method_str = frame_obj.get("method").and_then(|m| m.as_str());
                // `subscription/closed` is terminal: drop the subscription's
                // mpsc sender so the client's `recv()` returns `None` on the
                // next pull. The notification itself is not delivered as a
                // stream item — that would break the per-stream item-type
                // contract. v0.1.12.
                if method_str == Some(method::NOTIFICATION_SUBSCRIPTION_CLOSED) {
                    if let Some(params) = frame_obj.get("params") {
                        if let Some(sub_id_val) = params.get("id") {
                            if let Some(sub_id) = value_as_u64(sub_id_val) {
                                let mut guard = subscriptions.lock().await;
                                guard.remove(&sub_id);
                            }
                        }
                    }
                    continue;
                }
                if let Some(params) = frame_obj.get("params") {
                    if let Some(sub_id_val) = params.get("id") {
                        if let Some(sub_id) = value_as_u64(sub_id_val) {
                            let data = params.get("data").cloned().unwrap_or(Value::Null);
                            let sender = {
                                let guard = subscriptions.lock().await;
                                guard.get(&sub_id).cloned()
                            };
                            if let Some(sender) = sender {
                                let _ = sender.send(data).await;
                            }
                        }
                    }
                }
                continue;
            }

            // Otherwise treat as a response.
            let response: RpcResponse = match serde_json::from_value(frame) {
                Ok(r) => r,
                Err(_) => continue,
            };
            let resp_id = match response.id.as_ref().and_then(value_as_u64) {
                Some(id) => id,
                None => continue,
            };
            let sender = {
                let mut guard = pending.lock().await;
                guard.remove(&resp_id)
            };
            if let Some(tx) = sender {
                let _ = tx.send(response);
            }
        }

        // Socket closed: drain any in-flight callers so they don't hang.
        {
            let mut guard = pending.lock().await;
            guard.clear();
        }
        {
            let mut guard = subscriptions.lock().await;
            guard.clear();
        }
    }

    async fn decode_loop<T: DeserializeOwned + Send + 'static>(
        mut input: mpsc::Receiver<Value>,
        output: mpsc::Sender<T>,
    ) {
        while let Some(value) = input.recv().await {
            match serde_json::from_value::<T>(value) {
                Ok(item) => {
                    if output.send(item).await.is_err() {
                        break;
                    }
                }
                Err(_) => continue,
            }
        }
    }

    fn value_as_u64(v: &Value) -> Option<u64> {
        match v {
            Value::Number(n) => n.as_u64(),
            Value::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    fn rpc_error_to_anyhow(method: &str, err: RpcError) -> anyhow::Error {
        anyhow!("{method} failed (code {}): {}", err.code, err.message)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::types::WorkflowStatus;
        use animus_plugin_protocol::RpcNotification;
        use animus_subject_protocol::{
            ChangeKind, Subject, SubjectChangedEvent, SubjectId, SubjectStatus,
        };
        use chrono::Utc;
        use std::sync::Arc;
        use tempfile::TempDir;
        use tokio::net::UnixListener;
        use tokio::sync::Mutex as AsyncMutex;
        use tokio::time::{timeout, Duration};

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
                // Keep the connection alive briefly so the client reader
                // task has time to consume the frame before the socket
                // half-closes.
                tokio::time::sleep(Duration::from_millis(50)).await;
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
                    "version": "0.1.9"
                }),
            )
            .await;
            let client = ControlClient::connect(&socket).await.unwrap();
            let status = client.daemon_status().await.unwrap();
            assert!(status.running);
            assert_eq!(status.pid, Some(4242));
            assert_eq!(status.version.as_deref(), Some("0.1.9"));
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
                    actor: None,
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
                tokio::time::sleep(Duration::from_millis(50)).await;
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

        fn sample_event(seq: u64) -> SubjectChangedEvent {
            let id = SubjectId::new(format!("native:T{seq}"));
            SubjectChangedEvent {
                id: id.clone(),
                change_kind: ChangeKind::Updated,
                subject: Subject {
                    id,
                    kind: "task".into(),
                    title: format!("task {seq}"),
                    description: None,
                    status: SubjectStatus::Ready,
                    priority: None,
                    assignee: None,
                    labels: vec![],
                    parent: None,
                    children: vec![],
                    url: None,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    custom: Default::default(),
                    native_status: None,
                    status_metadata: serde_json::Value::Null,
                    attachments: vec![],
                },
                previous_native_status: None,
                previous_dispatch_label: None,
            }
        }

        #[tokio::test]
        async fn subject_watch_streams_events_and_cancels_on_drop() {
            let tmp = TempDir::new().unwrap();
            let socket = tmp.path().join("control.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            let socket_path = socket.clone();

            let received_cancel = Arc::new(AsyncMutex::new(false));
            let received_cancel_clone = Arc::clone(&received_cancel);

            tokio::spawn(async move {
                let (conn, _) = listener.accept().await.unwrap();
                let (read_half, write_half) = conn.into_split();
                let write_half = Arc::new(AsyncMutex::new(write_half));
                let mut reader = BufReader::new(read_half);

                // Read the initial subject/watch request.
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let req: RpcRequest = serde_json::from_str(line.trim()).unwrap();
                assert_eq!(req.method, method::METHOD_SUBJECT_WATCH);
                let req_id = req.id.clone();

                // Send the ack.
                let ack = RpcResponse {
                    jsonrpc: "2.0".into(),
                    id: req_id.clone(),
                    result: Some(serde_json::json!({ "watching": true })),
                    error: None,
                };
                {
                    let mut g = write_half.lock().await;
                    let mut frame = serde_json::to_vec(&ack).unwrap();
                    frame.push(b'\n');
                    g.write_all(&frame).await.unwrap();
                    g.flush().await.unwrap();
                }

                // Stream 3 events.
                for i in 0..3 {
                    let event = sample_event(i);
                    let notification = RpcNotification::new(
                        method::NOTIFICATION_SUBJECT_CHANGED.to_string(),
                        Some(serde_json::json!({
                            "id": req_id,
                            "data": event,
                        })),
                    );
                    let mut g = write_half.lock().await;
                    let mut frame = serde_json::to_vec(&notification).unwrap();
                    frame.push(b'\n');
                    g.write_all(&frame).await.unwrap();
                    g.flush().await.unwrap();
                    drop(g);
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }

                // Wait for the cancel notification from the client.
                let mut cancel_line = String::new();
                if timeout(Duration::from_secs(2), reader.read_line(&mut cancel_line))
                    .await
                    .is_ok()
                {
                    if let Ok(v) = serde_json::from_str::<Value>(cancel_line.trim()) {
                        if v.get("method").and_then(|m| m.as_str()) == Some("$/cancelRequest") {
                            *received_cancel_clone.lock().await = true;
                        }
                    }
                }
            });

            let client = ControlClient::connect(&socket_path).await.unwrap();
            let mut sub = client
                .subject_watch(SubjectWatchRequest::default())
                .await
                .unwrap();

            for expected in 0..3 {
                let ev = timeout(Duration::from_secs(2), sub.recv())
                    .await
                    .expect("recv timeout")
                    .expect("stream closed early");
                assert_eq!(ev.id.as_str(), format!("native:T{expected}"));
            }

            drop(sub);

            // Give the cancel task time to run.
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert!(
                *received_cancel.lock().await,
                "server did not see $/cancelRequest"
            );
        }

        #[tokio::test]
        async fn workflow_events_streams_events_and_cancels_on_drop() {
            let tmp = TempDir::new().unwrap();
            let socket = tmp.path().join("control.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            let socket_path = socket.clone();

            let received_cancel = Arc::new(AsyncMutex::new(false));
            let received_cancel_clone = Arc::clone(&received_cancel);

            tokio::spawn(async move {
                let (conn, _) = listener.accept().await.unwrap();
                let (read_half, write_half) = conn.into_split();
                let write_half = Arc::new(AsyncMutex::new(write_half));
                let mut reader = BufReader::new(read_half);

                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let req: RpcRequest = serde_json::from_str(line.trim()).unwrap();
                assert_eq!(req.method, method::METHOD_WORKFLOW_EVENTS);
                let req_id = req.id.clone();

                let ack = RpcResponse {
                    jsonrpc: "2.0".into(),
                    id: req_id.clone(),
                    result: Some(serde_json::json!({ "watching": true })),
                    error: None,
                };
                {
                    let mut g = write_half.lock().await;
                    let mut frame = serde_json::to_vec(&ack).unwrap();
                    frame.push(b'\n');
                    g.write_all(&frame).await.unwrap();
                    g.flush().await.unwrap();
                }

                for i in 0..3u64 {
                    let kind = if i % 2 == 0 {
                        "phase_started"
                    } else {
                        "phase_completed"
                    };
                    let event = serde_json::json!({
                        "workflow_id": "wf-1",
                        "kind": kind,
                        "payload": { "seq": i },
                        "occurred_at": Utc::now().to_rfc3339(),
                    });
                    let notification = RpcNotification::new(
                        method::NOTIFICATION_WORKFLOW_EVENT.to_string(),
                        Some(serde_json::json!({
                            "id": req_id,
                            "data": event,
                        })),
                    );
                    let mut g = write_half.lock().await;
                    let mut frame = serde_json::to_vec(&notification).unwrap();
                    frame.push(b'\n');
                    g.write_all(&frame).await.unwrap();
                    g.flush().await.unwrap();
                    drop(g);
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }

                let mut cancel_line = String::new();
                if timeout(Duration::from_secs(2), reader.read_line(&mut cancel_line))
                    .await
                    .is_ok()
                {
                    if let Ok(v) = serde_json::from_str::<Value>(cancel_line.trim()) {
                        if v.get("method").and_then(|m| m.as_str()) == Some("$/cancelRequest") {
                            *received_cancel_clone.lock().await = true;
                        }
                    }
                }
            });

            let client = ControlClient::connect(&socket_path).await.unwrap();
            let mut sub = client
                .workflow_events(WorkflowEventsRequest::default())
                .await
                .unwrap();

            for expected in 0..3u64 {
                let ev = timeout(Duration::from_secs(2), sub.recv())
                    .await
                    .expect("recv timeout")
                    .expect("stream closed early");
                assert_eq!(ev.workflow_id, "wf-1");
                assert_eq!(
                    ev.payload.get("seq").and_then(|v| v.as_u64()),
                    Some(expected)
                );
                let expected_kind = if expected % 2 == 0 {
                    "phase_started"
                } else {
                    "phase_completed"
                };
                assert_eq!(ev.kind, expected_kind);
            }

            drop(sub);

            tokio::time::sleep(Duration::from_millis(200)).await;
            assert!(
                *received_cancel.lock().await,
                "server did not see $/cancelRequest"
            );
        }

        #[tokio::test]
        async fn workflow_events_filters_by_workflow_id_in_request() {
            let tmp = TempDir::new().unwrap();
            let socket = tmp.path().join("control.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            let socket_path = socket.clone();

            let captured_params: Arc<AsyncMutex<Option<Value>>> = Arc::new(AsyncMutex::new(None));
            let captured_clone = Arc::clone(&captured_params);

            tokio::spawn(async move {
                let (conn, _) = listener.accept().await.unwrap();
                let (read_half, mut write_half) = conn.into_split();
                let mut reader = BufReader::new(read_half);

                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let req: RpcRequest = serde_json::from_str(line.trim()).unwrap();
                assert_eq!(req.method, method::METHOD_WORKFLOW_EVENTS);
                *captured_clone.lock().await = req.params.clone();

                let ack = RpcResponse {
                    jsonrpc: "2.0".into(),
                    id: req.id.clone(),
                    result: Some(serde_json::json!({ "watching": true })),
                    error: None,
                };
                let mut frame = serde_json::to_vec(&ack).unwrap();
                frame.push(b'\n');
                write_half.write_all(&frame).await.unwrap();
                write_half.flush().await.unwrap();
                tokio::time::sleep(Duration::from_millis(50)).await;
            });

            let client = ControlClient::connect(&socket_path).await.unwrap();
            let _sub = client
                .workflow_events(WorkflowEventsRequest {
                    workflow_id: Some("wf-42".into()),
                    kinds: Some(vec!["phase_completed".into(), "workflow_failed".into()]),
                })
                .await
                .unwrap();

            tokio::time::sleep(Duration::from_millis(50)).await;
            let params = captured_params.lock().await.clone().expect("no params");
            assert_eq!(
                params.get("workflow_id").and_then(|v| v.as_str()),
                Some("wf-42")
            );
            let kinds = params
                .get("kinds")
                .and_then(|v| v.as_array())
                .expect("kinds not an array");
            let kinds: Vec<&str> = kinds.iter().filter_map(|v| v.as_str()).collect();
            assert_eq!(kinds, vec!["phase_completed", "workflow_failed"]);
        }

        #[tokio::test]
        async fn concurrent_rpc_and_subscriptions() {
            let tmp = TempDir::new().unwrap();
            let socket = tmp.path().join("control.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            let socket_path = socket.clone();

            tokio::spawn(async move {
                let (conn, _) = listener.accept().await.unwrap();
                let (read_half, write_half) = conn.into_split();
                let write_half = Arc::new(AsyncMutex::new(write_half));
                let mut reader = BufReader::new(read_half);

                let mut watch_ids: Vec<Value> = Vec::new();

                // Read 3 requests: two subject/watch, one queue/stats.
                for _ in 0..3 {
                    let mut line = String::new();
                    reader.read_line(&mut line).await.unwrap();
                    let req: RpcRequest = serde_json::from_str(line.trim()).unwrap();
                    let resp = match req.method.as_str() {
                        m if m == method::METHOD_SUBJECT_WATCH => {
                            watch_ids.push(req.id.clone().unwrap());
                            RpcResponse {
                                jsonrpc: "2.0".into(),
                                id: req.id,
                                result: Some(serde_json::json!({ "watching": true })),
                                error: None,
                            }
                        }
                        m if m == method::METHOD_QUEUE_STATS => RpcResponse {
                            jsonrpc: "2.0".into(),
                            id: req.id,
                            result: Some(serde_json::json!({
                                "ready": 1u64,
                                "held": 0u64,
                                "in_flight": 0u64,
                                "done_recent": 0u64,
                                "dropped_recent": 0u64,
                            })),
                            error: None,
                        },
                        other => panic!("unexpected method {other}"),
                    };
                    let mut g = write_half.lock().await;
                    let mut frame = serde_json::to_vec(&resp).unwrap();
                    frame.push(b'\n');
                    g.write_all(&frame).await.unwrap();
                    g.flush().await.unwrap();
                }

                // Push one event to each watch subscription.
                for (i, id) in watch_ids.iter().enumerate() {
                    let event = sample_event(100 + i as u64);
                    let notification = RpcNotification::new(
                        method::NOTIFICATION_SUBJECT_CHANGED.to_string(),
                        Some(serde_json::json!({
                            "id": id,
                            "data": event,
                        })),
                    );
                    let mut g = write_half.lock().await;
                    let mut frame = serde_json::to_vec(&notification).unwrap();
                    frame.push(b'\n');
                    g.write_all(&frame).await.unwrap();
                    g.flush().await.unwrap();
                }

                // Keep socket alive so the client can drain.
                tokio::time::sleep(Duration::from_millis(500)).await;
            });

            let client = ControlClient::connect(&socket_path).await.unwrap();
            let mut sub_a = client
                .subject_watch(SubjectWatchRequest::default())
                .await
                .unwrap();
            let mut sub_b = client
                .subject_watch(SubjectWatchRequest::default())
                .await
                .unwrap();
            let stats = client.queue_stats().await.unwrap();
            assert_eq!(stats.ready, 1);

            let ev_a = timeout(Duration::from_secs(2), sub_a.recv())
                .await
                .unwrap()
                .unwrap();
            let ev_b = timeout(Duration::from_secs(2), sub_b.recv())
                .await
                .unwrap()
                .unwrap();

            let ids: std::collections::HashSet<String> =
                [ev_a.id.as_str().to_string(), ev_b.id.as_str().to_string()]
                    .into_iter()
                    .collect();
            assert!(ids.contains("native:T100"));
            assert!(ids.contains("native:T101"));
        }

        #[tokio::test]
        async fn client_subscription_recv_returns_none_after_server_emits_subscription_closed() {
            let tmp = TempDir::new().unwrap();
            let socket = tmp.path().join("control.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            let socket_path = socket.clone();

            tokio::spawn(async move {
                let (conn, _) = listener.accept().await.unwrap();
                let (read_half, write_half) = conn.into_split();
                let write_half = Arc::new(AsyncMutex::new(write_half));
                let mut reader = BufReader::new(read_half);

                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let req: RpcRequest = serde_json::from_str(line.trim()).unwrap();
                assert_eq!(req.method, method::METHOD_SUBJECT_WATCH);
                let req_id = req.id.clone();

                let ack = RpcResponse {
                    jsonrpc: "2.0".into(),
                    id: req_id.clone(),
                    result: Some(serde_json::json!({ "watching": true })),
                    error: None,
                };
                {
                    let mut g = write_half.lock().await;
                    let mut frame = serde_json::to_vec(&ack).unwrap();
                    frame.push(b'\n');
                    g.write_all(&frame).await.unwrap();
                    g.flush().await.unwrap();
                }

                let event = sample_event(7);
                let notification = RpcNotification::new(
                    method::NOTIFICATION_SUBJECT_CHANGED.to_string(),
                    Some(serde_json::json!({
                        "id": req_id,
                        "data": event,
                    })),
                );
                {
                    let mut g = write_half.lock().await;
                    let mut frame = serde_json::to_vec(&notification).unwrap();
                    frame.push(b'\n');
                    g.write_all(&frame).await.unwrap();
                    g.flush().await.unwrap();
                }

                tokio::time::sleep(Duration::from_millis(20)).await;

                let closed = RpcNotification::new(
                    method::NOTIFICATION_SUBSCRIPTION_CLOSED.to_string(),
                    Some(serde_json::json!({
                        "id": req_id,
                        "reason": "subscription budget exceeded",
                    })),
                );
                {
                    let mut g = write_half.lock().await;
                    let mut frame = serde_json::to_vec(&closed).unwrap();
                    frame.push(b'\n');
                    g.write_all(&frame).await.unwrap();
                    g.flush().await.unwrap();
                }

                tokio::time::sleep(Duration::from_millis(500)).await;
            });

            let client = ControlClient::connect(&socket_path).await.unwrap();
            let mut sub = client
                .subject_watch(SubjectWatchRequest::default())
                .await
                .unwrap();

            let first = timeout(Duration::from_secs(2), sub.recv())
                .await
                .expect("recv timeout")
                .expect("stream closed before first event");
            assert_eq!(first.id.as_str(), "native:T7");

            let terminal = timeout(Duration::from_secs(2), sub.recv())
                .await
                .expect("recv timeout waiting for terminal close");
            assert!(
                terminal.is_none(),
                "subscription/closed should terminate the stream with None"
            );
        }
    }
}

#[cfg(unix)]
pub use imp::{ControlClient, Subscription};

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
                "animus-control-protocol ControlClient is Unix-only in v0.1.9; Windows named-pipe support is reserved for a future release"
            ))
        }
    }

    /// Stub `Subscription` for non-Unix targets.
    pub struct Subscription<T>(std::marker::PhantomData<T>);
}

#[cfg(not(unix))]
pub use stub::{ControlClient, Subscription};

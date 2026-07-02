//! Generic Animus stdio plugin shell.
//!
//! [`Plugin`] is a typed builder + driver any plugin kind can use to skip the
//! JSON-RPC stdio boilerplate. The author wires plugin identity, registers
//! typed request and notification handlers, and optionally provides
//! `on_init` / `on_shutdown` hooks. [`Plugin::run`] owns the stdin/stdout
//! loop end-to-end: framing, `initialize` + `shutdown` lifecycle, error
//! envelopes, in-flight tracking, `$/cancelRequest`, and outbound
//! notification fan-out via a clonable [`Notifier`].
//!
//! This shell is protocol-compliant with `animus-plugin-protocol`. It does
//! not retrofit the existing `run_provider` entry point — author-owned
//! plugins remain free to keep their hand-rolled loops. New plugin kinds
//! and future rewrites of the existing Rust plugins are the intended
//! consumers.

use std::collections::HashMap;
use std::future::Future;
use std::io::{self, IsTerminal, Write};
use std::pin::Pin;
use std::sync::Arc;

use animus_plugin_protocol::{
    error_codes, HealthCheckResult, HealthStatus, InitializeParams, InitializeResult,
    KindCapability, PluginCapabilities, PluginInfo, PluginManifest, RpcError, RpcNotification,
    RpcRequest, RpcResponse, PROTOCOL_VERSION,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, Notify, RwLock};
use tokio::task::JoinSet;

/// Cooperative cancellation token used by the shell to signal that an
/// in-flight request has been cancelled via `$/cancelRequest`.
///
/// Clonable handle around an `Arc<Notify>` + atomic flag. Handlers should
/// `select!` on [`CancellationToken::cancelled`] or poll
/// [`CancellationToken::is_cancelled`] at progress checkpoints.
#[derive(Clone, Default)]
pub struct CancellationToken {
    inner: Arc<CancellationInner>,
}

#[derive(Default)]
struct CancellationInner {
    cancelled: std::sync::atomic::AtomicBool,
    notify: Notify,
}

impl CancellationToken {
    /// Build a fresh, uncancelled token.
    pub fn new() -> Self {
        Self::default()
    }

    /// Trip the token. Idempotent.
    pub fn cancel(&self) {
        self.inner
            .cancelled
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.inner.notify.notify_waiters();
    }

    /// `true` once any clone of this token has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.inner
            .cancelled
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Resolve once the token has been cancelled. Cheap to await — backed by
    /// a `tokio::sync::Notify`.
    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        // Subscribe before re-checking to avoid the lost-wake race.
        let notified = self.inner.notify.notified();
        if self.is_cancelled() {
            return;
        }
        notified.await;
    }
}

/// Clonable handle a plugin author uses to push notifications back to the host.
///
/// Notifications are JSON-RPC `RpcNotification` frames with no id. The
/// underlying writer is reference counted and shared with the shell's
/// response path so all frames serialize through the same mutex.
#[derive(Clone)]
pub struct Notifier {
    writer: Arc<Mutex<DynStdout>>,
}

impl Notifier {
    /// Send a notification with the given method and structured params.
    pub async fn notify(&self, method: impl Into<String>, params: Value) {
        let frame = RpcNotification::new(method, Some(params));
        write_frame(&self.writer, &frame).await;
    }

    /// Send a notification carrying a typed Serialize payload. Drops the
    /// frame on serialization failure after logging via `tracing::warn!`.
    pub async fn notify_typed<T: Serialize>(&self, method: impl Into<String>, params: &T) {
        match serde_json::to_value(params) {
            Ok(value) => self.notify(method, value).await,
            Err(error) => {
                tracing::warn!(%error, "plugin notification: failed to serialize params");
            }
        }
    }
}

/// Per-request context handed to a registered method handler.
///
/// Carries the originating request id (for streaming notifications that need
/// it), a [`Notifier`] for fanning out events on subscription channels, and
/// a [`CancellationToken`] tripped by the shell when the host sends
/// `$/cancelRequest` for this id.
pub struct MethodContext {
    /// Originating request id (echoed back on the response by the shell).
    pub request_id: Option<Value>,
    /// Clonable notifier handle.
    pub notifier: Notifier,
    /// Cancellation token tripped when the host issues `$/cancelRequest`
    /// for this request id.
    pub cancellation: CancellationToken,
    /// When `true`, the shell keeps the cancellation token registered in its
    /// in-flight table after the handler returns so a later `$/cancelRequest`
    /// for the same id can still trip the clone the handler retained.
    /// Streaming subscriptions (`subject/watch`, `log_storage/tail`,
    /// `trigger/watch`) that return an ack and continue emitting
    /// notifications should call [`MethodContext::keep_cancellation`].
    keep_cancellation: Arc<std::sync::atomic::AtomicBool>,
}

impl MethodContext {
    /// Opt the cancellation token into outliving the handler return. The
    /// shell will not remove it from its in-flight table when this handler
    /// returns, so a later `$/cancelRequest` for the same id can still trip
    /// the clone the handler retained for its long-lived subscription.
    pub fn keep_cancellation(&self) {
        self.keep_cancellation
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Context handed to the `on_init` hook.
///
/// The shell forwards the deserialized [`InitializeParams::init_extensions`]
/// map plus the resolved `host_info` and `capabilities` so plugin authors
/// can wire `project_binding`, `memory_mcp_stdio_command`, and similar
/// per-plugin-kind extensions without hand-rolling the parse path.
pub struct InitContext {
    /// Forwarded from `InitializeParams::init_extensions`.
    pub init_extensions: HashMap<String, Value>,
    /// Forwarded from `InitializeParams.host_info`.
    pub host_info: animus_plugin_protocol::HostInfo,
    /// Forwarded from `InitializeParams.capabilities`.
    pub host_capabilities: animus_plugin_protocol::HostCapabilities,
    /// Clonable notifier handle — same writer the request handlers will use.
    pub notifier: Notifier,
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type MethodHandler =
    Arc<dyn Fn(Value, MethodContext) -> BoxFuture<Result<Value, RpcError>> + Send + Sync>;
type NotificationHandler = Arc<dyn Fn(Value, Notifier) -> BoxFuture<()> + Send + Sync>;
type InitHook = Arc<dyn Fn(InitContext) -> BoxFuture<Result<(), RpcError>> + Send + Sync>;
type ShutdownHook = Arc<dyn Fn() -> BoxFuture<()> + Send + Sync>;
type HealthHook = Arc<dyn Fn() -> BoxFuture<Result<HealthCheckResult, RpcError>> + Send + Sync>;

/// Builder + driver for an Animus stdio plugin.
///
/// Workflow:
///
/// ```ignore
/// Plugin::new("my-plugin", env!("CARGO_PKG_VERSION"), PLUGIN_KIND_QUEUE)
///     .description("Reference queue plugin")
///     .methods(["queue/enqueue", "queue/lease"])
///     .kind_capability(KIND, KindCapability { crate_version: "0.2.0".into(), extra: Value::Null })
///     .on_init(|ctx| async move { /* parse init_extensions, build state */ Ok(()) })
///     .register_method::<MyReq, MyResp, _, _>("queue/enqueue", |req, ctx| async move {
///         Ok(MyResp { ... })
///     })
///     .run()
///     .await
/// ```
pub struct Plugin {
    name: String,
    version: String,
    plugin_kind: String,
    description: String,
    protocol_version: String,
    methods: Vec<String>,
    streaming: bool,
    progress: bool,
    cancellation: bool,
    projections: Vec<String>,
    subject_kinds: Vec<String>,
    mcp_tools: Vec<animus_plugin_protocol::McpTool>,
    env_required: Vec<animus_plugin_protocol::EnvRequirement>,
    notification_buffer_size: Option<usize>,
    kind_capabilities: HashMap<String, KindCapability>,

    method_handlers: HashMap<String, MethodHandler>,
    notification_handlers: HashMap<String, NotificationHandler>,
    on_init: Option<InitHook>,
    on_shutdown: Option<ShutdownHook>,
    on_health: Option<HealthHook>,
}

impl Plugin {
    /// Start a new plugin shell with the given identity.
    ///
    /// `name` is the published plugin name. `version` is the plugin semver
    /// (typically `env!("CARGO_PKG_VERSION")`). `plugin_kind` is one of the
    /// `PLUGIN_KIND_*` constants from `animus-plugin-protocol`.
    pub fn new(
        name: impl Into<String>,
        version: impl Into<String>,
        plugin_kind: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            plugin_kind: plugin_kind.into(),
            description: String::new(),
            protocol_version: PROTOCOL_VERSION.to_string(),
            methods: Vec::new(),
            streaming: false,
            progress: false,
            cancellation: false,
            projections: Vec::new(),
            subject_kinds: Vec::new(),
            mcp_tools: Vec::new(),
            env_required: Vec::new(),
            notification_buffer_size: None,
            kind_capabilities: HashMap::new(),
            method_handlers: HashMap::new(),
            notification_handlers: HashMap::new(),
            on_init: None,
            on_shutdown: None,
            on_health: None,
        }
    }

    /// Human-readable description for the manifest + `initialize` response.
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Override the protocol version the plugin reports. Defaults to
    /// `animus_plugin_protocol::PROTOCOL_VERSION`. Most plugins should keep
    /// the default.
    pub fn protocol_version(mut self, version: impl Into<String>) -> Self {
        self.protocol_version = version.into();
        self
    }

    /// Set the declared methods list reported in the manifest and the
    /// `initialize` response. Replaces any previously set value.
    pub fn methods<I, S>(mut self, methods: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.methods = methods.into_iter().map(Into::into).collect();
        self
    }

    /// Advertise that the plugin emits server-streaming notifications.
    pub fn streaming(mut self, value: bool) -> Self {
        self.streaming = value;
        self
    }

    /// Advertise `$/progress` notifications.
    pub fn progress(mut self, value: bool) -> Self {
        self.progress = value;
        self
    }

    /// Advertise `$/cancelRequest` support. When enabled the shell creates a
    /// [`CancellationToken`] per in-flight request and trips it on receipt
    /// of `$/cancelRequest`. The handler is responsible for observing the
    /// token.
    pub fn cancellation(mut self, value: bool) -> Self {
        self.cancellation = value;
        self
    }

    /// Subject-backend projection names (subject backends only).
    pub fn projections<I, S>(mut self, projections: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.projections = projections.into_iter().map(Into::into).collect();
        self
    }

    /// Subject kinds (subject backends only).
    pub fn subject_kinds<I, S>(mut self, kinds: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.subject_kinds = kinds.into_iter().map(Into::into).collect();
        self
    }

    /// MCP tools exposed by the plugin (custom plugins only).
    pub fn mcp_tools<I>(mut self, tools: I) -> Self
    where
        I: IntoIterator<Item = animus_plugin_protocol::McpTool>,
    {
        self.mcp_tools = tools.into_iter().collect();
        self
    }

    /// Environment variables to declare in the manifest.
    pub fn env_required<I>(mut self, env: I) -> Self
    where
        I: IntoIterator<Item = animus_plugin_protocol::EnvRequirement>,
    {
        self.env_required = env.into_iter().collect();
        self
    }

    /// Author-supplied broadcast channel hint reported in the manifest.
    pub fn notification_buffer_size(mut self, size: usize) -> Self {
        self.notification_buffer_size = Some(size);
        self
    }

    /// Add a typed per-kind capability blob, surfaced in
    /// `InitializeResult.kind_capabilities`.
    pub fn kind_capability(mut self, kind: impl Into<String>, capability: KindCapability) -> Self {
        self.kind_capabilities.insert(kind.into(), capability);
        self
    }

    /// Register an `on_init` hook.
    ///
    /// Runs once when the host sends `initialize`. Receives an [`InitContext`]
    /// carrying the host's `init_extensions` blob (where plugin authors look
    /// for `project_binding`, `memory_mcp_stdio_command`, etc.) plus a
    /// [`Notifier`] handle. Returning an error fails the `initialize`
    /// response with the supplied [`RpcError`].
    pub fn on_init<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(InitContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), RpcError>> + Send + 'static,
    {
        let hook: InitHook = Arc::new(move |ctx| Box::pin(hook(ctx)));
        self.on_init = Some(hook);
        self
    }

    /// Register a shutdown hook. Runs once when the host sends `shutdown`,
    /// before the shell emits the `{}` ack response.
    pub fn on_shutdown<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let hook: ShutdownHook = Arc::new(move || Box::pin(hook()));
        self.on_shutdown = Some(hook);
        self
    }

    /// Register a `health/check` hook. The shell still owns the `health/check`
    /// method dispatch; the hook decides what gets reported. Without a hook
    /// the shell answers `HealthStatus::Healthy` with empty fields, which is
    /// fine for stateless plugins but masks upstream outages for backends
    /// that connect to remote APIs. Subject and provider backends should
    /// wire their backend-specific health check through this hook.
    pub fn on_health<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<HealthCheckResult, RpcError>> + Send + 'static,
    {
        let hook: HealthHook = Arc::new(move || Box::pin(hook()));
        self.on_health = Some(hook);
        self
    }

    /// Register a typed request method handler.
    ///
    /// The handler receives a deserialized `Req` and a [`MethodContext`].
    /// Returning `Ok(Resp)` produces a successful JSON-RPC response;
    /// returning `Err(RpcError)` produces an error envelope with the
    /// supplied code/message/data forwarded verbatim.
    pub fn register_method<Req, Resp, F, Fut>(
        mut self,
        method: impl Into<String>,
        handler: F,
    ) -> Self
    where
        Req: DeserializeOwned + Send + 'static,
        Resp: Serialize + Send + 'static,
        F: Fn(Req, MethodContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Resp, RpcError>> + Send + 'static,
    {
        let method_name: String = method.into();
        let method_name_for_handler = method_name.clone();
        let handler = Arc::new(handler);
        let dispatch: MethodHandler = Arc::new(move |params, ctx| {
            let method_name = method_name_for_handler.clone();
            let handler = handler.clone();
            Box::pin(async move {
                let req: Req = match serde_json::from_value(params) {
                    Ok(value) => value,
                    Err(error) => {
                        return Err(RpcError {
                            code: error_codes::INVALID_PARAMS,
                            message: format!("invalid {method_name} params: {error}"),
                            data: None,
                        });
                    }
                };
                let resp = handler(req, ctx).await?;
                match serde_json::to_value(resp) {
                    Ok(value) => Ok(value),
                    Err(error) => Err(RpcError {
                        code: error_codes::INTERNAL_ERROR,
                        message: format!("failed to encode {method_name} response: {error}"),
                        data: None,
                    }),
                }
            })
        });
        self.method_handlers.insert(method_name, dispatch);
        self
    }

    /// Register a raw (untyped) request method handler. Useful when the
    /// params shape varies on the wire or the handler wants to surface a
    /// raw `Value` response.
    pub fn register_raw_method<F, Fut>(mut self, method: impl Into<String>, handler: F) -> Self
    where
        F: Fn(Value, MethodContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, RpcError>> + Send + 'static,
    {
        let handler: MethodHandler = Arc::new(move |params, ctx| Box::pin(handler(params, ctx)));
        self.method_handlers.insert(method.into(), handler);
        self
    }

    /// Read the methods list this plugin would report in its manifest /
    /// `initialize` response. Test helper — read-only view of the builder
    /// state.
    pub fn advertised_methods(&self) -> &[String] {
        &self.methods
    }

    /// `true` if a method handler has been registered for `method`. Test
    /// helper — read-only view of the builder state.
    pub fn has_method_handler(&self, method: &str) -> bool {
        self.method_handlers.contains_key(method)
    }

    /// Register a notification handler. The handler receives the raw params
    /// value and a [`Notifier`] for fanning out follow-up notifications.
    pub fn register_notification<F, Fut>(mut self, method: impl Into<String>, handler: F) -> Self
    where
        F: Fn(Value, Notifier) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handler: NotificationHandler =
            Arc::new(move |params, notifier| Box::pin(handler(params, notifier)));
        self.notification_handlers.insert(method.into(), handler);
        self
    }

    fn manifest(&self) -> PluginManifest {
        let mut capabilities: Vec<String> = self.methods.clone();
        if !capabilities.iter().any(|m| m == "health/check") {
            capabilities.push("health/check".to_string());
        }
        PluginManifest {
            name: self.name.clone(),
            version: self.version.clone(),
            plugin_kind: self.plugin_kind.clone(),
            plugin_kinds: Vec::new(),
            description: self.description.clone(),
            protocol_version: self.protocol_version.clone(),
            capabilities,
            env_required: self.env_required.clone(),
            notification_buffer_size: self.notification_buffer_size,
        }
    }

    fn initialize_result(&self) -> InitializeResult {
        let mut methods: Vec<String> = self.methods.clone();
        if !methods.iter().any(|m| m == "health/check") {
            methods.push("health/check".to_string());
        }
        InitializeResult {
            protocol_version: self.protocol_version.clone(),
            plugin_info: PluginInfo {
                name: self.name.clone(),
                version: self.version.clone(),
                plugin_kind: self.plugin_kind.clone(),
                plugin_kinds: Vec::new(),
                description: if self.description.is_empty() {
                    None
                } else {
                    Some(self.description.clone())
                },
            },
            capabilities: PluginCapabilities {
                methods,
                streaming: self.streaming,
                progress: self.progress,
                cancellation: self.cancellation,
                projections: self.projections.clone(),
                subject_kinds: self.subject_kinds.clone(),
                mcp_tools: self.mcp_tools.clone(),
            },
            kind_capabilities: self.kind_capabilities.clone(),
        }
    }

    fn handle_cli_args(&self) -> bool {
        for arg in std::env::args().skip(1) {
            match arg.as_str() {
                "--manifest" | "-m" => {
                    let mut stdout = io::stdout().lock();
                    let _ = writeln!(
                        stdout,
                        "{}",
                        serde_json::to_string(&self.manifest()).expect("serialize manifest")
                    );
                    let _ = stdout.flush();
                    return true;
                }
                "--help" | "-h" => {
                    eprintln!("{} {} — STDIO plugin for Animus", self.name, self.version);
                    eprintln!("Usage:");
                    eprintln!(
                        "  {} --manifest    Print plugin manifest as JSON and exit",
                        self.name
                    );
                    eprintln!(
                        "  {}               Run JSON-RPC loop on stdin/stdout",
                        self.name
                    );
                    return true;
                }
                _ => {}
            }
        }
        false
    }

    /// Drive the plugin: parse CLI args, then run the JSON-RPC loop.
    ///
    /// Consumes the builder. Most plugin authors should call this directly
    /// from `#[tokio::main]`.
    pub async fn run(self) -> anyhow::Result<()> {
        if self.handle_cli_args() {
            return Ok(());
        }
        if io::stdin().is_terminal() {
            eprintln!(
                "{} is a STDIO plugin; pipe JSON-RPC on stdin or pass --manifest",
                self.name
            );
            std::process::exit(2);
        }
        let stdin = tokio::io::stdin();
        let stdout = tokio::io::stdout();
        self.run_with_io(stdin, stdout).await
    }

    /// Drive the plugin against caller-supplied I/O.
    ///
    /// Test entry point — equivalent to [`Plugin::run`] but with the stdin
    /// reader and stdout writer injected. Production code typically calls
    /// [`Plugin::run`].
    pub async fn run_with_io<R, W>(self, mut stdin: R, stdout: W) -> anyhow::Result<()>
    where
        R: tokio::io::AsyncRead + Unpin + Send,
        W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let writer: Arc<Mutex<DynStdout>> = Arc::new(Mutex::new(DynStdout::new(stdout)));
        let notifier = Notifier {
            writer: Arc::clone(&writer),
        };

        let initialize_result = self.initialize_result();
        let state = Arc::new(PluginState {
            initialize_succeeded: RwLock::new(false),
            initialized: RwLock::new(false),
            in_flight: Mutex::new(HashMap::new()),
            method_handlers: self.method_handlers,
            notification_handlers: self.notification_handlers,
            on_init: self.on_init,
            on_shutdown: self.on_shutdown,
            on_health: self.on_health,
            initialize_result,
            cancellation_enabled: self.cancellation,
            plugin_name: self.name,
        });

        let mut handler_tasks: JoinSet<()> = JoinSet::new();
        let mut buffer: Vec<u8> = Vec::with_capacity(8 * 1024);
        let mut chunk = [0u8; 4096];
        loop {
            let n = stdin.read(&mut chunk).await?;
            if n == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..n]);

            loop {
                let leading_ws = buffer
                    .iter()
                    .take_while(|b| b.is_ascii_whitespace())
                    .count();
                if leading_ws > 0 {
                    buffer.drain(..leading_ws);
                }
                if buffer.is_empty() {
                    break;
                }

                let mut stream =
                    serde_json::Deserializer::from_slice(&buffer).into_iter::<RpcRequest>();
                match stream.next() {
                    Some(Ok(request)) => {
                        let consumed = stream.byte_offset();
                        drop(stream);
                        // Frames are newline-delimited. Wait for the trailing
                        // newline before dispatching, even when the JSON
                        // prefix parses cleanly, so a split read that
                        // drops a valid prefix here cannot dispatch a
                        // mutating call that later turns out to be a
                        // malformed frame (e.g. `{...}garbage\n`).
                        let mut newline_idx: Option<usize> = None;
                        let mut garbage_found = false;
                        for (i, b) in buffer[consumed..].iter().enumerate() {
                            if *b == b'\n' {
                                newline_idx = Some(consumed + i);
                                break;
                            }
                            if !b.is_ascii_whitespace() {
                                garbage_found = true;
                            }
                        }
                        let Some(terminator) = newline_idx else {
                            // Frame terminator not yet in the buffer. Wait
                            // for more input so we can either confirm a
                            // clean frame or resync on the next newline.
                            break;
                        };
                        if garbage_found {
                            tracing::warn!(
                                plugin = %state.plugin_name,
                                "discarding frame with trailing non-JSON garbage"
                            );
                            buffer.drain(..=terminator);
                            continue;
                        }
                        buffer.drain(..=terminator);
                        let shutdown_id = if request.method == "shutdown" {
                            Some(request.id.clone())
                        } else {
                            None
                        };
                        // Opportunistically reap completed handler tasks so
                        // `JoinSet` does not accumulate finished outputs for
                        // the life of the plugin process.
                        while handler_tasks.try_join_next().is_some() {}
                        match dispatch_frame(
                            request,
                            Arc::clone(&state),
                            notifier.clone(),
                            &mut handler_tasks,
                        )
                        .await
                        {
                            FrameOutcome::Continue => {}
                            FrameOutcome::Exit => return Ok(()),
                            FrameOutcome::Shutdown => {
                                // Trip retained subscription cancellation
                                // tokens (handlers that called
                                // `MethodContext::keep_cancellation`) so
                                // background streams stop before we ack.
                                let retained: Vec<CancellationToken> = state
                                    .in_flight
                                    .lock()
                                    .await
                                    .drain()
                                    .map(|(_, token)| token)
                                    .collect();
                                for token in retained {
                                    token.cancel();
                                }
                                while handler_tasks.join_next().await.is_some() {}
                                if let Some(hook) = state.on_shutdown.clone() {
                                    hook().await;
                                }
                                let id = shutdown_id.unwrap_or(None);
                                write_response(&notifier, RpcResponse::ok(id, json!({}))).await;
                            }
                        }
                    }
                    Some(Err(error)) if error.is_eof() => break,
                    Some(Err(error)) => {
                        tracing::warn!(plugin = %state.plugin_name, %error, "invalid JSON-RPC frame");
                        if let Some(pos) = buffer.iter().position(|b| *b == b'\n') {
                            buffer.drain(..=pos);
                            continue;
                        }
                        break;
                    }
                    None => break,
                }
            }
        }
        // Stdin EOF without a prior `exit`/`shutdown` typically means the
        // host crashed or otherwise dropped the connection. Abort any
        // in-flight handler tasks rather than waiting for them — there is
        // nobody left to receive their responses, and a blocking watch
        // would otherwise keep this process alive indefinitely.
        handler_tasks.abort_all();
        while handler_tasks.join_next().await.is_some() {}
        Ok(())
    }
}

enum FrameOutcome {
    Continue,
    Exit,
    Shutdown,
}

async fn dispatch_frame(
    request: RpcRequest,
    state: Arc<PluginState>,
    notifier: Notifier,
    handler_tasks: &mut JoinSet<()>,
) -> FrameOutcome {
    let id = request.id.clone();
    let method = request.method.clone();
    let params = request.params.unwrap_or(Value::Null);

    match method.as_str() {
        "exit" => return FrameOutcome::Exit,
        "initialize" => {
            let response = handle_initialize(id, params, &state, &notifier).await;
            write_response(&notifier, response).await;
        }
        "initialized" => {
            if *state.initialize_succeeded.read().await {
                *state.initialized.write().await = true;
            }
        }
        "$/ping" => {
            write_response(&notifier, RpcResponse::ok(id, json!({}))).await;
        }
        "$/cancelRequest" => {
            handle_cancel_notification(params, &state).await;
        }
        "health/check" => {
            let health_result: Result<HealthCheckResult, RpcError> = match &state.on_health {
                Some(hook) => hook().await,
                None => Ok(HealthCheckResult {
                    status: HealthStatus::Healthy,
                    uptime_ms: None,
                    memory_usage_bytes: None,
                    last_error: None,
                }),
            };
            let response = match health_result {
                Ok(value) => match serde_json::to_value(value) {
                    Ok(value) => RpcResponse::ok(id, value),
                    Err(error) => RpcResponse::err(
                        id,
                        RpcError {
                            code: error_codes::INTERNAL_ERROR,
                            message: format!("failed to encode health result: {error}"),
                            data: None,
                        },
                    ),
                },
                Err(error) => RpcResponse::err(id, error),
            };
            write_response(&notifier, response).await;
        }
        "shutdown" => {
            return FrameOutcome::Shutdown;
        }
        other if other.starts_with("$/") => {
            // Unknown protocol-meta notifications are silently dropped per
            // JSON-RPC convention. Requests with an id still get a
            // method_not_found response so the host does not hang.
            if request.id.is_some() {
                let response = RpcResponse::err(
                    id,
                    RpcError {
                        code: error_codes::METHOD_NOT_FOUND,
                        message: format!(
                            "method '{other}' not implemented by {}",
                            state.plugin_name
                        ),
                        data: None,
                    },
                );
                write_response(&notifier, response).await;
            }
        }
        other => {
            if request.id.is_none() {
                if let Some(handler) = state.notification_handlers.get(other).cloned() {
                    let notifier = notifier.clone();
                    handler_tasks.spawn(async move {
                        handler(params, notifier).await;
                    });
                }
                return FrameOutcome::Continue;
            }
            let Some(handler) = state.method_handlers.get(other).cloned() else {
                let response = RpcResponse::err(
                    id,
                    RpcError {
                        code: error_codes::METHOD_NOT_FOUND,
                        message: format!(
                            "method '{other}' not implemented by {}",
                            state.plugin_name
                        ),
                        data: None,
                    },
                );
                write_response(&notifier, response).await;
                return FrameOutcome::Continue;
            };
            if !*state.initialized.read().await {
                let response = RpcResponse::err(
                    id,
                    RpcError {
                        code: error_codes::PLUGIN_NOT_INITIALIZED,
                        message: format!(
                            "{} received '{other}' before initialize",
                            state.plugin_name
                        ),
                        data: None,
                    },
                );
                write_response(&notifier, response).await;
                return FrameOutcome::Continue;
            }
            let cancellation = CancellationToken::new();
            let token_key = id_token_key(&id);
            if state.cancellation_enabled {
                if let Some(key) = token_key.as_ref() {
                    state
                        .in_flight
                        .lock()
                        .await
                        .insert(key.clone(), cancellation.clone());
                }
            }
            let keep_cancellation = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let ctx = MethodContext {
                request_id: id.clone(),
                notifier: notifier.clone(),
                cancellation,
                keep_cancellation: Arc::clone(&keep_cancellation),
            };
            let state_for_task = Arc::clone(&state);
            let notifier_for_task = notifier.clone();
            handler_tasks.spawn(async move {
                let result = handler(params, ctx).await;
                if state_for_task.cancellation_enabled
                    && !keep_cancellation.load(std::sync::atomic::Ordering::SeqCst)
                {
                    if let Some(key) = token_key.as_ref() {
                        state_for_task.in_flight.lock().await.remove(key);
                    }
                }
                let response = match result {
                    Ok(value) => RpcResponse::ok(id, value),
                    Err(error) => RpcResponse::err(id, error),
                };
                write_response(&notifier_for_task, response).await;
            });
        }
    }
    FrameOutcome::Continue
}

struct DynStdout {
    inner: Box<dyn tokio::io::AsyncWrite + Unpin + Send>,
}

impl DynStdout {
    fn new<W>(writer: W) -> Self
    where
        W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        Self {
            inner: Box::new(writer),
        }
    }

    async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.inner.write_all(bytes).await
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.inner.flush().await
    }
}

struct PluginState {
    initialize_succeeded: RwLock<bool>,
    initialized: RwLock<bool>,
    in_flight: Mutex<HashMap<String, CancellationToken>>,
    method_handlers: HashMap<String, MethodHandler>,
    notification_handlers: HashMap<String, NotificationHandler>,
    on_init: Option<InitHook>,
    on_shutdown: Option<ShutdownHook>,
    on_health: Option<HealthHook>,
    initialize_result: InitializeResult,
    cancellation_enabled: bool,
    plugin_name: String,
}

async fn handle_initialize(
    id: Option<Value>,
    params: Value,
    state: &Arc<PluginState>,
    notifier: &Notifier,
) -> RpcResponse {
    let init: InitializeParams = match serde_json::from_value(params) {
        Ok(value) => value,
        Err(error) => {
            return RpcResponse::err(
                id,
                RpcError {
                    code: error_codes::INVALID_PARAMS,
                    message: format!("invalid initialize params: {error}"),
                    data: None,
                },
            );
        }
    };

    // Reject hosts on an incompatible major. The protocol's compatibility
    // rule (spec §15) is `host_major == plugin_major`.
    let plugin_major = major_of(&state.initialize_result.protocol_version);
    let host_major = major_of(&init.protocol_version);
    match (plugin_major, host_major) {
        (Some(plugin), Some(host)) if plugin != host => {
            return RpcResponse::err(
                id,
                RpcError {
                    code: error_codes::INVALID_PARAMS,
                    message: format!(
                        "incompatible protocol major: host '{}' vs plugin '{}'",
                        init.protocol_version, state.initialize_result.protocol_version
                    ),
                    data: None,
                },
            );
        }
        _ => {}
    }

    if let Some(hook) = state.on_init.clone() {
        let ctx = InitContext {
            init_extensions: init.init_extensions.clone(),
            host_info: init.host_info.clone(),
            host_capabilities: init.capabilities.clone(),
            notifier: notifier.clone(),
        };
        if let Err(error) = hook(ctx).await {
            return RpcResponse::err(id, error);
        }
    }

    *state.initialize_succeeded.write().await = true;
    match serde_json::to_value(&state.initialize_result) {
        Ok(value) => RpcResponse::ok(id, value),
        Err(error) => RpcResponse::err(
            id,
            RpcError {
                code: error_codes::INTERNAL_ERROR,
                message: format!("failed to encode initialize result: {error}"),
                data: None,
            },
        ),
    }
}

async fn handle_cancel_notification(params: Value, state: &Arc<PluginState>) {
    if !state.cancellation_enabled {
        return;
    }
    let id_value = match params.get("id") {
        Some(value) => value.clone(),
        None => return,
    };
    if let Some(key) = id_token_key(&Some(id_value)) {
        if let Some(token) = state.in_flight.lock().await.remove(&key) {
            token.cancel();
        }
    }
}

fn major_of(version: &str) -> Option<u64> {
    version.split('.').next()?.parse().ok()
}

fn id_token_key(id: &Option<Value>) -> Option<String> {
    let value = id.as_ref()?;
    Some(match value {
        Value::Null => "null".to_string(),
        Value::String(s) => format!("s:{s}"),
        Value::Number(n) => format!("n:{n}"),
        other => format!("v:{other}"),
    })
}

async fn write_response(notifier: &Notifier, response: RpcResponse) {
    write_frame(&notifier.writer, &response).await;
}

async fn write_frame<T: Serialize>(stdout: &Arc<Mutex<DynStdout>>, frame: &T) {
    if let Ok(mut payload) = serde_json::to_string(frame) {
        payload.push('\n');
        let mut guard = stdout.lock().await;
        let _ = guard.write_all(payload.as_bytes()).await;
        let _ = guard.flush().await;
    }
}

/// Convenience macro for registering a typed method handler.
///
/// Equivalent to calling `Plugin::register_method::<Req, Resp, _, _>(name, handler)`
/// but avoids the turbofish ceremony when the request/response types can be
/// named at the call site.
///
/// ```ignore
/// let plugin = animus_plugin_runtime::register_method!(
///     plugin,
///     "queue/enqueue",
///     EnqueueRequest => EnqueueResponse,
///     |req, ctx| async move { ... },
/// );
/// ```
#[macro_export]
macro_rules! register_method {
    ($plugin:expr, $name:expr, $req:ty => $resp:ty, $handler:expr $(,)?) => {{
        $plugin.register_method::<$req, $resp, _, _>($name, $handler)
    }};
}

#[cfg(test)]
mod tests;

//! Unit tests for the generic plugin shell.

use std::sync::Arc;
use std::time::Duration;

use animus_plugin_protocol::{error_codes, RpcError, RpcResponse, PROTOCOL_VERSION};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::plugin::{InitContext, MethodContext, Notifier, Plugin};

#[derive(Debug, Deserialize)]
struct EchoRequest {
    message: String,
}

#[derive(Debug, Serialize)]
struct EchoResponse {
    echoed: String,
}

fn initialize_frame(id: u64) -> String {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "initialize",
        "params": {
            "protocol_version": PROTOCOL_VERSION,
            "host_info": { "name": "animus", "version": "0.5.0" },
            "capabilities": { "streaming": true, "cancellation": true },
            "init_extensions": {
                "project_binding": { "project_root": "/tmp/test" }
            }
        }
    });
    let initialized = json!({ "jsonrpc": "2.0", "method": "initialized" });
    format!("{payload}\n{initialized}\n")
}

async fn read_frame_line<R>(reader: &mut R) -> RpcResponse
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut buffer = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = reader.read(&mut byte).await.expect("read byte");
        if n == 0 {
            panic!("unexpected EOF while reading frame");
        }
        if byte[0] == b'\n' {
            break;
        }
        buffer.push(byte[0]);
    }
    serde_json::from_slice(&buffer).expect("parse response frame")
}

#[tokio::test]
async fn typed_method_round_trips_request_and_response() {
    let (mut host_to_plugin, plugin_in) = duplex(8 * 1024);
    let (plugin_out, mut host_from_plugin) = duplex(8 * 1024);

    let plugin = Plugin::new("test-shell", "0.0.1", "custom")
        .description("shell unit test")
        .methods(["echo/say"])
        .on_init(|_ctx: InitContext| async { Ok(()) })
        .register_method::<EchoRequest, EchoResponse, _, _>("echo/say", |req, _ctx| async move {
            Ok(EchoResponse {
                echoed: format!("{}!", req.message),
            })
        });

    let join = tokio::spawn(async move { plugin.run_with_io(plugin_in, plugin_out).await });

    host_to_plugin
        .write_all(initialize_frame(1).as_bytes())
        .await
        .unwrap();
    let init_response = read_frame_line(&mut host_from_plugin).await;
    assert_eq!(init_response.id, Some(json!(1)));
    assert!(init_response.result.is_some());

    let request = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "echo/say",
        "params": { "message": "hi" }
    });
    host_to_plugin
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();

    let response = read_frame_line(&mut host_from_plugin).await;
    assert_eq!(response.id, Some(json!(2)));
    assert_eq!(response.result.unwrap()["echoed"], "hi!");

    drop(host_to_plugin);
    join.await.unwrap().unwrap();
}

#[tokio::test]
async fn notifier_fans_out_typed_notifications() {
    #[derive(Serialize)]
    struct Tick {
        n: u32,
    }

    let (mut host_to_plugin, plugin_in) = duplex(8 * 1024);
    let (plugin_out, mut host_from_plugin) = duplex(8 * 1024);

    let plugin = Plugin::new("test-fanout", "0.0.1", "custom")
        .methods(["stream/start"])
        .streaming(true)
        .register_raw_method("stream/start", |_params, ctx| async move {
            for n in 0..3 {
                ctx.notifier.notify_typed("stream/tick", &Tick { n }).await;
            }
            Ok(json!({ "done": true }))
        });

    let join = tokio::spawn(async move { plugin.run_with_io(plugin_in, plugin_out).await });

    host_to_plugin
        .write_all(initialize_frame(1).as_bytes())
        .await
        .unwrap();
    let _ = read_frame_line(&mut host_from_plugin).await;

    let request = json!({
        "jsonrpc": "2.0",
        "id": 7,
        "method": "stream/start",
        "params": {}
    });
    host_to_plugin
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();

    let mut ticks = Vec::new();
    let mut final_response: Option<RpcResponse> = None;
    while final_response.is_none() {
        let frame = read_frame_line(&mut host_from_plugin).await;
        if frame.id == Some(json!(7)) {
            final_response = Some(frame);
        } else {
            ticks.push(frame);
        }
    }
    assert_eq!(ticks.len(), 3);
    let final_response = final_response.unwrap();
    assert_eq!(final_response.result.unwrap()["done"], true);

    drop(host_to_plugin);
    join.await.unwrap().unwrap();
}

#[tokio::test]
async fn initialize_propagates_extensions_to_on_init() {
    let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    let (mut host_to_plugin, plugin_in) = duplex(8 * 1024);
    let (plugin_out, mut host_from_plugin) = duplex(8 * 1024);

    let captured_clone = Arc::clone(&captured);
    let plugin = Plugin::new("test-init", "0.0.1", "custom").on_init(move |ctx: InitContext| {
        let captured_clone = Arc::clone(&captured_clone);
        async move {
            let project = ctx
                .init_extensions
                .get("project_binding")
                .and_then(|v| v.get("project_root"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            *captured_clone.lock().await = project;
            Ok(())
        }
    });

    let join = tokio::spawn(async move { plugin.run_with_io(plugin_in, plugin_out).await });

    host_to_plugin
        .write_all(initialize_frame(1).as_bytes())
        .await
        .unwrap();
    let init_response = read_frame_line(&mut host_from_plugin).await;
    assert!(init_response.result.is_some());

    drop(host_to_plugin);
    join.await.unwrap().unwrap();
    assert_eq!(captured.lock().await.as_deref(), Some("/tmp/test"));
}

#[tokio::test]
async fn shutdown_hook_runs_then_response_is_emitted() {
    let ran: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

    let (mut host_to_plugin, plugin_in) = duplex(8 * 1024);
    let (plugin_out, mut host_from_plugin) = duplex(8 * 1024);

    let ran_clone = Arc::clone(&ran);
    let plugin = Plugin::new("test-shutdown", "0.0.1", "custom").on_shutdown(move || {
        let ran_clone = Arc::clone(&ran_clone);
        async move {
            *ran_clone.lock().await = true;
        }
    });

    let join = tokio::spawn(async move { plugin.run_with_io(plugin_in, plugin_out).await });

    host_to_plugin
        .write_all(initialize_frame(1).as_bytes())
        .await
        .unwrap();
    let _ = read_frame_line(&mut host_from_plugin).await;

    let shutdown = json!({ "jsonrpc": "2.0", "id": 99, "method": "shutdown" });
    host_to_plugin
        .write_all(format!("{shutdown}\n").as_bytes())
        .await
        .unwrap();
    let resp = read_frame_line(&mut host_from_plugin).await;
    assert_eq!(resp.id, Some(json!(99)));
    assert!(resp.result.is_some());

    drop(host_to_plugin);
    join.await.unwrap().unwrap();
    assert!(*ran.lock().await);
}

#[tokio::test]
async fn on_health_hook_routes_response_through_the_backend() {
    use animus_plugin_protocol::{HealthCheckResult, HealthStatus};

    let (mut host_to_plugin, plugin_in) = duplex(8 * 1024);
    let (plugin_out, mut host_from_plugin) = duplex(8 * 1024);

    let plugin = Plugin::new("test-health", "0.0.1", "custom").on_health(|| async {
        Ok(HealthCheckResult {
            status: HealthStatus::Degraded,
            uptime_ms: Some(7777),
            memory_usage_bytes: None,
            last_error: Some("upstream timeout".into()),
        })
    });

    let join = tokio::spawn(async move { plugin.run_with_io(plugin_in, plugin_out).await });

    host_to_plugin
        .write_all(initialize_frame(1).as_bytes())
        .await
        .unwrap();
    let _ = read_frame_line(&mut host_from_plugin).await;

    let health = json!({ "jsonrpc": "2.0", "id": 5, "method": "health/check" });
    host_to_plugin
        .write_all(format!("{health}\n").as_bytes())
        .await
        .unwrap();
    let resp = read_frame_line(&mut host_from_plugin).await;
    let result = resp.result.expect("health result");
    assert_eq!(result["status"], "degraded");
    assert_eq!(result["uptime_ms"], 7777);
    assert_eq!(result["last_error"], "upstream timeout");

    drop(host_to_plugin);
    join.await.unwrap().unwrap();
}

#[tokio::test]
async fn on_health_hook_propagates_backend_failure_as_error_envelope() {
    let (mut host_to_plugin, plugin_in) = duplex(8 * 1024);
    let (plugin_out, mut host_from_plugin) = duplex(8 * 1024);

    let plugin = Plugin::new("test-health-fail", "0.0.1", "custom").on_health(|| async {
        Err(RpcError {
            code: -32099,
            message: "backend down".into(),
            data: None,
        })
    });

    let join = tokio::spawn(async move { plugin.run_with_io(plugin_in, plugin_out).await });

    host_to_plugin
        .write_all(initialize_frame(1).as_bytes())
        .await
        .unwrap();
    let _ = read_frame_line(&mut host_from_plugin).await;

    let health = json!({ "jsonrpc": "2.0", "id": 5, "method": "health/check" });
    host_to_plugin
        .write_all(format!("{health}\n").as_bytes())
        .await
        .unwrap();
    let resp = read_frame_line(&mut host_from_plugin).await;
    assert!(resp.result.is_none());
    let error = resp.error.expect("error envelope");
    assert_eq!(error.code, -32099);
    assert_eq!(error.message, "backend down");

    drop(host_to_plugin);
    join.await.unwrap().unwrap();
}

#[tokio::test]
async fn handler_error_becomes_json_rpc_error_envelope() {
    let (mut host_to_plugin, plugin_in) = duplex(8 * 1024);
    let (plugin_out, mut host_from_plugin) = duplex(8 * 1024);

    let plugin = Plugin::new("test-error", "0.0.1", "custom")
        .methods(["thing/do"])
        .register_raw_method("thing/do", |_params, _ctx| async move {
            Err(RpcError {
                code: -32099,
                message: "domain-specific failure".to_string(),
                data: Some(json!({ "hint": "retry" })),
            })
        });

    let join = tokio::spawn(async move { plugin.run_with_io(plugin_in, plugin_out).await });

    host_to_plugin
        .write_all(initialize_frame(1).as_bytes())
        .await
        .unwrap();
    let _ = read_frame_line(&mut host_from_plugin).await;

    let request = json!({
        "jsonrpc": "2.0",
        "id": 42,
        "method": "thing/do",
        "params": {}
    });
    host_to_plugin
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();
    let response = read_frame_line(&mut host_from_plugin).await;
    assert_eq!(response.id, Some(json!(42)));
    let error = response.error.expect("error envelope");
    assert_eq!(error.code, -32099);
    assert_eq!(error.message, "domain-specific failure");
    assert_eq!(error.data, Some(json!({ "hint": "retry" })));

    drop(host_to_plugin);
    join.await.unwrap().unwrap();
}

#[tokio::test]
async fn invalid_params_returns_invalid_params_error() {
    let (mut host_to_plugin, plugin_in) = duplex(8 * 1024);
    let (plugin_out, mut host_from_plugin) = duplex(8 * 1024);

    let plugin = Plugin::new("test-bad-params", "0.0.1", "custom")
        .methods(["echo/say"])
        .register_method::<EchoRequest, EchoResponse, _, _>("echo/say", |req, _ctx| async move {
            Ok(EchoResponse {
                echoed: req.message,
            })
        });

    let join = tokio::spawn(async move { plugin.run_with_io(plugin_in, plugin_out).await });

    host_to_plugin
        .write_all(initialize_frame(1).as_bytes())
        .await
        .unwrap();
    let _ = read_frame_line(&mut host_from_plugin).await;

    let bad = json!({
        "jsonrpc": "2.0",
        "id": 5,
        "method": "echo/say",
        "params": { "wrong_field": 42 }
    });
    host_to_plugin
        .write_all(format!("{bad}\n").as_bytes())
        .await
        .unwrap();
    let response = read_frame_line(&mut host_from_plugin).await;
    let error = response.error.expect("error envelope");
    assert_eq!(error.code, error_codes::INVALID_PARAMS);

    drop(host_to_plugin);
    join.await.unwrap().unwrap();
}

#[tokio::test]
async fn method_before_initialize_returns_plugin_not_initialized() {
    let (mut host_to_plugin, plugin_in) = duplex(8 * 1024);
    let (plugin_out, mut host_from_plugin) = duplex(8 * 1024);

    let plugin = Plugin::new("test-pre-init", "0.0.1", "custom")
        .methods(["echo/say"])
        .register_method::<EchoRequest, EchoResponse, _, _>("echo/say", |req, _ctx| async move {
            Ok(EchoResponse {
                echoed: req.message,
            })
        });

    let join = tokio::spawn(async move { plugin.run_with_io(plugin_in, plugin_out).await });

    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "echo/say",
        "params": { "message": "early" }
    });
    host_to_plugin
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();
    let response = read_frame_line(&mut host_from_plugin).await;
    let error = response.error.expect("error envelope");
    assert_eq!(error.code, error_codes::PLUGIN_NOT_INITIALIZED);

    drop(host_to_plugin);
    join.await.unwrap().unwrap();
}

#[tokio::test]
async fn cancel_request_trips_handler_token_mid_flight() {
    let (mut host_to_plugin, plugin_in) = duplex(8 * 1024);
    let (plugin_out, mut host_from_plugin) = duplex(8 * 1024);

    let plugin = Plugin::new("test-cancel", "0.0.1", "custom")
        .methods(["work/run"])
        .cancellation(true)
        .register_raw_method("work/run", |_params, ctx| async move {
            tokio::select! {
                _ = ctx.cancellation.cancelled() => Err(RpcError {
                    code: error_codes::REQUEST_CANCELLED,
                    message: "cancelled by host".to_string(),
                    data: None,
                }),
                _ = tokio::time::sleep(Duration::from_secs(3)) => Ok(json!({ "done": true })),
            }
        });

    let join = tokio::spawn(async move { plugin.run_with_io(plugin_in, plugin_out).await });

    host_to_plugin
        .write_all(initialize_frame(1).as_bytes())
        .await
        .unwrap();
    let _ = read_frame_line(&mut host_from_plugin).await;

    let request = json!({
        "jsonrpc": "2.0",
        "id": 11,
        "method": "work/run",
        "params": {}
    });
    host_to_plugin
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let cancel = json!({
        "jsonrpc": "2.0",
        "method": "$/cancelRequest",
        "params": { "id": 11 }
    });
    host_to_plugin
        .write_all(format!("{cancel}\n").as_bytes())
        .await
        .unwrap();

    let response = read_frame_line(&mut host_from_plugin).await;
    let error = response.error.expect("cancelled response should be error");
    assert_eq!(error.code, error_codes::REQUEST_CANCELLED);

    drop(host_to_plugin);
    join.await.unwrap().unwrap();
}

#[tokio::test]
async fn pretty_printed_multi_line_frame_parses_correctly() {
    let (mut host_to_plugin, plugin_in) = duplex(8 * 1024);
    let (plugin_out, mut host_from_plugin) = duplex(8 * 1024);

    let plugin = Plugin::new("test-pretty", "0.0.1", "custom")
        .methods(["echo/say"])
        .register_method::<EchoRequest, EchoResponse, _, _>("echo/say", |req, _ctx| async move {
            Ok(EchoResponse {
                echoed: req.message,
            })
        });

    let join = tokio::spawn(async move { plugin.run_with_io(plugin_in, plugin_out).await });

    host_to_plugin
        .write_all(initialize_frame(1).as_bytes())
        .await
        .unwrap();
    let _ = read_frame_line(&mut host_from_plugin).await;

    let pretty = "{\n  \"jsonrpc\": \"2.0\",\n  \"id\": 8,\n  \"method\": \"echo/say\",\n  \"params\": {\n    \"message\": \"prismatic\"\n  }\n}\n";
    host_to_plugin.write_all(pretty.as_bytes()).await.unwrap();

    let response = read_frame_line(&mut host_from_plugin).await;
    assert_eq!(response.id, Some(json!(8)));
    assert_eq!(response.result.unwrap()["echoed"], "prismatic");

    drop(host_to_plugin);
    join.await.unwrap().unwrap();
}

#[tokio::test]
async fn unknown_method_returns_method_not_found() {
    let (mut host_to_plugin, plugin_in) = duplex(8 * 1024);
    let (plugin_out, mut host_from_plugin) = duplex(8 * 1024);

    let plugin = Plugin::new("test-unknown", "0.0.1", "custom").methods(["echo/say"]);
    let join = tokio::spawn(async move { plugin.run_with_io(plugin_in, plugin_out).await });

    host_to_plugin
        .write_all(initialize_frame(1).as_bytes())
        .await
        .unwrap();
    let _ = read_frame_line(&mut host_from_plugin).await;

    let request = json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "thing/missing",
        "params": {}
    });
    host_to_plugin
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();
    let response = read_frame_line(&mut host_from_plugin).await;
    let error = response.error.expect("error envelope");
    assert_eq!(error.code, error_codes::METHOD_NOT_FOUND);

    drop(host_to_plugin);
    join.await.unwrap().unwrap();
}

#[tokio::test]
async fn notification_handler_receives_params_and_no_response_is_emitted() {
    let captured: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));

    let (mut host_to_plugin, plugin_in) = duplex(8 * 1024);
    let (plugin_out, mut host_from_plugin) = duplex(8 * 1024);

    let captured_clone = Arc::clone(&captured);
    let plugin = Plugin::new("test-notify", "0.0.1", "custom").register_notification(
        "host/event",
        move |params: Value, _notifier: Notifier| {
            let captured_clone = Arc::clone(&captured_clone);
            async move {
                *captured_clone.lock().await = Some(params);
            }
        },
    );

    let join = tokio::spawn(async move { plugin.run_with_io(plugin_in, plugin_out).await });

    host_to_plugin
        .write_all(initialize_frame(1).as_bytes())
        .await
        .unwrap();
    let _ = read_frame_line(&mut host_from_plugin).await;

    let notification = json!({
        "jsonrpc": "2.0",
        "method": "host/event",
        "params": { "kind": "ping" }
    });
    host_to_plugin
        .write_all(format!("{notification}\n").as_bytes())
        .await
        .unwrap();

    // Bracket the assertion with a real request so we know the notification
    // dispatch has had a chance to run on the shell's task pool.
    let request = json!({
        "jsonrpc": "2.0",
        "id": 50,
        "method": "$/ping",
        "params": {}
    });
    host_to_plugin
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();
    let pong = read_frame_line(&mut host_from_plugin).await;
    assert_eq!(pong.id, Some(json!(50)));

    // Give the spawned notification handler a tick to land.
    for _ in 0..20 {
        if captured.lock().await.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(
        captured.lock().await.clone(),
        Some(json!({ "kind": "ping" }))
    );

    drop(host_to_plugin);
    join.await.unwrap().unwrap();
}

#[tokio::test]
async fn method_between_initialize_and_initialized_returns_not_initialized() {
    let (mut host_to_plugin, plugin_in) = duplex(8 * 1024);
    let (plugin_out, mut host_from_plugin) = duplex(8 * 1024);

    let plugin = Plugin::new("test-init-gate", "0.0.1", "custom")
        .methods(["echo/say"])
        .register_method::<EchoRequest, EchoResponse, _, _>("echo/say", |req, _ctx| async move {
            Ok(EchoResponse {
                echoed: req.message,
            })
        });

    let join = tokio::spawn(async move { plugin.run_with_io(plugin_in, plugin_out).await });

    let initialize = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocol_version": PROTOCOL_VERSION,
            "host_info": { "name": "animus", "version": "0.5.0" },
            "capabilities": {},
            "init_extensions": {}
        }
    });
    host_to_plugin
        .write_all(format!("{initialize}\n").as_bytes())
        .await
        .unwrap();
    let init_response = read_frame_line(&mut host_from_plugin).await;
    assert!(init_response.result.is_some());

    // Domain call before `initialized` notification — must be rejected.
    let request = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "echo/say",
        "params": { "message": "before-initialized" }
    });
    host_to_plugin
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();
    let response = read_frame_line(&mut host_from_plugin).await;
    let error = response.error.expect("error envelope");
    assert_eq!(error.code, error_codes::PLUGIN_NOT_INITIALIZED);

    drop(host_to_plugin);
    join.await.unwrap().unwrap();
}

#[tokio::test]
async fn shutdown_waits_for_in_flight_handlers_before_acking() {
    let work_done: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

    let (mut host_to_plugin, plugin_in) = duplex(8 * 1024);
    let (plugin_out, mut host_from_plugin) = duplex(8 * 1024);

    let work_done_clone = Arc::clone(&work_done);
    let plugin = Plugin::new("test-shutdown-drain", "0.0.1", "custom")
        .methods(["work/slow"])
        .register_raw_method("work/slow", move |_params, _ctx| {
            let work_done_clone = Arc::clone(&work_done_clone);
            async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                *work_done_clone.lock().await = true;
                Ok(json!({ "ok": true }))
            }
        });

    let join = tokio::spawn(async move { plugin.run_with_io(plugin_in, plugin_out).await });

    host_to_plugin
        .write_all(initialize_frame(1).as_bytes())
        .await
        .unwrap();
    let _ = read_frame_line(&mut host_from_plugin).await;

    let request = json!({
        "jsonrpc": "2.0",
        "id": 60,
        "method": "work/slow",
        "params": {}
    });
    let shutdown = json!({ "jsonrpc": "2.0", "id": 61, "method": "shutdown" });
    let mut combined = format!("{request}\n");
    combined.push_str(&format!("{shutdown}\n"));
    host_to_plugin.write_all(combined.as_bytes()).await.unwrap();

    // The shutdown ack must arrive after the in-flight handler has finished.
    // Collect frames until we see both responses, then assert the worker ran.
    let mut saw_work = false;
    let mut saw_shutdown = false;
    while !saw_shutdown {
        let frame = read_frame_line(&mut host_from_plugin).await;
        if frame.id == Some(json!(60)) {
            saw_work = true;
        } else if frame.id == Some(json!(61)) {
            saw_shutdown = true;
        }
    }
    assert!(
        saw_work,
        "method response should arrive before shutdown ack"
    );
    assert!(
        *work_done.lock().await,
        "shutdown ack must wait until handler set work_done"
    );

    drop(host_to_plugin);
    join.await.unwrap().unwrap();
}

#[tokio::test]
async fn cancel_in_same_batch_as_request_still_trips_token() {
    let (mut host_to_plugin, plugin_in) = duplex(8 * 1024);
    let (plugin_out, mut host_from_plugin) = duplex(8 * 1024);

    let plugin = Plugin::new("test-cancel-batch", "0.0.1", "custom")
        .methods(["work/run"])
        .cancellation(true)
        .register_raw_method("work/run", |_params, ctx| async move {
            tokio::select! {
                _ = ctx.cancellation.cancelled() => Err(RpcError {
                    code: error_codes::REQUEST_CANCELLED,
                    message: "cancelled by host".to_string(),
                    data: None,
                }),
                _ = tokio::time::sleep(Duration::from_secs(3)) => Ok(json!({ "done": true })),
            }
        });

    let join = tokio::spawn(async move { plugin.run_with_io(plugin_in, plugin_out).await });

    host_to_plugin
        .write_all(initialize_frame(1).as_bytes())
        .await
        .unwrap();
    let _ = read_frame_line(&mut host_from_plugin).await;

    let request = json!({
        "jsonrpc": "2.0",
        "id": 71,
        "method": "work/run",
        "params": {}
    });
    let cancel = json!({
        "jsonrpc": "2.0",
        "method": "$/cancelRequest",
        "params": { "id": 71 }
    });
    // Write both frames in a single batch — the shell must register the
    // in-flight token before processing the cancel notification, otherwise
    // the cancellation would be lost.
    let mut combined = format!("{request}\n");
    combined.push_str(&format!("{cancel}\n"));
    host_to_plugin.write_all(combined.as_bytes()).await.unwrap();

    let response = read_frame_line(&mut host_from_plugin).await;
    let error = response.error.expect("cancelled response should be error");
    assert_eq!(error.code, error_codes::REQUEST_CANCELLED);

    drop(host_to_plugin);
    join.await.unwrap().unwrap();
}

#[tokio::test]
async fn streaming_subscription_cancellation_survives_handler_return() {
    let cancel_observed: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

    let (mut host_to_plugin, plugin_in) = duplex(8 * 1024);
    let (plugin_out, mut host_from_plugin) = duplex(8 * 1024);

    let cancel_observed_clone = Arc::clone(&cancel_observed);
    let plugin = Plugin::new("test-stream-cancel", "0.0.1", "custom")
        .methods(["stream/watch"])
        .cancellation(true)
        .streaming(true)
        .register_raw_method("stream/watch", move |_params, ctx| {
            let cancel_observed_clone = Arc::clone(&cancel_observed_clone);
            async move {
                // Subscription pattern: opt the cancellation token into
                // outliving the handler return so a later $/cancelRequest
                // can still trip the background stream.
                ctx.keep_cancellation();
                let token = ctx.cancellation.clone();
                tokio::spawn(async move {
                    token.cancelled().await;
                    *cancel_observed_clone.lock().await = true;
                });
                Ok(json!({ "watching": true }))
            }
        });

    let join = tokio::spawn(async move { plugin.run_with_io(plugin_in, plugin_out).await });

    host_to_plugin
        .write_all(initialize_frame(1).as_bytes())
        .await
        .unwrap();
    let _ = read_frame_line(&mut host_from_plugin).await;

    let request = json!({
        "jsonrpc": "2.0",
        "id": 81,
        "method": "stream/watch",
        "params": {}
    });
    host_to_plugin
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();
    let ack = read_frame_line(&mut host_from_plugin).await;
    assert_eq!(ack.id, Some(json!(81)));
    assert!(ack.result.is_some());

    // Now send the cancel for the same id. The token must still be live in
    // the shell's in-flight table.
    let cancel = json!({
        "jsonrpc": "2.0",
        "method": "$/cancelRequest",
        "params": { "id": 81 }
    });
    host_to_plugin
        .write_all(format!("{cancel}\n").as_bytes())
        .await
        .unwrap();

    for _ in 0..50 {
        if *cancel_observed.lock().await {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        *cancel_observed.lock().await,
        "cancellation must reach the long-lived subscription clone"
    );

    drop(host_to_plugin);
    join.await.unwrap().unwrap();
}

#[tokio::test]
async fn frame_with_trailing_garbage_is_discarded() {
    let invoked: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));

    let (mut host_to_plugin, plugin_in) = duplex(8 * 1024);
    let (plugin_out, mut host_from_plugin) = duplex(8 * 1024);

    let invoked_clone = Arc::clone(&invoked);
    let plugin = Plugin::new("test-garbage", "0.0.1", "custom")
        .methods(["mutate/do"])
        .register_raw_method("mutate/do", move |_params, _ctx| {
            let invoked_clone = Arc::clone(&invoked_clone);
            async move {
                *invoked_clone.lock().await += 1;
                Ok(json!({ "did": true }))
            }
        });

    let join = tokio::spawn(async move { plugin.run_with_io(plugin_in, plugin_out).await });

    host_to_plugin
        .write_all(initialize_frame(1).as_bytes())
        .await
        .unwrap();
    let _ = read_frame_line(&mut host_from_plugin).await;

    // Malformed frame: valid JSON object followed by garbage before \n.
    let bad =
        "{\"jsonrpc\":\"2.0\",\"id\":33,\"method\":\"mutate/do\",\"params\":{}}garbage_suffix\n";
    host_to_plugin.write_all(bad.as_bytes()).await.unwrap();

    // Follow with a real frame so we know the read loop has had a chance to
    // process the malformed one and resync.
    let real = json!({
        "jsonrpc": "2.0",
        "id": 34,
        "method": "mutate/do",
        "params": {}
    });
    host_to_plugin
        .write_all(format!("{real}\n").as_bytes())
        .await
        .unwrap();

    let response = read_frame_line(&mut host_from_plugin).await;
    assert_eq!(response.id, Some(json!(34)));
    assert!(response.result.is_some());

    drop(host_to_plugin);
    join.await.unwrap().unwrap();

    assert_eq!(
        *invoked.lock().await,
        1,
        "malformed frame must not have dispatched the mutating method"
    );
}

#[tokio::test]
async fn split_read_without_newline_defers_dispatch_until_newline_arrives() {
    let invoked: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));

    let (mut host_to_plugin, plugin_in) = duplex(8 * 1024);
    let (plugin_out, mut host_from_plugin) = duplex(8 * 1024);

    let invoked_clone = Arc::clone(&invoked);
    let plugin = Plugin::new("test-split", "0.0.1", "custom")
        .methods(["mutate/do"])
        .register_raw_method("mutate/do", move |_params, _ctx| {
            let invoked_clone = Arc::clone(&invoked_clone);
            async move {
                *invoked_clone.lock().await += 1;
                Ok(json!({ "did": true }))
            }
        });

    let join = tokio::spawn(async move { plugin.run_with_io(plugin_in, plugin_out).await });

    host_to_plugin
        .write_all(initialize_frame(1).as_bytes())
        .await
        .unwrap();
    let _ = read_frame_line(&mut host_from_plugin).await;

    // Write the JSON prefix WITHOUT a terminating newline. The shell must
    // not dispatch yet — it must wait until the terminator arrives.
    let prefix = "{\"jsonrpc\":\"2.0\",\"id\":42,\"method\":\"mutate/do\",\"params\":{}}";
    host_to_plugin.write_all(prefix.as_bytes()).await.unwrap();

    // Settle for a moment, then verify the handler has not yet run.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        *invoked.lock().await,
        0,
        "handler must not run before frame terminator arrives"
    );

    // Now send garbage and a newline — this should be discarded as a
    // malformed frame, and the count must still be 0.
    host_to_plugin
        .write_all(b"garbage_after_json\n")
        .await
        .unwrap();

    // Follow with a real frame so we know the loop has caught up.
    let real = json!({
        "jsonrpc": "2.0",
        "id": 43,
        "method": "mutate/do",
        "params": {}
    });
    host_to_plugin
        .write_all(format!("{real}\n").as_bytes())
        .await
        .unwrap();
    let response = read_frame_line(&mut host_from_plugin).await;
    assert_eq!(response.id, Some(json!(43)));

    drop(host_to_plugin);
    join.await.unwrap().unwrap();

    assert_eq!(
        *invoked.lock().await,
        1,
        "only the real frame should have dispatched"
    );
}

#[tokio::test]
async fn shutdown_trips_retained_subscription_tokens() {
    let cancel_observed: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));

    let (mut host_to_plugin, plugin_in) = duplex(8 * 1024);
    let (plugin_out, mut host_from_plugin) = duplex(8 * 1024);

    let cancel_observed_clone = Arc::clone(&cancel_observed);
    let plugin = Plugin::new("test-shutdown-watch", "0.0.1", "custom")
        .methods(["stream/watch"])
        .cancellation(true)
        .streaming(true)
        .register_raw_method("stream/watch", move |_params, ctx| {
            let cancel_observed_clone = Arc::clone(&cancel_observed_clone);
            async move {
                ctx.keep_cancellation();
                let token = ctx.cancellation.clone();
                tokio::spawn(async move {
                    token.cancelled().await;
                    *cancel_observed_clone.lock().await = true;
                });
                Ok(json!({ "watching": true }))
            }
        });

    let join = tokio::spawn(async move { plugin.run_with_io(plugin_in, plugin_out).await });

    host_to_plugin
        .write_all(initialize_frame(1).as_bytes())
        .await
        .unwrap();
    let _ = read_frame_line(&mut host_from_plugin).await;

    let request = json!({
        "jsonrpc": "2.0",
        "id": 91,
        "method": "stream/watch",
        "params": {}
    });
    host_to_plugin
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();
    let _ack = read_frame_line(&mut host_from_plugin).await;

    let shutdown = json!({ "jsonrpc": "2.0", "id": 92, "method": "shutdown" });
    host_to_plugin
        .write_all(format!("{shutdown}\n").as_bytes())
        .await
        .unwrap();
    let resp = read_frame_line(&mut host_from_plugin).await;
    assert_eq!(resp.id, Some(json!(92)));

    drop(host_to_plugin);
    join.await.unwrap().unwrap();

    assert!(
        *cancel_observed.lock().await,
        "shutdown must cancel retained subscription tokens"
    );
}

#[tokio::test]
async fn incompatible_protocol_major_rejects_initialize() {
    let (mut host_to_plugin, plugin_in) = duplex(8 * 1024);
    let (plugin_out, mut host_from_plugin) = duplex(8 * 1024);

    let plugin = Plugin::new("test-version", "0.0.1", "custom");
    let join = tokio::spawn(async move { plugin.run_with_io(plugin_in, plugin_out).await });

    // Plugin is on 1.x; host claims 2.x — must be rejected.
    let initialize = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocol_version": "2.0.0",
            "host_info": { "name": "animus", "version": "9.9.9" },
            "capabilities": {},
            "init_extensions": {}
        }
    });
    host_to_plugin
        .write_all(format!("{initialize}\n").as_bytes())
        .await
        .unwrap();
    let response = read_frame_line(&mut host_from_plugin).await;
    let error = response.error.expect("incompatible major must error");
    assert_eq!(error.code, error_codes::INVALID_PARAMS);
    assert!(
        error.message.contains("incompatible protocol major"),
        "unexpected message: {}",
        error.message
    );

    drop(host_to_plugin);
    join.await.unwrap().unwrap();
}

#[test]
fn register_method_macro_compiles_and_threads_types() {
    let plugin = Plugin::new("test-macro", "0.0.1", "custom").methods(["echo/say"]);
    let _ = crate::register_method!(
        plugin,
        "echo/say",
        EchoRequest => EchoResponse,
        |req, _ctx: MethodContext| async move {
            Ok(EchoResponse { echoed: req.message })
        },
    );
}

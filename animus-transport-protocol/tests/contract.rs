//! Wire-shape contract tests for `animus-transport-protocol`.
//!
//! These tests pin the JSON shapes that hosts and plugins MUST agree on. If
//! you change a field name, default, or rename rule, expect this file to
//! break — that's the point. Update `spec.md` alongside any change here.

use std::path::PathBuf;

use animus_transport_protocol::{
    TransportConfig, TransportInfo, TransportSchema, PLUGIN_KIND_TRANSPORT_BACKEND,
    TRANSPORT_METHOD_SCHEMA, TRANSPORT_METHOD_SHUTDOWN, TRANSPORT_METHOD_START,
};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};

fn fixed_ts() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-05-23T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
}

#[test]
fn transport_config_round_trips_json() {
    let config = TransportConfig {
        control_socket_path: PathBuf::from("/Users/op/.animus/scope/control.sock"),
        project_root: PathBuf::from("/Users/op/code/animus"),
        bind_addr: Some("127.0.0.1:8080".into()),
        config: json!({
            "cors": {"allowed_origins": ["*"]},
            "auth_token": "redacted",
        }),
    };

    let value = serde_json::to_value(&config).expect("serialize");
    assert_eq!(
        value["control_socket_path"],
        json!("/Users/op/.animus/scope/control.sock")
    );
    assert_eq!(value["project_root"], json!("/Users/op/code/animus"));
    assert_eq!(value["bind_addr"], json!("127.0.0.1:8080"));
    assert_eq!(value["config"]["auth_token"], json!("redacted"));
    assert_eq!(value["config"]["cors"]["allowed_origins"], json!(["*"]));

    let back: TransportConfig = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, config);
}

#[test]
fn transport_schema_round_trips() {
    let schema = TransportSchema {
        kinds: vec!["http".into(), "rest".into()],
        supports_streaming: true,
        supports_websocket: false,
        default_port: Some(8080),
    };

    let value = serde_json::to_value(&schema).expect("serialize");
    assert_eq!(value["kinds"], json!(["http", "rest"]));
    assert_eq!(value["supports_streaming"], json!(true));
    assert_eq!(value["supports_websocket"], json!(false));
    assert_eq!(value["default_port"], json!(8080));

    let back: TransportSchema = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, schema);

    // Schemas advertising no default port omit the key entirely so older
    // hosts that hard-fail on unexpected nulls don't trip on the field.
    let grpc = TransportSchema {
        kinds: vec!["grpc".into()],
        supports_streaming: true,
        supports_websocket: false,
        default_port: None,
    };
    let value = serde_json::to_value(&grpc).expect("serialize");
    assert!(value.get("default_port").is_none());
}

#[test]
fn transport_info_round_trips() {
    let info = TransportInfo {
        bound_addr: "127.0.0.1:8080".into(),
        started_at: fixed_ts(),
    };

    let value = serde_json::to_value(&info).expect("serialize");
    assert_eq!(value["bound_addr"], json!("127.0.0.1:8080"));
    // RFC 3339 with `Z` zone, matching chrono's default serializer.
    assert_eq!(value["started_at"], json!("2026-05-23T12:00:00Z"));

    let back: TransportInfo = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, info);
}

#[test]
fn manifest_serializes_omits_default_config() {
    // Backward compatibility: a `TransportConfig` with `bind_addr = None`
    // and `config = Value::Null` must serialize without introducing
    // surprise keys older clients would have to handle.
    let config = TransportConfig {
        control_socket_path: PathBuf::from("/tmp/control.sock"),
        project_root: PathBuf::from("/tmp/proj"),
        bind_addr: None,
        config: Value::Null,
    };

    let value = serde_json::to_value(&config).expect("serialize");
    assert!(
        value.get("bind_addr").is_none(),
        "bind_addr should be omitted when None"
    );
    assert!(
        value.get("config").is_none(),
        "config should be omitted when Null"
    );

    // Round-trip back through serde to confirm the omissions are accepted
    // on deserialize as the documented defaults.
    let back: TransportConfig = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, config);
}

#[test]
fn transport_method_constants_pin_wire_strings() {
    assert_eq!(TRANSPORT_METHOD_START, "transport/start");
    assert_eq!(TRANSPORT_METHOD_SHUTDOWN, "transport/shutdown");
    assert_eq!(TRANSPORT_METHOD_SCHEMA, "transport/schema");
    assert_eq!(PLUGIN_KIND_TRANSPORT_BACKEND, "transport_backend");
}

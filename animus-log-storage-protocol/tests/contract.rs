//! Wire-shape contract tests for `animus-log-storage-protocol`.
//!
//! These tests pin the JSON shapes that hosts and plugins MUST agree on. If
//! you change a field name, default, or rename rule, expect this file to
//! break — that's the point. Update `spec.md` alongside any change here.

use animus_log_storage_protocol::{
    LogEntry, LogLevel, LogQuery, LogQueryResult, LogSource, LogStorageSchema, SupportsFiltering,
};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};

fn fixed_ts() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-05-17T18:20:34Z")
        .unwrap()
        .with_timezone(&Utc)
}

#[test]
fn log_entry_round_trips_json() {
    let entry = LogEntry {
        id: "evt-001".into(),
        ts: fixed_ts(),
        level: LogLevel::Info,
        source: LogSource::Plugin,
        source_name: Some("animus-subject-linear".into()),
        target: "plugin.animus-subject-linear.client".into(),
        message: "fetched 14 issues".into(),
        fields: json!({"count": 14, "tenant": "acme"}),
    };

    let value = serde_json::to_value(&entry).expect("serialize");
    assert_eq!(value["id"], json!("evt-001"));
    assert_eq!(value["level"], json!("info"));
    assert_eq!(value["source"], json!("plugin"));
    assert_eq!(value["source_name"], json!("animus-subject-linear"));
    assert_eq!(
        value["target"],
        json!("plugin.animus-subject-linear.client")
    );
    assert_eq!(value["fields"], json!({"count": 14, "tenant": "acme"}));

    let back: LogEntry = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, entry);
}

#[test]
fn log_query_round_trips_with_all_filters_set() {
    let query = LogQuery {
        min_level: Some(LogLevel::Warn),
        source: Some(LogSource::Daemon),
        source_name: Some("scheduler".into()),
        target_glob: Some("daemon.scheduler.*".into()),
        since: Some(fixed_ts()),
        until: Some(fixed_ts()),
        limit: Some(100),
        cursor: Some("opaque-page-2".into()),
        follow: true,
    };

    let value = serde_json::to_value(&query).expect("serialize");
    assert_eq!(value["min_level"], json!("warn"));
    assert_eq!(value["source"], json!("daemon"));
    assert_eq!(value["source_name"], json!("scheduler"));
    assert_eq!(value["target_glob"], json!("daemon.scheduler.*"));
    assert_eq!(value["limit"], json!(100));
    assert_eq!(value["cursor"], json!("opaque-page-2"));
    assert_eq!(value["follow"], json!(true));

    let back: LogQuery = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, query);
}

#[test]
fn log_query_default_round_trips_with_minimal_shape() {
    let query = LogQuery::default();
    let value = serde_json::to_value(&query).expect("serialize");

    // Optional fields are omitted entirely; only `follow` (a bool with no
    // skip rule) shows up at its default value.
    assert!(value.get("min_level").is_none());
    assert!(value.get("source").is_none());
    assert!(value.get("source_name").is_none());
    assert!(value.get("target_glob").is_none());
    assert!(value.get("since").is_none());
    assert!(value.get("until").is_none());
    assert!(value.get("limit").is_none());
    assert!(value.get("cursor").is_none());
    assert_eq!(value["follow"], json!(false));

    let back: LogQuery = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, query);
}

#[test]
fn log_storage_schema_round_trips() {
    let schema = LogStorageSchema {
        supports_query: true,
        supports_tail: true,
        supports_dedup: true,
        supports_filtering: SupportsFiltering {
            by_level: true,
            by_source: true,
            by_target: true,
            by_time_range: true,
            by_glob: false,
        },
        max_query_window: Some(chrono::Duration::days(30)),
        retention_hint: Some(chrono::Duration::days(7)),
    };

    let value = serde_json::to_value(&schema).expect("serialize");
    assert_eq!(value["supports_query"], json!(true));
    assert_eq!(value["supports_tail"], json!(true));
    assert_eq!(value["supports_dedup"], json!(true));
    assert_eq!(value["supports_filtering"]["by_glob"], json!(false));
    assert_eq!(
        value["max_query_window"],
        json!(chrono::Duration::days(30).num_milliseconds())
    );
    assert_eq!(
        value["retention_hint"],
        json!(chrono::Duration::days(7).num_milliseconds())
    );

    let back: LogStorageSchema = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, schema);
}

#[test]
fn log_entry_omits_empty_fields_in_serialization() {
    // Backward compatibility: a minimal-shape `LogEntry` does not introduce
    // surprise keys older query libraries would have to handle.
    let entry = LogEntry {
        id: "evt-002".into(),
        ts: fixed_ts(),
        level: LogLevel::Error,
        source: LogSource::Daemon,
        source_name: None,
        target: "daemon".into(),
        message: "panic".into(),
        fields: Value::Null,
    };

    let value = serde_json::to_value(&entry).expect("serialize");
    assert!(
        value.get("source_name").is_none(),
        "source_name should be omitted when None"
    );
    assert!(
        value.get("fields").is_none(),
        "fields should be omitted when Null"
    );
}

#[test]
fn log_level_serializes_lowercase() {
    for (level, expected) in [
        (LogLevel::Trace, "trace"),
        (LogLevel::Debug, "debug"),
        (LogLevel::Info, "info"),
        (LogLevel::Warn, "warn"),
        (LogLevel::Error, "error"),
    ] {
        let value = serde_json::to_value(level).expect("serialize");
        assert_eq!(value, json!(expected), "level {level:?} serializes wrong");
    }
}

#[test]
fn log_source_serializes_snake_case() {
    for (source, expected) in [
        (LogSource::Daemon, "daemon"),
        (LogSource::Plugin, "plugin"),
        (LogSource::Cli, "cli"),
        (LogSource::Workflow, "workflow"),
    ] {
        let value = serde_json::to_value(source).expect("serialize");
        assert_eq!(value, json!(expected), "source {source:?} serializes wrong");
    }
}

#[test]
fn log_query_result_round_trips() {
    let result = LogQueryResult {
        entries: vec![LogEntry {
            id: "evt-003".into(),
            ts: fixed_ts(),
            level: LogLevel::Info,
            source: LogSource::Workflow,
            source_name: Some("wf-7b8a".into()),
            target: "workflow.code-review".into(),
            message: "phase started".into(),
            fields: json!({"phase": "code-review"}),
        }],
        next_cursor: Some("cursor-abc".into()),
    };

    let value = serde_json::to_value(&result).expect("serialize");
    assert_eq!(value["entries"][0]["id"], json!("evt-003"));
    assert_eq!(value["next_cursor"], json!("cursor-abc"));

    let back: LogQueryResult = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, result);
}

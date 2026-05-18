//! Wire-shape contract tests for `animus-trigger-protocol`.
//!
//! These tests pin the JSON shapes that hosts and plugins MUST agree on. If
//! you change a field name, default, or rename rule, expect this file to
//! break — that's the point. Update `spec.md` alongside any change here.

use animus_trigger_protocol::{TriggerEvent, TriggerSchema};
use chrono::{DateTime, Utc};
use serde_json::json;

#[test]
fn trigger_event_round_trips_through_json() {
    let occurred_at = DateTime::parse_from_rfc3339("2026-05-14T18:20:34Z")
        .unwrap()
        .with_timezone(&Utc);
    let event = TriggerEvent {
        id: "slack:T123/C456/1715701234.000100".into(),
        occurred_at,
        kind: "slack_mention".into(),
        payload: json!({"user": "U1", "text": "@animus please review"}),
        subject_id: Some("linear:ENG-123".into()),
        action_hint: Some("run-workflow:review".into()),
    };

    let value = serde_json::to_value(&event).expect("serialize");
    assert_eq!(value["id"], json!("slack:T123/C456/1715701234.000100"));
    assert_eq!(value["kind"], json!("slack_mention"));
    assert_eq!(value["subject_id"], json!("linear:ENG-123"));
    assert_eq!(value["action_hint"], json!("run-workflow:review"));

    let back: TriggerEvent = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, event);
}

#[test]
fn trigger_event_omits_optional_fields_when_none() {
    let event = TriggerEvent {
        id: "tick-0".into(),
        occurred_at: Utc::now(),
        kind: "cron_tick".into(),
        payload: json!({}),
        subject_id: None,
        action_hint: None,
    };

    let value = serde_json::to_value(&event).expect("serialize");
    assert!(
        value.get("subject_id").is_none(),
        "subject_id should be omitted when None"
    );
    assert!(
        value.get("action_hint").is_none(),
        "action_hint should be omitted when None"
    );
}

#[test]
fn trigger_schema_round_trips_through_json() {
    let schema = TriggerSchema {
        kinds: vec!["slack_mention".into(), "slack_channel_message".into()],
        supports_resume: true,
        supports_dedup: true,
        supports_ack: true,
    };

    let value = serde_json::to_value(&schema).expect("serialize");
    assert_eq!(
        value["kinds"],
        json!(["slack_mention", "slack_channel_message"])
    );
    assert_eq!(value["supports_resume"], json!(true));
    assert_eq!(value["supports_dedup"], json!(true));
    assert_eq!(value["supports_ack"], json!(true));

    let back: TriggerSchema = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, schema);
}

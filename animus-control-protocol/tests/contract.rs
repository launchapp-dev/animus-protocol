//! Cross-cutting contract tests for the control protocol wire format.
//!
//! Inline tests inside `src/` cover individual modules; this file covers
//! invariants that span modules — method-name uniqueness, naming convention,
//! type re-use across crates.

use std::collections::HashSet;

use animus_control_protocol::method::*;
use animus_control_protocol::types::{
    PluginInstallRequest, QueueEnqueueRequest, QueueReorderPosition, QueueReorderRequest,
    SubjectGetRequest, SubjectListRequest, SubjectListResponse, WorkflowRunRequest,
    WorkflowRunStart, WorkflowStatus,
};
use animus_control_protocol::ControlError;
use animus_subject_protocol::{SubjectId, SubjectStatus};
use chrono::{TimeZone, Utc};

fn all_methods() -> Vec<&'static str> {
    vec![
        // subject
        METHOD_SUBJECT_LIST,
        METHOD_SUBJECT_GET,
        METHOD_SUBJECT_CREATE,
        METHOD_SUBJECT_UPDATE,
        METHOD_SUBJECT_NEXT,
        METHOD_SUBJECT_STATUS,
        METHOD_SUBJECT_WATCH,
        NOTIFICATION_SUBJECT_CHANGED,
        // plugin
        METHOD_PLUGIN_LIST,
        METHOD_PLUGIN_INFO,
        METHOD_PLUGIN_INSTALL,
        METHOD_PLUGIN_UNINSTALL,
        METHOD_PLUGIN_PING,
        METHOD_PLUGIN_CALL,
        METHOD_PLUGIN_SEARCH,
        METHOD_PLUGIN_BROWSE,
        METHOD_PLUGIN_UPDATE,
        // daemon
        METHOD_DAEMON_STATUS,
        METHOD_DAEMON_HEALTH,
        METHOD_DAEMON_START,
        METHOD_DAEMON_STOP,
        METHOD_DAEMON_RESTART,
        METHOD_DAEMON_AGENTS,
        METHOD_DAEMON_EVENTS,
        NOTIFICATION_DAEMON_EVENT,
        METHOD_DAEMON_LOGS,
        NOTIFICATION_DAEMON_LOG,
        // workflow
        METHOD_WORKFLOW_LIST,
        METHOD_WORKFLOW_GET,
        METHOD_WORKFLOW_RUN,
        METHOD_WORKFLOW_EXECUTE,
        METHOD_WORKFLOW_PAUSE,
        METHOD_WORKFLOW_RESUME,
        METHOD_WORKFLOW_CANCEL,
        // agent
        METHOD_AGENT_RUN,
        METHOD_AGENT_STATUS,
        METHOD_AGENT_CANCEL,
        // queue
        METHOD_QUEUE_LIST,
        METHOD_QUEUE_ENQUEUE,
        METHOD_QUEUE_DROP,
        METHOD_QUEUE_HOLD,
        METHOD_QUEUE_RELEASE,
        METHOD_QUEUE_REORDER,
        METHOD_QUEUE_STATS,
        // project
        METHOD_PROJECT_INIT,
        METHOD_PROJECT_SETUP,
        METHOD_PROJECT_STATUS,
    ]
}

#[test]
fn method_constants_unique() {
    let methods = all_methods();
    let mut seen: HashSet<&'static str> = HashSet::new();
    for m in &methods {
        assert!(
            seen.insert(m),
            "duplicate method constant: {m} appears more than once",
        );
    }
    // Spot-check the count so a regression that drops a method shows up.
    assert!(
        methods.len() >= 45,
        "expected at least 45 method constants, got {}",
        methods.len()
    );
}

#[test]
fn streaming_method_names_use_consistent_suffix() {
    // Streaming methods MUST end in `/watch` or `/events` or `/logs` and pair
    // with a notification whose name uses singular form (`<group>/changed`,
    // `<group>/event`, `<group>/log`).
    for m in [
        METHOD_SUBJECT_WATCH,
        METHOD_DAEMON_EVENTS,
        METHOD_DAEMON_LOGS,
    ] {
        let ok = m.ends_with("/watch") || m.ends_with("/events") || m.ends_with("/logs");
        assert!(
            ok,
            "streaming method {m} does not end in /watch|/events|/logs"
        );
    }
    for n in [
        NOTIFICATION_SUBJECT_CHANGED,
        NOTIFICATION_DAEMON_EVENT,
        NOTIFICATION_DAEMON_LOG,
    ] {
        let group = n.split('/').next().unwrap();
        let verb = n.split('/').nth(1).unwrap();
        assert!(
            !group.is_empty() && !verb.is_empty(),
            "notification {n} must be <group>/<verb>"
        );
    }
}

#[test]
fn method_names_use_slash_separator() {
    for m in all_methods() {
        assert!(
            m.contains('/'),
            "method name {m} must use slash separator (not dot)"
        );
        // No leading / trailing slashes, no double slashes.
        assert!(!m.starts_with('/'), "method {m} starts with /");
        assert!(!m.ends_with('/'), "method {m} ends with /");
        assert!(!m.contains("//"), "method {m} contains //");
    }
}

#[test]
fn request_round_trip_through_json() {
    // Subject list
    let subject_list = SubjectListRequest::default();
    let v = serde_json::to_value(&subject_list).unwrap();
    let back: SubjectListRequest = serde_json::from_value(v).unwrap();
    assert_eq!(back, subject_list);

    // Subject get
    let get = SubjectGetRequest {
        id: SubjectId::new("linear:ENG-1"),
    };
    let v = serde_json::to_value(&get).unwrap();
    let back: SubjectGetRequest = serde_json::from_value(v).unwrap();
    assert_eq!(back, get);

    // Workflow run
    let run = WorkflowRunRequest {
        task_id: "TASK-001".into(),
        definition: Some("default".into()),
        params: Default::default(),
        actor: None,
    };
    let v = serde_json::to_value(&run).unwrap();
    let back: WorkflowRunRequest = serde_json::from_value(v).unwrap();
    assert_eq!(back, run);

    // Plugin install
    let install = PluginInstallRequest {
        source: "animus-subject-linear".into(),
        version: Some("0.2.0".into()),
        yes: true,
        allow_unsigned: false,
    };
    let v = serde_json::to_value(&install).unwrap();
    let back: PluginInstallRequest = serde_json::from_value(v).unwrap();
    assert_eq!(back, install);

    // Queue enqueue
    let enqueue = QueueEnqueueRequest {
        task_id: "TASK-002".into(),
        priority: Some(3),
    };
    let v = serde_json::to_value(&enqueue).unwrap();
    let back: QueueEnqueueRequest = serde_json::from_value(v).unwrap();
    assert_eq!(back, enqueue);

    // Queue reorder (single-entry form)
    let reorder = QueueReorderRequest {
        id: Some("q-1".into()),
        subject_ids: Vec::new(),
        anchor_id: Some("q-9".into()),
        position: QueueReorderPosition::Before,
    };
    let v = serde_json::to_value(&reorder).unwrap();
    let back: QueueReorderRequest = serde_json::from_value(v).unwrap();
    assert_eq!(back, reorder);

    // Queue reorder (multi-entry form)
    let multi = QueueReorderRequest {
        id: None,
        subject_ids: vec!["q-1".into(), "q-2".into(), "q-3".into()],
        anchor_id: Some("q-9".into()),
        position: QueueReorderPosition::After,
    };
    let v = serde_json::to_value(&multi).unwrap();
    let back: QueueReorderRequest = serde_json::from_value(v).unwrap();
    assert_eq!(back, multi);
}

#[test]
fn response_round_trip_through_json() {
    let ts = Utc.with_ymd_and_hms(2026, 5, 20, 12, 0, 0).unwrap();

    let list = SubjectListResponse {
        subjects: vec![],
        next_cursor: Some("page-2".into()),
        fetched_at: ts,
    };
    let v = serde_json::to_value(&list).unwrap();
    let back: SubjectListResponse = serde_json::from_value(v).unwrap();
    assert_eq!(back, list);

    let start = WorkflowRunStart {
        workflow_id: "wf-123".into(),
        status: WorkflowStatus::Running,
        started_at: ts,
    };
    let v = serde_json::to_value(&start).unwrap();
    let back: WorkflowRunStart = serde_json::from_value(v).unwrap();
    assert_eq!(back, start);
}

#[test]
fn control_error_serializes_compactly() {
    let err = ControlError::InvalidRequest("bad id".into());
    let v = serde_json::to_value(&err).unwrap();
    // Only `category` + `message` on the wire — no extra metadata.
    assert_eq!(v.as_object().unwrap().len(), 2);
    assert_eq!(
        v.get("category"),
        Some(&serde_json::json!("invalid_request"))
    );
    assert_eq!(v.get("message"), Some(&serde_json::json!("bad id")));
}

#[test]
fn subject_request_reuses_protocol_types() {
    // Compile-time check: SubjectGetRequest must reference SubjectId from
    // the upstream protocol crate, not a locally re-declared id type. The
    // following assignment only compiles when the types are identical.
    let upstream_id: SubjectId = SubjectId::new("linear:ENG-1");
    let req = SubjectGetRequest {
        id: upstream_id.clone(),
    };
    assert_eq!(req.id, upstream_id);

    // SubjectStatus from the upstream crate flows through the control
    // protocol's SubjectListRequest filter without translation.
    let mut filter = animus_subject_protocol::SubjectFilter::default();
    filter.status.push(SubjectStatus::Ready);
    let _wrapped = SubjectListRequest { filter };
}

#[test]
fn workflow_status_serializes_kebab_case() {
    let pairs = [
        (WorkflowStatus::Pending, "pending"),
        (WorkflowStatus::Running, "running"),
        (WorkflowStatus::Paused, "paused"),
        (WorkflowStatus::Completed, "completed"),
        (WorkflowStatus::Failed, "failed"),
        (WorkflowStatus::Cancelled, "cancelled"),
    ];
    for (status, expected) in pairs {
        assert_eq!(
            serde_json::to_value(status).unwrap(),
            serde_json::json!(expected)
        );
    }
}

#[test]
fn empty_request_objects_serialize_as_empty_json() {
    use animus_control_protocol::types::Unit;
    let unit = Unit::default();
    let v = serde_json::to_value(unit).unwrap();
    assert_eq!(v, serde_json::json!({}));
    let back: Unit = serde_json::from_value(v).unwrap();
    assert_eq!(back, unit);
}

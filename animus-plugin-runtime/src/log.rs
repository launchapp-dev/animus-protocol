//! Plugin-side log emission macros.
//!
//! Plugins emit structured log lines back to the host as JSON-RPC
//! notifications on the `log/entry` channel. The host forwards these into
//! its observability pipeline (daemon log file + log-storage backend) so
//! plugin output is queryable alongside core engine logs.
//!
//! # Usage
//!
//! Each `*_main` entrypoint in this crate installs the global emitter
//! transparently — plugin authors do not need to wire anything up. From
//! plugin code, just call the macros:
//!
//! ```ignore
//! use animus_plugin_runtime::{info, warn, error};
//! use serde_json::json;
//!
//! info!(target: "linear", message: "fetched issues", fields: json!({"count": 42}));
//! warn!(target: "linear", message: "rate limited", fields: json!({"retry_after_ms": 1500}));
//! error!(target: "linear", message: "auth failed", fields: json!({"status": 401}));
//! ```
//!
//! Macro output is best-effort: if the emitter is not yet installed (e.g.
//! during `--manifest` printing) the call is a no-op rather than a panic.
//!
//! # Wire format
//!
//! Each call emits a JSON-RPC notification with `method = "log/entry"` and
//! params shaped like:
//!
//! ```json
//! {
//!   "level": "info",
//!   "target": "linear",
//!   "message": "fetched issues",
//!   "fields": { "count": 42 },
//!   "ts": "2026-05-22T15:04:05.123456Z"
//! }
//! ```

use std::sync::OnceLock;

use serde_json::{json, Value};
use tokio::sync::mpsc::UnboundedSender;

use animus_plugin_protocol::RpcNotification;

/// JSON-RPC notification method emitted by the log macros.
pub const NOTIFICATION_LOG_ENTRY: &str = "log/entry";

/// A log level surfaced to the host as the `level` field on `log/entry`.
#[derive(Debug, Clone, Copy)]
pub enum LogLevel {
    /// Diagnostic-level detail.
    Trace,
    /// Debug-level detail.
    Debug,
    /// Informational event.
    Info,
    /// Recoverable problem.
    Warn,
    /// Error event.
    Error,
}

impl LogLevel {
    /// Lowercase wire representation.
    pub fn as_str(self) -> &'static str {
        match self {
            LogLevel::Trace => "trace",
            LogLevel::Debug => "debug",
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
        }
    }
}

static EMITTER: OnceLock<UnboundedSender<RpcNotification>> = OnceLock::new();

/// Install the global notification sender. Called by the `*_main`
/// entrypoints in this crate during startup. Subsequent calls are ignored —
/// the first installation wins for the lifetime of the process.
pub fn install_emitter(sender: UnboundedSender<RpcNotification>) {
    let _ = EMITTER.set(sender);
}

/// Construct a `log/entry` notification and push it onto the emitter
/// channel. Returns silently if no emitter is installed.
///
/// Intended to be called via the [`trace!`], [`debug!`], [`info!`],
/// [`warn!`], and [`error!`] macros — direct calls are supported for
/// non-macro callers.
pub fn emit(level: LogLevel, target: &str, message: &str, fields: Value) {
    let Some(sender) = EMITTER.get() else {
        return;
    };
    let ts = current_rfc3339();
    let params = json!({
        "level": level.as_str(),
        "target": target,
        "message": message,
        "fields": fields,
        "ts": ts,
    });
    let notification = RpcNotification::new(NOTIFICATION_LOG_ENTRY, Some(params));
    let _ = sender.send(notification);
}

fn current_rfc3339() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    let nanos = dur.subsec_nanos();
    let days = (secs / 86_400) as i64;
    let rem = (secs % 86_400) as u32;
    let hour = rem / 3600;
    let minute = (rem % 3600) / 60;
    let second = rem % 60;
    let (year, month, day) = days_to_ymd(days);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}Z",
        year,
        month,
        day,
        hour,
        minute,
        second,
        nanos / 1000
    )
}

fn days_to_ymd(mut days: i64) -> (i32, u32, u32) {
    days += 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let doe = (days - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = if m <= 2 { y + 1 } else { y };
    (year as i32, m, d)
}

/// Emit a `trace`-level log entry.
#[macro_export]
macro_rules! trace {
    (target: $target:expr, message: $message:expr, fields: $fields:expr) => {
        $crate::log::emit($crate::log::LogLevel::Trace, $target, $message, $fields)
    };
    (target: $target:expr, message: $message:expr) => {
        $crate::log::emit(
            $crate::log::LogLevel::Trace,
            $target,
            $message,
            ::serde_json::Value::Null,
        )
    };
}

/// Emit a `debug`-level log entry.
#[macro_export]
macro_rules! debug {
    (target: $target:expr, message: $message:expr, fields: $fields:expr) => {
        $crate::log::emit($crate::log::LogLevel::Debug, $target, $message, $fields)
    };
    (target: $target:expr, message: $message:expr) => {
        $crate::log::emit(
            $crate::log::LogLevel::Debug,
            $target,
            $message,
            ::serde_json::Value::Null,
        )
    };
}

/// Emit an `info`-level log entry.
#[macro_export]
macro_rules! info {
    (target: $target:expr, message: $message:expr, fields: $fields:expr) => {
        $crate::log::emit($crate::log::LogLevel::Info, $target, $message, $fields)
    };
    (target: $target:expr, message: $message:expr) => {
        $crate::log::emit(
            $crate::log::LogLevel::Info,
            $target,
            $message,
            ::serde_json::Value::Null,
        )
    };
}

/// Emit a `warn`-level log entry.
#[macro_export]
macro_rules! warn {
    (target: $target:expr, message: $message:expr, fields: $fields:expr) => {
        $crate::log::emit($crate::log::LogLevel::Warn, $target, $message, $fields)
    };
    (target: $target:expr, message: $message:expr) => {
        $crate::log::emit(
            $crate::log::LogLevel::Warn,
            $target,
            $message,
            ::serde_json::Value::Null,
        )
    };
}

/// Emit an `error`-level log entry.
#[macro_export]
macro_rules! error {
    (target: $target:expr, message: $message:expr, fields: $fields:expr) => {
        $crate::log::emit($crate::log::LogLevel::Error, $target, $message, $fields)
    };
    (target: $target:expr, message: $message:expr) => {
        $crate::log::emit(
            $crate::log::LogLevel::Error,
            $target,
            $message,
            ::serde_json::Value::Null,
        )
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::unbounded_channel;

    #[test]
    fn emit_without_installed_sender_is_noop() {
        // Should not panic when no emitter is installed.
        emit(LogLevel::Info, "test", "noop", json!({}));
    }

    #[tokio::test]
    async fn emit_routes_to_installed_sender() {
        let (tx, mut rx) = unbounded_channel();
        install_emitter(tx);
        emit(
            LogLevel::Warn,
            "unit",
            "hello",
            json!({"k": "v"}),
        );
        let notification = rx.recv().await.expect("notification");
        assert_eq!(notification.method, NOTIFICATION_LOG_ENTRY);
        let params = notification.params.expect("params");
        assert_eq!(params["level"], "warn");
        assert_eq!(params["target"], "unit");
        assert_eq!(params["message"], "hello");
        assert_eq!(params["fields"]["k"], "v");
        assert!(params["ts"].is_string());
    }
}

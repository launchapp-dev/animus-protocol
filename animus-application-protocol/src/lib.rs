//! Shared, application-facing Animus wire contracts.
//!
//! This crate owns only the language-neutral values that must agree across the
//! Animus runtime and application clients. A portal remains responsible for
//! authentication, authorization, projection, and deriving `allowed_actions`;
//! a spatial client remains responsible for its world protocol. Neither policy
//! nor world state belongs in this crate.

#![warn(missing_docs)]

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Schema identifier for canonical application chat controls.
pub const APPLICATION_CHAT_CONTROLS_SCHEMA: &str = "animus.chat.application_controls.v1";
/// Maximum encoded JSON size accepted for a controls envelope.
pub const MAX_APPLICATION_CHAT_CONTROLS_BYTES: usize = 2_048;
/// Maximum byte length of a configured profile or skill reference.
pub const MAX_APPLICATION_CHAT_CONTROL_REF_BYTES: usize = 64;
/// Maximum UTF-8 byte length of an application receipt identifier.
pub const MAX_APPLICATION_PROTOCOL_STRING_BYTES: usize = 512;
/// Maximum UTF-8 byte length of a safe application receipt error message.
pub const MAX_APPLICATION_CHAT_ERROR_BYTES: usize = 1_024;
/// Largest exact integer representable by every supported JSON consumer.
pub const MAX_APPLICATION_CHAT_SEQUENCE: u64 = 9_007_199_254_740_991;

/// Portal-derived action a client may render for a projected resource.
///
/// This enum standardizes the wire vocabulary; it does not grant an action.
/// The authenticated application boundary remains the authority that derives
/// the array for each caller and resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AllowedAction {
    /// Read the resource projection.
    Read,
    /// Inspect an agent's safe configuration projection.
    Inspect,
    /// Launch a workflow run from the resource.
    Launch,
    /// Mint or consume a stream grant for a run.
    Stream,
    /// Send a message to a conversation.
    Send,
    /// Respond to an interaction.
    Respond,
}

/// Resource kinds exposed by the application projection boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ApplicationResourceKind {
    /// Configured agent profile.
    Agent,
    /// Durable chat conversation.
    Chat,
    /// Workflow definition.
    Workflow,
    /// Workflow execution.
    Run,
    /// Backend-owned subject.
    Subject,
    /// Queue entry.
    QueueEntry,
    /// Durable operation record.
    Operation,
    /// Human interaction request.
    Interaction,
}

/// Visibility vocabulary carried by application resource projections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ResourceVisibility {
    /// Visible only through an explicit relationship or administration.
    Private,
    /// Visible within the authenticated organization boundary.
    Org,
    /// Publicly readable.
    Public,
}

/// Stable schema discriminator for [`ApplicationChatControls`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ApplicationChatControlsSchema {
    /// `animus.chat.application_controls.v1`.
    #[default]
    #[serde(rename = "animus.chat.application_controls.v1")]
    V1,
}

/// Application-safe reasoning effort requested from an authorized profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationReasoningEffort {
    /// Low reasoning effort.
    Low,
    /// Medium reasoning effort.
    Medium,
    /// High reasoning effort.
    High,
}

/// Provider-neutral permission intent selected by an application.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationPermissionIntent {
    /// Inherit safe provider defaults.
    Default,
    /// Review without broad edit permission.
    Review,
    /// Permit ordinary automated edits.
    AutoEdit,
    /// Request the profile's explicitly authorized unrestricted mode.
    Unrestricted,
}

impl ApplicationReasoningEffort {
    /// Return the canonical wire value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

impl ApplicationPermissionIntent {
    /// Return the canonical wire value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Review => "review",
            Self::AutoEdit => "auto_edit",
            Self::Unrestricted => "unrestricted",
        }
    }
}

fn deserialize_non_null_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)?
        .map(Some)
        .ok_or_else(|| serde::de::Error::custom("explicit null is not allowed; omit the field"))
}

/// A bounded reference to a server-configured agent profile or skill.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct ApplicationConfiguredRef(
    #[schemars(
        length(min = 1, max = 64),
        regex(pattern = r"^(?!.*\.\.)[A-Za-z0-9][A-Za-z0-9._-]*$")
    )]
    String,
);

impl ApplicationConfiguredRef {
    /// Validate and construct a configured reference.
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        validate_application_configured_ref(&value)?;
        Ok(Self(value))
    }

    /// Return the canonical wire value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ApplicationConfiguredRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Validate the exact configured-reference grammar shared by all consumers.
pub fn validate_application_configured_ref(value: &str) -> Result<(), String> {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_APPLICATION_CHAT_CONTROL_REF_BYTES {
        return Err(format!(
            "configured reference must contain 1..={MAX_APPLICATION_CHAT_CONTROL_REF_BYTES} bytes"
        ));
    }
    if !bytes[0].is_ascii_alphanumeric()
        || !bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        || value.contains("..")
    {
        return Err("configured reference must start with an ASCII letter or digit, contain only ASCII letters, digits, '.', '_', or '-', and must not contain '..'".to_string());
    }
    Ok(())
}

/// Closed application controls envelope accepted by `animus chat send`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApplicationChatControls {
    /// Exact controls schema discriminator.
    pub schema: ApplicationChatControlsSchema,
    /// Request kernel-mediated approvals when authorized by the profile.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_non_null_option"
    )]
    #[schemars(with = "bool")]
    pub approvals: Option<bool>,
    /// Requested reasoning effort.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_non_null_option"
    )]
    #[schemars(with = "ApplicationReasoningEffort")]
    pub reasoning_effort: Option<ApplicationReasoningEffort>,
    /// Requested provider-neutral permission intent.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_non_null_option"
    )]
    #[schemars(with = "ApplicationPermissionIntent")]
    pub permission_intent: Option<ApplicationPermissionIntent>,
    /// Assertion of the conversation's canonical configured profile.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_non_null_option"
    )]
    #[schemars(with = "ApplicationConfiguredRef")]
    pub profile_ref: Option<ApplicationConfiguredRef>,
    /// Configured skill selected from the profile's authorized options.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_non_null_option"
    )]
    #[schemars(with = "ApplicationConfiguredRef")]
    pub skill_ref: Option<ApplicationConfiguredRef>,
}

/// Allowed application control values projected by an authenticated portal.
///
/// This is a transport shape only. The portal decides which values appear.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AllowedApplicationChatControls {
    /// Exact controls schema discriminator.
    pub schema: ApplicationChatControlsSchema,
    /// Allowed approval selections.
    #[serde(default)]
    pub approvals: Vec<bool>,
    /// Allowed reasoning effort selections.
    #[serde(default)]
    pub reasoning_effort: Vec<ApplicationReasoningEffort>,
    /// Allowed permission intent selections.
    #[serde(default)]
    pub permission_intent: Vec<ApplicationPermissionIntent>,
    /// Allowed canonical profile assertion.
    #[serde(default)]
    pub profile_ref: Vec<ApplicationConfiguredRef>,
    /// Allowed configured skill references.
    #[serde(default)]
    pub skill_ref: Vec<ApplicationConfiguredRef>,
}

/// A bounded, non-empty receipt identifier with no ASCII control characters.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct ApplicationProtocolString(
    #[schemars(
        length(min = 1, max = 512),
        regex(pattern = r"^[^\u0000-\u001F\u007F]+$")
    )]
    String,
);

impl ApplicationProtocolString {
    /// Validate and construct a protocol string.
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_APPLICATION_PROTOCOL_STRING_BYTES
            || value.chars().any(|character| character.is_ascii_control())
        {
            return Err(format!(
                "application protocol string must contain 1..={MAX_APPLICATION_PROTOCOL_STRING_BYTES} UTF-8 bytes and no ASCII controls"
            ));
        }
        Ok(Self(value))
    }

    /// Return the canonical wire value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ApplicationProtocolString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Safe bounded diagnostic for a confirmed assistant failure receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct ApplicationChatErrorMessage(#[schemars(length(min = 1, max = 1024))] String);

impl ApplicationChatErrorMessage {
    /// Validate and construct a safe error message.
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        if value.is_empty() || value.len() > MAX_APPLICATION_CHAT_ERROR_BYTES {
            return Err(format!(
                "application chat error must contain 1..={MAX_APPLICATION_CHAT_ERROR_BYTES} UTF-8 bytes"
            ));
        }
        Ok(Self(value))
    }

    /// Return the canonical wire value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ApplicationChatErrorMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Terminal status carried by an application chat receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationChatTurnStatus {
    /// Assistant message persisted successfully.
    Completed,
    /// User message persisted but the provider failed.
    AssistantFailed,
    /// User message persisted but the provider was interrupted.
    AssistantInterrupted,
}

/// A non-negative chat sequence exactly representable by JavaScript clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct ApplicationChatSequence(#[schemars(range(max = 9_007_199_254_740_991_u64))] u64);

impl ApplicationChatSequence {
    /// Validate and construct a cross-language-safe sequence.
    pub fn new(value: u64) -> Result<Self, String> {
        if value > MAX_APPLICATION_CHAT_SEQUENCE {
            return Err(format!(
                "application chat sequence must be <= {MAX_APPLICATION_CHAT_SEQUENCE}"
            ));
        }
        Ok(Self(value))
    }

    /// Return the canonical numeric value.
    pub fn get(self) -> u64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for ApplicationChatSequence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::new(u64::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Exact durable JSONL receipt frames used by application chat callers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApplicationChatReceiptFrame {
    /// Canonical user row was durably accepted.
    UserMessageAccepted {
        /// Must be `user_accepted`.
        status: UserAcceptedStatus,
        /// Conversation identity.
        conversation_id: ApplicationProtocolString,
        /// Canonical user-message sequence.
        seq: ApplicationChatSequence,
        /// Canonical user-message identity.
        message_id: ApplicationProtocolString,
        /// Durable operation identity.
        operation_id: ApplicationProtocolString,
    },
    /// Assistant row was durably completed.
    TurnCompleted {
        /// Must be `completed`.
        status: CompletedStatus,
        /// Conversation identity.
        conversation_id: ApplicationProtocolString,
        /// Canonical assistant-message sequence.
        seq: ApplicationChatSequence,
        /// Canonical assistant-message identity.
        message_id: ApplicationProtocolString,
        /// Accepted user-message sequence.
        user_seq: ApplicationChatSequence,
        /// Accepted user-message identity.
        user_message_id: ApplicationProtocolString,
        /// Durable operation identity.
        operation_id: ApplicationProtocolString,
        /// Optional provider continuity pointer.
        session_id: Option<ApplicationProtocolString>,
    },
    /// User row is durable but assistant execution did not complete.
    TurnFailed {
        /// Confirmed partial-success status.
        status: ApplicationChatFailureStatus,
        /// Conversation identity.
        conversation_id: ApplicationProtocolString,
        /// Accepted user-message sequence.
        user_seq: ApplicationChatSequence,
        /// Accepted user-message identity.
        user_message_id: ApplicationProtocolString,
        /// Durable operation identity.
        operation_id: ApplicationProtocolString,
        /// Stable, non-sensitive failure code.
        error_code: ApplicationProtocolString,
        /// Safe bounded failure description.
        error_message: ApplicationChatErrorMessage,
    },
}

/// Literal accepted-user status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum UserAcceptedStatus {
    /// `user_accepted`.
    #[serde(rename = "user_accepted")]
    UserAccepted,
}

/// Literal completed-turn status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum CompletedStatus {
    /// `completed`.
    #[serde(rename = "completed")]
    Completed,
}

/// Confirmed partial-success terminal status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationChatFailureStatus {
    /// Provider or assistant execution failed.
    AssistantFailed,
    /// Provider or assistant execution was interrupted.
    AssistantInterrupted,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn controls_are_closed_and_refs_are_exact() {
        let controls: ApplicationChatControls = serde_json::from_value(json!({
            "schema": APPLICATION_CHAT_CONTROLS_SCHEMA,
            "approvals": true,
            "reasoning_effort": "high",
            "permission_intent": "review",
            "profile_ref": "research.agent-1",
            "skill_ref": "code_review"
        }))
        .unwrap();
        assert_eq!(controls.profile_ref.unwrap().as_str(), "research.agent-1");

        for rejected in [
            json!({"schema": APPLICATION_CHAT_CONTROLS_SCHEMA, "argv": ["--danger"]}),
            json!({"schema": "animus.chat.application_controls.v2"}),
            json!({"schema": APPLICATION_CHAT_CONTROLS_SCHEMA, "profile_ref": "../secret"}),
            json!({"schema": APPLICATION_CHAT_CONTROLS_SCHEMA, "approvals": null}),
        ] {
            assert!(serde_json::from_value::<ApplicationChatControls>(rejected).is_err());
        }
    }

    #[test]
    fn receipt_frames_are_closed_and_utf8_byte_bounded() {
        let accepted = json!({
            "type": "user_message_accepted",
            "status": "user_accepted",
            "conversation_id": "chat-1",
            "seq": 6,
            "message_id": "message-user",
            "operation_id": "operation-1"
        });
        assert!(serde_json::from_value::<ApplicationChatReceiptFrame>(accepted.clone()).is_ok());
        let mut extra = accepted;
        extra["agent_id"] = json!("caller-controlled");
        assert!(serde_json::from_value::<ApplicationChatReceiptFrame>(extra).is_err());
        assert!(ApplicationProtocolString::new("é".repeat(256)).is_ok());
        assert!(ApplicationProtocolString::new(format!("{}x", "é".repeat(256))).is_err());
        assert!(ApplicationChatErrorMessage::new("é".repeat(512)).is_ok());
        assert!(ApplicationChatErrorMessage::new(format!("{}x", "é".repeat(512))).is_err());
        assert!(ApplicationChatSequence::new(MAX_APPLICATION_CHAT_SEQUENCE).is_ok());
        assert!(ApplicationChatSequence::new(MAX_APPLICATION_CHAT_SEQUENCE + 1).is_err());
        assert!(
            serde_json::from_value::<ApplicationChatReceiptFrame>(json!({
                "type": "user_message_accepted",
                "status": "user_accepted",
                "conversation_id": "chat-1",
                "seq": MAX_APPLICATION_CHAT_SEQUENCE + 1,
                "message_id": "message-user",
                "operation_id": "operation-1"
            }))
            .is_err()
        );
    }
}

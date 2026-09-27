use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const PLUGIN_KIND_CHANNEL_BACKEND: &str = "channel_backend";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ChannelVersion {
    #[serde(rename = "animus.channel.v1")]
    V1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChannelScope {
    pub tenant_id: String,
    pub connection_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChannelMessage {
    pub schema: ChannelVersion,
    pub scope: ChannelScope,
    pub message_id: String,
    pub peer_id: String,
    pub occurred_at: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChannelReceiveResult {
    pub schema: ChannelVersion,
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChannelSendParams {
    pub schema: ChannelVersion,
    pub scope: ChannelScope,
    pub delivery_id: String,
    pub peer_id: String,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ChannelDeliveryState {
    Queued,
    Sending,
    Accepted,
    Delivered,
    Read,
    Failed,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChannelStatusParams {
    pub schema: ChannelVersion,
    pub scope: ChannelScope,
    pub delivery_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChannelDelivery {
    pub schema: ChannelVersion,
    pub scope: ChannelScope,
    pub delivery_id: String,
    pub state: ChannelDeliveryState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChannelSchema {
    pub schema: ChannelVersion,
    pub providers: Vec<String>,
    pub supports_text: bool,
    pub supports_media: bool,
    pub supports_templates: bool,
    pub supports_delivery_status: bool,
    pub supports_durable_receive: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn receive_rejects_untrusted_control_fields_and_versions() {
        let original = json!({"schema":"animus.channel.v1","scope":{"tenant_id":"a","connection_id":"b"},"message_id":"m","peer_id":"p","occurred_at":"2026-09-27T00:00:00Z","text":"hi"});
        assert!(serde_json::from_value::<ChannelMessage>(original.clone()).is_ok());
        for key in ["actor", "agent", "tools", "conversation_id"] {
            let mut altered = original.clone();
            altered[key] = json!("admin");
            assert!(serde_json::from_value::<ChannelMessage>(altered).is_err());
        }
        let mut altered = original;
        altered["schema"] = json!("animus.channel.v2");
        assert!(serde_json::from_value::<ChannelMessage>(altered).is_err());
    }

    #[test]
    fn ambiguous_delivery_is_distinct_from_failure_and_acceptance() {
        assert_eq!(
            serde_json::to_value(ChannelDeliveryState::Unknown).unwrap(),
            "unknown"
        );
        assert_ne!(ChannelDeliveryState::Unknown, ChannelDeliveryState::Failed);
        assert_ne!(
            ChannelDeliveryState::Accepted,
            ChannelDeliveryState::Delivered
        );
    }
}

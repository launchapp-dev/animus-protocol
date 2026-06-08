//! Protocol types for Animus Chat: the conversation message schema and the
//! `chat_provider` plugin-kind streaming RPC.
//!
//! This is the foundation crate of the v0.6 Animus Chat wave (see
//! `docs/architecture/animus-chat.md` in the `animus-cli` repo). Everything
//! else in the wave — the `subject_kind=conversation` backend, the daemon chat
//! loop, the `chat_provider` reference plugins, the TUI client — depends on the
//! wire shapes defined here.
//!
//! # Design
//!
//! Animus owns conversation state; `chat_provider` plugins are stateless
//! translators. The internal message schema is modeled on **Anthropic content
//! blocks** ([`ContentBlock`]) because they are the lossless superset: OpenAI's
//! `{role, content, tool_calls}` shape is projectable *from* content blocks but
//! not the reverse (it splits tool calls into a sibling field and tool results
//! into separate `role: tool` messages, losing ordering for interleaved
//! text+tool turns). Extended-thinking [`ContentBlock::Thinking`] blocks must
//! round-trip verbatim across turns, which only a block-structured schema
//! preserves.
//!
//! # Streaming
//!
//! [`METHOD_CHAT_STREAM`] reuses the **existing plugin-host server-streaming
//! contract** (`animus_plugin_protocol::HostCapabilities::streaming`): one
//! request id fans out many notification frames carrying that id in `params`.
//! This is the same mechanism provider plugins already use for
//! `agent/run` → `agent/output`. The [`ChatStreamEvent`] variants mirror the
//! verified Anthropic streaming event sequence
//! (`message_start` → `content_block_start` → `content_block_delta` …
//! → `content_block_stop` → `message_delta` → `message_stop`). Providers
//! translate their native stream **into** this normalized envelope; the core
//! never sees a vendor format.
//!
//! # Persistence
//!
//! Conversations are a subject kind ([`SUBJECT_KIND_CONVERSATION`]). The
//! backend stores a [`ConversationMeta`] `meta.json` plus an append-only
//! `messages.jsonl` of [`ChatMessage`] objects, mirroring how `runs/` and
//! `artifacts/` already persist under scoped state.

#![warn(missing_docs)]

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// `PluginKind` wire value for chat providers. A `chat_provider` plugin speaks
/// streaming chat against an upstream API (Anthropic Messages, OpenAI
/// Responses, local Ollama) — distinct from the CLI-wrapping `provider` role.
///
/// Re-exported from `animus-plugin-protocol`, the single source of truth for
/// plugin-kind wire strings (it also registers the matching
/// `PluginKind::ChatProvider` enum arm for typed discovery).
pub use animus_plugin_protocol::PLUGIN_KIND_CHAT_PROVIDER;

/// Subject-kind wire string for conversations. Routes through the existing
/// `SubjectRouter`, so `animus subject list --kind conversation` and the
/// `animus.subject.*` MCP tools work on conversations.
pub const SUBJECT_KIND_CONVERSATION: &str = "conversation";

/// Per-crate semver protocol version.
pub const PROTOCOL_VERSION: &str = "0.1.0";

/// Schema discriminator value carried by every [`ChatMessage`].
pub const MESSAGE_SCHEMA: &str = "animus.chat.message.v1";

/// Schema discriminator value carried by every [`ConversationMeta`].
pub const CONVERSATION_SCHEMA: &str = "animus.chat.conversation.v1";

// =====================================================================
// RPC method names
// =====================================================================

/// Stream a chat turn. Request carries a [`ChatStreamRequest`]; the result is
/// delivered as a sequence of [`ChatStreamEvent`] notification frames over the
/// host's server-streaming channel, terminated by
/// [`ChatStreamEvent::MessageStop`].
pub const METHOD_CHAT_STREAM: &str = "chat/stream";

/// List the models this provider exposes. Request is empty; the response is a
/// `Vec<`[`ChatModelInfo`]`>`.
pub const METHOD_CHAT_MODELS: &str = "chat/models";

/// Count the input tokens a request would consume without running it. Optional
/// — providers MAY return an error if unsupported. Request carries a
/// [`ChatStreamRequest`] (or its `messages`/`system`/`tools`); the response is
/// a [`CountTokensResponse`].
pub const METHOD_CHAT_COUNT_TOKENS: &str = "chat/count_tokens";

/// JSON-RPC method name for the streaming notifications a [`METHOD_CHAT_STREAM`]
/// request fans out. Each frame echoes the originating request id in
/// `params.id` (the server-streaming contract — see [`ChatStreamNotification`]
/// and `animus_plugin_protocol::RpcNotification`) and carries one
/// [`ChatStreamEvent`] in `params.event`.
pub const NOTIFICATION_CHAT_DELTA: &str = "chat/delta";

// =====================================================================
// Message schema (animus.chat.message.v1)
// =====================================================================

/// The role of a [`ChatMessage`]'s author.
///
/// Serializes lowercase: `"user"`, `"assistant"`, `"system"`, `"tool"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ChatRole {
    /// A human (or external client) turn.
    User,
    /// A model-authored turn.
    Assistant,
    /// A system prompt / instruction turn.
    System,
    /// A tool-result-bearing turn.
    Tool,
}

/// Why the model stopped generating.
///
/// Serializes as a snake_case string matching Anthropic's `stop_reason` values:
/// `"end_turn"`, `"tool_use"`, `"max_tokens"`, `"stop_sequence"`, `"refusal"`,
/// `"pause_turn"`. Any value this crate version does not recognize round-trips
/// byte-for-byte through [`StopReason::Other`] so new upstream stop reasons
/// never get dropped before downstream SDKs upgrade.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(from = "String", into = "String")]
#[schemars(
    with = "String",
    description = "Stop reason. snake_case string; unknown values round-trip via Other."
)]
#[non_exhaustive]
pub enum StopReason {
    /// The model reached a natural end of turn.
    EndTurn,
    /// The model emitted a tool-use block and is waiting for a result.
    ToolUse,
    /// The model hit the `max_tokens` ceiling.
    MaxTokens,
    /// The model emitted a configured stop sequence.
    StopSequence,
    /// The model declined to respond (Anthropic `refusal`).
    Refusal,
    /// The model paused a long-running turn and expects to be re-invoked with
    /// the same history to continue (Anthropic `pause_turn`).
    PauseTurn,
    /// Any stop reason not understood by this crate version. Preserves the wire
    /// string so unknown reasons round-trip losslessly.
    Other(String),
}

impl StopReason {
    /// Return the canonical wire-string form of this stop reason.
    pub fn as_str(&self) -> &str {
        match self {
            StopReason::EndTurn => "end_turn",
            StopReason::ToolUse => "tool_use",
            StopReason::MaxTokens => "max_tokens",
            StopReason::StopSequence => "stop_sequence",
            StopReason::Refusal => "refusal",
            StopReason::PauseTurn => "pause_turn",
            StopReason::Other(value) => value.as_str(),
        }
    }

    /// `true` for variants this crate version recognizes natively.
    pub fn is_known(&self) -> bool {
        !matches!(self, StopReason::Other(_))
    }
}

impl std::fmt::Display for StopReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<String> for StopReason {
    fn from(value: String) -> Self {
        match value.as_str() {
            "end_turn" => StopReason::EndTurn,
            "tool_use" => StopReason::ToolUse,
            "max_tokens" => StopReason::MaxTokens,
            "stop_sequence" => StopReason::StopSequence,
            "refusal" => StopReason::Refusal,
            "pause_turn" => StopReason::PauseTurn,
            _ => StopReason::Other(value),
        }
    }
}

impl From<StopReason> for String {
    fn from(value: StopReason) -> Self {
        value.as_str().to_string()
    }
}

/// Token accounting for a model turn.
///
/// Every counter defaults to zero so a conversation's cost is a simple sum over
/// its assistant messages — no separate ledger. Each field is `#[serde(default)]`
/// so providers and non-Rust clients may omit zero-valued counters (Anthropic's
/// `message_start` usage, for instance, often omits the cache fields).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
pub struct Usage {
    /// Input (prompt) tokens billed for this turn.
    #[serde(default)]
    pub input_tokens: u64,
    /// Output (completion) tokens generated this turn.
    #[serde(default)]
    pub output_tokens: u64,
    /// Tokens served from the prompt cache (billed at the cache-read rate).
    #[serde(default)]
    pub cache_read_input_tokens: u64,
    /// Tokens written into the prompt cache (billed at the cache-write rate).
    #[serde(default)]
    pub cache_creation_input_tokens: u64,
}

/// Source of an [`ContentBlock::Image`] block.
///
/// Serializes with a `type` discriminator: `{"type":"base64",...}` or
/// `{"type":"url",...}`. Vision is deferred in v0.6, but the schema carries
/// images so it is complete and downstream impls agree on the shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ImageSource {
    /// Inline base64-encoded image bytes.
    Base64 {
        /// MIME media type, e.g. `"image/png"`.
        media_type: String,
        /// Base64-encoded image data (no data-URL prefix).
        data: String,
    },
    /// A fetchable image URL.
    Url {
        /// Absolute URL of the image.
        url: String,
    },
}

/// A single content block within a [`ChatMessage`].
///
/// Serializes with a `type` discriminator matching Anthropic content blocks
/// exactly: `{"type":"text","text":"..."}`,
/// `{"type":"tool_use","id":"...","name":"...","input":{...}}`,
/// `{"type":"tool_result","tool_use_id":"...","is_error":false,"content":[...]}`,
/// `{"type":"thinking","thinking":"...","signature":"..."}`,
/// `{"type":"image","source":{...}}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    /// Plain text.
    Text {
        /// The text content.
        text: String,
    },
    /// A model request to invoke a tool. The `input` is the tool's arguments
    /// object (accumulated from `input_json_delta` fragments on the wire and
    /// parsed once on block stop).
    ToolUse {
        /// Provider-issued id correlating this call to its result (e.g.
        /// `"toolu_01A..."`).
        id: String,
        /// Tool name (e.g. `"animus.subject.list"`).
        name: String,
        /// Tool arguments as a JSON object.
        input: Value,
    },
    /// The result of executing a tool, fed back to the model. References the
    /// originating [`ContentBlock::ToolUse`] by id. The result body is itself a
    /// list of blocks (usually a single text block).
    ToolResult {
        /// The `id` of the [`ContentBlock::ToolUse`] this answers.
        tool_use_id: String,
        /// `true` if the tool errored (including RBAC denials) — the model sees
        /// the error and can recover; it does not crash the conversation.
        is_error: bool,
        /// The tool's output as content blocks.
        content: Vec<ContentBlock>,
    },
    /// Extended-thinking reasoning. The `signature` MUST round-trip verbatim
    /// across turns for multi-turn integrity.
    Thinking {
        /// The model's reasoning text.
        thinking: String,
        /// Opaque provider signature authenticating the thinking block.
        signature: String,
    },
    /// An image (vision deferred in v0.6; present for schema completeness).
    Image {
        /// Where the image bytes come from.
        source: ImageSource,
    },
}

/// A single conversation message — one line of `messages.jsonl`.
///
/// Schema id: [`MESSAGE_SCHEMA`] (`"animus.chat.message.v1"`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ChatMessage {
    /// Schema discriminator. Always [`MESSAGE_SCHEMA`].
    pub schema: String,
    /// Message id (ULID, monotonic within a conversation).
    pub id: String,
    /// Owning conversation id.
    pub conversation_id: String,
    /// Author role.
    pub role: ChatRole,
    /// Ordered content blocks.
    pub content: Vec<ContentBlock>,
    /// Model id that produced an assistant turn; `None` for user/system/tool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Why generation stopped; `None` for non-assistant turns or in-progress.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<StopReason>,
    /// Token accounting; `None` for non-assistant turns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    /// RFC3339 creation timestamp.
    pub created_at: String,
    /// RBAC principal that authored this turn; `None` when unattributed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_id: Option<String>,
}

impl ChatMessage {
    /// Construct a message with [`MESSAGE_SCHEMA`] pre-filled and the optional
    /// fields cleared. Set `model` / `stop_reason` / `usage` / `principal_id`
    /// afterwards as needed.
    pub fn new(
        id: impl Into<String>,
        conversation_id: impl Into<String>,
        role: ChatRole,
        content: Vec<ContentBlock>,
        created_at: impl Into<String>,
    ) -> Self {
        Self {
            schema: MESSAGE_SCHEMA.to_string(),
            id: id.into(),
            conversation_id: conversation_id.into(),
            role,
            content,
            model: None,
            stop_reason: None,
            usage: None,
            created_at: created_at.into(),
            principal_id: None,
        }
    }
}

// =====================================================================
// Streaming delta types
// =====================================================================

/// The shell of a content block as it opens in a stream, before any deltas.
///
/// Mirrors Anthropic `content_block_start.content_block`. Serializes with a
/// `type` discriminator: `text` / `tool_use` / `thinking`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlockStart {
    /// A text block opens (text accumulates via [`BlockDelta::TextDelta`]).
    Text {
        /// Initial text (usually empty).
        #[serde(default)]
        text: String,
    },
    /// A tool-use block opens. `input` arrives later as
    /// [`BlockDelta::InputJsonDelta`] fragments.
    ToolUse {
        /// Provider-issued tool-call id.
        id: String,
        /// Tool name.
        name: String,
    },
    /// A thinking block opens (accumulates via [`BlockDelta::ThinkingDelta`]
    /// and [`BlockDelta::SignatureDelta`]).
    Thinking {
        /// Initial thinking text (usually empty).
        #[serde(default)]
        thinking: String,
    },
}

/// An incremental update to an open content block.
///
/// Serializes with a `type` discriminator matching Anthropic deltas:
/// `text_delta` / `input_json_delta` / `thinking_delta` / `signature_delta`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BlockDelta {
    /// Append text to a text block.
    TextDelta {
        /// Text fragment.
        text: String,
    },
    /// Append a fragment of the tool-use `input` JSON. Fragments are
    /// concatenated and parsed once when the block stops — an individual
    /// fragment is **not** valid JSON on its own.
    InputJsonDelta {
        /// Partial JSON string fragment.
        partial_json: String,
    },
    /// Append text to a thinking block.
    ThinkingDelta {
        /// Thinking fragment.
        thinking: String,
    },
    /// Append to a thinking block's signature.
    SignatureDelta {
        /// Signature fragment.
        signature: String,
    },
}

/// One frame of the [`METHOD_CHAT_STREAM`] notification stream.
///
/// The sequence mirrors the verified Anthropic event order:
/// [`Self::MessageStart`] → (`[`Self::ContentBlockStart`] →
/// `[`Self::ContentBlockDelta`]`* → `[`Self::ContentBlockStop`]`)* →
/// [`Self::MessageDelta`] → [`Self::MessageStop`].
///
/// Serializes with a `type` discriminator: `message_start`,
/// `content_block_start`, `content_block_delta`, `content_block_stop`,
/// `message_delta`, `message_stop`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChatStreamEvent {
    /// The assistant message opens. Carries the new message id, its role, and
    /// the initial (input-side) usage.
    MessageStart {
        /// New assistant message id.
        message_id: String,
        /// Author role (always [`ChatRole::Assistant`] in practice).
        role: ChatRole,
        /// Usage known at message start (input/cache tokens).
        usage: Usage,
    },
    /// A content block opens at `index`.
    ContentBlockStart {
        /// Block position within the message.
        index: u32,
        /// The opening block shell.
        block: ContentBlockStart,
    },
    /// An incremental update to the block at `index`.
    ContentBlockDelta {
        /// Block position within the message.
        index: u32,
        /// The delta to apply.
        delta: BlockDelta,
    },
    /// The block at `index` is complete (accumulated `input_json` should be
    /// parsed now).
    ContentBlockStop {
        /// Block position within the message.
        index: u32,
    },
    /// Top-level message metadata update — the final stop reason and the
    /// cumulative output usage.
    MessageDelta {
        /// Why the message stopped; `None` until known.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stop_reason: Option<StopReason>,
        /// Cumulative usage (notably `output_tokens`).
        usage: Usage,
    },
    /// The stream is finished.
    MessageStop,
}

/// The `params` of a [`NOTIFICATION_CHAT_DELTA`] frame.
///
/// The plugin-host server-streaming contract requires every notification fanned
/// out from one request to echo that request's id (see
/// `animus_plugin_protocol::RpcNotification`). Because several `chat/stream`
/// requests may be in flight on a single plugin connection at once, the host
/// demultiplexes deltas and matches [`ChatStreamEvent::MessageStop`] to its
/// request by this [`id`](Self::id). Providers that build notification frames
/// directly should serialize this type as the notification `params` rather than
/// a bare [`ChatStreamEvent`], so the correlation id is never dropped.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ChatStreamNotification {
    /// The originating [`METHOD_CHAT_STREAM`] request id this frame belongs to.
    pub id: Value,
    /// The streamed event.
    pub event: ChatStreamEvent,
}

impl ChatStreamNotification {
    /// Wrap a [`ChatStreamEvent`] with the originating request id.
    pub fn new(id: Value, event: ChatStreamEvent) -> Self {
        Self { id, event }
    }
}

// =====================================================================
// RPC request / response param types
// =====================================================================

/// A tool the model may call, advertised on a [`ChatStreamRequest`].
///
/// Mirrors the Anthropic tool shape: name, description, and a JSON Schema for
/// the input object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ToolSchema {
    /// Tool name (e.g. `"animus.subject.list"`).
    pub name: String,
    /// Human-readable description shown to the model.
    pub description: String,
    /// JSON Schema for the tool's input object.
    pub input_schema: Value,
}

/// Request params for [`METHOD_CHAT_STREAM`].
///
/// Carries the full message history (providers are stateless — the daemon
/// resends history each turn), the system prompt, the available tools, and the
/// sampling parameters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ChatStreamRequest {
    /// Full conversation history to send to the model.
    pub messages: Vec<ChatMessage>,
    /// System prompt; `None` when no system turn applies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,
    /// Tools the model may call this turn.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolSchema>,
    /// Model id to run against.
    pub model: String,
    /// Maximum output tokens to generate.
    pub max_tokens: u32,
    /// Sampling temperature; `None` lets the provider use its default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
}

/// One entry of the [`METHOD_CHAT_MODELS`] response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ChatModelInfo {
    /// Model id (e.g. `"claude-opus-4-8"`).
    pub id: String,
    /// Maximum context window in tokens.
    pub context_window: u32,
    /// Whether the model supports tool/function calling.
    pub supports_tools: bool,
    /// Whether the model supports image input.
    pub supports_vision: bool,
}

/// Response for [`METHOD_CHAT_COUNT_TOKENS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CountTokensResponse {
    /// Number of input tokens the request would consume.
    pub input_tokens: u64,
}

// =====================================================================
// Conversation subject schema
// =====================================================================

/// Conversation metadata — the `meta.json` of a conversation subject.
///
/// Schema id: [`CONVERSATION_SCHEMA`] (`"animus.chat.conversation.v1"`). The
/// message log lives alongside in `messages.jsonl` as [`ChatMessage`] lines.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ConversationMeta {
    /// Schema discriminator. Always [`CONVERSATION_SCHEMA`].
    pub schema: String,
    /// Conversation id (ULID, e.g. `"conv_01H..."`).
    pub id: String,
    /// Optional human-readable title.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// The agent profile (persona) bound to this conversation; `None` for ad-hoc.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_profile: Option<String>,
    /// Default model id for this conversation.
    pub model: String,
    /// RFC3339 creation timestamp.
    pub created_at: String,
    /// RFC3339 timestamp of the most recent append.
    pub updated_at: String,
    /// RBAC principal that owns the conversation; `None` when unattributed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_id: Option<String>,
    /// Number of messages persisted in `messages.jsonl`.
    pub message_count: u64,
}

impl ConversationMeta {
    /// Construct conversation metadata with [`CONVERSATION_SCHEMA`] pre-filled
    /// and the optional fields cleared.
    pub fn new(
        id: impl Into<String>,
        model: impl Into<String>,
        created_at: impl Into<String>,
        updated_at: impl Into<String>,
    ) -> Self {
        Self {
            schema: CONVERSATION_SCHEMA.to_string(),
            id: id.into(),
            title: None,
            agent_profile: None,
            model: model.into(),
            created_at: created_at.into(),
            updated_at: updated_at.into(),
            principal_id: None,
            message_count: 0,
        }
    }
}

// =====================================================================
// Capabilities
// =====================================================================

/// Capability flags a `chat_provider` plugin advertises (mirrors the
/// `[capabilities]` block in its `plugin.toml`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
pub struct ChatProviderCapabilities {
    /// Streams responses token-by-token (always `true` for chat providers).
    #[serde(default)]
    pub streaming: bool,
    /// Supports tool/function calling.
    #[serde(default)]
    pub tool_use: bool,
    /// Supports image input.
    #[serde(default)]
    pub vision: bool,
    /// Supports prompt caching.
    #[serde(default)]
    pub prompt_caching: bool,
}

// =====================================================================
// Error codes
// =====================================================================

/// JSON-RPC error codes for the chat_provider protocol. The `-32800..-32899`
/// range is reserved for this kind.
///
/// The earlier per-kind sub-blocks inside the JSON-RPC reserved band
/// (`-32100..-32599`) are already taken by sibling protocols
/// (`-32200..-32299`, for instance, belongs to `animus-queue-protocol`), and
/// `-32600..-32700` are the JSON-RPC 2.0 standard errors. This range sits just
/// below the reserved band (`-32768..-32000`) in the application-defined space,
/// so a chat domain failure is never confused with another plugin kind's error
/// or with a protocol-level one.
pub mod error_codes {
    /// The requested model is unknown to this provider.
    pub const MODEL_NOT_FOUND: i32 = -32801;
    /// The upstream API key is missing or invalid.
    pub const AUTH_FAILED: i32 = -32802;
    /// The upstream API rate-limited the request. Caller should back off.
    pub const RATE_LIMITED: i32 = -32803;
    /// The request exceeded the model's context window.
    pub const CONTEXT_LENGTH_EXCEEDED: i32 = -32804;
    /// The upstream provider API is unavailable.
    pub const UPSTREAM_UNAVAILABLE: i32 = -32805;
    /// `chat/count_tokens` is not supported by this provider.
    pub const COUNT_TOKENS_UNSUPPORTED: i32 = -32806;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn round_trip<T>(value: &T) -> T
    where
        T: Serialize + for<'de> Deserialize<'de> + PartialEq + std::fmt::Debug,
    {
        let serialized = serde_json::to_value(value).expect("serialize");
        let back: T = serde_json::from_value(serialized).expect("deserialize");
        assert_eq!(&back, value);
        back
    }

    #[test]
    fn text_block_tag_shape_is_exact() {
        let block = ContentBlock::Text { text: "hi".into() };
        let s = serde_json::to_string(&block).unwrap();
        assert_eq!(s, r#"{"type":"text","text":"hi"}"#);
    }

    #[test]
    fn text_block_matches_design_doc_json() {
        let v = json!({ "type": "text", "text": "What's the weather in SF?" });
        let block: ContentBlock = serde_json::from_value(v.clone()).unwrap();
        assert_eq!(
            block,
            ContentBlock::Text {
                text: "What's the weather in SF?".into()
            }
        );
        assert_eq!(serde_json::to_value(&block).unwrap(), v);
    }

    #[test]
    fn tool_use_block_matches_design_doc_json() {
        let v = json!({
            "type": "tool_use",
            "id": "toolu_01A...",
            "name": "animus.subject.list",
            "input": { "kind": "task" }
        });
        let block: ContentBlock = serde_json::from_value(v.clone()).unwrap();
        assert_eq!(
            block,
            ContentBlock::ToolUse {
                id: "toolu_01A...".into(),
                name: "animus.subject.list".into(),
                input: json!({ "kind": "task" }),
            }
        );
        assert_eq!(serde_json::to_value(&block).unwrap(), v);
    }

    #[test]
    fn tool_result_block_matches_design_doc_json() {
        let v = json!({
            "type": "tool_result",
            "tool_use_id": "toolu_01A...",
            "is_error": false,
            "content": [ { "type": "text", "text": "[{...}]" } ]
        });
        let block: ContentBlock = serde_json::from_value(v.clone()).unwrap();
        assert_eq!(
            block,
            ContentBlock::ToolResult {
                tool_use_id: "toolu_01A...".into(),
                is_error: false,
                content: vec![ContentBlock::Text {
                    text: "[{...}]".into()
                }],
            }
        );
        assert_eq!(serde_json::to_value(&block).unwrap(), v);
    }

    #[test]
    fn thinking_block_matches_design_doc_json() {
        let v = json!({
            "type": "thinking",
            "thinking": "The user wants...",
            "signature": "EqQBCgIYAh..."
        });
        let block: ContentBlock = serde_json::from_value(v.clone()).unwrap();
        assert_eq!(
            block,
            ContentBlock::Thinking {
                thinking: "The user wants...".into(),
                signature: "EqQBCgIYAh...".into(),
            }
        );
        assert_eq!(serde_json::to_value(&block).unwrap(), v);
    }

    #[test]
    fn image_block_round_trips_both_sources() {
        let base64 = ContentBlock::Image {
            source: ImageSource::Base64 {
                media_type: "image/png".into(),
                data: "iVBORw0KG".into(),
            },
        };
        let v = serde_json::to_value(&base64).unwrap();
        assert_eq!(v["type"], "image");
        assert_eq!(v["source"]["type"], "base64");
        round_trip(&base64);

        let url = ContentBlock::Image {
            source: ImageSource::Url {
                url: "https://x/y.png".into(),
            },
        };
        let v = serde_json::to_value(&url).unwrap();
        assert_eq!(v["source"]["type"], "url");
        round_trip(&url);
    }

    #[test]
    fn chat_message_round_trips_with_all_fields() {
        let msg = ChatMessage {
            schema: MESSAGE_SCHEMA.into(),
            id: "msg_01H".into(),
            conversation_id: "conv_01H".into(),
            role: ChatRole::Assistant,
            content: vec![
                ContentBlock::Thinking {
                    thinking: "reasoning".into(),
                    signature: "sig".into(),
                },
                ContentBlock::Text {
                    text: "Hello".into(),
                },
                ContentBlock::ToolUse {
                    id: "toolu_1".into(),
                    name: "animus.subject.list".into(),
                    input: json!({ "kind": "task" }),
                },
            ],
            model: Some("claude-opus-4-8".into()),
            stop_reason: Some(StopReason::ToolUse),
            usage: Some(Usage {
                input_tokens: 25,
                output_tokens: 142,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
            }),
            created_at: "2026-06-08T15:00:00Z".into(),
            principal_id: Some("sami".into()),
        };
        round_trip(&msg);
    }

    #[test]
    fn chat_message_omits_none_optionals() {
        let msg = ChatMessage::new(
            "msg_1",
            "conv_1",
            ChatRole::User,
            vec![ContentBlock::Text { text: "hi".into() }],
            "2026-06-08T15:00:00Z",
        );
        let v = serde_json::to_value(&msg).unwrap();
        assert!(v.get("model").is_none());
        assert!(v.get("stop_reason").is_none());
        assert!(v.get("usage").is_none());
        assert!(v.get("principal_id").is_none());
        assert_eq!(v["schema"], MESSAGE_SCHEMA);
        assert_eq!(v["role"], "user");
    }

    #[test]
    fn message_with_tool_use_and_tool_result_serializes_as_documented() {
        // The shape downstream impls must agree on for a tool round-trip.
        let assistant = ChatMessage {
            schema: MESSAGE_SCHEMA.into(),
            id: "msg_a".into(),
            conversation_id: "conv_1".into(),
            role: ChatRole::Assistant,
            content: vec![
                ContentBlock::Text {
                    text: "Let me check.".into(),
                },
                ContentBlock::ToolUse {
                    id: "toolu_01A".into(),
                    name: "animus.subject.list".into(),
                    input: json!({ "kind": "task" }),
                },
            ],
            model: Some("claude-opus-4-8".into()),
            stop_reason: Some(StopReason::ToolUse),
            usage: Some(Usage::default()),
            created_at: "2026-06-08T15:00:00Z".into(),
            principal_id: Some("sami".into()),
        };
        let v = serde_json::to_value(&assistant).unwrap();
        assert_eq!(v["content"][1]["type"], "tool_use");
        assert_eq!(v["content"][1]["name"], "animus.subject.list");
        assert_eq!(v["stop_reason"], "tool_use");
        round_trip(&assistant);

        let tool = ChatMessage::new(
            "msg_t",
            "conv_1",
            ChatRole::Tool,
            vec![ContentBlock::ToolResult {
                tool_use_id: "toolu_01A".into(),
                is_error: false,
                content: vec![ContentBlock::Text { text: "[]".into() }],
            }],
            "2026-06-08T15:00:01Z",
        );
        let v = serde_json::to_value(&tool).unwrap();
        assert_eq!(v["content"][0]["type"], "tool_result");
        assert_eq!(v["content"][0]["tool_use_id"], "toolu_01A");
        round_trip(&tool);
    }

    #[test]
    fn stream_event_full_sequence_round_trips() {
        let sequence = vec![
            ChatStreamEvent::MessageStart {
                message_id: "msg_1".into(),
                role: ChatRole::Assistant,
                usage: Usage {
                    input_tokens: 25,
                    ..Default::default()
                },
            },
            ChatStreamEvent::ContentBlockStart {
                index: 0,
                block: ContentBlockStart::Text {
                    text: String::new(),
                },
            },
            ChatStreamEvent::ContentBlockDelta {
                index: 0,
                delta: BlockDelta::TextDelta { text: "Hel".into() },
            },
            ChatStreamEvent::ContentBlockDelta {
                index: 0,
                delta: BlockDelta::TextDelta { text: "lo".into() },
            },
            ChatStreamEvent::ContentBlockStop { index: 0 },
            ChatStreamEvent::MessageDelta {
                stop_reason: Some(StopReason::EndTurn),
                usage: Usage {
                    output_tokens: 2,
                    ..Default::default()
                },
            },
            ChatStreamEvent::MessageStop,
        ];
        for ev in &sequence {
            round_trip(ev);
        }
    }

    #[test]
    fn stream_notification_carries_request_id_for_demux() {
        let note = ChatStreamNotification::new(
            json!("req-7"),
            ChatStreamEvent::ContentBlockDelta {
                index: 0,
                delta: BlockDelta::TextDelta { text: "hi".into() },
            },
        );
        let v = serde_json::to_value(&note).unwrap();
        // The originating request id must be present so the host can match
        // deltas (and message_stop) to the right in-flight chat/stream request.
        assert_eq!(v["id"], "req-7");
        assert_eq!(v["event"]["type"], "content_block_delta");
        round_trip(&note);

        // Numeric ids round-trip too (JSON-RPC ids may be string or number).
        let numeric = ChatStreamNotification::new(json!(42), ChatStreamEvent::MessageStop);
        assert_eq!(serde_json::to_value(&numeric).unwrap()["id"], 42);
        round_trip(&numeric);
    }

    #[test]
    fn stream_event_tag_shapes_match_anthropic() {
        let start = ChatStreamEvent::MessageStart {
            message_id: "m".into(),
            role: ChatRole::Assistant,
            usage: Usage::default(),
        };
        assert_eq!(
            serde_json::to_value(&start).unwrap()["type"],
            "message_start"
        );

        let cbs = ChatStreamEvent::ContentBlockStart {
            index: 0,
            block: ContentBlockStart::ToolUse {
                id: "t".into(),
                name: "n".into(),
            },
        };
        let v = serde_json::to_value(&cbs).unwrap();
        assert_eq!(v["type"], "content_block_start");
        assert_eq!(v["block"]["type"], "tool_use");

        let stop = ChatStreamEvent::MessageStop;
        assert_eq!(
            serde_json::to_string(&stop).unwrap(),
            r#"{"type":"message_stop"}"#
        );
    }

    #[test]
    fn block_delta_tag_shapes_are_exact() {
        let td = BlockDelta::TextDelta { text: "x".into() };
        assert_eq!(
            serde_json::to_string(&td).unwrap(),
            r#"{"type":"text_delta","text":"x"}"#
        );

        let ijd = BlockDelta::InputJsonDelta {
            partial_json: "{\"a".into(),
        };
        let v = serde_json::to_value(&ijd).unwrap();
        assert_eq!(v["type"], "input_json_delta");
        assert_eq!(v["partial_json"], "{\"a");
    }

    #[test]
    fn input_json_delta_accumulation_parses() {
        let fragments = ["{\"loc", "ation\":\"SF\"}"];
        let mut acc = String::new();
        for f in fragments {
            let delta = BlockDelta::InputJsonDelta {
                partial_json: f.to_string(),
            };
            match round_trip(&delta) {
                BlockDelta::InputJsonDelta { partial_json } => acc.push_str(&partial_json),
                _ => unreachable!(),
            }
        }
        let parsed: Value = serde_json::from_str(&acc).expect("accumulated json parses");
        assert_eq!(parsed, json!({ "location": "SF" }));
    }

    #[test]
    fn chat_stream_request_round_trips_and_omits_empty() {
        let req = ChatStreamRequest {
            messages: vec![ChatMessage::new(
                "m",
                "c",
                ChatRole::User,
                vec![ContentBlock::Text { text: "hi".into() }],
                "2026-06-08T15:00:00Z",
            )],
            system: None,
            tools: vec![],
            model: "claude-opus-4-8".into(),
            max_tokens: 1024,
            temperature: None,
        };
        let v = serde_json::to_value(&req).unwrap();
        assert!(v.get("system").is_none());
        assert!(v.get("tools").is_none());
        assert!(v.get("temperature").is_none());
        round_trip(&req);

        let with_tools = ChatStreamRequest {
            tools: vec![ToolSchema {
                name: "animus.subject.list".into(),
                description: "List subjects".into(),
                input_schema: json!({ "type": "object" }),
            }],
            ..req
        };
        round_trip(&with_tools);
    }

    #[test]
    fn conversation_meta_round_trips() {
        let mut meta = ConversationMeta::new(
            "conv_01H",
            "claude-opus-4-8",
            "2026-06-08T15:00:00Z",
            "2026-06-08T15:05:00Z",
        );
        meta.title = Some("Weather chat".into());
        meta.agent_profile = Some("default".into());
        meta.principal_id = Some("sami".into());
        meta.message_count = 4;
        let v = serde_json::to_value(&meta).unwrap();
        assert_eq!(v["schema"], CONVERSATION_SCHEMA);
        round_trip(&meta);
    }

    #[test]
    fn role_and_stop_reason_wire_strings() {
        assert_eq!(serde_json::to_value(ChatRole::User).unwrap(), "user");
        assert_eq!(
            serde_json::to_value(ChatRole::Assistant).unwrap(),
            "assistant"
        );
        assert_eq!(serde_json::to_value(ChatRole::System).unwrap(), "system");
        assert_eq!(serde_json::to_value(ChatRole::Tool).unwrap(), "tool");
        assert_eq!(
            serde_json::to_value(StopReason::EndTurn).unwrap(),
            "end_turn"
        );
        assert_eq!(
            serde_json::to_value(StopReason::ToolUse).unwrap(),
            "tool_use"
        );
        assert_eq!(
            serde_json::to_value(StopReason::MaxTokens).unwrap(),
            "max_tokens"
        );
        assert_eq!(
            serde_json::to_value(StopReason::StopSequence).unwrap(),
            "stop_sequence"
        );
        assert_eq!(
            serde_json::to_value(StopReason::Refusal).unwrap(),
            "refusal"
        );
        assert_eq!(
            serde_json::to_value(StopReason::PauseTurn).unwrap(),
            "pause_turn"
        );
    }

    #[test]
    fn stop_reason_unknown_round_trips_losslessly() {
        // A future Anthropic stop reason this crate version doesn't model must
        // survive a round-trip rather than fail to deserialize.
        let parsed: StopReason = serde_json::from_value(json!("model_context_window")).unwrap();
        assert_eq!(parsed, StopReason::Other("model_context_window".into()));
        assert!(!parsed.is_known());
        assert_eq!(
            serde_json::to_value(&parsed).unwrap(),
            json!("model_context_window")
        );
        // Known values are still recognized.
        let known: StopReason = serde_json::from_value(json!("pause_turn")).unwrap();
        assert_eq!(known, StopReason::PauseTurn);
        assert!(known.is_known());
    }

    #[test]
    fn usage_deserializes_with_omitted_zero_counters() {
        // Anthropic's message_start usage omits cache fields; a client may send
        // only the fields it has. Missing counters must default to zero.
        let partial: Usage = serde_json::from_value(json!({ "input_tokens": 25 })).unwrap();
        assert_eq!(
            partial,
            Usage {
                input_tokens: 25,
                ..Default::default()
            }
        );
        let empty: Usage = serde_json::from_value(json!({})).unwrap();
        assert_eq!(empty, Usage::default());
    }

    #[test]
    fn constants_match_design_doc() {
        assert_eq!(PLUGIN_KIND_CHAT_PROVIDER, "chat_provider");
        assert_eq!(SUBJECT_KIND_CONVERSATION, "conversation");
        assert_eq!(MESSAGE_SCHEMA, "animus.chat.message.v1");
        assert_eq!(CONVERSATION_SCHEMA, "animus.chat.conversation.v1");
        assert_eq!(METHOD_CHAT_STREAM, "chat/stream");
        assert_eq!(METHOD_CHAT_MODELS, "chat/models");
        assert_eq!(METHOD_CHAT_COUNT_TOKENS, "chat/count_tokens");
        assert_eq!(NOTIFICATION_CHAT_DELTA, "chat/delta");
    }

    #[test]
    fn error_codes_live_in_a_collision_free_range() {
        // All chat codes must sit in this kind's reserved -32800..-32899 block,
        // clear of sibling per-kind blocks (-32100..-32599) and the JSON-RPC 2.0
        // standard errors (-32700..-32600).
        let codes = [
            error_codes::MODEL_NOT_FOUND,
            error_codes::AUTH_FAILED,
            error_codes::RATE_LIMITED,
            error_codes::CONTEXT_LENGTH_EXCEEDED,
            error_codes::UPSTREAM_UNAVAILABLE,
            error_codes::COUNT_TOKENS_UNSUPPORTED,
        ];
        for c in codes {
            assert!(
                (-32899..=-32800).contains(&c),
                "error code {c} outside the reserved -32800..-32899 chat range"
            );
        }
        // No collision with JSON-RPC standard / sibling codes we know about.
        assert!(!codes.contains(&-32601)); // METHOD_NOT_FOUND (standard)
        assert!(!codes.contains(&-32201)); // queue QUEUE_ENTRY_NOT_FOUND
    }
}

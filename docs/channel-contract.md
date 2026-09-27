# Channel messaging v1

`animus-channel-protocol` owns the `channel_backend` wire vocabulary. It is a
text-first adapter contract, not a new agent runtime. Implementations may expose
it alongside `transport_backend` through the existing multi-kind handshake.
Hosts must negotiate this capability; adding these types does not install a
channel supervisor in older daemons.

| Method | Request | Response |
|---|---|---|
| `channel/schema` | `{}` | `ChannelSchema` |
| `channel/receive` | `ChannelMessage` | `ChannelReceiveResult` |
| `channel/send` | `ChannelSendParams` | `ChannelDelivery` |
| `channel/status` | `ChannelStatusParams` | `ChannelDelivery` |

All versioned messages use `schema: "animus.channel.v1"`. Unknown fields and
versions are rejected. IDs are opaque, nonempty strings validated by the adapter
against configured accounts. Timestamps must be valid RFC 3339 instants. Provider
text size and response-window restrictions remain adapter responsibilities.
Capabilities must advertise only implemented behavior; unsupported media and
templates must not be silently converted to text.

## Authority

A connection is identified by `(tenant_id, connection_id)` from trusted local
configuration. A verified provider account selects that connection. An incoming
provider payload must never choose the tenant, agent, tools, actor claims, or
Animus conversation. `channel/receive` is a trusted host boundary, not an
unauthenticated HTTP endpoint. Verify webhook signatures over the original bytes
before parsing. Deny unapproved senders and ignore outbound echoes.

Derive the external principal and conversation from the full connection scope
plus peer ID. Bind a configured, scoped application profile. Route chat through
Animus shared conversation authority with a stable operation idempotency key.
Forward only the persisted final assistant message; never publish reasoning,
tool events, credentials, or raw backend errors.

## Durability and ordering

Inbound identity is `(scope, message_id)`. Commit normalized intake durably
before acknowledging the provider or returning `ChannelReceiveResult`.
Identical retries return `duplicate: true`; reuse of an identity with different
content is a conflict. Serialize turns per scoped peer and retain completion
records across restarts. A chat retry reuses the same operation key and shared
authority; it does not create a second conversation or invoke a second agent.

Connector inbox/outbox state owns provider receipt and delivery reconciliation.
It does not replace the shared trigger ingestion work tracked as TASK-307.
Optional workflow-trigger publication needs an explicit bridge to the host's
negotiated trigger shape and its acknowledgement boundary.

## Outbound reconciliation

`delivery_id` is an adapter idempotency key within a connection. Reusing it with
different destination/content is a conflict. `channel/send` records intent before
network I/O. `queued` becomes `sending` before contacting the provider.
`accepted` means the provider returned an identifier, not that a person received
the message. Verified callbacks advance to `delivered` or `read`; stale callbacks
must not regress state. Definite rejections become `failed` with a sanitized code.

Timeouts and crashes after sending can mean the provider accepted a message.
Represent that uncertainty as `unknown`, retain any provider ID/correlation, and
reconcile through verified callbacks or operator evidence. Never automatically
resend an ambiguous attempt. This contract does not promise exactly-once network
delivery. A process takeover must fence the old worker before recovery.

Enforce provider reply windows at send time. Human takeover and sender opt-out
must suppress queued automatic replies as well as new turns. A management UI
must require tenant-scoped operator authorization; channel messages themselves
cannot grant administrative access.

## Code generation

Run `cargo run -p animus-channel-protocol --bin animus-channel-protocol-export-schema`.
After merging and tagging the protocol release, run the TypeScript SDK's
`schemas:sync -- --protocol-root <checkout> --ref <exact-release>` and commit
the resulting schemas, generated types, and provenance together.

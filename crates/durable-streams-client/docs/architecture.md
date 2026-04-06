# Architecture Notes

## Public API

The crate intentionally exposes a small consumer-oriented API:

- `DurableStreamsClient`
- `DurableStream`
- `StreamPath`
- `Offset`
- `ReadRequest` / `ReadResponse`
- `StreamMetadata`
- `SubscribeRequest`
- `SubscriptionEvent`

The stream handle is the main entry point because phase 2 will want to layer a
Kafka bridge on top of a stable, typed stream abstraction rather than ad hoc
URLs and headers.

This is a pattern-oriented foundation, not a claim that an HTTP client bridge is
the terminal or most efficient deployment architecture. A native Kafka
connector/plugin that reads Durable Streams directly, or a server-side forwarding
feature that publishes to Kafka during append processing, may be materially more
efficient in production. Those alternatives are intentionally out of scope for
this phase.

## Internal structure

- `client.rs`: root configuration and client construction
- `stream.rs`: public stream operations
- `transport.rs`: reqwest transport and retry loop
- `protocol/headers.rs`: HTTP header parsing and status classification
- `protocol/sse.rs`: low-level SSE parsing
- `types/`: domain models
- `error.rs`: transport, HTTP, and protocol errors

## Conformance coverage

Directly covered in this phase:

- catch-up reads
- long-poll reads
- offset resumption
- cursor forwarding
- SSE data/control parsing
- SSE base64 decoding for binary streams
- SSE reconnect using `streamNextOffset`
- retry handling for `500`, `503`, and `429`

Not directly covered yet:

- create/append/close/delete lifecycle
- idempotent producer semantics
- JSON batching conveniences
- a full upstream adapter for all client conformance suites

## Known protocol ambiguities

- The protocol calls offsets opaque for clients, so the crate does not parse
  server-generated offset structure beyond preserving `-1` and `now`.
- `400 Bad Request` response bodies are not standardized, so detailed error
  classification such as `invalid offset` remains heuristic unless a server
  emits a helpful body.

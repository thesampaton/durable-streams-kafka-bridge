# durable-streams-client

Idiomatic Rust client for the Durable Streams protocol, focused on consumer
operations for phase 1 of `durable-streams-kafka-bridge`.

## Scope

This crate currently provides:

- typed stream handles rooted at a base server URL
- `HEAD` metadata reads
- catch-up reads over HTTP `GET`
- long-poll reads with `Stream-Cursor` forwarding
- SSE subscriptions with reconnection from the last checkpoint
- explicit protocol/domain types for offsets, stream paths, read requests, read
  responses, and subscription checkpoints

This crate does not yet provide create, append, close, delete, or idempotent
producer APIs.

## Scope and efficiency

This repository phase is intentionally about documenting and proving the pattern,
not about claiming the final bridge shape is the most efficient architecture.

For some production deployments, more efficient options are likely to exist,
including:

- a native Kafka connector or plugin that can treat Durable Streams as a source
- a server-side feature in a Durable Streams implementation such as the Rust
  server that can fork or forward appended data directly to Kafka as a producer

Those options may reduce hops, polling overhead, or protocol translation work,
but they are beyond the scope of this phase. The goal here is a clear,
reusable, idiomatic Rust client foundation that a later Kafka bridge can build
on top of.

## Design

- Offsets are treated as opaque client values.
- Transport and protocol parsing are kept separate from the public API.
- The public surface is small:

```rust
use durable_streams_client::{DurableStreamsClient, ReadMode, ReadRequest, SubscribeRequest};

let client = DurableStreamsClient::new("http://localhost:4437")?;
let stream = client.stream("/v1/stream/orders")?;

let metadata = stream.metadata().await?;

let snapshot = stream
    .read(ReadRequest {
        mode: ReadMode::CatchUp,
        ..Default::default()
    })
    .await?;

let events = stream.subscribe(SubscribeRequest::default());
```

## Testing

```bash
cargo test -p durable-streams-client
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

## Conformance notes

The published upstream `@durable-streams/client-conformance-tests` package is
broader than this phase. Its consumer suites still rely on create/append/delete
setup operations, so it cannot validate this crate's read-only public API
directly without adapter-side helpers.

For this phase, the crate validates:

- protocol header handling
- retry classification
- SSE parsing and base64 decoding
- SSE reconnection semantics
- integration-style reads against a local test server

See [`docs/architecture.md`](docs/architecture.md) for the module layout and
known gaps.

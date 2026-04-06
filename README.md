# durable-streams-kafka-bridge

This repository demonstrates a lightweight pattern for forwarding Durable
Streams into Kafka.

Phase 1 established a reusable Rust client for the Durable Streams protocol.
Phase 2 builds a small bridge binary on top of that client. The bridge is
intentionally simple: it subscribes to one or more Durable Streams, forwards
events into Kafka, and stores a minimal resume offset per stream in a local
JSON file.

It is intentionally not:

- a Durable Streams server extension with built-in Kafka support
- a Kafka Connect source plugin
- a general-purpose streaming platform
- an exactly-once delivery system

## Workspace layout

- `crates/durable-streams-client`: reusable protocol client
- `crates/durable-streams-kafka-bridge`: thin Kafka bridge binary

## Bridge architecture

The bridge keeps Durable Streams and Kafka concerns separated:

- `durable-streams-client` handles HTTP transport, retries, offsets, and SSE
  checkpoint parsing
- the bridge crate owns configuration, topic/key mapping, Kafka publishing, and
  minimal offset persistence
- one async task runs per configured stream
- each task buffers a single event until the Durable Streams checkpoint arrives
- the Kafka record is produced using the checkpoint offset as stable identity
- the offset store is updated only after Kafka acknowledges delivery

That gives a small, boring application layer with at-least-once forwarding and
restart resumption, without adding Kafka-specific logic to the client crate.

## Topic and key strategy

The default strategy is topic-per-stream:

- `/v1/stream/orders` becomes `durable-streams.v1.stream.orders`
- invalid Kafka topic characters are replaced with `-`
- a stream can override the topic explicitly in config

Kafka message keys are deterministic:

- key format: `stream_path:next_offset`
- example: `/v1/stream/orders:o42`

The bridge also adds Kafka headers for `durable-stream-path` and
`durable-stream-next-offset`.

This is enough for downstream consumers to tolerate duplicates without the
bridge implementing semantic deduplication.

## Resumability

The bridge persists only one thing per stream: the last acknowledged
`next_offset`.

- storage format: local JSON file
- default path: `.durable-streams-kafka-bridge-offsets.json` next to the config
  file
- write point: after the Kafka producer acknowledges the record
- restart behavior: resume from stored offset, otherwise from configured
  `offset`, otherwise from `start`

This is intentionally minimal. There are no transactions, dedupe tables,
delivery-state machines, or offset coordination layers.
The offset file is written via temp-file-and-rename so a partial write does not
become the persisted checkpoint state.

## Guarantees and limitations

The bridge provides:

- ordered reads from Durable Streams
- deterministic Kafka keys for duplicate tolerance
- best-effort resumable forwarding
- at-least-once delivery

The bridge does not provide:

- end-to-end exactly-once semantics
- guaranteed deduplication
- schema registry integration
- transformation pipelines
- complex stream processing

## Configuration

Example configuration:

```toml
[durable_streams]
base_url = "http://localhost:4437"

[kafka]
bootstrap_servers = "localhost:9092"
client_id = "durable-streams-kafka-bridge"
delivery_timeout_ms = 30000

[offset_store]
path = ".durable-streams-kafka-bridge-offsets.json"

[[streams]]
path = "/v1/stream/orders"
topic = "orders"
offset = "start"

[[streams]]
path = "/v1/stream/payments"
```

See `bridge.example.toml`.

## Running locally

Start Kafka:

```bash
docker compose up -d
```

Run the bridge:

```bash
cargo run -p durable-streams-kafka-bridge -- --config bridge.example.toml
```

The bridge expects a Durable Streams server to be running separately at the
configured `base_url`.

## Native prerequisites

The bridge uses `rdkafka`. Building the real Kafka producer requires native
`librdkafka` tooling. On machines without that toolchain, install either:

- `cmake` so the vendored `librdkafka` build can run
- or a system `librdkafka` and corresponding `pkg-config` metadata

## Testing

Core bridge tests do not require a local Kafka broker:

```bash
cargo test -p durable-streams-client
cargo test -p durable-streams-kafka-bridge --no-default-features
cargo fmt --all --check
```

The bridge crate includes:

- unit tests for topic mapping, keying, and offset persistence
- integration-style tests for forwarding and offset checkpointing with a fake
  sink and local test Durable Streams server

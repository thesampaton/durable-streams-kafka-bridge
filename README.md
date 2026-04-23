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
- named `targets` let multiple stream entries share one Kafka destination
- a bridge-level `topic_mapping.default_topic` can send all configured streams
  to one shared topic
- a stream can still override the topic explicitly in config

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

[kafka.auth]
security_protocol = "SASL_SSL"
sasl_mechanism = "PLAIN"
username_env = "KAFKA_USERNAME"
password_env = "KAFKA_PASSWORD"

[preflight]
enabled = true
timeout_ms = 5000
require_topics_exist = true

[targets.enterprise_documents]
topic = "enterprise.documents"

[offset_store]
path = ".durable-streams-kafka-bridge-offsets.json"

[[streams]]
path = "/v1/stream/docs/1"
target = "enterprise_documents"
offset = "start"

[[streams]]
path = "/v1/stream/docs/2"
target = "enterprise_documents"

[[streams]]
path = "/v1/stream/orders"
topic = "orders"
```

See `bridge.example.toml`.

This keeps the config close to the real operational shape:

- stream path = source
- target name = destination intent
- target topic = Kafka destination

Topic precedence is:

- `[[streams]].topic`
- `[[streams]].target`
- `topic_mapping.default_topic`
- derived topic per stream

## Dynamic discovery

The bridge can watch a "control stream" whose JSON events announce new streams,
and dynamically spawn forwarders for them at runtime. This is useful when the set
of streams is not known ahead of time.

```toml
[[discovery]]
control_stream = "/v1/stream/admin/activity"
filter_field   = "kind"
filter_value   = "stream-created"
path_field     = "metadata.streamPath"
path_prefix    = "/v1/stream/"
default_offset = "start"
topic_template = "durable-streams.{path}"
```

How it works:

- on startup, static `[[streams]]` tasks launch as before
- for each `[[discovery]]` block, a separate subscription watches the control
  stream
- on each checkpoint, the buffered payload is parsed as JSON
- if `filter_field` / `filter_value` are set, only matching events proceed
- the stream path is extracted via `path_field` (dotted JSON path), prepended
  with `path_prefix` if set, and validated
- if the path is new, a forwarder task is spawned; duplicates are skipped
- topic naming uses `topic_template` (with `{path}` substitution and character
  sanitization) or falls back to the default `durable-streams.<path>` convention
- discovered streams persist offsets through the same `OffsetStore`; on restart,
  re-reading the control stream re-discovers them and the forwarder resumes from
  its last acknowledged offset
- malformed JSON or missing fields are logged and skipped without crashing

The `[[discovery]]` block is optional. Existing configs without it continue to
work unchanged.

## Managed Kafka auth

The bridge now supports a small env-backed Kafka auth block:

```toml
[kafka.auth]
security_protocol = "SASL_SSL"
sasl_mechanism = "PLAIN"
username_env = "KAFKA_USERNAME"
password_env = "KAFKA_PASSWORD"
```

At startup, the bridge reads the named environment variables and applies them to
the `rdkafka` producer as:

- `security.protocol`
- `sasl.mechanism`
- `sasl.username`
- `sasl.password`

This keeps secrets out of the config file while staying simple for blog and demo
use cases.

### Confluent Cloud

For Confluent Cloud, use your Kafka API key and secret:

```bash
export KAFKA_USERNAME="<confluent-api-key>"
export KAFKA_PASSWORD="<confluent-api-secret>"
```

The bridge config can stay on `SASL_SSL` + `PLAIN`.

### Google Cloud Managed Kafka

For Google Cloud Managed Service for Apache Kafka, this bridge can use the same
`SASL_SSL` + `PLAIN` shape for simple testing and blog demos, with a principal
in `KAFKA_USERNAME` and a short-lived token or other provider-issued secret in
`KAFKA_PASSWORD`.

Example:

```bash
export KAFKA_USERNAME="<managed-kafka-principal>"
export KAFKA_PASSWORD="$(gcloud auth print-access-token)"
```

Important limitation:

- the bridge reads auth env vars once at startup
- it does not refresh short-lived tokens automatically
- for long-running production use with expiring credentials, a future
  `OAUTHBEARER` flow or an external restart/rotation mechanism is the better fit

That limitation is intentional for this version of the bridge. The goal here is
to document and demonstrate the pattern, not to build a full auth subsystem.

## Preflight checks

Before the bridge starts forwarding, it can perform a small operational
preflight:

- resolve Kafka auth env vars
- verify each configured Durable Stream is reachable via a metadata request
- verify the Kafka cluster is reachable
- optionally verify that each resolved Kafka topic already exists

Configuration:

```toml
[preflight]
enabled = true
timeout_ms = 5000
require_topics_exist = true
```

This is intentionally a readiness check, not a reconciliation system:

- it does not create topics
- it does not retry forever during startup
- it does not validate every downstream policy
- it fails fast before the bridge enters the forwarding loop

For the blog and demo shape, this gives a cleaner operational model:

- define sources
- define destinations
- validate the bridge can reach both sides
- then start forwarding with lightweight local offset state

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

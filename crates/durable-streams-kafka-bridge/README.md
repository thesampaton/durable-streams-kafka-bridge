# durable-streams-kafka-bridge

Small binary crate that subscribes to Durable Streams and produces those events
into Kafka.

## Why it exists

This crate demonstrates a deliberately lightweight application layer on top of
`durable-streams-client`.

- keep protocol logic in the reusable client crate
- keep Kafka concerns in a tiny bridge binary
- make restart resumption simple and explicit

## What it does

- subscribes to one or more Durable Streams
- reads events in order
- publishes them to Kafka with deterministic keys
- stores the last acknowledged `next_offset` in a local JSON file

## What it does not do

- exactly-once delivery
- deduplication
- schema registry integration
- transformations or processing pipelines

See the repository [`README.md`](/Users/sampaton/IdeaProjects/durable-streams-kafka-bridge/README.md)
for the full architecture, config, and local run instructions.

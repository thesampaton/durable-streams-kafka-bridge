use async_trait::async_trait;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeRecord {
    pub topic: String,
    pub key: String,
    pub payload: Vec<u8>,
    pub stream_path: String,
    pub next_offset: String,
}

#[async_trait]
pub trait RecordSink: Send + Sync {
    async fn send(&self, record: BridgeRecord) -> Result<(), SinkError>;
}

#[cfg(feature = "rdkafka-producer")]
#[derive(Clone)]
pub struct KafkaSink {
    producer: rdkafka::producer::FutureProducer,
    delivery_timeout: Duration,
}

#[cfg(feature = "rdkafka-producer")]
impl KafkaSink {
    /// Create a Kafka record sink backed by `rdkafka`.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying Kafka producer cannot be created.
    pub fn new(
        bootstrap_servers: &str,
        client_id: &str,
        delivery_timeout: Duration,
    ) -> Result<Self, SinkError> {
        let producer = rdkafka::config::ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("client.id", client_id)
            .set("acks", "all")
            .create()
            .map_err(SinkError::CreateProducer)?;
        Ok(Self {
            producer,
            delivery_timeout,
        })
    }
}

#[cfg(feature = "rdkafka-producer")]
#[async_trait]
impl RecordSink for KafkaSink {
    async fn send(&self, record: BridgeRecord) -> Result<(), SinkError> {
        let headers = rdkafka::message::OwnedHeaders::new()
            .insert(rdkafka::message::Header {
                key: "durable-stream-path",
                value: Some(record.stream_path.as_bytes()),
            })
            .insert(rdkafka::message::Header {
                key: "durable-stream-next-offset",
                value: Some(record.next_offset.as_bytes()),
            });

        self.producer
            .send(
                rdkafka::producer::FutureRecord::to(&record.topic)
                    .key(&record.key)
                    .payload(&record.payload)
                    .headers(headers),
                self.delivery_timeout,
            )
            .await
            .map(|_| ())
            .map_err(|(source, _)| SinkError::Deliver(source))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[cfg(feature = "rdkafka-producer")]
    #[error("failed to create Kafka producer")]
    CreateProducer(#[source] rdkafka::error::KafkaError),
    #[cfg(feature = "rdkafka-producer")]
    #[error("failed to deliver Kafka record")]
    Deliver(#[source] rdkafka::error::KafkaError),
    #[error("{0}")]
    Message(String),
}

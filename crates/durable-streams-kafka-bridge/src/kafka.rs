#[cfg(feature = "rdkafka-producer")]
use crate::config::KafkaAuthConfig;
use async_trait::async_trait;
#[cfg(feature = "rdkafka-producer")]
use rdkafka::producer::Producer;
#[cfg(feature = "rdkafka-producer")]
use std::collections::BTreeSet;
#[cfg(feature = "rdkafka-producer")]
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
        auth: Option<&KafkaAuthConfig>,
    ) -> Result<Self, SinkError> {
        let mut config = rdkafka::config::ClientConfig::new();
        config
            .set("bootstrap.servers", bootstrap_servers)
            .set("client.id", client_id)
            .set("acks", "all");
        if let Some(auth) = auth {
            let resolved = resolve_auth(auth, |name| std::env::var(name).ok())?;
            config
                .set("security.protocol", &resolved.security_protocol)
                .set("sasl.mechanism", &resolved.sasl_mechanism)
                .set("sasl.username", &resolved.username)
                .set("sasl.password", &resolved.password);
            if let Some(ssl_ca_location) = resolved.ssl_ca_location.as_deref() {
                config.set("ssl.ca.location", ssl_ca_location);
            }
        }
        let producer = config.create().map_err(SinkError::CreateProducer)?;
        Ok(Self {
            producer,
            delivery_timeout,
        })
    }

    /// Perform a lightweight Kafka preflight check.
    ///
    /// # Errors
    ///
    /// Returns an error when the broker cannot be reached or when a required
    /// topic is missing.
    pub fn preflight(
        &self,
        topics: &[String],
        metadata_timeout: Duration,
        require_topics_exist: bool,
    ) -> Result<(), SinkError> {
        self.producer
            .client()
            .fetch_metadata(None, metadata_timeout)
            .map_err(SinkError::MetadataFetch)?;

        if require_topics_exist {
            let unique_topics: BTreeSet<_> = topics.iter().map(String::as_str).collect();
            for topic in unique_topics {
                let metadata = self
                    .producer
                    .client()
                    .fetch_metadata(Some(topic), metadata_timeout)
                    .map_err(SinkError::MetadataFetch)?;
                let exists = metadata.topics().iter().any(|entry| entry.name() == topic);
                if !exists {
                    return Err(SinkError::MissingTopic {
                        topic: topic.to_string(),
                    });
                }
            }
        }

        Ok(())
    }
}

#[cfg(feature = "rdkafka-producer")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedKafkaAuth {
    security_protocol: String,
    sasl_mechanism: String,
    username: String,
    password: String,
    ssl_ca_location: Option<String>,
}

#[cfg(feature = "rdkafka-producer")]
fn resolve_auth(
    auth: &KafkaAuthConfig,
    lookup_env: impl Fn(&str) -> Option<String>,
) -> Result<ResolvedKafkaAuth, SinkError> {
    let username = lookup_env(&auth.username_env).ok_or_else(|| SinkError::MissingEnvVar {
        name: auth.username_env.clone(),
    })?;
    let password = lookup_env(&auth.password_env).ok_or_else(|| SinkError::MissingEnvVar {
        name: auth.password_env.clone(),
    })?;

    Ok(ResolvedKafkaAuth {
        security_protocol: auth.security_protocol.clone(),
        sasl_mechanism: auth.sasl_mechanism.clone(),
        username,
        password,
        ssl_ca_location: auth.ssl_ca_location.clone(),
    })
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
    #[cfg(feature = "rdkafka-producer")]
    #[error("failed to fetch Kafka metadata")]
    MetadataFetch(#[source] rdkafka::error::KafkaError),
    #[cfg(feature = "rdkafka-producer")]
    #[error("required Kafka topic `{topic}` was not found during preflight")]
    MissingTopic { topic: String },
    #[cfg(feature = "rdkafka-producer")]
    #[error("required Kafka auth env var `{name}` was not set")]
    MissingEnvVar { name: String },
    #[error("{0}")]
    Message(String),
}

#[cfg(all(test, feature = "rdkafka-producer"))]
mod tests {
    use super::resolve_auth;
    use crate::config::KafkaAuthConfig;
    use std::collections::BTreeMap;

    #[test]
    fn resolves_auth_from_env_lookup() {
        let auth = KafkaAuthConfig {
            security_protocol: "SASL_SSL".to_string(),
            sasl_mechanism: "PLAIN".to_string(),
            username_env: "KAFKA_USERNAME".to_string(),
            password_env: "KAFKA_PASSWORD".to_string(),
            ssl_ca_location: Some("/tmp/ca.pem".to_string()),
        };
        let env = BTreeMap::from([
            ("KAFKA_USERNAME".to_string(), "user".to_string()),
            ("KAFKA_PASSWORD".to_string(), "secret".to_string()),
        ]);

        let resolved = resolve_auth(&auth, |name| env.get(name).cloned()).unwrap();
        assert_eq!(resolved.username, "user");
        assert_eq!(resolved.password, "secret");
        assert_eq!(resolved.ssl_ca_location.as_deref(), Some("/tmp/ca.pem"));
    }
}

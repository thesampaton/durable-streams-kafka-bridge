use crate::config::{StreamConfig, TargetConfig, TopicMappingConfig};
use crate::kafka::{BridgeRecord, RecordSink, SinkError};
use crate::offset_store::{OffsetStore, OffsetStoreError};
use crate::topic::{TopicResolutionContext, message_key, resolve_topic};
use durable_streams_client::{
    DurableStreamsClient, Error as ClientError, Offset, StreamCheckpoint, SubscribeRequest,
    SubscriptionEvent,
};
use futures_util::StreamExt;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct StreamRuntime {
    pub path: String,
    pub topic: String,
    pub start_offset: Offset,
}

impl StreamRuntime {
    #[must_use]
    pub fn from_config(
        config: &StreamConfig,
        targets: &BTreeMap<String, TargetConfig>,
        topic_mapping: &TopicMappingConfig,
    ) -> Self {
        Self {
            path: config.path.clone(),
            topic: resolve_topic(
                &config.path,
                &TopicResolutionContext {
                    explicit_topic: config.topic.as_deref(),
                    target: config.target.as_deref(),
                    targets,
                    topic_mapping,
                },
            ),
            start_offset: config.start_offset(),
        }
    }
}

/// Run the forwarding loop for a single configured stream.
///
/// # Errors
///
/// Returns an error when the Durable Streams subscription fails, when Kafka
/// publishing fails, or when offset persistence fails.
pub async fn run_stream<S>(
    client: DurableStreamsClient,
    stream: StreamRuntime,
    offset_store: Arc<OffsetStore>,
    sink: Arc<S>,
) -> Result<(), BridgeError>
where
    S: RecordSink + 'static,
{
    let durable_stream = client.stream(&stream.path)?;
    let offset = offset_store
        .load(&stream.path)
        .await
        .unwrap_or_else(|| stream.start_offset.clone());
    let mut subscription = durable_stream.subscribe(SubscribeRequest { offset });
    let mut pending_payload = Vec::new();

    while let Some(event) = subscription.next().await {
        match event? {
            SubscriptionEvent::Data(bytes) => {
                pending_payload.extend_from_slice(&bytes);
            }
            SubscriptionEvent::Checkpoint(checkpoint) => {
                handle_checkpoint(
                    &stream,
                    checkpoint,
                    std::mem::take(&mut pending_payload),
                    offset_store.as_ref(),
                    sink.as_ref(),
                )
                .await?;
            }
        }
    }

    if !pending_payload.is_empty() {
        return Err(BridgeError::UnexpectedSequence {
            stream_path: stream.path.clone(),
            reason: "stream ended with buffered data that never received a checkpoint".to_string(),
        });
    }

    Ok(())
}

async fn handle_checkpoint(
    stream: &StreamRuntime,
    checkpoint: StreamCheckpoint,
    pending_payload: Vec<u8>,
    offset_store: &OffsetStore,
    sink: &dyn RecordSink,
) -> Result<(), BridgeError> {
    if !pending_payload.is_empty() {
        let record = BridgeRecord {
            topic: stream.topic.clone(),
            key: message_key(&stream.path, &checkpoint.next_offset),
            payload: pending_payload,
            stream_path: stream.path.clone(),
            next_offset: checkpoint.next_offset.as_str().to_string(),
        };
        sink.send(record).await?;
        offset_store
            .save(&stream.path, &checkpoint.next_offset)
            .await?;
    }

    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error(transparent)]
    Sink(#[from] SinkError),
    #[error(transparent)]
    OffsetStore(#[from] OffsetStoreError),
    #[error("unexpected event sequence for `{stream_path}`: {reason}")]
    UnexpectedSequence { stream_path: String, reason: String },
}

#[cfg(test)]
mod tests {
    use super::{StreamRuntime, handle_checkpoint};
    use crate::config::{StreamConfig, TargetConfig, TopicMappingConfig};
    use crate::kafka::{BridgeRecord, RecordSink, SinkError};
    use crate::offset_store::OffsetStore;
    use async_trait::async_trait;
    use durable_streams_client::{Offset, StreamCheckpoint};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct FakeSink {
        records: Mutex<Vec<BridgeRecord>>,
    }

    #[async_trait]
    impl RecordSink for FakeSink {
        async fn send(&self, record: BridgeRecord) -> Result<(), SinkError> {
            self.records.lock().await.push(record);
            Ok(())
        }
    }

    #[tokio::test]
    async fn checkpoint_persists_offset_after_send() {
        let path = std::env::temp_dir().join(format!(
            "durable-streams-kafka-bridge-bridge-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = OffsetStore::open(&path).await.unwrap();
        let sink = Arc::new(FakeSink::default());
        let stream = StreamRuntime {
            path: "/v1/stream/orders".to_string(),
            topic: "orders".to_string(),
            start_offset: Offset::start(),
        };

        handle_checkpoint(
            &stream,
            StreamCheckpoint {
                next_offset: Offset::from("o9"),
                cursor: None,
                up_to_date: false,
                stream_closed: false,
            },
            br#"{"id":"1"}"#.to_vec(),
            &store,
            sink.as_ref(),
        )
        .await
        .unwrap();

        let records = sink.records.lock().await.clone();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, "/v1/stream/orders:o9");
        assert_eq!(
            store.load("/v1/stream/orders").await,
            Some(Offset::from("o9"))
        );
        let _ = tokio::fs::remove_file(path).await;
    }

    #[test]
    fn stream_runtime_uses_bridge_level_default_topic() {
        let runtime = StreamRuntime::from_config(
            &StreamConfig {
                path: "/v1/stream/docs/1".to_string(),
                topic: None,
                target: None,
                offset: None,
            },
            &BTreeMap::new(),
            &TopicMappingConfig {
                default_topic: Some("enterprise.documents".to_string()),
            },
        );

        assert_eq!(runtime.topic, "enterprise.documents");
    }

    #[test]
    fn stream_runtime_uses_named_target_topic() {
        let runtime = StreamRuntime::from_config(
            &StreamConfig {
                path: "/v1/stream/docs/1".to_string(),
                topic: None,
                target: Some("enterprise_documents".to_string()),
                offset: None,
            },
            &BTreeMap::from([(
                "enterprise_documents".to_string(),
                TargetConfig {
                    topic: "enterprise.documents".to_string(),
                },
            )]),
            &TopicMappingConfig::default(),
        );

        assert_eq!(runtime.topic, "enterprise.documents");
    }
}

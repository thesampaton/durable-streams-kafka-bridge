use async_trait::async_trait;
use axum::Router;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use durable_streams_client::DurableStreamsClient;
use durable_streams_kafka_bridge::bridge::BridgeError;
use durable_streams_kafka_bridge::config::DiscoveryConfig;
use durable_streams_kafka_bridge::discovery::{ActivePaths, SpawnRequest, run_discovery};
use durable_streams_kafka_bridge::kafka::{BridgeRecord, RecordSink, SinkError};
use durable_streams_kafka_bridge::offset_store::OffsetStore;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

#[derive(Default, Clone)]
struct FakeSink {
    sent: Arc<Mutex<Vec<BridgeRecord>>>,
}

#[async_trait]
impl RecordSink for FakeSink {
    async fn send(&self, record: BridgeRecord) -> Result<(), SinkError> {
        self.sent.lock().await.push(record);
        Ok(())
    }
}

/// Track how many times the discovered-stream endpoint has been hit, proving
/// that forwarders were actually spawned and connected.
#[derive(Clone, Default)]
struct HitCounter {
    count: Arc<Mutex<usize>>,
}

/// SSE control stream that emits three stream-created events separated by
/// short delays, then closes.
async fn slow_control_stream() -> impl IntoResponse {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<String, std::convert::Infallible>>(1);

    tokio::spawn(async move {
        for i in 0..3u8 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let path = format!("stream/{i}");
            let closed = if i == 2 { "true" } else { "false" };
            let chunk = format!(
                "event: data\n\
                 data: {{\"kind\":\"stream-created\",\"metadata\":{{\"streamPath\":\"{path}\"}}}}\n\n\
                 event: control\n\
                 data: {{\"streamNextOffset\":\"c{i}\",\"upToDate\":true,\"streamClosed\":{closed}}}\n\n"
            );
            if tx.send(Ok(chunk)).await.is_err() {
                break;
            }
        }
        // tx drops here, closing the body stream.
    });

    let body = axum::body::Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx));
    (
        [
            (axum::http::header::CONTENT_TYPE, "text/event-stream"),
            (axum::http::header::CACHE_CONTROL, "no-cache"),
        ],
        body,
    )
}

/// Each discovered stream emits one event and closes immediately.
async fn fast_discovered_stream(State(hits): State<HitCounter>) -> impl IntoResponse {
    *hits.count.lock().await += 1;
    (
        [
            (axum::http::header::CONTENT_TYPE, "text/event-stream"),
            (axum::http::header::CACHE_CONTROL, "no-cache"),
        ],
        "event: data\n\
         data: {\"msg\":\"discovered\"}\n\n\
         event: control\n\
         data: {\"streamNextOffset\":\"d1\",\"upToDate\":true,\"streamClosed\":true}\n\n",
    )
}

/// Regression test: with the old `Arc<Mutex<JoinSet>>` approach, the main loop
/// held the mutex across `join_next().await`, preventing discovery from ever
/// spawning a forwarder. This test verifies forwarders are spawned and produce
/// records within a bounded time.
#[tokio::test]
async fn discovery_does_not_deadlock_when_spawning_forwarders() {
    let hits = HitCounter::default();

    let app = Router::new()
        .route("/v1/stream/admin/activity", get(slow_control_stream))
        .route("/v1/stream/stream/0", get(fast_discovered_stream))
        .route("/v1/stream/stream/1", get(fast_discovered_stream))
        .route("/v1/stream/stream/2", get(fast_discovered_stream))
        .with_state(hits.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let client = DurableStreamsClient::new(format!("http://{address}")).unwrap();
    let store_path = std::env::temp_dir().join(format!(
        "durable-streams-kafka-bridge-deadlock-{}-{}.json",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let offset_store = Arc::new(OffsetStore::open(&store_path).await.unwrap());
    let sink = Arc::new(FakeSink::default());
    let active_paths: ActivePaths = Arc::new(Mutex::new(HashSet::new()));

    let config: DiscoveryConfig = toml::from_str(
        r#"
        control_stream = "/v1/stream/admin/activity"
        filter_field = "kind"
        filter_value = "stream-created"
        path_field = "metadata.streamPath"
        path_prefix = "/v1/stream/"
        "#,
    )
    .unwrap();

    let (spawn_tx, mut spawn_rx) = tokio::sync::mpsc::unbounded_channel::<SpawnRequest>();
    let mut tasks: JoinSet<Result<(), BridgeError>> = JoinSet::new();

    tasks.spawn({
        let client = client.clone();
        let offset_store = offset_store.clone();
        let sink = sink.clone();
        let active_paths = active_paths.clone();
        async move {
            run_discovery(client, config, offset_store, sink, active_paths, spawn_tx).await
        }
    });

    // Wait for forwarders to be spawned and produce at least one record. Under
    // the old deadlock code, spawn_rx.recv() would eventually return a future
    // but the JoinSet mutex would prevent it from being driven.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut records_seen = false;

    loop {
        // Check if we've gotten enough records.
        if !records_seen {
            let sent = sink.sent.lock().await;
            if sent.len() >= 3 {
                records_seen = true;
            }
        }

        tokio::select! {
            Some(fut) = spawn_rx.recv() => {
                tasks.spawn(async move { fut.await });
            }
            result = tasks.join_next(), if !tasks.is_empty() => match result {
                Some(Ok(Ok(()))) => {
                    // Check if all tasks are done and channel is closed.
                    if tasks.is_empty() && spawn_rx.is_empty() {
                        break;
                    }
                }
                Some(Ok(Err(e))) => panic!("task failed: {e}"),
                Some(Err(e)) => panic!("join error: {e}"),
                None => break,
            },
            _ = tokio::time::sleep_until(deadline) => {
                let sent = sink.sent.lock().await;
                let hit_count = *hits.count.lock().await;
                panic!(
                    "timed out — likely deadlocked. Records: {}, hits: {hit_count}",
                    sent.len()
                );
            }
            else => break,
        }
    }

    let sent = sink.sent.lock().await.clone();
    assert!(
        !sent.is_empty(),
        "expected at least one record from a discovered forwarder"
    );

    // All three should have been spawned.
    let paths: HashSet<_> = sent.iter().map(|r| r.stream_path.clone()).collect();
    assert_eq!(
        paths.len(),
        3,
        "expected 3 distinct forwarders, got {paths:?}"
    );

    let _ = tokio::fs::remove_file(store_path).await;
}

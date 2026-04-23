use async_trait::async_trait;
use axum::Router;
use axum::response::IntoResponse;
use axum::routing::get;
use durable_streams_client::DurableStreamsClient;
use durable_streams_kafka_bridge::bridge::BridgeError;
use durable_streams_kafka_bridge::config::DiscoveryConfig;
use durable_streams_kafka_bridge::discovery::{ActivePaths, run_discovery};
use durable_streams_kafka_bridge::kafka::{BridgeRecord, RecordSink, SinkError};
use durable_streams_kafka_bridge::offset_store::OffsetStore;
use std::collections::HashSet;
use std::sync::Arc;
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

fn temp_offset_path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "durable-streams-kafka-bridge-discovery-{label}-{}-{}.json",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

/// Build an SSE response body from raw SSE text.
fn sse_response(body: &'static str) -> impl IntoResponse {
    (
        [
            (axum::http::header::CONTENT_TYPE, "text/event-stream"),
            (axum::http::header::CACHE_CONTROL, "no-cache"),
        ],
        body,
    )
}

// ── Handlers ──────────────────────────────────────────────────────────────

/// Control stream that emits two stream-created events and one document-deleted.
async fn control_stream_handler() -> impl IntoResponse {
    sse_response(
        // Event 1: stream-created for slides/a
        "event: data\n\
         data: {\"kind\":\"stream-created\",\"metadata\":{\"streamPath\":\"slides/a\"}}\n\n\
         event: control\n\
         data: {\"streamNextOffset\":\"c1\",\"upToDate\":false}\n\n\
         event: data\n\
         data: {\"kind\":\"document-deleted\",\"metadata\":{\"streamPath\":\"slides/gone\"}}\n\n\
         event: control\n\
         data: {\"streamNextOffset\":\"c2\",\"upToDate\":false}\n\n\
         event: data\n\
         data: {\"kind\":\"stream-created\",\"metadata\":{\"streamPath\":\"doc/b\"}}\n\n\
         event: control\n\
         data: {\"streamNextOffset\":\"c3\",\"upToDate\":true,\"streamClosed\":true}\n\n",
    )
}

/// Control stream that emits the same path twice.
async fn control_stream_dedupe_handler() -> impl IntoResponse {
    sse_response(
        "event: data\n\
         data: {\"kind\":\"stream-created\",\"metadata\":{\"streamPath\":\"slides/dup\"}}\n\n\
         event: control\n\
         data: {\"streamNextOffset\":\"c1\",\"upToDate\":false}\n\n\
         event: data\n\
         data: {\"kind\":\"stream-created\",\"metadata\":{\"streamPath\":\"slides/dup\"}}\n\n\
         event: control\n\
         data: {\"streamNextOffset\":\"c2\",\"upToDate\":true,\"streamClosed\":true}\n\n",
    )
}

/// Control stream that emits malformed JSON.
async fn control_stream_malformed_handler() -> impl IntoResponse {
    sse_response(
        "event: data\n\
         data: NOT_JSON\n\n\
         event: control\n\
         data: {\"streamNextOffset\":\"c1\",\"upToDate\":false}\n\n\
         event: data\n\
         data: {\"kind\":\"stream-created\",\"metadata\":{\"streamPath\":\"slides/ok\"}}\n\n\
         event: control\n\
         data: {\"streamNextOffset\":\"c2\",\"upToDate\":true,\"streamClosed\":true}\n\n",
    )
}

/// A simple discovered stream that emits one event then closes.
async fn discovered_stream_handler() -> impl IntoResponse {
    sse_response(
        "event: data\n\
         data: {\"msg\":\"hello from discovered\"}\n\n\
         event: control\n\
         data: {\"streamNextOffset\":\"d1\",\"upToDate\":true,\"streamClosed\":true}\n\n",
    )
}

fn discovery_config(control_stream: &str) -> DiscoveryConfig {
    toml::from_str(&format!(
        r#"
        control_stream = "{control_stream}"
        filter_field = "kind"
        filter_value = "stream-created"
        path_field = "metadata.streamPath"
        path_prefix = "/v1/stream/"
        default_offset = "start"
        topic_template = "discovered.{{path}}"
        "#
    ))
    .unwrap()
}

#[tokio::test]
async fn discovery_spawns_forwarders_for_created_streams() {
    let app = Router::new()
        .route("/v1/stream/admin/activity", get(control_stream_handler))
        .route("/v1/stream/slides/a", get(discovered_stream_handler))
        .route("/v1/stream/doc/b", get(discovered_stream_handler));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let client = DurableStreamsClient::new(format!("http://{address}")).unwrap();
    let store_path = temp_offset_path("spawn");
    let offset_store = Arc::new(OffsetStore::open(&store_path).await.unwrap());
    let sink = Arc::new(FakeSink::default());
    let active_paths: ActivePaths = Arc::new(Mutex::new(HashSet::new()));
    let tasks: Arc<Mutex<JoinSet<Result<(), BridgeError>>>> = Arc::new(Mutex::new(JoinSet::new()));

    let config = discovery_config("/v1/stream/admin/activity");

    run_discovery(
        client,
        config,
        offset_store.clone(),
        sink.clone(),
        active_paths.clone(),
        tasks.clone(),
    )
    .await
    .unwrap();

    // Wait for spawned forwarder tasks to complete.
    let mut tasks_guard = tasks.lock().await;
    while let Some(result) = tasks_guard.join_next().await {
        result.unwrap().unwrap();
    }
    drop(tasks_guard);

    let sent = sink.sent.lock().await.clone();
    let topics: HashSet<_> = sent.iter().map(|r| r.topic.clone()).collect();
    let paths: HashSet<_> = sent.iter().map(|r| r.stream_path.clone()).collect();

    assert!(
        paths.contains("/v1/stream/slides/a"),
        "expected slides/a forwarder, got {paths:?}"
    );
    assert!(
        paths.contains("/v1/stream/doc/b"),
        "expected doc/b forwarder, got {paths:?}"
    );
    assert!(
        topics.contains("discovered..v1.stream.slides.a"),
        "expected topic for slides/a, got {topics:?}"
    );
    // document-deleted path should NOT appear
    assert!(
        !paths.contains("/v1/stream/slides/gone"),
        "document-deleted should have been filtered out"
    );

    // Control stream offset was persisted.
    assert!(
        offset_store
            .load("/v1/stream/admin/activity")
            .await
            .is_some()
    );

    let _ = tokio::fs::remove_file(store_path).await;
}

#[tokio::test]
async fn discovery_ignores_filtered_events() {
    let app = Router::new().route("/v1/stream/admin/activity", get(control_stream_handler));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let client = DurableStreamsClient::new(format!("http://{address}")).unwrap();
    let store_path = temp_offset_path("filter");
    let offset_store = Arc::new(OffsetStore::open(&store_path).await.unwrap());
    let sink = Arc::new(FakeSink::default());
    let active_paths: ActivePaths = Arc::new(Mutex::new(HashSet::new()));
    let tasks: Arc<Mutex<JoinSet<Result<(), BridgeError>>>> = Arc::new(Mutex::new(JoinSet::new()));

    // Use a filter_value that matches nothing.
    let config: DiscoveryConfig = toml::from_str(
        r#"
        control_stream = "/v1/stream/admin/activity"
        filter_field = "kind"
        filter_value = "no-such-kind"
        path_field = "metadata.streamPath"
        path_prefix = "/v1/stream/"
        "#,
    )
    .unwrap();

    run_discovery(
        client,
        config,
        offset_store,
        sink,
        active_paths.clone(),
        tasks.clone(),
    )
    .await
    .unwrap();

    let paths = active_paths.lock().await;
    assert!(paths.is_empty(), "no streams should have been discovered");

    let _ = tokio::fs::remove_file(store_path).await;
}

#[tokio::test]
async fn discovery_deduplicates_same_path() {
    let app = Router::new()
        .route(
            "/v1/stream/admin/activity",
            get(control_stream_dedupe_handler),
        )
        .route("/v1/stream/slides/dup", get(discovered_stream_handler));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let client = DurableStreamsClient::new(format!("http://{address}")).unwrap();
    let store_path = temp_offset_path("dedupe");
    let offset_store = Arc::new(OffsetStore::open(&store_path).await.unwrap());
    let sink = Arc::new(FakeSink::default());
    let active_paths: ActivePaths = Arc::new(Mutex::new(HashSet::new()));
    let tasks: Arc<Mutex<JoinSet<Result<(), BridgeError>>>> = Arc::new(Mutex::new(JoinSet::new()));

    let config = discovery_config("/v1/stream/admin/activity");

    run_discovery(
        client,
        config,
        offset_store,
        sink.clone(),
        active_paths,
        tasks.clone(),
    )
    .await
    .unwrap();

    // Wait for spawned tasks.
    let mut tasks_guard = tasks.lock().await;
    while let Some(result) = tasks_guard.join_next().await {
        result.unwrap().unwrap();
    }
    drop(tasks_guard);

    // Only one record should be sent despite two stream-created for same path.
    let sent = sink.sent.lock().await.clone();
    let dup_records: Vec<_> = sent
        .iter()
        .filter(|r| r.stream_path == "/v1/stream/slides/dup")
        .collect();
    assert_eq!(
        dup_records.len(),
        1,
        "expected exactly one record for deduplicated path, got {}",
        dup_records.len()
    );

    let _ = tokio::fs::remove_file(store_path).await;
}

#[tokio::test]
async fn discovery_skips_malformed_json_without_crashing() {
    let app = Router::new()
        .route(
            "/v1/stream/admin/activity",
            get(control_stream_malformed_handler),
        )
        .route("/v1/stream/slides/ok", get(discovered_stream_handler));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let client = DurableStreamsClient::new(format!("http://{address}")).unwrap();
    let store_path = temp_offset_path("malformed");
    let offset_store = Arc::new(OffsetStore::open(&store_path).await.unwrap());
    let sink = Arc::new(FakeSink::default());
    let active_paths: ActivePaths = Arc::new(Mutex::new(HashSet::new()));
    let tasks: Arc<Mutex<JoinSet<Result<(), BridgeError>>>> = Arc::new(Mutex::new(JoinSet::new()));

    let config = discovery_config("/v1/stream/admin/activity");

    // Should not panic or return error despite malformed first event.
    run_discovery(
        client,
        config,
        offset_store,
        sink.clone(),
        active_paths,
        tasks.clone(),
    )
    .await
    .unwrap();

    // Wait for spawned tasks.
    let mut tasks_guard = tasks.lock().await;
    while let Some(result) = tasks_guard.join_next().await {
        result.unwrap().unwrap();
    }
    drop(tasks_guard);

    // The valid event after the malformed one should still be processed.
    let sent = sink.sent.lock().await.clone();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].stream_path, "/v1/stream/slides/ok");

    let _ = tokio::fs::remove_file(store_path).await;
}

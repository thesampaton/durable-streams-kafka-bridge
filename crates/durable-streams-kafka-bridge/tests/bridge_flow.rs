use async_trait::async_trait;
use axum::Router;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use durable_streams_client::DurableStreamsClient;
use durable_streams_kafka_bridge::bridge::{StreamRuntime, run_stream};
use durable_streams_kafka_bridge::kafka::{BridgeRecord, RecordSink, SinkError};
use durable_streams_kafka_bridge::offset_store::OffsetStore;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

#[derive(Default)]
struct FakeSink {
    sent: Mutex<Vec<BridgeRecord>>,
}

#[async_trait]
impl RecordSink for FakeSink {
    async fn send(&self, record: BridgeRecord) -> Result<(), SinkError> {
        self.sent.lock().await.push(record);
        Ok(())
    }
}

#[derive(Clone)]
struct AppState {
    rounds: Arc<Mutex<usize>>,
}

#[tokio::test]
async fn bridge_forwards_and_persists_checkpoint_offsets() {
    let state = AppState {
        rounds: Arc::new(Mutex::new(0)),
    };
    let app = Router::new()
        .route("/v1/stream/orders", get(stream_handler))
        .with_state(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = DurableStreamsClient::new(format!("http://{address}")).unwrap();
    let store_path = std::env::temp_dir().join(format!(
        "durable-streams-kafka-bridge-test-offsets-{}.json",
        std::process::id()
    ));
    let _ = tokio::fs::remove_file(&store_path).await;
    let offset_store = Arc::new(OffsetStore::open(&store_path).await.unwrap());
    let sink = Arc::new(FakeSink::default());

    run_stream(
        client,
        StreamRuntime {
            path: "/v1/stream/orders".to_string(),
            topic: "orders".to_string(),
            start_offset: durable_streams_client::Offset::start(),
        },
        offset_store.clone(),
        sink.clone(),
    )
    .await
    .unwrap();

    let sent = sink.sent.lock().await.clone();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0].key, "/v1/stream/orders:o5");
    assert_eq!(sent[1].key, "/v1/stream/orders:o11");
    assert_eq!(
        offset_store.load("/v1/stream/orders").await,
        Some(durable_streams_client::Offset::from("o11"))
    );
}

#[tokio::test]
async fn bridge_combines_multiple_data_frames_before_checkpoint() {
    let app = Router::new().route("/v1/stream/orders", get(multi_frame_handler));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = DurableStreamsClient::new(format!("http://{address}")).unwrap();
    let store_path = std::env::temp_dir().join(format!(
        "durable-streams-kafka-bridge-test-multiframe-{}-{}.json",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let offset_store = Arc::new(OffsetStore::open(&store_path).await.unwrap());
    let sink = Arc::new(FakeSink::default());

    run_stream(
        client,
        StreamRuntime {
            path: "/v1/stream/orders".to_string(),
            topic: "orders".to_string(),
            start_offset: durable_streams_client::Offset::start(),
        },
        offset_store,
        sink.clone(),
    )
    .await
    .unwrap();

    let sent = sink.sent.lock().await.clone();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].payload, b"hello world");
    let _ = tokio::fs::remove_file(store_path).await;
}

async fn stream_handler(State(state): State<AppState>) -> impl IntoResponse {
    let mut rounds = state.rounds.lock().await;
    let response = if *rounds == 0 {
        "event: data\n\
         data: first\n\n\
         event: control\n\
         data: {\"streamNextOffset\":\"o5\",\"upToDate\":false}\n\n\
         event: data\n\
         data: second\n\n\
         event: control\n\
         data: {\"streamNextOffset\":\"o11\",\"upToDate\":true,\"streamClosed\":true}\n\n"
    } else {
        "event: control\n\
         data: {\"streamNextOffset\":\"o11\",\"upToDate\":true,\"streamClosed\":true}\n\n"
    };
    *rounds += 1;

    (
        [
            (axum::http::header::CONTENT_TYPE, "text/event-stream"),
            (axum::http::header::CACHE_CONTROL, "no-cache"),
        ],
        response,
    )
}

async fn multi_frame_handler() -> impl IntoResponse {
    (
        [
            (axum::http::header::CONTENT_TYPE, "text/event-stream"),
            (axum::http::header::CACHE_CONTROL, "no-cache"),
        ],
        "event: data\n\
         data: hello \n\n\
         event: data\n\
         data: world\n\n\
         event: control\n\
         data: {\"streamNextOffset\":\"o11\",\"upToDate\":true,\"streamClosed\":true}\n\n",
    )
}

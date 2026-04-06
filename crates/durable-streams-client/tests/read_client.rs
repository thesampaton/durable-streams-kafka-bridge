use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE, ETAG};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bytes::Bytes;
use durable_streams_client::{
    DurableStreamsClient, Error, ErrorKind, Offset, ReadMode, ReadRequest, StreamPath,
    SubscribeRequest, SubscriptionEvent,
};
use futures_util::StreamExt;
use serde::Deserialize;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify};

#[derive(Debug, Clone)]
struct AppState {
    stream: Arc<Mutex<StreamState>>,
    notify: Arc<Notify>,
    last_cursor: Arc<Mutex<Option<String>>>,
    head_count: Arc<Mutex<usize>>,
    retry_after_fails: Arc<Mutex<usize>>,
}

#[derive(Debug)]
struct StreamState {
    content_type: &'static str,
    body: Bytes,
    open_cursor: String,
    fail_gets_remaining: usize,
    sse_round: usize,
}

#[derive(Debug, Deserialize)]
struct ReadQuery {
    offset: Option<String>,
    live: Option<String>,
    cursor: Option<String>,
}

#[tokio::test]
async fn metadata_and_catch_up_read_expose_typed_protocol_state() {
    let server = TestServer::spawn(b"hello world".to_vec(), 0).await;
    let client = DurableStreamsClient::new(server.base_url()).unwrap();
    let stream = client.stream("/v1/stream/orders").unwrap();

    let metadata = stream.metadata().await.unwrap();
    assert_eq!(metadata.content_type.as_deref(), Some("text/plain"));
    assert_eq!(metadata.next_offset, Offset::from("o11"));
    assert!(!metadata.stream_closed);

    let response = stream.read(ReadRequest::default()).await.unwrap();
    assert_eq!(response.bytes, Bytes::from_static(b"hello world"));
    assert_eq!(response.next_offset, Offset::from("o11"));
    assert!(response.up_to_date);
    assert_eq!(response.etag.as_deref(), Some("\"-1:o11\""));
}

#[tokio::test]
async fn long_poll_forwards_cursor_and_resumes_from_offset() {
    let server = TestServer::spawn(b"before".to_vec(), 0).await;
    server.append(b"after").await;

    let client = DurableStreamsClient::new(server.base_url()).unwrap();
    let stream = client.stream("/v1/stream/orders").unwrap();

    let response = stream
        .read(ReadRequest {
            offset: Offset::from("o6"),
            mode: ReadMode::LongPoll,
            cursor: Some("cursor-1".to_string()),
        })
        .await
        .unwrap();

    assert_eq!(response.bytes, Bytes::from_static(b"after"));
    assert_eq!(response.cursor.as_deref(), Some("cursor-2"));
    assert_eq!(server.last_cursor().await.as_deref(), Some("cursor-1"));
}

#[tokio::test]
async fn retries_transient_get_failures() {
    let server = TestServer::spawn(b"retry-me".to_vec(), 2).await;
    let client = DurableStreamsClient::new(server.base_url()).unwrap();
    let stream = client.stream("/v1/stream/orders").unwrap();

    let response = stream.read(ReadRequest::default()).await.unwrap();
    assert_eq!(response.bytes, Bytes::from_static(b"retry-me"));
}

#[tokio::test]
async fn subscribe_reconnects_until_stream_closes() {
    let server = TestServer::spawn(b"".to_vec(), 0).await;
    let client = DurableStreamsClient::new(server.base_url()).unwrap();
    let stream = client.stream("/v1/stream/orders").unwrap();
    let mut subscription = stream.subscribe(SubscribeRequest::default());

    let first = subscription.next().await.unwrap().unwrap();
    let second = subscription.next().await.unwrap().unwrap();
    let third = subscription.next().await.unwrap().unwrap();
    let fourth = subscription.next().await.unwrap().unwrap();
    let done = subscription.next().await;

    assert_eq!(first, SubscriptionEvent::Data(Bytes::from_static(b"first")));
    assert_eq!(
        second,
        SubscriptionEvent::Checkpoint(durable_streams_client::StreamCheckpoint {
            next_offset: Offset::from("o5"),
            cursor: Some("cursor-a".to_string()),
            up_to_date: false,
            stream_closed: false,
        })
    );
    assert_eq!(
        third,
        SubscriptionEvent::Data(Bytes::from_static(b"second"))
    );
    assert_eq!(
        fourth,
        SubscriptionEvent::Checkpoint(durable_streams_client::StreamCheckpoint {
            next_offset: Offset::from("o11"),
            cursor: None,
            up_to_date: true,
            stream_closed: true,
        })
    );
    assert!(done.is_none());
}

#[tokio::test]
async fn classifies_stream_closed_conflicts() {
    let server = TestServer::spawn_closed_conflict().await;
    let client = DurableStreamsClient::new(server.base_url()).unwrap();
    let stream = client.stream("/v1/stream/orders").unwrap();

    let error = stream.read(ReadRequest::default()).await.unwrap_err();
    match error {
        Error::Http(http_error) => {
            assert_eq!(http_error.kind, ErrorKind::StreamClosed);
            assert!(http_error.stream_closed);
            assert_eq!(http_error.next_offset.as_deref(), Some("o11"));
        }
        other => panic!("expected HTTP error, got {other:?}"),
    }
}

#[tokio::test]
async fn invalid_stream_paths_are_rejected_without_panic() {
    let client = DurableStreamsClient::new("http://localhost:4437").unwrap();
    let error = client.stream("v1/stream/orders").unwrap_err();
    match error {
        Error::InvalidStreamPath { .. } => {}
        other => panic!("expected invalid path error, got {other:?}"),
    }

    let error = StreamPath::try_from("orders").unwrap_err();
    match error {
        Error::InvalidStreamPath { .. } => {}
        other => panic!("expected invalid path error, got {other:?}"),
    }
}

#[tokio::test]
async fn head_does_not_retry_non_success_responses() {
    let server = TestServer::spawn_head_not_found().await;
    let client = DurableStreamsClient::new(server.base_url()).unwrap();
    let stream = client.stream("/v1/stream/orders").unwrap();

    let error = stream.metadata().await.unwrap_err();
    match error {
        Error::Http(http_error) => assert_eq!(http_error.kind, ErrorKind::NotFound),
        other => panic!("expected HTTP error, got {other:?}"),
    }
    assert_eq!(server.head_count().await, 1);
}

#[tokio::test]
async fn retry_after_header_is_respected() {
    let server = TestServer::spawn_retry_after().await;
    let client = DurableStreamsClient::new(server.base_url()).unwrap();
    let stream = client.stream("/v1/stream/orders").unwrap();

    let started = tokio::time::Instant::now();
    let response = stream.read(ReadRequest::default()).await.unwrap();
    let elapsed = started.elapsed();

    assert_eq!(response.bytes, Bytes::from_static(b"after-retry-after"));
    assert!(elapsed >= std::time::Duration::from_secs(1));
}

#[tokio::test]
async fn connection_failures_surface_as_transport_errors() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);

    let client = DurableStreamsClient::new(format!("http://{address}")).unwrap();
    let stream = client.stream("/v1/stream/orders").unwrap();
    let error = stream.read(ReadRequest::default()).await.unwrap_err();

    match error {
        Error::Transport { .. } => {}
        other => panic!("expected transport error, got {other:?}"),
    }
}

struct TestServer {
    base_url: String,
    state: AppState,
}

impl TestServer {
    async fn spawn(initial_body: Vec<u8>, fail_gets_remaining: usize) -> Self {
        let state = AppState {
            stream: Arc::new(Mutex::new(StreamState {
                content_type: "text/plain",
                body: Bytes::from(initial_body),
                open_cursor: "cursor-2".to_string(),
                fail_gets_remaining,
                sse_round: 0,
            })),
            notify: Arc::new(Notify::new()),
            last_cursor: Arc::new(Mutex::new(None)),
            head_count: Arc::new(Mutex::new(0)),
            retry_after_fails: Arc::new(Mutex::new(0)),
        };

        let app = Router::new()
            .route("/v1/stream/{*path}", get(get_stream).head(head_stream))
            .with_state(state.clone());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        Self {
            base_url: format!("http://{address}"),
            state,
        }
    }

    async fn spawn_closed_conflict() -> Self {
        let state = AppState {
            stream: Arc::new(Mutex::new(StreamState {
                content_type: "text/plain",
                body: Bytes::from_static(b"hello world"),
                open_cursor: "cursor-2".to_string(),
                fail_gets_remaining: 0,
                sse_round: usize::MAX,
            })),
            notify: Arc::new(Notify::new()),
            last_cursor: Arc::new(Mutex::new(None)),
            head_count: Arc::new(Mutex::new(0)),
            retry_after_fails: Arc::new(Mutex::new(0)),
        };

        let app = Router::new()
            .route("/v1/stream/{*path}", get(closed_conflict_handler))
            .with_state(state.clone());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        Self {
            base_url: format!("http://{address}"),
            state,
        }
    }

    async fn spawn_head_not_found() -> Self {
        let state = AppState {
            stream: Arc::new(Mutex::new(StreamState {
                content_type: "text/plain",
                body: Bytes::new(),
                open_cursor: "cursor-2".to_string(),
                fail_gets_remaining: 0,
                sse_round: 0,
            })),
            notify: Arc::new(Notify::new()),
            last_cursor: Arc::new(Mutex::new(None)),
            head_count: Arc::new(Mutex::new(0)),
            retry_after_fails: Arc::new(Mutex::new(0)),
        };

        let app = Router::new()
            .route("/v1/stream/{*path}", get(get_stream).head(head_not_found))
            .with_state(state.clone());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        Self {
            base_url: format!("http://{address}"),
            state,
        }
    }

    async fn spawn_retry_after() -> Self {
        let state = AppState {
            stream: Arc::new(Mutex::new(StreamState {
                content_type: "text/plain",
                body: Bytes::from_static(b"after-retry-after"),
                open_cursor: "cursor-2".to_string(),
                fail_gets_remaining: 0,
                sse_round: 0,
            })),
            notify: Arc::new(Notify::new()),
            last_cursor: Arc::new(Mutex::new(None)),
            head_count: Arc::new(Mutex::new(0)),
            retry_after_fails: Arc::new(Mutex::new(1)),
        };

        let app = Router::new()
            .route("/v1/stream/{*path}", get(get_retry_after).head(head_stream))
            .with_state(state.clone());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        Self {
            base_url: format!("http://{address}"),
            state,
        }
    }

    fn base_url(&self) -> &str {
        &self.base_url
    }

    async fn append(&self, bytes: &[u8]) {
        let mut state = self.state.stream.lock().await;
        let mut combined = state.body.to_vec();
        combined.extend_from_slice(bytes);
        state.body = Bytes::from(combined);
        drop(state);
        self.state.notify.notify_waiters();
    }

    async fn last_cursor(&self) -> Option<String> {
        self.state.last_cursor.lock().await.clone()
    }

    async fn head_count(&self) -> usize {
        *self.state.head_count.lock().await
    }
}

async fn closed_conflict_handler() -> Response {
    let mut headers = HeaderMap::new();
    headers.insert("stream-closed", HeaderValue::from_static("true"));
    headers.insert("stream-next-offset", HeaderValue::from_static("o11"));
    (StatusCode::CONFLICT, headers, "").into_response()
}

async fn head_stream(State(state): State<AppState>, Path(_path): Path<String>) -> Response {
    let stream = state.stream.lock().await;
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(stream.content_type));
    headers.insert(
        "stream-next-offset",
        header_value(offset_for(stream.body.len())),
    );
    headers.insert(
        ETAG,
        header_value(format!("\"-1:{}\"", offset_for(stream.body.len()))),
    );
    (StatusCode::OK, headers).into_response()
}

async fn head_not_found(State(state): State<AppState>) -> Response {
    let mut count = state.head_count.lock().await;
    *count += 1;
    StatusCode::NOT_FOUND.into_response()
}

async fn get_stream(
    State(state): State<AppState>,
    Path(_path): Path<String>,
    Query(query): Query<ReadQuery>,
) -> Response {
    {
        let mut stream = state.stream.lock().await;
        if stream.fail_gets_remaining > 0 {
            stream.fail_gets_remaining -= 1;
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
    }

    match query.live.as_deref() {
        Some("sse") => sse_response(state).await.into_response(),
        Some("long-poll") => long_poll_response(state, query).await,
        _ => catch_up_response(state, query).await,
    }
}

async fn get_retry_after(State(state): State<AppState>, Path(_path): Path<String>) -> Response {
    let mut fails = state.retry_after_fails.lock().await;
    if *fails > 0 {
        *fails -= 1;
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", HeaderValue::from_static("1"));
        return (StatusCode::TOO_MANY_REQUESTS, headers, "retry later").into_response();
    }

    let stream = state.stream.lock().await;
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(stream.content_type));
    headers.insert(
        "stream-next-offset",
        header_value(offset_for(stream.body.len())),
    );
    headers.insert("stream-up-to-date", HeaderValue::from_static("true"));
    (StatusCode::OK, headers, stream.body.clone()).into_response()
}

async fn catch_up_response(state: AppState, query: ReadQuery) -> Response {
    let stream = state.stream.lock().await;
    let body = stream.body.clone();
    let offset = query.offset.unwrap_or_else(|| "-1".to_string());
    let next_offset = offset_for(body.len());
    let bytes = bytes_from_offset(&body, &offset);

    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(stream.content_type));
    headers.insert("stream-next-offset", header_value(next_offset.clone()));
    headers.insert("stream-up-to-date", HeaderValue::from_static("true"));
    headers.insert(ETAG, header_value(format!("\"-1:{next_offset}\"")));
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));

    (StatusCode::OK, headers, bytes).into_response()
}

async fn long_poll_response(state: AppState, query: ReadQuery) -> Response {
    if let Some(cursor) = query.cursor {
        *state.last_cursor.lock().await = Some(cursor);
    }

    let offset = query.offset.unwrap_or_else(|| "-1".to_string());
    let initial = {
        let stream = state.stream.lock().await;
        let bytes = bytes_from_offset(&stream.body, &offset);
        if !bytes.is_empty() {
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, HeaderValue::from_static(stream.content_type));
            headers.insert(
                "stream-next-offset",
                header_value(offset_for(stream.body.len())),
            );
            headers.insert("stream-up-to-date", HeaderValue::from_static("true"));
            headers.insert("stream-cursor", header_value(stream.open_cursor.clone()));
            return (StatusCode::OK, headers, bytes).into_response();
        }
        stream.body.len()
    };

    state.notify.notified().await;

    let stream = state.stream.lock().await;
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(stream.content_type));
    headers.insert(
        "stream-next-offset",
        header_value(offset_for(stream.body.len())),
    );
    headers.insert("stream-up-to-date", HeaderValue::from_static("true"));
    headers.insert("stream-cursor", header_value(stream.open_cursor.clone()));
    let bytes = stream.body.slice(initial..);
    (StatusCode::OK, headers, bytes).into_response()
}

async fn sse_response(
    state: AppState,
) -> Sse<impl futures_core::Stream<Item = Result<Event, Infallible>>> {
    let round = {
        let mut stream = state.stream.lock().await;
        let current = stream.sse_round;
        stream.sse_round += 1;
        current
    };

    let stream = async_stream::stream! {
        if round == 0 {
            yield Ok(Event::default().event("data").data("first"));
            yield Ok(Event::default().event("control").data("{\"streamNextOffset\":\"o5\",\"streamCursor\":\"cursor-a\"}"));
        } else {
            yield Ok(Event::default().event("data").data("second"));
            yield Ok(Event::default().event("control").data("{\"streamNextOffset\":\"o11\",\"streamClosed\":true}"));
        }
    };

    Sse::new(stream)
}

fn bytes_from_offset(body: &Bytes, offset: &str) -> Bytes {
    match offset {
        "-1" => body.clone(),
        "o6" => body.slice(6..),
        _ => Bytes::new(),
    }
}

fn offset_for(len: usize) -> String {
    format!("o{len}")
}

fn header_value(value: impl AsRef<str>) -> HeaderValue {
    HeaderValue::from_str(value.as_ref()).unwrap()
}

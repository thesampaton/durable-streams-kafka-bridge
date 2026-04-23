use crate::error::Result;
use crate::protocol::headers;
use crate::protocol::sse::SseParser;
use crate::transport::HttpTransport;
use crate::types::{
    ReadMode, ReadRequest, ReadResponse, StreamCheckpoint, StreamMetadata, StreamPath,
    SubscribeRequest, SubscriptionEvent,
};
use futures_core::Stream;
use futures_util::StreamExt;
use reqwest::Method;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DurableStream {
    transport: Arc<HttpTransport>,
    path: StreamPath,
}

impl DurableStream {
    pub(crate) fn new(transport: Arc<HttpTransport>, path: StreamPath) -> Self {
        Self { transport, path }
    }

    #[must_use]
    pub fn path(&self) -> &StreamPath {
        &self.path
    }

    /// Fetch stream metadata without transferring stream bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, when the server returns a
    /// non-success response, or when required protocol headers are missing.
    pub async fn metadata(&self) -> Result<StreamMetadata> {
        let path = self.path.as_str().to_string();
        let response = self
            .transport
            .execute_with_retry("HEAD", &path, Method::HEAD, || {
                self.transport.bounded_request(Method::HEAD, &path)
            })
            .await?;
        let headers = response.headers();
        Ok(StreamMetadata {
            content_type: headers::optional_string(headers, headers::CONTENT_TYPE),
            next_offset: headers::require_offset(headers, "HEAD", &path)?,
            stream_closed: headers::has_true_header(headers, headers::STREAM_CLOSED),
            etag: headers::optional_string(headers, headers::ETAG),
        })
    }

    /// Read stream bytes from a specific offset using catch-up or long-poll mode.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, when the server returns a
    /// non-success response, or when required protocol headers are missing.
    pub async fn read(&self, request: ReadRequest) -> Result<ReadResponse> {
        let path = self.path.as_str().to_string();
        let response = self
            .transport
            .execute_with_retry("GET", &path, Method::GET, || {
                let mut builder = self.transport.bounded_request(Method::GET, &path)?;
                builder = builder.query(&[("offset", request.offset.as_str())]);
                if request.mode == ReadMode::LongPoll {
                    builder = builder.query(&[("live", "long-poll")]);
                }
                if let Some(cursor) = request.cursor.as_deref() {
                    builder = builder.query(&[("cursor", cursor)]);
                }
                Ok(builder)
            })
            .await?;

        let headers = response.headers().clone();
        let bytes = response
            .bytes()
            .await
            .map_err(|source| crate::error::Error::Transport {
                operation: "GET",
                path: path.clone(),
                source,
            })?;
        Ok(ReadResponse {
            bytes,
            content_type: headers::optional_string(&headers, headers::CONTENT_TYPE),
            next_offset: headers::require_offset(&headers, "GET", &path)?,
            up_to_date: headers::has_true_header(&headers, headers::STREAM_UP_TO_DATE),
            stream_closed: headers::has_true_header(&headers, headers::STREAM_CLOSED),
            cursor: headers::optional_string(&headers, headers::STREAM_CURSOR),
            etag: headers::optional_string(&headers, headers::ETAG),
        })
    }

    #[must_use]
    pub fn subscribe(
        &self,
        request: SubscribeRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<SubscriptionEvent>> + Send>> {
        let stream = self.clone();
        Box::pin(async_stream::try_stream! {
            let mut offset = request.offset;
            let path = stream.path.as_str().to_string();
            loop {
                let response = stream
                    .transport
                    .execute_with_retry("GET", &path, Method::GET, || {
                        let builder = stream.transport.request(Method::GET, &path)?;
                        Ok(builder.query(&[("offset", offset.as_str()), ("live", "sse")]))
                    })
                    .await?;

                let base64_data = headers::optional_string(response.headers(), headers::STREAM_SSE_DATA_ENCODING)
                    .is_some_and(|value| value.eq_ignore_ascii_case("base64"));
                let mut parser = SseParser::new();
                let mut body = response.bytes_stream();
                let mut should_stop = false;

                while let Some(chunk) = body.next().await {
                    let chunk = chunk.map_err(|source| crate::error::Error::Transport {
                        operation: "GET",
                        path: path.clone(),
                        source,
                    })?;

                    for event in parser.push(&chunk, &path, base64_data)? {
                        if let SubscriptionEvent::Checkpoint(StreamCheckpoint {
                            next_offset,
                            stream_closed,
                            ..
                        }) = &event
                        {
                            offset = next_offset.clone();
                            should_stop = *stream_closed;
                        }

                        yield event;
                    }
                }

                for event in parser.finish(&path, base64_data)? {
                    if let SubscriptionEvent::Checkpoint(StreamCheckpoint {
                        next_offset,
                        stream_closed,
                        ..
                    }) = &event
                    {
                        offset = next_offset.clone();
                        should_stop = *stream_closed;
                    }
                    yield event;
                }

                if should_stop {
                    break;
                }
            }
        })
    }
}

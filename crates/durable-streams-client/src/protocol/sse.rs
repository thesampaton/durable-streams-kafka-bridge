use crate::error::{Error, Result};
use crate::types::{Offset, StreamCheckpoint, SubscriptionEvent};
use base64::Engine;
use bytes::{Bytes, BytesMut};
use serde::Deserialize;

#[derive(Debug, Default)]
pub struct SseParser {
    buffer: BytesMut,
    current_event_type: Option<String>,
    current_data_lines: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ControlPayload {
    stream_next_offset: String,
    #[serde(default)]
    stream_cursor: Option<String>,
    #[serde(default)]
    up_to_date: Option<bool>,
    #[serde(default)]
    stream_closed: Option<bool>,
}

impl SseParser {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(
        &mut self,
        chunk: &Bytes,
        path: &str,
        base64_data: bool,
    ) -> Result<Vec<SubscriptionEvent>> {
        self.buffer.extend_from_slice(chunk);
        let mut events = Vec::new();

        while let Some((line, ending_len)) = next_line(&self.buffer) {
            let line_bytes = self.buffer.split_to(line + ending_len).freeze();
            let line_bytes = &line_bytes[..line];
            let line = std::str::from_utf8(line_bytes).map_err(|_| Error::InvalidSse {
                path: path.to_string(),
                reason: "event stream was not valid UTF-8".to_string(),
            })?;

            if line.is_empty() {
                if let Some(event) = self.finish_event(path, base64_data)? {
                    events.push(event);
                }
                continue;
            }

            if line.starts_with(':') {
                continue;
            }

            let (field, value) = match line.split_once(':') {
                Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
                None => (line, ""),
            };

            match field {
                "event" => self.current_event_type = Some(value.to_string()),
                "data" => self.current_data_lines.push(value.to_string()),
                _ => {}
            }
        }

        Ok(events)
    }

    pub fn finish(&mut self, path: &str, base64_data: bool) -> Result<Vec<SubscriptionEvent>> {
        let mut events = Vec::new();
        if let Some(event) = self.finish_event(path, base64_data)? {
            events.push(event);
        }
        Ok(events)
    }

    fn finish_event(&mut self, path: &str, base64_data: bool) -> Result<Option<SubscriptionEvent>> {
        let event_type = self.current_event_type.take().unwrap_or_default();
        let data = self.current_data_lines.join("\n");
        self.current_data_lines.clear();

        if event_type.is_empty() && data.is_empty() {
            return Ok(None);
        }

        match event_type.as_str() {
            "data" => {
                if base64_data {
                    let compact = data.replace(['\n', '\r'], "");
                    let decoded = base64::engine::general_purpose::STANDARD
                        .decode(compact)
                        .map_err(|source| Error::InvalidSse {
                            path: path.to_string(),
                            reason: format!("invalid base64 SSE data: {source}"),
                        })?;
                    Ok(Some(SubscriptionEvent::Data(Bytes::from(decoded))))
                } else {
                    Ok(Some(SubscriptionEvent::Data(Bytes::from(data))))
                }
            }
            "control" => {
                let payload: ControlPayload =
                    serde_json::from_str(&data).map_err(|source| Error::InvalidSse {
                        path: path.to_string(),
                        reason: format!("invalid control event JSON: {source}"),
                    })?;
                Ok(Some(SubscriptionEvent::Checkpoint(StreamCheckpoint {
                    next_offset: Offset::from(payload.stream_next_offset),
                    cursor: payload.stream_cursor,
                    up_to_date: payload
                        .up_to_date
                        .unwrap_or(payload.stream_closed.unwrap_or(false)),
                    stream_closed: payload.stream_closed.unwrap_or(false),
                })))
            }
            _ => Ok(None),
        }
    }
}

fn next_line(buffer: &BytesMut) -> Option<(usize, usize)> {
    let bytes = buffer.as_ref();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\n' => return Some((index, 1)),
            b'\r' => {
                if bytes.get(index + 1) == Some(&b'\n') {
                    return Some((index, 2));
                }
                return Some((index, 1));
            }
            _ => index += 1,
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::SseParser;
    use crate::types::{Offset, StreamCheckpoint, SubscriptionEvent};
    use bytes::Bytes;

    #[test]
    fn parses_text_data_and_control_events() {
        let mut parser = SseParser::new();
        let payload = Bytes::from_static(
            b"event: data\r\ndata: hello\r\n\r\nevent: control\r\ndata: {\"streamNextOffset\":\"o1\",\"upToDate\":true}\r\n\r\n",
        );
        let events = parser.push(&payload, "/stream", false).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0], SubscriptionEvent::Data(Bytes::from("hello")));
        assert_eq!(
            events[1],
            SubscriptionEvent::Checkpoint(StreamCheckpoint {
                next_offset: Offset::from("o1"),
                cursor: None,
                up_to_date: true,
                stream_closed: false,
            })
        );
    }

    #[test]
    fn strips_inserted_newlines_before_base64_decode() {
        let mut parser = SseParser::new();
        let payload = Bytes::from_static(b"event: data\ndata: AQID\ndata: BA==\n\n");
        let events = parser.push(&payload, "/stream", true).unwrap();
        assert_eq!(
            events,
            vec![SubscriptionEvent::Data(Bytes::from(vec![1, 2, 3, 4]))]
        );
    }
}

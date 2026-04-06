use durable_streams_client::Offset;

#[must_use]
pub fn resolve_topic(stream_path: &str, explicit_topic: Option<&str>) -> String {
    explicit_topic
        .filter(|topic| !topic.trim().is_empty())
        .map_or_else(|| default_topic_for_stream(stream_path), ToOwned::to_owned)
}

#[must_use]
pub fn default_topic_for_stream(stream_path: &str) -> String {
    let mut topic = String::from("durable-streams");
    let trimmed = stream_path.trim_matches('/');
    if !trimmed.is_empty() {
        topic.push('.');
    }
    for ch in trimmed.chars() {
        let mapped = match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '_' | '-' => ch,
            '/' => '.',
            _ => '-',
        };
        topic.push(mapped);
    }
    if topic == "durable-streams" {
        topic.push_str(".stream");
    }
    topic
}

#[must_use]
pub fn message_key(stream_path: &str, next_offset: &Offset) -> String {
    format!("{stream_path}:{}", next_offset.as_str())
}

#[cfg(test)]
mod tests {
    use super::{default_topic_for_stream, message_key, resolve_topic};
    use durable_streams_client::Offset;

    #[test]
    fn uses_explicit_topic_when_provided() {
        assert_eq!(
            resolve_topic("/v1/stream/orders", Some("orders")),
            "orders".to_string()
        );
    }

    #[test]
    fn derives_topic_from_stream_path() {
        assert_eq!(
            default_topic_for_stream("/v1/stream/orders"),
            "durable-streams.v1.stream.orders".to_string()
        );
    }

    #[test]
    fn sanitizes_invalid_topic_characters() {
        assert_eq!(
            default_topic_for_stream("/orders/tenant:alpha"),
            "durable-streams.orders.tenant-alpha".to_string()
        );
    }

    #[test]
    fn keys_records_with_stream_and_offset() {
        assert_eq!(
            message_key("/v1/stream/orders", &Offset::from("o42")),
            "/v1/stream/orders:o42".to_string()
        );
    }
}

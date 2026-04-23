use crate::config::{TargetConfig, TopicMappingConfig};
use durable_streams_client::Offset;
use std::collections::BTreeMap;

pub struct TopicResolutionContext<'a> {
    pub explicit_topic: Option<&'a str>,
    pub target: Option<&'a str>,
    pub targets: &'a BTreeMap<String, TargetConfig>,
    pub topic_mapping: &'a TopicMappingConfig,
}

#[must_use]
pub fn resolve_topic(stream_path: &str, context: &TopicResolutionContext<'_>) -> String {
    if let Some(topic) = context
        .explicit_topic
        .filter(|topic| !topic.trim().is_empty())
    {
        return topic.to_string();
    }
    if let Some(topic) = resolve_target_topic(context) {
        return topic;
    }
    if let Some(topic) = context
        .topic_mapping
        .default_topic
        .as_deref()
        .filter(|topic| !topic.trim().is_empty())
    {
        return topic.to_string();
    }
    default_topic_for_stream(stream_path)
}

fn resolve_target_topic(context: &TopicResolutionContext<'_>) -> Option<String> {
    context
        .target
        .and_then(|name| context.targets.get(name))
        .map(|target| target.topic.clone())
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
    use super::{TopicResolutionContext, default_topic_for_stream, message_key, resolve_topic};
    use crate::config::{TargetConfig, TopicMappingConfig};
    use durable_streams_client::Offset;
    use std::collections::BTreeMap;

    #[test]
    fn uses_explicit_topic_when_provided() {
        assert_eq!(
            resolve_topic(
                "/v1/stream/orders",
                &TopicResolutionContext {
                    explicit_topic: Some("orders"),
                    target: None,
                    targets: &BTreeMap::new(),
                    topic_mapping: &TopicMappingConfig::default(),
                }
            ),
            "orders".to_string()
        );
    }

    #[test]
    fn explicit_topic_wins_over_target() {
        let targets = BTreeMap::from([(
            "enterprise_documents".to_string(),
            TargetConfig {
                topic: "enterprise.documents".to_string(),
            },
        )]);

        assert_eq!(
            resolve_topic(
                "/v1/stream/orders",
                &TopicResolutionContext {
                    explicit_topic: Some("orders"),
                    target: Some("enterprise_documents"),
                    targets: &targets,
                    topic_mapping: &TopicMappingConfig::default(),
                }
            ),
            "orders".to_string()
        );
    }

    #[test]
    fn uses_target_topic_when_present() {
        let targets = BTreeMap::from([(
            "enterprise_documents".to_string(),
            TargetConfig {
                topic: "enterprise.documents".to_string(),
            },
        )]);

        assert_eq!(
            resolve_topic(
                "/v1/stream/orders",
                &TopicResolutionContext {
                    explicit_topic: None,
                    target: Some("enterprise_documents"),
                    targets: &targets,
                    topic_mapping: &TopicMappingConfig::default(),
                }
            ),
            "enterprise.documents".to_string()
        );
    }

    #[test]
    fn uses_bridge_level_default_topic_when_present() {
        assert_eq!(
            resolve_topic(
                "/v1/stream/orders",
                &TopicResolutionContext {
                    explicit_topic: None,
                    target: None,
                    targets: &BTreeMap::new(),
                    topic_mapping: &TopicMappingConfig {
                        default_topic: Some("enterprise.documents".to_string()),
                    },
                }
            ),
            "enterprise.documents".to_string()
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

use crate::bridge::{BridgeError, StreamRuntime, run_stream};
use crate::config::DiscoveryConfig;
use crate::kafka::RecordSink;
use crate::offset_store::OffsetStore;
use crate::topic::default_topic_for_stream;
use durable_streams_client::{
    DurableStreamsClient, StreamPath, SubscribeRequest, SubscriptionEvent,
};
use futures_util::StreamExt;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;

pub type ActivePaths = Arc<Mutex<HashSet<String>>>;
pub type SpawnRequest = Pin<Box<dyn Future<Output = Result<(), BridgeError>> + Send>>;
pub type Spawner = tokio::sync::mpsc::UnboundedSender<SpawnRequest>;

/// Run a discovery loop that watches a control stream and dynamically spawns
/// forwarders for newly discovered stream paths.
///
/// # Errors
///
/// Returns an error only when the control stream subscription itself fails
/// fatally. Errors parsing individual events are logged and skipped.
pub async fn run_discovery<S>(
    client: DurableStreamsClient,
    config: DiscoveryConfig,
    offset_store: Arc<OffsetStore>,
    sink: Arc<S>,
    active_paths: ActivePaths,
    spawner: Spawner,
) -> Result<(), BridgeError>
where
    S: RecordSink + 'static,
{
    let durable_stream = client.stream(&config.control_stream)?;
    let offset = offset_store
        .load(&config.control_stream)
        .await
        .unwrap_or_else(|| config.start_offset());
    let mut subscription = durable_stream.subscribe(SubscribeRequest { offset });
    let mut pending_payload = Vec::new();

    while let Some(event) = subscription.next().await {
        match event? {
            SubscriptionEvent::Data(bytes) => {
                pending_payload.extend_from_slice(&bytes);
            }
            SubscriptionEvent::Checkpoint(checkpoint) => {
                let payload = std::mem::take(&mut pending_payload);
                if !payload.is_empty() {
                    process_discovery_event(
                        &payload,
                        &config,
                        &client,
                        &offset_store,
                        &sink,
                        &active_paths,
                        &spawner,
                    )
                    .await;
                }
                // Persist the control stream's own offset after processing.
                offset_store
                    .save(&config.control_stream, &checkpoint.next_offset)
                    .await?;
            }
        }
    }

    Ok(())
}

async fn process_discovery_event<S>(
    payload: &[u8],
    config: &DiscoveryConfig,
    client: &DurableStreamsClient,
    offset_store: &Arc<OffsetStore>,
    sink: &Arc<S>,
    active_paths: &ActivePaths,
    spawner: &Spawner,
) where
    S: RecordSink + 'static,
{
    let root: serde_json::Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(err) => {
            eprintln!("discovery: failed to parse JSON from control stream: {err}");
            return;
        }
    };

    let events: Vec<&serde_json::Value> = match &root {
        serde_json::Value::Array(items) => items.iter().collect(),
        other => vec![other],
    };

    for event in events {
        process_single_event(
            event,
            config,
            client,
            offset_store,
            sink,
            active_paths,
            spawner,
        )
        .await;
    }
}

async fn process_single_event<S>(
    value: &serde_json::Value,
    config: &DiscoveryConfig,
    client: &DurableStreamsClient,
    offset_store: &Arc<OffsetStore>,
    sink: &Arc<S>,
    active_paths: &ActivePaths,
    spawner: &Spawner,
) where
    S: RecordSink + 'static,
{
    // Apply filter if configured.
    if let (Some(field), Some(expected)) = (&config.filter_field, &config.filter_value) {
        match get_dotted(value, field) {
            Some(serde_json::Value::String(actual)) if actual == expected => {}
            _ => return,
        }
    }

    // Extract the stream path.
    let extracted =
        if let Some(serde_json::Value::String(s)) = get_dotted(value, &config.path_field) {
            s.clone()
        } else {
            eprintln!(
                "discovery: missing or non-string path_field `{}` in event",
                config.path_field
            );
            return;
        };

    let resolved = resolve_discovered_path(&extracted, config.path_prefix.as_deref());

    // Validate the resolved path.
    if let Err(err) = StreamPath::new(resolved.clone()) {
        eprintln!("discovery: invalid resolved stream path `{resolved}`: {err}");
        return;
    }

    // Dedupe: only spawn if this path is new.
    {
        let mut paths = active_paths.lock().await;
        if !paths.insert(resolved.clone()) {
            return;
        }
    }

    let topic = topic_for_discovered_stream(&resolved, config.topic_template.as_deref());
    let start_offset = config.start_offset();
    let runtime = StreamRuntime {
        path: resolved.clone(),
        topic: topic.clone(),
        start_offset,
    };

    eprintln!("discovery: spawning forwarder {resolved} -> {topic}");
    let client = client.clone();
    let offset_store = offset_store.clone();
    let sink = sink.clone();
    let _ = spawner.send(Box::pin(run_stream(client, runtime, offset_store, sink)));
}

#[must_use]
pub fn resolve_discovered_path(extracted: &str, prefix: Option<&str>) -> String {
    match prefix {
        Some(p) => format!("{p}{extracted}"),
        None => extracted.to_string(),
    }
}

#[must_use]
pub fn topic_for_discovered_stream(path: &str, template: Option<&str>) -> String {
    match template {
        Some(tmpl) => sanitize_topic(&tmpl.replace("{path}", path)),
        None => default_topic_for_stream(path),
    }
}

fn sanitize_topic(raw: &str) -> String {
    let trimmed = raw.trim_matches('/');
    let mut topic = String::with_capacity(trimmed.len());
    for ch in trimmed.chars() {
        let mapped = match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '_' | '-' => ch,
            '/' => '.',
            _ => '-',
        };
        topic.push(mapped);
    }
    topic
}

fn get_dotted<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    if path.is_empty() {
        return Some(value);
    }
    path.split('.').try_fold(value, |acc, seg| acc.get(seg))
}

/// Extract resolved stream paths from a raw payload, applying the same JSON
/// parsing, array fan-out, filter, and path resolution as discovery. Useful for
/// testing without a full client/sink/spawner stack.
#[cfg(test)]
fn extract_paths_from_payload(payload: &[u8], config: &DiscoveryConfig) -> Vec<String> {
    let root: serde_json::Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(_) => return vec![],
    };

    let events: Vec<&serde_json::Value> = match &root {
        serde_json::Value::Array(items) => items.iter().collect(),
        other => vec![other],
    };

    let mut paths = Vec::new();
    for event in events {
        if let (Some(field), Some(expected)) = (&config.filter_field, &config.filter_value) {
            match get_dotted(event, field) {
                Some(serde_json::Value::String(actual)) if actual == expected => {}
                _ => continue,
            }
        }
        if let Some(serde_json::Value::String(s)) = get_dotted(event, &config.path_field) {
            paths.push(resolve_discovered_path(s, config.path_prefix.as_deref()));
        }
    }
    paths
}

#[cfg(test)]
mod tests {
    use super::{
        extract_paths_from_payload, get_dotted, resolve_discovered_path, sanitize_topic,
        topic_for_discovered_stream,
    };
    use crate::config::DiscoveryConfig;
    use serde_json::json;

    // --- get_dotted tests ---

    #[test]
    fn dotted_happy_path() {
        let v = json!({"metadata": {"streamPath": "slides/deck-1"}});
        assert_eq!(
            get_dotted(&v, "metadata.streamPath"),
            Some(&json!("slides/deck-1"))
        );
    }

    #[test]
    fn dotted_missing_segment() {
        let v = json!({"metadata": {"other": 1}});
        assert_eq!(get_dotted(&v, "metadata.streamPath"), None);
    }

    #[test]
    fn dotted_non_object_traversal() {
        let v = json!({"metadata": "not-an-object"});
        assert_eq!(get_dotted(&v, "metadata.streamPath"), None);
    }

    #[test]
    fn dotted_empty_path_returns_root() {
        let v = json!({"a": 1});
        assert_eq!(get_dotted(&v, ""), Some(&v));
    }

    // --- resolve_discovered_path tests ---

    #[test]
    fn resolve_with_prefix() {
        assert_eq!(
            resolve_discovered_path("slides/deck-1", Some("/v1/stream/")),
            "/v1/stream/slides/deck-1"
        );
    }

    #[test]
    fn resolve_without_prefix() {
        assert_eq!(
            resolve_discovered_path("/v1/stream/slides/deck-1", None),
            "/v1/stream/slides/deck-1"
        );
    }

    // --- topic_for_discovered_stream tests ---

    #[test]
    fn topic_template_substitution() {
        assert_eq!(
            topic_for_discovered_stream("/v1/stream/slides/deck-1", Some("durable-streams.{path}")),
            "durable-streams..v1.stream.slides.deck-1"
        );
    }

    #[test]
    fn topic_template_with_plain_path() {
        assert_eq!(
            topic_for_discovered_stream("slides/deck-1", Some("discovered.{path}")),
            "discovered.slides.deck-1"
        );
    }

    #[test]
    fn topic_falls_back_to_default() {
        assert_eq!(
            topic_for_discovered_stream("/v1/stream/slides/deck-1", None),
            "durable-streams.v1.stream.slides.deck-1"
        );
    }

    // --- sanitize_topic tests ---

    #[test]
    fn sanitize_replaces_slashes_and_special_chars() {
        assert_eq!(sanitize_topic("/foo/bar:baz"), "foo.bar-baz");
    }

    // --- config validation tests ---

    #[test]
    fn discovery_config_rejects_empty_control_stream() {
        let config: Result<DiscoveryConfig, _> = toml::from_str(
            r#"
            control_stream = ""
            path_field = "metadata.streamPath"
            "#,
        );
        // The validation happens via BridgeConfig, but we can check the struct
        // deserializes and then validate.
        let config = config.unwrap();
        let err = config.validate();
        assert!(err.is_err());
    }

    #[test]
    fn discovery_config_rejects_empty_path_field() {
        let config: DiscoveryConfig = toml::from_str(
            r#"
            control_stream = "/v1/stream/admin/activity"
            path_field = ""
            "#,
        )
        .unwrap();
        let err = config.validate();
        assert!(err.is_err());
    }

    #[test]
    fn discovery_config_rejects_non_absolute_control_stream() {
        let config: DiscoveryConfig = toml::from_str(
            r#"
            control_stream = "v1/stream/admin/activity"
            path_field = "metadata.streamPath"
            "#,
        )
        .unwrap();
        let err = config.validate();
        assert!(err.is_err());
    }

    #[test]
    fn discovery_config_accepts_valid_config() {
        let config: DiscoveryConfig = toml::from_str(
            r#"
            control_stream = "/v1/stream/admin/activity"
            path_field = "metadata.streamPath"
            filter_field = "kind"
            filter_value = "stream-created"
            path_prefix = "/v1/stream/"
            topic_template = "discovered.{path}"
            "#,
        )
        .unwrap();
        assert!(config.validate().is_ok());
    }

    // --- JSON array fan-out tests ---

    fn array_test_config() -> DiscoveryConfig {
        toml::from_str(
            r#"
            control_stream = "/v1/stream/admin/activity"
            filter_field = "resourceType"
            filter_value = "deck"
            path_field = "metadata.streamPath"
            path_prefix = "/v1/stream/"
            "#,
        )
        .unwrap()
    }

    #[test]
    fn array_payload_with_matching_event_spawns() {
        let config = array_test_config();
        let payload = br#"[{"resourceType":"deck","metadata":{"streamPath":"slides/abc"}}]"#;
        let paths = extract_paths_from_payload(payload, &config);
        assert_eq!(paths, vec!["/v1/stream/slides/abc"]);
    }

    #[test]
    fn array_payload_with_mixed_events_spawns_only_matches() {
        let config = array_test_config();
        let payload = br#"[
            {"resourceType":"deck","metadata":{"streamPath":"slides/abc"}},
            {"resourceType":"document","metadata":{"streamPath":"docs/xyz"}}
        ]"#;
        let paths = extract_paths_from_payload(payload, &config);
        assert_eq!(paths, vec!["/v1/stream/slides/abc"]);
    }

    #[test]
    fn object_payload_still_works() {
        let config = array_test_config();
        let payload = br#"{"resourceType":"deck","metadata":{"streamPath":"slides/abc"}}"#;
        let paths = extract_paths_from_payload(payload, &config);
        assert_eq!(paths, vec!["/v1/stream/slides/abc"]);
    }

    #[test]
    fn empty_array_is_noop() {
        let config = array_test_config();
        let payload = b"[]";
        let paths = extract_paths_from_payload(payload, &config);
        assert!(paths.is_empty());
    }
}

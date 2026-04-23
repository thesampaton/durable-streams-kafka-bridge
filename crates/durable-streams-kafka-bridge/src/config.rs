use durable_streams_client::{Offset, StreamPath};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BridgeConfig {
    pub durable_streams: DurableStreamsConfig,
    pub kafka: KafkaConfig,
    #[serde(default)]
    pub targets: BTreeMap<String, TargetConfig>,
    #[serde(default)]
    pub topic_mapping: TopicMappingConfig,
    #[serde(default)]
    pub offset_store: OffsetStoreConfig,
    #[serde(default)]
    pub preflight: PreflightConfig,
    #[serde(default)]
    pub streams: Vec<StreamConfig>,
    #[serde(default)]
    pub discovery: Vec<DiscoveryConfig>,
}

impl BridgeConfig {
    /// Load bridge configuration from a TOML file.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read, cannot be parsed as TOML,
    /// or fails validation.
    pub async fn from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let raw = tokio::fs::read_to_string(path)
            .await
            .map_err(|source| ConfigError::Read {
                path: path.to_path_buf(),
                source,
            })?;
        let mut config: Self = toml::from_str(&raw).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
        config.validate()?;
        config.offset_store.resolve_default(path);
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.streams.is_empty() && self.discovery.is_empty() {
            return Err(ConfigError::Validation(
                "at least one `[[streams]]` or `[[discovery]]` entry is required".to_string(),
            ));
        }
        for discovery in &self.discovery {
            discovery.validate()?;
        }
        for stream in &self.streams {
            StreamPath::new(stream.path.clone()).map_err(|error| {
                ConfigError::Validation(format!(
                    "invalid stream path `{}` in config: {error}",
                    stream.path
                ))
            })?;
            if let Some(target) = stream
                .target
                .as_deref()
                .filter(|target| !self.targets.contains_key(*target))
            {
                return Err(ConfigError::Validation(format!(
                    "stream `{}` references unknown target `{target}`",
                    stream.path
                )));
            }
        }
        for (name, target) in &self.targets {
            if target.topic.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "target `{name}` must define a non-empty topic"
                )));
            }
        }
        if let Some(auth) = &self.kafka.auth {
            if auth.username_env.trim().is_empty() {
                return Err(ConfigError::Validation(
                    "`kafka.auth.username_env` must define a non-empty env var name".to_string(),
                ));
            }
            if auth.password_env.trim().is_empty() {
                return Err(ConfigError::Validation(
                    "`kafka.auth.password_env` must define a non-empty env var name".to_string(),
                ));
            }
        }
        match self.topic_mapping.default_topic.as_deref() {
            Some(default_topic) if default_topic.trim().is_empty() => {
                return Err(ConfigError::Validation(
                    "`topic_mapping.default_topic` must define a non-empty topic".to_string(),
                ));
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct DurableStreamsConfig {
    pub base_url: String,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

fn default_request_timeout_ms() -> u64 {
    30_000
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    #[serde(default = "default_client_id")]
    pub client_id: String,
    #[serde(default = "default_delivery_timeout_ms")]
    pub delivery_timeout_ms: u64,
    #[serde(default)]
    pub auth: Option<KafkaAuthConfig>,
}

fn default_client_id() -> String {
    "durable-streams-kafka-bridge".to_string()
}

fn default_delivery_timeout_ms() -> u64 {
    30_000
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct KafkaAuthConfig {
    #[serde(default = "default_security_protocol")]
    pub security_protocol: String,
    #[serde(default = "default_sasl_mechanism")]
    pub sasl_mechanism: String,
    pub username_env: String,
    pub password_env: String,
    #[serde(default)]
    pub ssl_ca_location: Option<String>,
}

fn default_security_protocol() -> String {
    "SASL_SSL".to_string()
}

fn default_sasl_mechanism() -> String {
    "PLAIN".to_string()
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Default)]
pub struct TopicMappingConfig {
    pub default_topic: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TargetConfig {
    pub topic: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Default)]
pub struct OffsetStoreConfig {
    pub path: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct PreflightConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_preflight_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_true")]
    pub require_topics_exist: bool,
}

impl Default for PreflightConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            timeout_ms: default_preflight_timeout_ms(),
            require_topics_exist: default_true(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_preflight_timeout_ms() -> u64 {
    5_000
}

impl OffsetStoreConfig {
    fn resolve_default(&mut self, config_path: &Path) {
        if self.path.is_none() {
            let parent = config_path.parent().unwrap_or_else(|| Path::new("."));
            self.path = Some(parent.join(".durable-streams-kafka-bridge-offsets.json"));
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct StreamConfig {
    pub path: String,
    pub topic: Option<String>,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub offset: Option<String>,
}

impl StreamConfig {
    #[must_use]
    pub fn start_offset(&self) -> Offset {
        self.offset
            .as_deref()
            .map_or_else(Offset::start, Offset::from)
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct DiscoveryConfig {
    pub control_stream: String,
    #[serde(default)]
    pub filter_field: Option<String>,
    #[serde(default)]
    pub filter_value: Option<String>,
    pub path_field: String,
    #[serde(default)]
    pub path_prefix: Option<String>,
    #[serde(default)]
    pub default_offset: Option<String>,
    #[serde(default)]
    pub topic_template: Option<String>,
}

impl DiscoveryConfig {
    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        if self.control_stream.trim().is_empty() {
            return Err(ConfigError::Validation(
                "`discovery.control_stream` must be non-empty".to_string(),
            ));
        }
        StreamPath::new(self.control_stream.clone()).map_err(|error| {
            ConfigError::Validation(format!(
                "invalid discovery control_stream `{}`: {error}",
                self.control_stream
            ))
        })?;
        if self.path_field.trim().is_empty() {
            return Err(ConfigError::Validation(
                "`discovery.path_field` must be non-empty".to_string(),
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn start_offset(&self) -> Offset {
        self.default_offset
            .as_deref()
            .map_or_else(Offset::start, Offset::from)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file `{path}`")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config file `{path}`")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("{0}")]
    Validation(String),
}

#[cfg(test)]
mod tests {
    use super::{BridgeConfig, ConfigError};

    #[tokio::test]
    async fn config_defaults_offset_store_next_to_config() {
        let temp_dir = std::env::temp_dir().join(format!(
            "durable-streams-kafka-bridge-config-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();
        let path = temp_dir.join("bridge.toml");
        tokio::fs::write(
            &path,
            r#"
[durable_streams]
base_url = "http://localhost:4437"

[kafka]
bootstrap_servers = "localhost:9092"

[[streams]]
path = "/v1/stream/orders"
"#,
        )
        .await
        .unwrap();

        let config = BridgeConfig::from_path(&path).await.unwrap();
        assert_eq!(
            config.offset_store.path.unwrap(),
            temp_dir.join(".durable-streams-kafka-bridge-offsets.json")
        );
        assert_eq!(config.topic_mapping.default_topic, None);
        assert!(config.targets.is_empty());
        assert!(config.preflight.enabled);
        assert!(config.preflight.require_topics_exist);
    }

    #[tokio::test]
    async fn config_rejects_invalid_stream_paths() {
        let temp_dir = std::env::temp_dir().join(format!(
            "durable-streams-kafka-bridge-config-invalid-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();
        let path = temp_dir.join("bridge.toml");
        tokio::fs::write(
            &path,
            r#"
[durable_streams]
base_url = "http://localhost:4437"

[kafka]
bootstrap_servers = "localhost:9092"

[[streams]]
path = "v1/stream/orders"
"#,
        )
        .await
        .unwrap();

        let error = BridgeConfig::from_path(&path).await.unwrap_err();
        match error {
            ConfigError::Validation(message) => assert!(message.contains("invalid stream path")),
            other => panic!("expected validation error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn config_loads_bridge_level_default_topic_mapping() {
        let temp_dir = std::env::temp_dir().join(format!(
            "durable-streams-kafka-bridge-config-topic-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();
        let path = temp_dir.join("bridge.toml");
        tokio::fs::write(
            &path,
            r#"
[durable_streams]
base_url = "http://localhost:4437"

[kafka]
bootstrap_servers = "localhost:9092"

[topic_mapping]
default_topic = "enterprise.documents"

[[streams]]
path = "/v1/stream/docs/1"
"#,
        )
        .await
        .unwrap();

        let config = BridgeConfig::from_path(&path).await.unwrap();
        assert_eq!(
            config.topic_mapping.default_topic.as_deref(),
            Some("enterprise.documents")
        );
    }

    #[tokio::test]
    async fn config_loads_named_targets() {
        let temp_dir = std::env::temp_dir().join(format!(
            "durable-streams-kafka-bridge-config-targets-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();
        let path = temp_dir.join("bridge.toml");
        tokio::fs::write(
            &path,
            r#"
[durable_streams]
base_url = "http://localhost:4437"

[kafka]
bootstrap_servers = "localhost:9092"

[targets.enterprise_documents]
topic = "enterprise.documents"

[[streams]]
path = "/v1/stream/docs/1"
target = "enterprise_documents"
"#,
        )
        .await
        .unwrap();

        let config = BridgeConfig::from_path(&path).await.unwrap();
        assert_eq!(
            config
                .targets
                .get("enterprise_documents")
                .map(|t| t.topic.as_str()),
            Some("enterprise.documents")
        );
    }

    #[tokio::test]
    async fn config_loads_kafka_auth_settings() {
        let temp_dir = std::env::temp_dir().join(format!(
            "durable-streams-kafka-bridge-config-auth-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();
        let path = temp_dir.join("bridge.toml");
        tokio::fs::write(
            &path,
            r#"
[durable_streams]
base_url = "http://localhost:4437"

[kafka]
bootstrap_servers = "localhost:9092"

[kafka.auth]
username_env = "KAFKA_USERNAME"
password_env = "KAFKA_PASSWORD"

[[streams]]
path = "/v1/stream/docs/1"
"#,
        )
        .await
        .unwrap();

        let config = BridgeConfig::from_path(&path).await.unwrap();
        let auth = config.kafka.auth.unwrap();
        assert_eq!(auth.security_protocol, "SASL_SSL");
        assert_eq!(auth.sasl_mechanism, "PLAIN");
        assert_eq!(auth.username_env, "KAFKA_USERNAME");
        assert_eq!(auth.password_env, "KAFKA_PASSWORD");
    }

    #[tokio::test]
    async fn config_loads_preflight_settings() {
        let temp_dir = std::env::temp_dir().join(format!(
            "durable-streams-kafka-bridge-config-preflight-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();
        let path = temp_dir.join("bridge.toml");
        tokio::fs::write(
            &path,
            r#"
[durable_streams]
base_url = "http://localhost:4437"

[kafka]
bootstrap_servers = "localhost:9092"

[preflight]
enabled = true
timeout_ms = 2000
require_topics_exist = false

[[streams]]
path = "/v1/stream/docs/1"
"#,
        )
        .await
        .unwrap();

        let config = BridgeConfig::from_path(&path).await.unwrap();
        assert_eq!(config.preflight.timeout_ms, 2000);
        assert!(!config.preflight.require_topics_exist);
    }

    #[tokio::test]
    async fn config_rejects_unknown_stream_targets() {
        let temp_dir = std::env::temp_dir().join(format!(
            "durable-streams-kafka-bridge-config-missing-target-{}",
            std::process::id()
        ));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();
        let path = temp_dir.join("bridge.toml");
        tokio::fs::write(
            &path,
            r#"
[durable_streams]
base_url = "http://localhost:4437"

[kafka]
bootstrap_servers = "localhost:9092"

[[streams]]
path = "/v1/stream/docs/1"
target = "enterprise_documents"
"#,
        )
        .await
        .unwrap();

        let error = BridgeConfig::from_path(&path).await.unwrap_err();
        match error {
            ConfigError::Validation(message) => assert!(message.contains("unknown target")),
            other => panic!("expected validation error, got {other:?}"),
        }
    }
}

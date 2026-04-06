use durable_streams_client::{Offset, StreamPath};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BridgeConfig {
    pub durable_streams: DurableStreamsConfig,
    pub kafka: KafkaConfig,
    #[serde(default)]
    pub offset_store: OffsetStoreConfig,
    pub streams: Vec<StreamConfig>,
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
        if self.streams.is_empty() {
            return Err(ConfigError::Validation(
                "at least one `[[streams]]` entry is required".to_string(),
            ));
        }
        for stream in &self.streams {
            StreamPath::new(stream.path.clone()).map_err(|error| {
                ConfigError::Validation(format!(
                    "invalid stream path `{}` in config: {error}",
                    stream.path
                ))
            })?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct DurableStreamsConfig {
    pub base_url: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    #[serde(default = "default_client_id")]
    pub client_id: String,
    #[serde(default = "default_delivery_timeout_ms")]
    pub delivery_timeout_ms: u64,
}

fn default_client_id() -> String {
    "durable-streams-kafka-bridge".to_string()
}

fn default_delivery_timeout_ms() -> u64 {
    30_000
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Default)]
pub struct OffsetStoreConfig {
    pub path: Option<PathBuf>,
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
}

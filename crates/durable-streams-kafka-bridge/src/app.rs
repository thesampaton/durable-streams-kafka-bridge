#![cfg(feature = "rdkafka-producer")]

use crate::bridge::{BridgeError, StreamRuntime, run_stream};
use crate::config::{BridgeConfig, ConfigError};
use crate::discovery::{ActivePaths, run_discovery};
use crate::kafka::KafkaSink;
use crate::offset_store::{OffsetStore, OffsetStoreError};
use clap::Parser;
use durable_streams_client::{ClientConfig, DurableStreamsClient};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

#[derive(Debug, Parser)]
pub struct Cli {
    #[arg(long, default_value = "bridge.toml")]
    pub config: PathBuf,
}

pub struct App {
    config: BridgeConfig,
}

impl App {
    /// Load the bridge application from a config file path.
    ///
    /// # Errors
    ///
    /// Returns an error when the config file cannot be read, parsed, or
    /// validated.
    pub async fn from_path(path: impl AsRef<Path>) -> Result<Self, AppError> {
        let config = BridgeConfig::from_path(path).await?;
        Ok(Self { config })
    }

    /// Run the bridge until all stream tasks complete or the process receives
    /// `SIGINT`.
    ///
    /// # Errors
    ///
    /// Returns an error when startup fails or when any stream task exits with
    /// an error.
    pub async fn run(self) -> Result<(), AppError> {
        let client = DurableStreamsClient::with_config(
            &self.config.durable_streams.base_url,
            ClientConfig {
                request_timeout: Duration::from_millis(
                    self.config.durable_streams.request_timeout_ms,
                ),
                ..ClientConfig::default()
            },
        )?;
        let sink = Arc::new(KafkaSink::new(
            &self.config.kafka.bootstrap_servers,
            &self.config.kafka.client_id,
            Duration::from_millis(self.config.kafka.delivery_timeout_ms),
            self.config.kafka.auth.as_ref(),
        )?);
        let runtimes: Vec<_> = self
            .config
            .streams
            .iter()
            .map(|stream| {
                StreamRuntime::from_config(stream, &self.config.targets, &self.config.topic_mapping)
            })
            .collect();

        if self.config.preflight.enabled {
            run_preflight(
                &client,
                sink.as_ref(),
                &runtimes,
                Duration::from_millis(self.config.preflight.timeout_ms),
                self.config.preflight.require_topics_exist,
            )
            .await?;
        }

        let offset_store_path = self
            .config
            .offset_store
            .path
            .clone()
            .ok_or_else(|| AppError::MissingOffsetStorePath)?;
        let offset_store = Arc::new(OffsetStore::open(offset_store_path).await?);
        let mut tasks = JoinSet::new();

        // Seed active-paths set with static stream paths.
        let active_paths: ActivePaths = Arc::new(Mutex::new(
            runtimes
                .iter()
                .map(|r| r.path.clone())
                .collect::<HashSet<_>>(),
        ));

        for runtime in runtimes {
            eprintln!(
                "bridging {} -> {} (offset store: {})",
                runtime.path,
                runtime.topic,
                offset_store.path().display()
            );
            tasks.spawn(run_stream(
                client.clone(),
                runtime,
                offset_store.clone(),
                sink.clone(),
            ));
        }

        // Wrap the JoinSet so discovery tasks can spawn new forwarders.
        let shared_tasks = Arc::new(Mutex::new(tasks));

        // Spawn one discovery task per [[discovery]] block.
        // TODO: if a control stream is also a static stream, both subscriptions
        // run independently — consider deduping in the future.
        for discovery_config in self.config.discovery {
            eprintln!(
                "discovery: watching control stream {}",
                discovery_config.control_stream
            );
            let client = client.clone();
            let offset_store = offset_store.clone();
            let sink = sink.clone();
            let active_paths = active_paths.clone();
            let shared_tasks_clone = shared_tasks.clone();
            shared_tasks.lock().await.spawn(async move {
                run_discovery(
                    client,
                    discovery_config,
                    offset_store,
                    sink,
                    active_paths,
                    shared_tasks_clone,
                )
                .await
            });
        }

        loop {
            let result = {
                let mut tasks = shared_tasks.lock().await;
                if tasks.is_empty() {
                    return Ok(());
                }
                tokio::select! {
                    result = tasks.join_next() => result,
                    signal = tokio::signal::ctrl_c() => {
                        signal?;
                        tasks.abort_all();
                        return Ok(());
                    }
                }
            };
            match result {
                Some(Ok(Ok(()))) => {}
                Some(Ok(Err(error))) => return Err(AppError::Bridge(error)),
                Some(Err(error)) => return Err(AppError::Join(error)),
                None => return Ok(()),
            }
        }
    }
}

async fn run_preflight(
    client: &DurableStreamsClient,
    sink: &KafkaSink,
    runtimes: &[StreamRuntime],
    timeout: Duration,
    require_topics_exist: bool,
) -> Result<(), AppError> {
    for runtime in runtimes {
        client
            .stream(&runtime.path)?
            .metadata()
            .await
            .map_err(|source| AppError::PreflightStream {
                stream_path: runtime.path.clone(),
                source,
            })?;
    }

    let topics: Vec<_> = runtimes
        .iter()
        .map(|runtime| runtime.topic.clone())
        .collect();
    sink.preflight(&topics, timeout, require_topics_exist)
        .map_err(AppError::PreflightKafka)?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Client(#[from] durable_streams_client::Error),
    #[error(transparent)]
    Kafka(#[from] crate::kafka::SinkError),
    #[error("durable streams preflight failed for `{stream_path}`")]
    PreflightStream {
        stream_path: String,
        #[source]
        source: durable_streams_client::Error,
    },
    #[error("kafka preflight failed")]
    PreflightKafka(#[source] crate::kafka::SinkError),
    #[error(transparent)]
    OffsetStore(#[from] OffsetStoreError),
    #[error(transparent)]
    Bridge(#[from] BridgeError),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
    #[error(transparent)]
    Signal(#[from] std::io::Error),
    #[error("offset store path was not resolved")]
    MissingOffsetStorePath,
}

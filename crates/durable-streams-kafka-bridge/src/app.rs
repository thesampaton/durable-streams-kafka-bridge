#![cfg(feature = "rdkafka-producer")]

use crate::bridge::{BridgeError, StreamRuntime, run_stream};
use crate::config::{BridgeConfig, ConfigError};
use crate::kafka::KafkaSink;
use crate::offset_store::{OffsetStore, OffsetStoreError};
use clap::Parser;
use durable_streams_client::DurableStreamsClient;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
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
        let client = DurableStreamsClient::new(&self.config.durable_streams.base_url)?;
        let sink = Arc::new(KafkaSink::new(
            &self.config.kafka.bootstrap_servers,
            &self.config.kafka.client_id,
            Duration::from_millis(self.config.kafka.delivery_timeout_ms),
        )?);
        let offset_store_path = self
            .config
            .offset_store
            .path
            .clone()
            .ok_or_else(|| AppError::MissingOffsetStorePath)?;
        let offset_store = Arc::new(OffsetStore::open(offset_store_path).await?);
        let mut tasks = JoinSet::new();

        for stream in &self.config.streams {
            let runtime = StreamRuntime::from_config(stream);
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

        loop {
            tokio::select! {
                result = tasks.join_next(), if !tasks.is_empty() => match result {
                    Some(Ok(Ok(()))) => {}
                    Some(Ok(Err(error))) => return Err(AppError::Bridge(error)),
                    Some(Err(error)) => return Err(AppError::Join(error)),
                    None => return Ok(()),
                },
                signal = tokio::signal::ctrl_c() => {
                    signal?;
                    tasks.abort_all();
                    return Ok(());
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Client(#[from] durable_streams_client::Error),
    #[error(transparent)]
    Kafka(#[from] crate::kafka::SinkError),
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

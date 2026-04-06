#![cfg(feature = "rdkafka-producer")]

use clap::Parser;
use durable_streams_kafka_bridge::app::{App, Cli};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let app = App::from_path(&cli.config).await?;
    app.run().await?;
    Ok(())
}

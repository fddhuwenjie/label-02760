mod protocol;
mod proxy;
mod config;

use anyhow::Result;
use clap::Parser;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use crate::config::Config;
use crate::proxy::KafkaProxy;

#[tokio::main]
async fn main() -> Result<()> {
    FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .init();

    let config = Config::parse();
    info!("Starting Kafka Relay on {}:{}", config.listen_host, config.listen_port);
    info!("Forwarding to {}:{}", config.upstream_host, config.upstream_port);

    let proxy = KafkaProxy::new(config);
    proxy.run().await
}

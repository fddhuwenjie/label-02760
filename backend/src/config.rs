use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(name = "kafka-relay")]
#[command(about = "A Kafka protocol relay/proxy")]
pub struct Config {
    /// Host to listen on
    #[arg(long, env = "LISTEN_HOST", default_value = "0.0.0.0")]
    pub listen_host: String,

    /// Port to listen on
    #[arg(long, env = "LISTEN_PORT", default_value = "9092")]
    pub listen_port: u16,

    /// Advertised host for Metadata response rewriting (defaults to listen_host)
    #[arg(long, env = "ADVERTISED_HOST")]
    pub advertised_host: Option<String>,

    /// Advertised port for Metadata response rewriting (defaults to listen_port)
    #[arg(long, env = "ADVERTISED_PORT")]
    pub advertised_port: Option<u16>,

    /// Upstream Kafka broker host
    #[arg(long, env = "UPSTREAM_HOST", default_value = "localhost")]
    pub upstream_host: String,

    /// Upstream Kafka broker port
    #[arg(long, env = "UPSTREAM_PORT", default_value = "9093")]
    pub upstream_port: u16,
}

impl Config {
    /// Get the advertised host (for Metadata response rewriting)
    pub fn get_advertised_host(&self) -> &str {
        self.advertised_host.as_deref().unwrap_or(&self.listen_host)
    }

    /// Get the advertised port (for Metadata response rewriting)
    pub fn get_advertised_port(&self) -> u16 {
        self.advertised_port.unwrap_or(self.listen_port)
    }
}

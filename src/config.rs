use std::env;
use std::net::SocketAddr;

pub struct Config {
    pub gcs_bucket: Option<String>,
    pub listen_addr: SocketAddr,
    pub worker_count: usize,
    pub channel_capacity: usize,
    pub webhook_secret: Option<String>,
    pub retry_buffer_bytes: usize,
    pub retry_interval_secs: u64,
}

impl Config {
    pub fn from_env() -> Self {
        let gcs_bucket = env::var("GCS_BUCKET").ok().filter(|s| !s.is_empty());

        let listen_addr: SocketAddr = env::var("LISTEN_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:8080".into())
            .parse()
            .expect("LISTEN_ADDR must be a valid socket address");

        let worker_count: usize = env::var("WORKER_COUNT")
            .unwrap_or_else(|_| "32".into())
            .parse()
            .expect("WORKER_COUNT must be a positive integer");

        let channel_capacity: usize = env::var("CHANNEL_CAPACITY")
            .unwrap_or_else(|_| "10000".into())
            .parse()
            .expect("CHANNEL_CAPACITY must be a positive integer");

        let webhook_secret = env::var("WEBHOOK_SECRET").ok().filter(|s| !s.is_empty());

        let retry_buffer_mb: usize = env::var("RETRY_BUFFER_MB")
            .unwrap_or_else(|_| "500".into())
            .parse()
            .expect("RETRY_BUFFER_MB must be a positive integer");

        let retry_interval_secs: u64 = env::var("RETRY_INTERVAL_SECS")
            .unwrap_or_else(|_| "10".into())
            .parse()
            .expect("RETRY_INTERVAL_SECS must be a positive integer");

        Self {
            gcs_bucket,
            listen_addr,
            worker_count,
            channel_capacity,
            webhook_secret,
            retry_buffer_bytes: retry_buffer_mb * 1024 * 1024,
            retry_interval_secs,
        }
    }
}

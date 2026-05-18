use anyhow::{Context, Result};
use bytes::BytesMut;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::protocol::*;

/// Statistics for monitoring proxy activity
#[derive(Debug, Default)]
pub struct ProxyStats {
    pub requests_total: AtomicU64,
    pub responses_total: AtomicU64,
    pub produce_requests: AtomicU64,
    pub fetch_requests: AtomicU64,
    pub metadata_requests: AtomicU64,
    pub bytes_from_client: AtomicU64,
    pub bytes_to_client: AtomicU64,
}

pub struct KafkaProxy {
    config: Config,
    stats: Arc<ProxyStats>,
}

impl KafkaProxy {
    pub fn new(config: Config) -> Self {
        Self { 
            config,
            stats: Arc::new(ProxyStats::default()),
        }
    }

    pub async fn run(&self) -> Result<()> {
        let addr = format!("{}:{}", self.config.listen_host, self.config.listen_port);
        let listener = TcpListener::bind(&addr).await
            .with_context(|| format!("Failed to bind to {}", addr))?;
        info!("Kafka Relay listening on {}", addr);

        // Spawn stats reporter
        let stats = self.stats.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                info!(
                    "Stats: requests={}, responses={}, produce={}, fetch={}, metadata={}, bytes_in={}, bytes_out={}",
                    stats.requests_total.load(Ordering::Relaxed),
                    stats.responses_total.load(Ordering::Relaxed),
                    stats.produce_requests.load(Ordering::Relaxed),
                    stats.fetch_requests.load(Ordering::Relaxed),
                    stats.metadata_requests.load(Ordering::Relaxed),
                    stats.bytes_from_client.load(Ordering::Relaxed),
                    stats.bytes_to_client.load(Ordering::Relaxed),
                );
            }
        });

        loop {
            match listener.accept().await {
                Ok((client_stream, client_addr)) => {
                    info!("New client connection from {}", client_addr);
                    let upstream_host = self.config.upstream_host.clone();
                    let upstream_port = self.config.upstream_port;
                    let advertised_host = self.config.get_advertised_host().to_string();
                    let advertised_port = self.config.get_advertised_port();
                    let stats = self.stats.clone();
                    
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(
                            client_stream,
                            &upstream_host,
                            upstream_port,
                            &advertised_host,
                            advertised_port,
                            stats,
                        ).await {
                            error!("Connection error from {}: {}", client_addr, e);
                        }
                        info!("Connection from {} closed", client_addr);
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }
}

/// Proxy configuration passed to connection handlers
#[derive(Clone)]
struct ProxyContext {
    /// Advertised host (used for rewriting Metadata responses)
    advertised_host: String,
    /// Advertised port (used for rewriting Metadata responses)
    advertised_port: u16,
    stats: Arc<ProxyStats>,
    /// Track pending requests to know which API was requested (correlation_id -> api_key, api_version)
    pending_requests: Arc<RwLock<HashMap<i32, (ApiKey, i16)>>>,
}

async fn handle_connection(
    client: TcpStream,
    upstream_host: &str,
    upstream_port: u16,
    advertised_host: &str,
    advertised_port: u16,
    stats: Arc<ProxyStats>,
) -> Result<()> {
    let upstream_addr = format!("{}:{}", upstream_host, upstream_port);
    
    let upstream = tokio::time::timeout(
        tokio::time::Duration::from_secs(10),
        TcpStream::connect(&upstream_addr)
    ).await
        .with_context(|| format!("Timeout connecting to upstream {}", upstream_addr))?
        .with_context(|| format!("Failed to connect to upstream {}", upstream_addr))?;
    
    info!("Connected to upstream Kafka at {}", upstream_addr);

    let ctx = ProxyContext {
        advertised_host: advertised_host.to_string(),
        advertised_port,
        stats,
        pending_requests: Arc::new(RwLock::new(HashMap::new())),
    };

    let (client_read, mut client_write) = client.into_split();
    let (upstream_read, upstream_write) = upstream.into_split();

    // Channel for sending responses back to client
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(100);

    let tx_clone = tx.clone();
    let ctx_clone = ctx.clone();
    let client_to_upstream = handle_client_to_upstream(client_read, upstream_write, tx_clone, ctx_clone);
    let upstream_to_client = handle_upstream_to_client(upstream_read, tx, ctx);

    // Task to write responses to client
    let write_to_client = async move {
        while let Some(data) = rx.recv().await {
            if client_write.write_all(&data).await.is_err() {
                break;
            }
        }
        Ok::<_, anyhow::Error>(())
    };

    tokio::select! {
        r = client_to_upstream => { 
            if let Err(e) = r {
                debug!("Client to upstream ended: {}", e);
            }
        }
        r = upstream_to_client => { 
            if let Err(e) = r {
                debug!("Upstream to client ended: {}", e);
            }
        }
        r = write_to_client => {
            if let Err(e) = r {
                debug!("Write to client ended: {}", e);
            }
        }
    }

    Ok(())
}

async fn handle_client_to_upstream(
    mut client_read: tokio::net::tcp::OwnedReadHalf,
    mut upstream_write: tokio::net::tcp::OwnedWriteHalf,
    tx: mpsc::Sender<Vec<u8>>,
    ctx: ProxyContext,
) -> Result<()> {
    const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;
    const MAX_TOTAL_BUFFER: usize = 256 * 1024 * 1024;
    
    let mut buf = vec![0u8; 64 * 1024];
    let mut total_allocated: usize = buf.len();
    
    loop {
        // Read message size (4 bytes)
        let mut size_buf = [0u8; 4];
        client_read.read_exact(&mut size_buf).await
            .context("Failed to read message size from client")?;
        let size = i32::from_be_bytes(size_buf) as usize;
        
        if size == 0 {
            warn!("Received zero-length message from client");
            continue;
        }
        
        if size > MAX_MESSAGE_SIZE {
            return Err(anyhow::anyhow!("Message size {} exceeds maximum allowed {}", size, MAX_MESSAGE_SIZE));
        }
        
        if size > buf.len() {
            let additional = size - buf.len();
            if total_allocated + additional > MAX_TOTAL_BUFFER {
                return Err(anyhow::anyhow!(
                    "Total buffer allocation {} exceeds maximum allowed {}",
                    total_allocated + additional,
                    MAX_TOTAL_BUFFER
                ));
            }
            buf.resize(size, 0);
            total_allocated += additional;
        }
        
        // Read message body
        client_read.read_exact(&mut buf[..size]).await
            .context("Failed to read message body from client")?;

        ctx.stats.requests_total.fetch_add(1, Ordering::Relaxed);
        ctx.stats.bytes_from_client.fetch_add((4 + size) as u64, Ordering::Relaxed);

        // Parse and process request
        match KafkaRequest::parse(&buf[..size]) {
            Ok(request) => {
                log_request(&request);
                
                // Handle ApiVersions locally for version negotiation
                if request.header.api_key_enum() == ApiKey::ApiVersions {
                    let response = if request.header.api_version >= 3 {
                        build_api_versions_response_v3(0, 0)
                    } else if request.header.api_version >= 1 {
                        build_api_versions_response_v1(0, 0)
                    } else {
                        build_api_versions_response(0)
                    };
                    let response_bytes = build_response(request.header.correlation_id, &response);
                    ctx.stats.bytes_to_client.fetch_add(response_bytes.len() as u64, Ordering::Relaxed);
                    ctx.stats.responses_total.fetch_add(1, Ordering::Relaxed);
                    tx.send(response_bytes).await
                        .context("Failed to send ApiVersions response")?;
                    continue;
                }
                
                // Track pending request for response processing
                {
                    let mut pending = ctx.pending_requests.write().unwrap();
                    pending.insert(
                        request.header.correlation_id,
                        (request.header.api_key_enum(), request.header.api_version),
                    );
                }
                
                // Process and potentially modify the request
                process_request(&request, &ctx);
            }
            Err(e) => {
                warn!("Failed to parse request: {}, forwarding as-is", e);
            }
        }

        // Forward to upstream
        upstream_write.write_all(&size_buf).await
            .context("Failed to write message size to upstream")?;
        upstream_write.write_all(&buf[..size]).await
            .context("Failed to write message body to upstream")?;
    }
}

/// Process and log details about the request
fn process_request(request: &KafkaRequest, ctx: &ProxyContext) {
    match request.header.api_key_enum() {
        ApiKey::Produce => {
            ctx.stats.produce_requests.fetch_add(1, Ordering::Relaxed);
            process_produce_request(request);
        }
        ApiKey::Fetch => {
            ctx.stats.fetch_requests.fetch_add(1, Ordering::Relaxed);
            process_fetch_request(request);
        }
        ApiKey::Metadata => {
            ctx.stats.metadata_requests.fetch_add(1, Ordering::Relaxed);
            process_metadata_request(request);
        }
        _ => {}
    }
}

/// Process Produce request - parse and log details
fn process_produce_request(request: &KafkaRequest) {
    let mut cursor = Cursor::new(request.body.as_ref());
    match parse_produce_request(&mut cursor, request.header.api_version) {
        Ok((transactional_id, acks, timeout_ms, topics)) => {
            let total_partitions: usize = topics.iter().map(|t| t.partitions.len()).sum();
            let total_records: usize = topics.iter()
                .flat_map(|t| &t.partitions)
                .filter(|p| p.records.is_some())
                .count();
            
            info!(
                "Produce: topics={}, partitions={}, records={}, acks={}, timeout={}ms, txn_id={:?}",
                topics.len(),
                total_partitions,
                total_records,
                acks,
                timeout_ms,
                transactional_id
            );
            
            for topic in &topics {
                debug!(
                    "  Topic '{}': {} partition(s)",
                    topic.name,
                    topic.partitions.len()
                );
            }
        }
        Err(e) => {
            warn!("Failed to parse Produce request: {}", e);
        }
    }
}

/// Process Fetch request - parse and log details
fn process_fetch_request(request: &KafkaRequest) {
    let mut cursor = Cursor::new(request.body.as_ref());
    match parse_fetch_request(&mut cursor, request.header.api_version) {
        Ok(fetch_data) => {
            let total_partitions: usize = fetch_data.topics.iter()
                .map(|t| t.partitions.len())
                .sum();
            
            info!(
                "Fetch: topics={}, partitions={}, max_wait={}ms, min_bytes={}, max_bytes={}, isolation={}",
                fetch_data.topics.len(),
                total_partitions,
                fetch_data.max_wait_ms,
                fetch_data.min_bytes,
                fetch_data.max_bytes,
                fetch_data.isolation_level
            );
            
            for topic in &fetch_data.topics {
                for partition in &topic.partitions {
                    debug!(
                        "  Topic '{}' partition {}: offset={}, max_bytes={}",
                        topic.name,
                        partition.partition,
                        partition.fetch_offset,
                        partition.partition_max_bytes
                    );
                }
            }
        }
        Err(e) => {
            warn!("Failed to parse Fetch request: {}", e);
        }
    }
}

/// Process Metadata request - parse and log details
fn process_metadata_request(request: &KafkaRequest) {
    let mut cursor = Cursor::new(request.body.as_ref());
    match parse_metadata_request_topics(&mut cursor) {
        Ok(topics) => {
            if topics.is_empty() {
                info!("Metadata: requesting all topics");
            } else {
                info!("Metadata: requesting topics {:?}", topics);
            }
        }
        Err(e) => {
            warn!("Failed to parse Metadata request: {}", e);
        }
    }
}

async fn handle_upstream_to_client(
    mut upstream_read: tokio::net::tcp::OwnedReadHalf,
    tx: mpsc::Sender<Vec<u8>>,
    ctx: ProxyContext,
) -> Result<()> {
    const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;
    const MAX_TOTAL_BUFFER: usize = 256 * 1024 * 1024;
    
    let mut buf = vec![0u8; 64 * 1024];
    let mut total_allocated: usize = buf.len();
    
    loop {
        // Read message size (4 bytes)
        let mut size_buf = [0u8; 4];
        upstream_read.read_exact(&mut size_buf).await
            .context("Failed to read response size from upstream")?;
        let size = i32::from_be_bytes(size_buf) as usize;
        
        if size == 0 {
            warn!("Received zero-length response from upstream");
            continue;
        }
        
        if size > MAX_MESSAGE_SIZE {
            return Err(anyhow::anyhow!("Response size {} exceeds maximum allowed {}", size, MAX_MESSAGE_SIZE));
        }
        
        if size > buf.len() {
            let additional = size - buf.len();
            if total_allocated + additional > MAX_TOTAL_BUFFER {
                return Err(anyhow::anyhow!(
                    "Total buffer allocation {} exceeds maximum allowed {}",
                    total_allocated + additional,
                    MAX_TOTAL_BUFFER
                ));
            }
            buf.resize(size, 0);
            total_allocated += additional;
        }
        
        // Read message body
        upstream_read.read_exact(&mut buf[..size]).await
            .context("Failed to read response body from upstream")?;

        ctx.stats.responses_total.fetch_add(1, Ordering::Relaxed);
        
        // Process response - potentially rewrite broker addresses in Metadata responses
        let response_data = process_response(&buf[..size], &ctx);
        
        let response_size = response_data.len();
        ctx.stats.bytes_to_client.fetch_add((4 + response_size) as u64, Ordering::Relaxed);

        // Forward to client
        let mut response = Vec::with_capacity(4 + response_size);
        response.extend_from_slice(&(response_size as i32).to_be_bytes());
        response.extend_from_slice(&response_data);
        
        tx.send(response).await
            .context("Failed to send response to client")?;
    }
}

/// Process response from upstream, potentially modifying it
fn process_response(data: &[u8], ctx: &ProxyContext) -> Vec<u8> {
    // Try to parse as a response to check if we need to modify it
    if let Ok(response) = KafkaResponse::parse(data) {
        debug!("Response: correlation_id={}, body_size={}", 
            response.header.correlation_id, 
            response.body.len()
        );
        
        // Check if this is a Metadata response that needs broker address rewriting
        let request_info = {
            let mut pending = ctx.pending_requests.write().unwrap();
            pending.remove(&response.header.correlation_id)
        };
        
        if let Some((ApiKey::Metadata, api_version)) = request_info {
            // Parse and rewrite Metadata response
            let mut cursor = Cursor::new(response.body.as_ref());
            match parse_metadata_response(&mut cursor, api_version) {
                Ok(metadata) => {
                    info!(
                        "Rewriting Metadata response: {} brokers, {} topics -> proxy {}:{}",
                        metadata.brokers.len(),
                        metadata.topics.len(),
                        ctx.advertised_host,
                        ctx.advertised_port
                    );
                    
                    for broker in &metadata.brokers {
                        debug!(
                            "  Broker {}: {}:{} -> {}:{}",
                            broker.node_id,
                            broker.host,
                            broker.port,
                            ctx.advertised_host,
                            ctx.advertised_port
                        );
                    }
                    
                    // Build rewritten response
                    let rewritten_body = build_metadata_response_rewritten(
                        api_version,
                        &metadata,
                        &ctx.advertised_host,
                        ctx.advertised_port as i32,
                    );
                    
                    // Reconstruct full response with correlation_id
                    let mut result = BytesMut::with_capacity(4 + rewritten_body.len());
                    write_i32(&mut result, response.header.correlation_id);
                    result.extend_from_slice(&rewritten_body);
                    
                    return result.to_vec();
                }
                Err(e) => {
                    warn!("Failed to parse Metadata response for rewriting: {}", e);
                }
            }
        }
    }
    
    // Return data as-is if no modification needed
    data.to_vec()
}

fn log_request(request: &KafkaRequest) {
    info!(
        "Request: api_key={:?}, api_version={}, correlation_id={}, client_id={:?}",
        request.header.api_key_enum(),
        request.header.api_version,
        request.header.correlation_id,
        request.header.client_id
    );
}

fn build_response(correlation_id: i32, body: &BytesMut) -> Vec<u8> {
    let total_size = 4 + body.len(); // correlation_id + body
    let mut result = Vec::with_capacity(4 + total_size);
    
    // Message size
    result.extend_from_slice(&(total_size as i32).to_be_bytes());
    // Correlation ID
    result.extend_from_slice(&correlation_id.to_be_bytes());
    // Body
    result.extend_from_slice(body);
    
    result
}

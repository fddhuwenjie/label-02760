use bytes::BytesMut;
use std::io::{self, Cursor};
use super::types::*;

/// Fetch request topic data
#[derive(Debug, Clone)]
pub struct FetchTopicRequest {
    pub name: String,
    pub partitions: Vec<FetchPartitionRequest>,
}

/// Fetch request partition data
#[derive(Debug, Clone)]
pub struct FetchPartitionRequest {
    pub partition: i32,
    #[allow(dead_code)]
    pub current_leader_epoch: i32,
    pub fetch_offset: i64,
    #[allow(dead_code)]
    pub log_start_offset: i64,
    pub partition_max_bytes: i32,
}

/// Parse Fetch request (v0-v11)
pub fn parse_fetch_request(
    buf: &mut Cursor<&[u8]>,
    api_version: i16,
) -> io::Result<FetchRequestData> {
    let replica_id = read_i32(buf)?;
    let max_wait_ms = read_i32(buf)?;
    let min_bytes = read_i32(buf)?;
    
    // Max bytes (v3+)
    let max_bytes = if api_version >= 3 {
        read_i32(buf)?
    } else {
        i32::MAX
    };
    
    // Isolation level (v4+)
    let isolation_level = if api_version >= 4 {
        read_i8(buf)?
    } else {
        0
    };
    
    // Session ID and epoch (v7+)
    let session_id = if api_version >= 7 {
        read_i32(buf)?
    } else {
        0
    };
    
    let session_epoch = if api_version >= 7 {
        read_i32(buf)?
    } else {
        -1
    };
    
    let topics = read_array(buf, |buf| {
        let name = read_string(buf)?;
        let partitions = read_array(buf, |buf| {
            let partition = read_i32(buf)?;
            
            // Current leader epoch (v9+)
            let current_leader_epoch = if api_version >= 9 {
                read_i32(buf)?
            } else {
                -1
            };
            
            let fetch_offset = read_i64(buf)?;
            
            // Log start offset (v5+)
            let log_start_offset = if api_version >= 5 {
                read_i64(buf)?
            } else {
                -1
            };
            
            let partition_max_bytes = read_i32(buf)?;
            
            Ok(FetchPartitionRequest {
                partition,
                current_leader_epoch,
                fetch_offset,
                log_start_offset,
                partition_max_bytes,
            })
        })?;
        Ok(FetchTopicRequest { name, partitions })
    })?;
    
    // Forgotten topics (v7+)
    let forgotten_topics = if api_version >= 7 {
        read_array(buf, |buf| {
            let name = read_string(buf)?;
            let partitions = read_array(buf, |buf| read_i32(buf))?;
            Ok(ForgottenTopic { name, partitions })
        })?
    } else {
        vec![]
    };
    
    // Rack ID (v11+)
    let rack_id = if api_version >= 11 {
        read_nullable_string(buf)?
    } else {
        None
    };
    
    Ok(FetchRequestData {
        replica_id,
        max_wait_ms,
        min_bytes,
        max_bytes,
        isolation_level,
        session_id,
        session_epoch,
        topics,
        forgotten_topics,
        rack_id,
    })
}

#[derive(Debug, Clone)]
pub struct FetchRequestData {
    #[allow(dead_code)]
    pub replica_id: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: i8,
    #[allow(dead_code)]
    pub session_id: i32,
    #[allow(dead_code)]
    pub session_epoch: i32,
    pub topics: Vec<FetchTopicRequest>,
    #[allow(dead_code)]
    pub forgotten_topics: Vec<ForgottenTopic>,
    #[allow(dead_code)]
    pub rack_id: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ForgottenTopic {
    pub name: String,
    pub partitions: Vec<i32>,
}

// The following structs and functions are kept for future extensibility
// (e.g., intercepting and modifying fetch responses)

/// Fetch response partition data
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct FetchPartitionResponse {
    pub partition: i32,
    pub error_code: i16,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub log_start_offset: i64,
    pub aborted_transactions: Vec<AbortedTransaction>,
    pub preferred_read_replica: i32,
    pub records: Option<bytes::Bytes>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct AbortedTransaction {
    pub producer_id: i64,
    pub first_offset: i64,
}

/// Fetch response topic data
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct FetchTopicResponse {
    pub name: String,
    pub partitions: Vec<FetchPartitionResponse>,
}

/// Build Fetch response (v0-v11)
#[allow(dead_code)]
pub fn build_fetch_response(
    api_version: i16,
    throttle_time_ms: i32,
    error_code: i16,
    session_id: i32,
    topics: &[FetchTopicResponse],
) -> BytesMut {
    let mut buf = BytesMut::new();
    
    // Throttle time (v1+)
    if api_version >= 1 {
        write_i32(&mut buf, throttle_time_ms);
    }
    
    // Error code (v7+)
    if api_version >= 7 {
        write_i16(&mut buf, error_code);
    }
    
    // Session ID (v7+)
    if api_version >= 7 {
        write_i32(&mut buf, session_id);
    }
    
    // Topics array
    write_array(&mut buf, topics, |buf, topic| {
        write_string(buf, &topic.name);
        write_array(buf, &topic.partitions, |buf, partition| {
            write_i32(buf, partition.partition);
            write_i16(buf, partition.error_code);
            write_i64(buf, partition.high_watermark);
            
            // Last stable offset (v4+)
            if api_version >= 4 {
                write_i64(buf, partition.last_stable_offset);
            }
            
            // Log start offset (v5+)
            if api_version >= 5 {
                write_i64(buf, partition.log_start_offset);
            }
            
            // Aborted transactions (v4+)
            if api_version >= 4 {
                write_array(buf, &partition.aborted_transactions, |buf, txn| {
                    write_i64(buf, txn.producer_id);
                    write_i64(buf, txn.first_offset);
                });
            }
            
            // Preferred read replica (v11+)
            if api_version >= 11 {
                write_i32(buf, partition.preferred_read_replica);
            }
            
            // Records
            write_bytes(buf, partition.records.as_ref());
        });
    });
    
    buf
}

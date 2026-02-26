use bytes::{Bytes, BytesMut};
use std::io::{self, Cursor};
use super::types::*;

/// Produce request topic data
#[derive(Debug, Clone)]
pub struct ProduceTopicData {
    pub name: String,
    pub partitions: Vec<ProducePartitionData>,
}

/// Produce request partition data
#[derive(Debug, Clone)]
pub struct ProducePartitionData {
    #[allow(dead_code)]
    pub index: i32,
    pub records: Option<Bytes>,
}

/// Parse Produce request (v0-v8)
pub fn parse_produce_request(
    buf: &mut Cursor<&[u8]>,
    api_version: i16,
) -> io::Result<(Option<String>, i16, i32, Vec<ProduceTopicData>)> {
    // Transactional ID (v3+)
    let transactional_id = if api_version >= 3 {
        read_nullable_string(buf)?
    } else {
        None
    };
    
    let acks = read_i16(buf)?;
    let timeout_ms = read_i32(buf)?;
    
    let topics = read_array(buf, |buf| {
        let name = read_string(buf)?;
        let partitions = read_array(buf, |buf| {
            let index = read_i32(buf)?;
            let records = read_bytes(buf)?;
            Ok(ProducePartitionData { index, records })
        })?;
        Ok(ProduceTopicData { name, partitions })
    })?;
    
    Ok((transactional_id, acks, timeout_ms, topics))
}

// The following structs and functions are kept for future extensibility
// (e.g., intercepting and modifying produce responses)

/// Produce response partition result
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ProducePartitionResult {
    pub index: i32,
    pub error_code: i16,
    pub base_offset: i64,
    pub log_append_time_ms: i64,
    pub log_start_offset: i64,
}

/// Produce response topic result
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ProduceTopicResult {
    pub name: String,
    pub partitions: Vec<ProducePartitionResult>,
}

/// Build Produce response (v0-v8)
#[allow(dead_code)]
pub fn build_produce_response(
    api_version: i16,
    results: &[ProduceTopicResult],
    throttle_time_ms: i32,
) -> BytesMut {
    let mut buf = BytesMut::new();
    
    // Topics array
    write_array(&mut buf, results, |buf, topic| {
        write_string(buf, &topic.name);
        write_array(buf, &topic.partitions, |buf, partition| {
            write_i32(buf, partition.index);
            write_i16(buf, partition.error_code);
            write_i64(buf, partition.base_offset);
            
            // Log append time (v2+)
            if api_version >= 2 {
                write_i64(buf, partition.log_append_time_ms);
            }
            
            // Log start offset (v5+)
            if api_version >= 5 {
                write_i64(buf, partition.log_start_offset);
            }
            
            // Record errors (v8+) - empty array
            if api_version >= 8 {
                write_i32(buf, 0); // empty array
                write_nullable_string(buf, None); // error_message
            }
        });
    });
    
    // Throttle time (v1+)
    if api_version >= 1 {
        write_i32(&mut buf, throttle_time_ms);
    }
    
    buf
}

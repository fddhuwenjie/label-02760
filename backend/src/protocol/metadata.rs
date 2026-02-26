use bytes::{BufMut, BytesMut};
use std::io::{self, Cursor};
use super::types::*;

/// Parse Metadata request topics (v0-v8 use legacy format, v9+ use flexible)
pub fn parse_metadata_request_topics(buf: &mut Cursor<&[u8]>) -> io::Result<Vec<String>> {
    read_array(buf, |buf| read_string(buf))
}

/// Parse Metadata response and extract broker info for rewriting
/// Supports both legacy (v0-v8) and flexible (v9+) versions
pub fn parse_metadata_response(buf: &mut Cursor<&[u8]>, api_version: i16) -> io::Result<MetadataResponse> {
    let is_flexible = api_version >= 9;
    
    // Throttle time (v3+)
    let throttle_time_ms = if api_version >= 3 {
        read_i32(buf)?
    } else {
        0
    };
    
    // Brokers array
    let brokers = if is_flexible {
        read_compact_array(buf, |buf| {
            let node_id = read_i32(buf)?;
            let host = read_compact_string(buf)?;
            let port = read_i32(buf)?;
            let rack = read_compact_nullable_string(buf)?;
            skip_tagged_fields(buf)?;
            Ok(BrokerInfo { node_id, host, port, rack })
        })?
    } else {
        read_array(buf, |buf| {
            let node_id = read_i32(buf)?;
            let host = read_string(buf)?;
            let port = read_i32(buf)?;
            let rack = if api_version >= 1 {
                read_nullable_string(buf)?
            } else {
                None
            };
            Ok(BrokerInfo { node_id, host, port, rack })
        })?
    };
    
    // Cluster ID (v2+)
    let cluster_id = if api_version >= 2 {
        if is_flexible {
            read_compact_nullable_string(buf)?
        } else {
            read_nullable_string(buf)?
        }
    } else {
        None
    };
    
    // Controller ID (v1+)
    let controller_id = if api_version >= 1 {
        read_i32(buf)?
    } else {
        -1
    };
    
    // Topics array
    let topics = if is_flexible {
        read_compact_array(buf, |buf| parse_topic_info_flexible(buf, api_version))?
    } else {
        read_array(buf, |buf| parse_topic_info_legacy(buf, api_version))?
    };
    
    // Cluster authorized operations (v8+)
    let cluster_authorized_operations = if api_version >= 8 {
        read_i32(buf)?
    } else {
        -2147483648
    };
    
    // Skip tagged fields for flexible versions
    if is_flexible {
        skip_tagged_fields(buf)?;
    }
    
    Ok(MetadataResponse {
        throttle_time_ms,
        brokers,
        cluster_id,
        controller_id,
        topics,
        cluster_authorized_operations,
    })
}

fn parse_topic_info_legacy(buf: &mut Cursor<&[u8]>, api_version: i16) -> io::Result<TopicInfo> {
    let error_code = read_i16(buf)?;
    let name = read_string(buf)?;
    
    let is_internal = if api_version >= 1 {
        read_i8(buf)? != 0
    } else {
        false
    };
    
    let partitions = read_array(buf, |buf| parse_partition_info_legacy(buf, api_version))?;
    
    let topic_authorized_operations = if api_version >= 8 {
        read_i32(buf)?
    } else {
        -2147483648
    };
    
    Ok(TopicInfo {
        error_code,
        name,
        is_internal,
        partitions,
        topic_authorized_operations,
    })
}

fn parse_topic_info_flexible(buf: &mut Cursor<&[u8]>, api_version: i16) -> io::Result<TopicInfo> {
    let error_code = read_i16(buf)?;
    let name = read_compact_string(buf)?;
    let is_internal = read_i8(buf)? != 0;
    
    let partitions = read_compact_array(buf, |buf| parse_partition_info_flexible(buf, api_version))?;
    
    let topic_authorized_operations = read_i32(buf)?;
    skip_tagged_fields(buf)?;
    
    Ok(TopicInfo {
        error_code,
        name,
        is_internal,
        partitions,
        topic_authorized_operations,
    })
}

fn parse_partition_info_legacy(buf: &mut Cursor<&[u8]>, api_version: i16) -> io::Result<PartitionInfo> {
    let error_code = read_i16(buf)?;
    let partition_index = read_i32(buf)?;
    let leader_id = read_i32(buf)?;
    
    let leader_epoch = if api_version >= 7 {
        read_i32(buf)?
    } else {
        -1
    };
    
    let replica_nodes = read_array(buf, |buf| read_i32(buf))?;
    let isr_nodes = read_array(buf, |buf| read_i32(buf))?;
    
    let offline_replicas = if api_version >= 5 {
        read_array(buf, |buf| read_i32(buf))?
    } else {
        Vec::new()
    };
    
    Ok(PartitionInfo {
        error_code,
        partition_index,
        leader_id,
        leader_epoch,
        replica_nodes,
        isr_nodes,
        offline_replicas,
    })
}

fn parse_partition_info_flexible(buf: &mut Cursor<&[u8]>, _api_version: i16) -> io::Result<PartitionInfo> {
    let error_code = read_i16(buf)?;
    let partition_index = read_i32(buf)?;
    let leader_id = read_i32(buf)?;
    let leader_epoch = read_i32(buf)?;
    
    let replica_nodes = read_compact_array(buf, |buf| read_i32(buf))?;
    let isr_nodes = read_compact_array(buf, |buf| read_i32(buf))?;
    let offline_replicas = read_compact_array(buf, |buf| read_i32(buf))?;
    
    skip_tagged_fields(buf)?;
    
    Ok(PartitionInfo {
        error_code,
        partition_index,
        leader_id,
        leader_epoch,
        replica_nodes,
        isr_nodes,
        offline_replicas,
    })
}

/// Metadata response structure
#[derive(Debug, Clone)]
pub struct MetadataResponse {
    pub throttle_time_ms: i32,
    pub brokers: Vec<BrokerInfo>,
    pub cluster_id: Option<String>,
    pub controller_id: i32,
    pub topics: Vec<TopicInfo>,
    pub cluster_authorized_operations: i32,
}

/// Broker info for metadata response
#[derive(Debug, Clone)]
pub struct BrokerInfo {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

/// Partition info for metadata response
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub error_code: i16,
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
    pub offline_replicas: Vec<i32>,
}

/// Topic info for metadata response
#[derive(Debug, Clone)]
pub struct TopicInfo {
    pub error_code: i16,
    pub name: String,
    pub is_internal: bool,
    pub partitions: Vec<PartitionInfo>,
    pub topic_authorized_operations: i32,
}

/// Build a metadata response with rewritten broker addresses
/// Supports both legacy (v0-v8) and flexible (v9+) versions
pub fn build_metadata_response_rewritten(
    api_version: i16,
    response: &MetadataResponse,
    proxy_host: &str,
    proxy_port: i32,
) -> BytesMut {
    let is_flexible = api_version >= 9;
    let mut buf = BytesMut::new();
    
    // Throttle time (v3+)
    if api_version >= 3 {
        write_i32(&mut buf, response.throttle_time_ms);
    }
    
    // Brokers array - rewrite all broker addresses to proxy address
    if is_flexible {
        write_compact_array(&mut buf, &response.brokers, |buf, broker| {
            write_i32(buf, broker.node_id);
            write_compact_string(buf, proxy_host);
            write_i32(buf, proxy_port);
            write_compact_nullable_string(buf, broker.rack.as_deref());
            write_empty_tagged_fields(buf);
        });
    } else {
        write_array(&mut buf, &response.brokers, |buf, broker| {
            write_i32(buf, broker.node_id);
            write_string(buf, proxy_host);
            write_i32(buf, proxy_port);
            if api_version >= 1 {
                write_nullable_string(buf, broker.rack.as_deref());
            }
        });
    }
    
    // Cluster ID (v2+)
    if api_version >= 2 {
        if is_flexible {
            write_compact_nullable_string(&mut buf, response.cluster_id.as_deref());
        } else {
            write_nullable_string(&mut buf, response.cluster_id.as_deref());
        }
    }
    
    // Controller ID (v1+)
    if api_version >= 1 {
        write_i32(&mut buf, response.controller_id);
    }
    
    // Topics array
    if is_flexible {
        write_compact_array(&mut buf, &response.topics, |buf, topic| {
            write_topic_info_flexible(buf, topic, api_version);
        });
    } else {
        write_array(&mut buf, &response.topics, |buf, topic| {
            write_topic_info_legacy(buf, topic, api_version);
        });
    }
    
    // Cluster authorized operations (v8+)
    if api_version >= 8 {
        write_i32(&mut buf, response.cluster_authorized_operations);
    }
    
    // Tagged fields for flexible versions
    if is_flexible {
        write_empty_tagged_fields(&mut buf);
    }
    
    buf
}

fn write_topic_info_legacy(buf: &mut BytesMut, topic: &TopicInfo, api_version: i16) {
    write_i16(buf, topic.error_code);
    write_string(buf, &topic.name);
    
    if api_version >= 1 {
        buf.put_u8(if topic.is_internal { 1 } else { 0 });
    }
    
    write_array(buf, &topic.partitions, |buf, partition| {
        write_partition_info_legacy(buf, partition, api_version);
    });
    
    if api_version >= 8 {
        write_i32(buf, topic.topic_authorized_operations);
    }
}

fn write_topic_info_flexible(buf: &mut BytesMut, topic: &TopicInfo, api_version: i16) {
    write_i16(buf, topic.error_code);
    write_compact_string(buf, &topic.name);
    buf.put_u8(if topic.is_internal { 1 } else { 0 });
    
    write_compact_array(buf, &topic.partitions, |buf, partition| {
        write_partition_info_flexible(buf, partition, api_version);
    });
    
    write_i32(buf, topic.topic_authorized_operations);
    write_empty_tagged_fields(buf);
}

fn write_partition_info_legacy(buf: &mut BytesMut, partition: &PartitionInfo, api_version: i16) {
    write_i16(buf, partition.error_code);
    write_i32(buf, partition.partition_index);
    write_i32(buf, partition.leader_id);
    
    if api_version >= 7 {
        write_i32(buf, partition.leader_epoch);
    }
    
    write_array(buf, &partition.replica_nodes, |buf, &node| {
        write_i32(buf, node);
    });
    
    write_array(buf, &partition.isr_nodes, |buf, &node| {
        write_i32(buf, node);
    });
    
    if api_version >= 5 {
        write_array(buf, &partition.offline_replicas, |buf, &node| {
            write_i32(buf, node);
        });
    }
}

fn write_partition_info_flexible(buf: &mut BytesMut, partition: &PartitionInfo, _api_version: i16) {
    write_i16(buf, partition.error_code);
    write_i32(buf, partition.partition_index);
    write_i32(buf, partition.leader_id);
    write_i32(buf, partition.leader_epoch);
    
    write_compact_array(buf, &partition.replica_nodes, |buf, &node| {
        write_i32(buf, node);
    });
    
    write_compact_array(buf, &partition.isr_nodes, |buf, &node| {
        write_i32(buf, node);
    });
    
    write_compact_array(buf, &partition.offline_replicas, |buf, &node| {
        write_i32(buf, node);
    });
    
    write_empty_tagged_fields(buf);
}

fn write_i16(buf: &mut BytesMut, value: i16) {
    buf.put_i16(value);
}

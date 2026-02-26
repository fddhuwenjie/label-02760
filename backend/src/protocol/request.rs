use bytes::{Bytes, BytesMut};
use std::io::{self, Cursor};
use super::types::*;
use super::ApiKey;

/// Kafka Request Header (v0/v1/v2)
#[derive(Debug, Clone)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

impl RequestHeader {
    pub fn parse(buf: &mut Cursor<&[u8]>, header_version: i16) -> io::Result<Self> {
        let api_key = read_i16(buf)?;
        let api_version = read_i16(buf)?;
        let correlation_id = read_i32(buf)?;
        
        let client_id = if header_version >= 1 {
            if header_version >= 2 {
                // Flexible header uses compact string
                read_compact_nullable_string(buf)?
            } else {
                read_nullable_string(buf)?
            }
        } else {
            None
        };
        
        // Skip tagged fields for header v2+
        if header_version >= 2 {
            skip_tagged_fields(buf)?;
        }

        Ok(Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
        })
    }

    /// Encode header to bytes (kept for future request modification)
    #[allow(dead_code)]
    pub fn encode(&self, buf: &mut BytesMut, header_version: i16) {
        write_i16(buf, self.api_key);
        write_i16(buf, self.api_version);
        write_i32(buf, self.correlation_id);
        
        if header_version >= 1 {
            if header_version >= 2 {
                write_compact_nullable_string(buf, self.client_id.as_deref());
            } else {
                write_nullable_string(buf, self.client_id.as_deref());
            }
        }
        
        if header_version >= 2 {
            write_empty_tagged_fields(buf);
        }
    }

    pub fn api_key_enum(&self) -> ApiKey {
        ApiKey::from(self.api_key)
    }
}

/// Determine request header version based on API key and version
fn get_request_header_version(api_key: i16, api_version: i16) -> i16 {
    // ApiVersions always uses header v0 for backwards compatibility
    if api_key == ApiKey::ApiVersions as i16 {
        return 0;
    }
    
    // Flexible versions (header v2) were introduced at different API versions
    // See: https://kafka.apache.org/protocol#protocol_api_keys
    let flexible_version = match ApiKey::from(api_key) {
        ApiKey::Produce => 9,
        ApiKey::Fetch => 12,
        ApiKey::Metadata => 9,
        ApiKey::OffsetCommit => 8,
        ApiKey::OffsetFetch => 6,
        ApiKey::FindCoordinator => 3,
        ApiKey::JoinGroup => 6,
        ApiKey::Heartbeat => 4,
        ApiKey::LeaveGroup => 4,
        ApiKey::SyncGroup => 4,
        ApiKey::DescribeGroups => 5,
        ApiKey::ListGroups => 3,
        ApiKey::CreateTopics => 5,
        ApiKey::DeleteTopics => 4,
        ApiKey::SaslHandshake => i16::MAX, // Never flexible
        ApiKey::SaslAuthenticate => 2,
        ApiKey::ListOffsets => 6,
        _ => i16::MAX, // Unknown APIs default to non-flexible
    };
    
    if api_version >= flexible_version {
        2 // Flexible header
    } else {
        1 // Legacy header with client_id
    }
}

/// A complete Kafka request with header and body
#[derive(Debug, Clone)]
pub struct KafkaRequest {
    pub header: RequestHeader,
    pub body: Bytes,
}

impl KafkaRequest {
    /// Parse a request from raw bytes (excluding the 4-byte size prefix)
    pub fn parse(data: &[u8]) -> io::Result<Self> {
        let mut cursor = Cursor::new(data);
        
        // Peek at api_key and api_version to determine header version
        let api_key = read_i16(&mut cursor)?;
        let api_version = read_i16(&mut cursor)?;
        cursor.set_position(0);
        
        let header_version = get_request_header_version(api_key, api_version);
        
        let header = RequestHeader::parse(&mut cursor, header_version)?;
        let pos = cursor.position() as usize;
        let body = Bytes::copy_from_slice(&data[pos..]);

        Ok(Self { header, body })
    }

    /// Encode the request back to bytes (kept for future request modification)
    #[allow(dead_code)]
    pub fn encode(&self) -> BytesMut {
        let header_version = get_request_header_version(self.header.api_key, self.header.api_version);
        
        let mut buf = BytesMut::new();
        self.header.encode(&mut buf, header_version);
        buf.extend_from_slice(&self.body);
        buf
    }

    /// Encode with size prefix (kept for future request modification)
    #[allow(dead_code)]
    pub fn encode_with_size(&self) -> BytesMut {
        let inner = self.encode();
        let mut buf = BytesMut::with_capacity(4 + inner.len());
        write_i32(&mut buf, inner.len() as i32);
        buf.extend_from_slice(&inner);
        buf
    }
}

use bytes::BytesMut;
use super::types::*;

/// Supported API version range
#[derive(Debug, Clone)]
pub struct ApiVersionRange {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

/// Get the list of supported APIs
fn get_supported_apis() -> Vec<ApiVersionRange> {
    vec![
        ApiVersionRange { api_key: 0, min_version: 0, max_version: 8 },   // Produce
        ApiVersionRange { api_key: 1, min_version: 0, max_version: 11 },  // Fetch
        ApiVersionRange { api_key: 2, min_version: 0, max_version: 5 },   // ListOffsets
        ApiVersionRange { api_key: 3, min_version: 0, max_version: 8 },   // Metadata
        ApiVersionRange { api_key: 8, min_version: 0, max_version: 7 },   // OffsetCommit
        ApiVersionRange { api_key: 9, min_version: 0, max_version: 5 },   // OffsetFetch
        ApiVersionRange { api_key: 10, min_version: 0, max_version: 2 },  // FindCoordinator
        ApiVersionRange { api_key: 11, min_version: 0, max_version: 5 },  // JoinGroup
        ApiVersionRange { api_key: 12, min_version: 0, max_version: 3 },  // Heartbeat
        ApiVersionRange { api_key: 13, min_version: 0, max_version: 3 },  // LeaveGroup
        ApiVersionRange { api_key: 14, min_version: 0, max_version: 3 },  // SyncGroup
        ApiVersionRange { api_key: 15, min_version: 0, max_version: 4 },  // DescribeGroups
        ApiVersionRange { api_key: 16, min_version: 0, max_version: 2 },  // ListGroups
        ApiVersionRange { api_key: 17, min_version: 0, max_version: 1 },  // SaslHandshake
        ApiVersionRange { api_key: 18, min_version: 0, max_version: 3 },  // ApiVersions
        ApiVersionRange { api_key: 19, min_version: 0, max_version: 4 },  // CreateTopics
        ApiVersionRange { api_key: 20, min_version: 0, max_version: 3 },  // DeleteTopics
        ApiVersionRange { api_key: 22, min_version: 0, max_version: 3 },  // InitProducerId
        ApiVersionRange { api_key: 36, min_version: 0, max_version: 1 },  // SaslAuthenticate
    ]
}

/// Build ApiVersions response body (v0-v2 format, legacy)
pub fn build_api_versions_response(error_code: i16) -> BytesMut {
    let mut buf = BytesMut::new();
    
    // Error code
    write_i16(&mut buf, error_code);
    
    let apis = get_supported_apis();
    
    write_array(&mut buf, &apis, |buf, api| {
        write_i16(buf, api.api_key);
        write_i16(buf, api.min_version);
        write_i16(buf, api.max_version);
    });
    
    buf
}

/// Build ApiVersions response body v1-v2 (includes throttle_time_ms)
pub fn build_api_versions_response_v1(error_code: i16, throttle_time_ms: i32) -> BytesMut {
    let mut buf = build_api_versions_response(error_code);
    write_i32(&mut buf, throttle_time_ms);
    buf
}

/// Build ApiVersions response body v3 (flexible format)
pub fn build_api_versions_response_v3(error_code: i16, throttle_time_ms: i32) -> BytesMut {
    let mut buf = BytesMut::new();
    
    // Error code
    write_i16(&mut buf, error_code);
    
    let apis = get_supported_apis();
    
    // Compact array of API versions
    write_compact_array(&mut buf, &apis, |buf, api| {
        write_i16(buf, api.api_key);
        write_i16(buf, api.min_version);
        write_i16(buf, api.max_version);
        // Tagged fields for each API entry
        write_empty_tagged_fields(buf);
    });
    
    // Throttle time
    write_i32(&mut buf, throttle_time_ms);
    
    // Tagged fields for response
    write_empty_tagged_fields(&mut buf);
    
    buf
}

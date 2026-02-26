mod types;
mod request;
mod response;
mod api_versions;
mod metadata;
mod produce;
mod fetch;

// Re-export commonly used types
pub use types::write_i32;
pub use request::*;
pub use response::*;
pub use api_versions::*;
pub use metadata::*;
pub use produce::*;
pub use fetch::*;

/// Kafka API Keys
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    DescribeGroups = 15,
    ListGroups = 16,
    SaslHandshake = 17,
    ApiVersions = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
    SaslAuthenticate = 36,
    Unknown = -1,
}

impl From<i16> for ApiKey {
    fn from(value: i16) -> Self {
        match value {
            0 => ApiKey::Produce,
            1 => ApiKey::Fetch,
            2 => ApiKey::ListOffsets,
            3 => ApiKey::Metadata,
            8 => ApiKey::OffsetCommit,
            9 => ApiKey::OffsetFetch,
            10 => ApiKey::FindCoordinator,
            11 => ApiKey::JoinGroup,
            12 => ApiKey::Heartbeat,
            13 => ApiKey::LeaveGroup,
            14 => ApiKey::SyncGroup,
            15 => ApiKey::DescribeGroups,
            16 => ApiKey::ListGroups,
            17 => ApiKey::SaslHandshake,
            18 => ApiKey::ApiVersions,
            19 => ApiKey::CreateTopics,
            20 => ApiKey::DeleteTopics,
            36 => ApiKey::SaslAuthenticate,
            _ => ApiKey::Unknown,
        }
    }
}

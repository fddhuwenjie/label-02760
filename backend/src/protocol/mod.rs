//! Kafka 协议解析和构建模块
//!
//! 本模块实现了 Kafka 协议的核心解析和构建功能，支持多版本协议格式。
//!
//! # 模块结构
//!
//! - `types`: 基础数据类型的读写函数（整数、字符串、数组、VARINT等）
//! - `request`: 请求头解析，支持 v0/v1/v2 三种头格式
//! - `response`: 响应头解析
//! - `api_versions`: ApiVersions 响应构建，用于版本协商
//! - `metadata`: Metadata 响应解析和 Broker 地址重写
//! - `produce`: Produce 请求解析
//! - `fetch`: Fetch 请求解析
//!
//! # 协议版本
//!
//! Kafka 协议分为传统格式和灵活格式（Flexible Versions）：
//! - 传统格式：使用固定长度前缀（INT16/INT32）
//! - 灵活格式：使用 VARINT 长度前缀，支持 Tagged Fields 扩展
//!
//! 不同 API 从特定版本开始使用灵活格式，详见各 API 的文档。
//!
//! # 示例
//!
//! ```ignore
//! use protocol::{KafkaRequest, ApiKey};
//!
//! // 解析请求
//! let request = KafkaRequest::parse(&data)?;
//! println!("API: {:?}, Version: {}", request.header.api_key_enum(), request.header.api_version);
//!
//! // 构建 ApiVersions 响应
//! let response = build_api_versions_response_v3(0, 0);
//! ```

mod types;
mod request;
mod response;
mod api_versions;
mod metadata;
mod produce;
mod fetch;

#[cfg(test)]
mod tests;

// Re-export commonly used types
pub use types::write_i32;
pub use request::*;
pub use response::*;
pub use api_versions::*;
pub use metadata::*;
pub use produce::*;
pub use fetch::*;

/// Kafka API Keys
///
/// 定义了 Kafka 协议支持的所有 API 类型。每个 API 有唯一的数字标识符。
///
/// # 支持的 API
///
/// | API Key | 名称 | 说明 |
/// |---------|------|------|
/// | 0 | Produce | 生产消息 |
/// | 1 | Fetch | 消费消息 |
/// | 2 | ListOffsets | 查询偏移量 |
/// | 3 | Metadata | 获取集群元数据 |
/// | 8-9 | OffsetCommit/Fetch | 偏移量管理 |
/// | 10-16 | 消费者组相关 | 组协调、心跳等 |
/// | 17-18 | SASL/ApiVersions | 认证和版本协商 |
/// | 19-20 | Topic 管理 | 创建/删除 Topic |
/// | 36 | SaslAuthenticate | SASL 认证 |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum ApiKey {
    /// 生产消息到 Topic
    Produce = 0,
    /// 从 Topic 消费消息
    Fetch = 1,
    /// 查询分区偏移量
    ListOffsets = 2,
    /// 获取集群元数据（Broker 列表、Topic 信息等）
    Metadata = 3,
    /// 提交消费者偏移量
    OffsetCommit = 8,
    /// 获取已提交的偏移量
    OffsetFetch = 9,
    /// 查找组协调者
    FindCoordinator = 10,
    /// 加入消费者组
    JoinGroup = 11,
    /// 消费者组心跳
    Heartbeat = 12,
    /// 离开消费者组
    LeaveGroup = 13,
    /// 同步消费者组状态
    SyncGroup = 14,
    /// 描述消费者组
    DescribeGroups = 15,
    /// 列出所有消费者组
    ListGroups = 16,
    /// SASL 握手
    SaslHandshake = 17,
    /// API 版本协商
    ApiVersions = 18,
    /// 创建 Topic
    CreateTopics = 19,
    /// 删除 Topic
    DeleteTopics = 20,
    /// SASL 认证
    SaslAuthenticate = 36,
    /// 未知 API
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

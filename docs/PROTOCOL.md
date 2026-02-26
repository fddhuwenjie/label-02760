# Kafka Relay 协议实现说明

本文档详细说明 Kafka Relay 的协议实现细节，帮助开发者理解和维护代码。

## 目录

- [代码结构](#代码结构)
- [核心模块说明](#核心模块说明)
- [协议解析实现](#协议解析实现)
- [代理转发逻辑](#代理转发逻辑)
- [扩展指南](#扩展指南)

## 代码结构

```
backend/src/
├── main.rs              # 程序入口
├── config.rs            # 配置管理
├── proxy.rs             # 代理核心逻辑
└── protocol/
    ├── mod.rs           # 模块导出和 ApiKey 定义
    ├── types.rs         # 基础数据类型读写
    ├── request.rs       # 请求头解析
    ├── response.rs      # 响应头解析
    ├── api_versions.rs  # ApiVersions 响应构建
    ├── metadata.rs      # Metadata 解析和重写
    ├── produce.rs       # Produce 请求解析
    ├── fetch.rs         # Fetch 请求解析
    └── tests.rs         # 单元测试
```

## 核心模块说明

### types.rs - 基础类型

实现 Kafka 协议的基础数据类型读写函数。

#### 整数类型

```rust
// 读取大端整数
pub fn read_i8(buf: &mut Cursor<&[u8]>) -> io::Result<i8>
pub fn read_i16(buf: &mut Cursor<&[u8]>) -> io::Result<i16>
pub fn read_i32(buf: &mut Cursor<&[u8]>) -> io::Result<i32>
pub fn read_i64(buf: &mut Cursor<&[u8]>) -> io::Result<i64>

// 写入大端整数
pub fn write_i16(buf: &mut BytesMut, value: i16)
pub fn write_i32(buf: &mut BytesMut, value: i32)
pub fn write_i64(buf: &mut BytesMut, value: i64)
```

#### 字符串类型

```rust
// 传统格式（INT16 长度前缀）
pub fn read_string(buf: &mut Cursor<&[u8]>) -> io::Result<String>
pub fn read_nullable_string(buf: &mut Cursor<&[u8]>) -> io::Result<Option<String>>
pub fn write_string(buf: &mut BytesMut, value: &str)
pub fn write_nullable_string(buf: &mut BytesMut, value: Option<&str>)

// 紧凑格式（VARINT 长度前缀，用于灵活版本）
pub fn read_compact_string(buf: &mut Cursor<&[u8]>) -> io::Result<String>
pub fn read_compact_nullable_string(buf: &mut Cursor<&[u8]>) -> io::Result<Option<String>>
pub fn write_compact_string(buf: &mut BytesMut, value: &str)
pub fn write_compact_nullable_string(buf: &mut BytesMut, value: Option<&str>)
```

#### 变长整数 (VARINT)

```rust
// 无符号变长整数，用于灵活版本的长度编码
pub fn read_unsigned_varint(buf: &mut Cursor<&[u8]>) -> io::Result<u32>
pub fn write_unsigned_varint(buf: &mut BytesMut, value: u32)
```

VARINT 编码规则：
- 每字节低7位存储数据
- 最高位为继续标志（1=还有后续字节，0=结束）
- 最多5字节，表示32位整数

#### 数组类型

```rust
// 传统格式（INT32 长度前缀）
pub fn read_array<T, F>(buf: &mut Cursor<&[u8]>, reader: F) -> io::Result<Vec<T>>
pub fn write_array<T, F>(buf: &mut BytesMut, items: &[T], writer: F)

// 紧凑格式（VARINT 长度前缀，长度=实际长度+1）
pub fn read_compact_array<T, F>(buf: &mut Cursor<&[u8]>, reader: F) -> io::Result<Vec<T>>
pub fn write_compact_array<T, F>(buf: &mut BytesMut, items: &[T], writer: F)
```

#### 标签字段

```rust
// 跳过标签字段（用于解析时忽略未知扩展）
pub fn skip_tagged_fields(buf: &mut Cursor<&[u8]>) -> io::Result<()>

// 写入空标签字段
pub fn write_empty_tagged_fields(buf: &mut BytesMut)
```

### request.rs - 请求解析

#### 请求头版本判断

```rust
fn get_request_header_version(api_key: i16, api_version: i16) -> i16 {
    // ApiVersions 始终使用 v0 头（向后兼容）
    if api_key == ApiKey::ApiVersions as i16 {
        return 0;
    }
    
    // 根据 API 类型和版本判断是否使用灵活格式
    let flexible_version = match ApiKey::from(api_key) {
        ApiKey::Produce => 9,
        ApiKey::Fetch => 12,
        ApiKey::Metadata => 9,
        // ... 其他 API
        _ => i16::MAX,
    };
    
    if api_version >= flexible_version { 2 } else { 1 }
}
```

#### 请求解析流程

```rust
impl KafkaRequest {
    pub fn parse(data: &[u8]) -> io::Result<Self> {
        // 1. 预读 api_key 和 api_version 确定头版本
        let api_key = read_i16(&mut cursor)?;
        let api_version = read_i16(&mut cursor)?;
        cursor.set_position(0);
        
        // 2. 根据头版本解析请求头
        let header_version = get_request_header_version(api_key, api_version);
        let header = RequestHeader::parse(&mut cursor, header_version)?;
        
        // 3. 剩余部分作为请求体
        let body = Bytes::copy_from_slice(&data[pos..]);
        
        Ok(Self { header, body })
    }
}
```

### api_versions.rs - 版本协商

构建 ApiVersions 响应，告知客户端支持的 API 版本范围。

```rust
fn get_supported_apis() -> Vec<ApiVersionRange> {
    vec![
        ApiVersionRange { api_key: 0, min_version: 0, max_version: 8 },   // Produce
        ApiVersionRange { api_key: 1, min_version: 0, max_version: 11 },  // Fetch
        // ... 其他 API
    ]
}

// v0 格式
pub fn build_api_versions_response(error_code: i16) -> BytesMut

// v1-v2 格式（增加 throttle_time_ms）
pub fn build_api_versions_response_v1(error_code: i16, throttle_time_ms: i32) -> BytesMut

// v3 格式（灵活格式）
pub fn build_api_versions_response_v3(error_code: i16, throttle_time_ms: i32) -> BytesMut
```

### metadata.rs - Metadata 处理

#### 解析 Metadata 响应

```rust
pub fn parse_metadata_response(
    buf: &mut Cursor<&[u8]>, 
    api_version: i16
) -> io::Result<MetadataResponse> {
    let is_flexible = api_version >= 9;
    
    // 根据版本选择解析方式
    let brokers = if is_flexible {
        read_compact_array(buf, |buf| { /* 紧凑格式解析 */ })?
    } else {
        read_array(buf, |buf| { /* 传统格式解析 */ })?
    };
    
    // ... 解析其他字段
}
```

#### 重写 Broker 地址

```rust
pub fn build_metadata_response_rewritten(
    api_version: i16,
    response: &MetadataResponse,
    proxy_host: &str,
    proxy_port: i32,
) -> BytesMut {
    // 遍历所有 Broker，将地址替换为代理地址
    if is_flexible {
        write_compact_array(&mut buf, &response.brokers, |buf, broker| {
            write_i32(buf, broker.node_id);
            write_compact_string(buf, proxy_host);  // 替换 host
            write_i32(buf, proxy_port);             // 替换 port
            // ...
        });
    }
    // ...
}
```

### produce.rs / fetch.rs - 请求解析

这些模块解析 Produce 和 Fetch 请求，提取关键信息用于日志记录。

```rust
// Produce 请求解析
pub fn parse_produce_request(
    buf: &mut Cursor<&[u8]>,
    api_version: i16,
) -> io::Result<(Option<String>, i16, i32, Vec<ProduceTopicData>)>

// Fetch 请求解析
pub fn parse_fetch_request(
    buf: &mut Cursor<&[u8]>,
    api_version: i16,
) -> io::Result<FetchRequestData>
```

## 代理转发逻辑

### proxy.rs 核心流程

```
┌─────────────────────────────────────────────────────────────────┐
│                        handle_connection                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────┐    ┌──────────────────┐                   │
│  │ client_to_upstream│    │upstream_to_client│                   │
│  │                  │    │                  │                   │
│  │ 1. 读取请求      │    │ 1. 读取响应      │                   │
│  │ 2. 解析请求头    │    │ 2. 解析响应头    │                   │
│  │ 3. 处理特殊请求  │    │ 3. 处理特殊响应  │                   │
│  │    - ApiVersions │    │    - Metadata    │                   │
│  │ 4. 记录日志      │    │ 4. 重写地址      │                   │
│  │ 5. 转发到上游    │    │ 5. 发送到客户端  │                   │
│  └──────────────────┘    └──────────────────┘                   │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    ProxyContext                           │   │
│  │  - advertised_host/port: 代理公告地址                     │   │
│  │  - stats: 统计信息                                        │   │
│  │  - pending_requests: 待处理请求映射 (correlation_id -> API)│   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### 请求处理流程

```rust
async fn handle_client_to_upstream(...) -> Result<()> {
    loop {
        // 1. 读取消息大小和内容
        let size = read_message_size(&mut client_read).await?;
        let data = read_message_body(&mut client_read, size).await?;
        
        // 2. 解析请求
        let request = KafkaRequest::parse(&data)?;
        
        // 3. 特殊处理 ApiVersions（本地响应）
        if request.header.api_key_enum() == ApiKey::ApiVersions {
            let response = build_api_versions_response(...);
            tx.send(response).await?;
            continue;  // 不转发到上游
        }
        
        // 4. 记录待处理请求（用于响应处理）
        pending_requests.insert(
            request.header.correlation_id,
            (request.header.api_key_enum(), request.header.api_version)
        );
        
        // 5. 记录日志并转发
        process_request(&request, &ctx);
        upstream_write.write_all(&data).await?;
    }
}
```

### 响应处理流程

```rust
fn process_response(data: &[u8], ctx: &ProxyContext) -> Vec<u8> {
    let response = KafkaResponse::parse(data)?;
    
    // 查找对应的请求信息
    let request_info = pending_requests.remove(&response.header.correlation_id);
    
    // 如果是 Metadata 响应，需要重写 Broker 地址
    if let Some((ApiKey::Metadata, api_version)) = request_info {
        let metadata = parse_metadata_response(&response.body, api_version)?;
        
        // 重写所有 Broker 地址为代理地址
        let rewritten = build_metadata_response_rewritten(
            api_version,
            &metadata,
            &ctx.advertised_host,
            ctx.advertised_port,
        );
        
        return build_full_response(response.header.correlation_id, &rewritten);
    }
    
    // 其他响应直接转发
    data.to_vec()
}
```

## 扩展指南

### 添加新的 API 支持

1. **在 mod.rs 中添加 ApiKey**:
```rust
pub enum ApiKey {
    // ...
    NewApi = 99,
}

impl From<i16> for ApiKey {
    fn from(value: i16) -> Self {
        match value {
            // ...
            99 => ApiKey::NewApi,
            _ => ApiKey::Unknown,
        }
    }
}
```

2. **在 api_versions.rs 中声明支持版本**:
```rust
fn get_supported_apis() -> Vec<ApiVersionRange> {
    vec![
        // ...
        ApiVersionRange { api_key: 99, min_version: 0, max_version: 2 },
    ]
}
```

3. **创建新的解析模块** (如 `new_api.rs`):
```rust
use super::types::*;

pub struct NewApiRequest {
    // 请求字段
}

pub fn parse_new_api_request(
    buf: &mut Cursor<&[u8]>,
    api_version: i16,
) -> io::Result<NewApiRequest> {
    // 解析逻辑
}
```

4. **在 proxy.rs 中添加处理逻辑**:
```rust
fn process_request(request: &KafkaRequest, ctx: &ProxyContext) {
    match request.header.api_key_enum() {
        // ...
        ApiKey::NewApi => {
            process_new_api_request(request);
        }
        _ => {}
    }
}
```

### 添加请求/响应修改

如果需要修改请求或响应内容（而不仅仅是记录日志）：

1. **解析原始数据**
2. **修改解析后的结构**
3. **重新编码为字节**

示例（修改 Produce 请求）:
```rust
fn modify_produce_request(request: &KafkaRequest) -> BytesMut {
    let mut cursor = Cursor::new(request.body.as_ref());
    let (txn_id, acks, timeout, topics) = parse_produce_request(&mut cursor, request.header.api_version)?;
    
    // 修改数据
    let modified_acks = -1;  // 强制使用 all acks
    
    // 重新编码
    build_produce_request(request.header.api_version, txn_id, modified_acks, timeout, &topics)
}
```

### 添加新的统计指标

在 `ProxyStats` 中添加新字段：

```rust
pub struct ProxyStats {
    // 现有字段...
    pub new_metric: AtomicU64,
}

// 在处理逻辑中更新
ctx.stats.new_metric.fetch_add(1, Ordering::Relaxed);
```

## 测试

### 运行单元测试

```bash
cd backend
cargo test
```

### 测试覆盖的场景

- 基础类型读写（整数、字符串、数组）
- VARINT 编码/解码
- 请求头解析（v0/v1/v2）
- ApiVersions 响应构建
- Metadata 响应解析和重写

### 添加新测试

在 `protocol/tests.rs` 中添加：

```rust
#[test]
fn test_new_feature() {
    // 准备测试数据
    let data = vec![...];
    
    // 执行测试
    let result = parse_something(&data);
    
    // 验证结果
    assert_eq!(result.field, expected_value);
}
```

## 调试技巧

### 启用详细日志

```bash
RUST_LOG=debug cargo run
```

### 查看协议数据

在代码中添加调试输出：
```rust
debug!("Raw data: {:02x?}", &data[..min(data.len(), 100)]);
```

### 使用 Wireshark

Kafka 协议可以用 Wireshark 解析：
1. 捕获 9092 端口流量
2. 右键 -> Decode As -> Kafka

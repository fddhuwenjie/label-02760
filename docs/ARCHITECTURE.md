# Kafka Relay 架构设计

本文档说明 Kafka Relay 的整体架构设计和设计决策。

## 系统架构

```
                                    ┌─────────────────────────────────────┐
                                    │           Kafka Relay               │
┌──────────────┐                    │  ┌─────────────────────────────┐   │                    ┌──────────────┐
│              │                    │  │        TCP Listener         │   │                    │              │
│    Kafka     │  TCP Connection    │  │     (0.0.0.0:9092)          │   │  TCP Connection    │    Kafka     │
│    Client    │ ─────────────────► │  └─────────────────────────────┘   │ ─────────────────► │    Broker    │
│              │                    │              │                      │                    │              │
│  (Producer/  │                    │              ▼                      │                    │  (Upstream)  │
│   Consumer)  │                    │  ┌─────────────────────────────┐   │                    │              │
│              │ ◄───────────────── │  │    Connection Handler       │   │ ◄───────────────── │              │
└──────────────┘                    │  │                             │   │                    └──────────────┘
                                    │  │  ┌───────────────────────┐  │   │
                                    │  │  │  Request Processing   │  │   │
                                    │  │  │  - Parse headers      │  │   │
                                    │  │  │  - Log details        │  │   │
                                    │  │  │  - Handle ApiVersions │  │   │
                                    │  │  └───────────────────────┘  │   │
                                    │  │                             │   │
                                    │  │  ┌───────────────────────┐  │   │
                                    │  │  │  Response Processing  │  │   │
                                    │  │  │  - Parse Metadata     │  │   │
                                    │  │  │  - Rewrite brokers    │  │   │
                                    │  │  └───────────────────────┘  │   │
                                    │  │                             │   │
                                    │  └─────────────────────────────┘   │
                                    │                                     │
                                    │  ┌─────────────────────────────┐   │
                                    │  │      Statistics Reporter    │   │
                                    │  │   (每分钟输出统计信息)       │   │
                                    │  └─────────────────────────────┘   │
                                    └─────────────────────────────────────┘
```

## 核心组件

### 1. TCP Listener

- 监听配置的地址和端口
- 接受客户端连接
- 为每个连接创建独立的处理任务

```rust
let listener = TcpListener::bind(&addr).await?;
loop {
    let (client_stream, client_addr) = listener.accept().await?;
    tokio::spawn(handle_connection(client_stream, ...));
}
```

### 2. Connection Handler

每个客户端连接由三个并发任务处理：

```
┌─────────────────────────────────────────────────────────────────┐
│                     Connection Handler                           │
│                                                                  │
│  ┌──────────────────┐                    ┌──────────────────┐   │
│  │ client_to_upstream│                    │upstream_to_client│   │
│  │                  │                    │                  │   │
│  │  Client ──────►  │                    │  ◄────── Upstream│   │
│  │  Read requests   │                    │  Read responses  │   │
│  │  Parse & process │                    │  Parse & process │   │
│  │  Forward to      │                    │  Rewrite if      │   │
│  │  upstream        │                    │  needed          │   │
│  └────────┬─────────┘                    └────────┬─────────┘   │
│           │                                       │              │
│           │         ┌──────────────────┐          │              │
│           └────────►│  write_to_client │◄─────────┘              │
│                     │                  │                         │
│                     │  Channel (mpsc)  │                         │
│                     │  Write to client │                         │
│                     └──────────────────┘                         │
└─────────────────────────────────────────────────────────────────┘
```

### 3. Protocol Parser

协议解析模块负责：
- 解析请求/响应头
- 解析请求体（Produce, Fetch, Metadata 等）
- 构建响应（ApiVersions, Metadata 重写）

### 4. Statistics Reporter

后台任务，每分钟输出统计信息：
- 请求/响应计数
- 各类型请求计数
- 字节流量统计

## 数据流

### 请求流程

```
1. Client 发送请求
   │
   ▼
2. 读取消息大小 (4 bytes)
   │
   ▼
3. 读取消息内容
   │
   ▼
4. 解析请求头
   │
   ├─── ApiVersions? ──► 本地处理，返回支持的版本
   │
   ▼
5. 记录请求信息 (correlation_id -> api_key, version)
   │
   ▼
6. 解析并记录请求详情 (Produce/Fetch/Metadata)
   │
   ▼
7. 转发到上游 Kafka
```

### 响应流程

```
1. 上游 Kafka 返回响应
   │
   ▼
2. 读取消息大小和内容
   │
   ▼
3. 解析响应头，获取 correlation_id
   │
   ▼
4. 查找对应的请求信息
   │
   ├─── Metadata 响应? ──► 解析并重写 Broker 地址
   │
   ▼
5. 发送到客户端
```

## 设计决策

### 1. 为什么本地处理 ApiVersions？

**问题**: 客户端首先发送 ApiVersions 请求了解服务端支持的 API 版本。

**决策**: 本地处理 ApiVersions，返回代理支持的版本范围。

**原因**:
- 代理可能不支持上游 Kafka 的所有 API 版本
- 确保客户端使用代理能正确处理的版本
- 避免版本不匹配导致的解析错误

### 2. 为什么重写 Metadata 响应？

**问题**: Metadata 响应包含所有 Broker 的真实地址，客户端会直接连接这些地址。

**决策**: 将所有 Broker 地址重写为代理地址。

**原因**:
- 确保所有流量都经过代理
- 客户端无需知道真实的 Kafka 集群拓扑
- 支持代理部署在不同网络环境

### 3. 为什么使用 mpsc channel？

**问题**: 需要将响应从多个来源（本地处理、上游转发）发送到客户端。

**决策**: 使用 tokio mpsc channel 统一响应发送。

**原因**:
- 简化并发控制
- 避免写入冲突
- 支持背压（channel 容量限制）

### 4. 为什么跟踪 pending_requests？

**问题**: 响应只包含 correlation_id，不包含 API 类型信息。

**决策**: 维护 correlation_id -> (api_key, api_version) 映射。

**原因**:
- 需要知道响应类型才能正确处理（如 Metadata 重写）
- 需要知道 API 版本才能正确解析响应格式

### 5. 为什么支持多版本协议？

**问题**: Kafka 协议有多个版本，不同版本格式不同。

**决策**: 支持常用 API 的多个版本范围。

**原因**:
- 兼容不同版本的 Kafka 客户端
- 支持协议版本协商
- 灵活版本（v9+）使用不同的编码格式

## 并发模型

```
Main Task
    │
    ├── TCP Listener (accept loop)
    │
    ├── Stats Reporter (periodic task)
    │
    └── Per-Connection Tasks
            │
            ├── client_to_upstream (read client, write upstream)
            │
            ├── upstream_to_client (read upstream, send to channel)
            │
            └── write_to_client (receive from channel, write client)
```

### 任务生命周期

1. **Main Task**: 程序运行期间持续存在
2. **Stats Reporter**: 程序运行期间持续存在
3. **Connection Tasks**: 连接建立时创建，连接关闭时结束

### 错误处理

- 单个连接的错误不影响其他连接
- 连接错误会记录日志并关闭连接
- 上游连接失败会导致客户端连接关闭

## 性能考虑

### 内存管理

- 使用 `bytes::BytesMut` 减少内存分配
- 复用读取缓冲区
- 限制最大消息大小（100MB）

### 异步 I/O

- 使用 tokio 异步运行时
- 非阻塞 I/O 操作
- 高效的任务调度

### 统计收集

- 使用原子操作更新统计
- 避免锁竞争
- 定期批量输出

## 限制和约束

### 当前限制

1. **单上游 Broker**: 只支持连接单个上游 Kafka Broker
2. **无认证支持**: SASL/SSL 请求会透传但不处理
3. **无事务支持**: 事务相关请求会透传但不特殊处理
4. **无 ACL 支持**: 不进行权限控制

### 消息大小限制

- 最大消息大小: 100MB
- 超过限制会断开连接

### 连接超时

- 上游连接超时: 10秒
- 无读取超时（依赖 TCP keepalive）

## 未来扩展方向

1. **多上游支持**: 支持连接多个 Kafka Broker
2. **负载均衡**: 在多个上游之间分发请求
3. **认证代理**: 处理 SASL 认证
4. **消息过滤**: 基于规则过滤消息
5. **消息转换**: 修改消息内容
6. **监控集成**: Prometheus metrics 导出

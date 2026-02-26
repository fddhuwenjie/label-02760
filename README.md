# Kafka Relay

一个 Rust 实现的 Kafka 协议代理/中继服务，能够伪装成 Kafka Server，接收 Kafka Client 连接，解析 Kafka 协议数据进行处理后转发到上游 Kafka Broker。

## 文档

- [API 文档](docs/API.md) - 详细的 API 接口和协议格式说明
- [协议实现说明](docs/PROTOCOL.md) - 代码结构和实现细节
- [架构设计](docs/ARCHITECTURE.md) - 系统架构和设计决策

## How to Run

### 快速测试

```bash
# 运行测试脚本（自动检测环境、安装依赖、启动服务并测试）
./run.sh

# 查看代理日志
./run.sh logs

# 停止服务
./run.sh stop
```

run.sh 脚本功能：
- 自动检测操作系统（Mac/Linux/Windows）
- 检查并提示安装 Docker 和 Docker Compose
- 启动所有服务（Zookeeper、Kafka、Kafka-Relay）
- 等待服务就绪
- 自动创建测试 topic，发送和消费消息验证代理功能
- 显示代理日志

### Docker 启动

```bash
# 启动所有服务
docker-compose up --build -d

# 查看日志
docker-compose logs -f kafka-relay

# 停止服务
docker-compose down
```

### 本地启动

1. 安装 Rust:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

2. 编译运行:
```bash
cd backend
cargo run --release -- \
  --listen-host 127.0.0.1 \
  --listen-port 9092 \
  --advertised-host localhost \
  --advertised-port 9092 \
  --upstream-host localhost \
  --upstream-port 9093
```

## Services

| 服务 | 端口 | 说明 |
|------|------|------|
| kafka-relay | 9092 | Kafka 代理服务，接收客户端连接 |
| kafka | 9093 | 上游 Kafka Broker |
| zookeeper | 2181 | Kafka 依赖的 Zookeeper |

## 测试账号

本项目无需认证，直接连接即可。

测试连接:
```bash
# 使用 kafka-console-producer
kafka-console-producer --broker-list localhost:9092 --topic test

# 使用 kafka-console-consumer
kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning
```

## 题目内容

创建一个 Rust 项目，名字是 kafka-relay。作用是把自己伪装成 Kafka Server，支持 Kafka Client 连接过来，然后解析 Kafka 数据进行简单处理后再转发。请仔细阅读 Kafka 协议，选择一些合适版本的 Kafka 通信协议进行实现。不需要实现所有 Kafka 通信协议的不同版本，Kafka 协议支持双向协商兼容。

---

## 功能特性

- **协议解析**: 完整解析 Kafka 请求/响应协议，支持多版本协商
- **请求处理**: 解析并记录 Produce、Fetch、Metadata 等核心请求的详细信息
- **Broker 地址重写**: 自动重写 Metadata 响应中的 Broker 地址，使客户端始终连接代理
- **统计监控**: 实时统计请求数量、字节流量等指标，每分钟输出统计报告
- **错误处理**: 完善的错误处理和上下文信息，便于问题定位
- **连接管理**: 支持连接超时、消息大小限制等安全机制

## 支持的功能范围

### 已支持
- 基本的 Kafka 协议代理转发
- ApiVersions 版本协商（本地处理）
- Metadata 响应 Broker 地址重写
- Produce/Fetch/Metadata 请求解析和日志记录
- 多版本协议兼容（见下方版本支持表）

### 暂不支持
- SASL/SSL 安全认证（请求会透传但不会处理认证逻辑）
- 事务支持（事务相关请求会透传但不会特殊处理）
- ACL 权限控制
- 配额管理

## 配置参数

| 参数 | 环境变量 | 默认值 | 说明 |
|------|---------|--------|------|
| --listen-host | LISTEN_HOST | 0.0.0.0 | 监听地址 |
| --listen-port | LISTEN_PORT | 9092 | 监听端口 |
| --advertised-host | ADVERTISED_HOST | (同 listen-host) | Metadata 响应中的 Broker 地址 |
| --advertised-port | ADVERTISED_PORT | (同 listen-port) | Metadata 响应中的 Broker 端口 |
| --upstream-host | UPSTREAM_HOST | localhost | 上游 Kafka 地址 |
| --upstream-port | UPSTREAM_PORT | 9093 | 上游 Kafka 端口 |

## 实现说明

### 协议版本支持

项目实现了 Kafka 协议的双向协商兼容，通过 ApiVersions 请求告知客户端支持的 API 版本范围：

| API | 版本范围 | 说明 |
|-----|---------|------|
| Produce | v0-v8 | 生产消息 |
| Fetch | v0-v11 | 消费消息 |
| ListOffsets | v0-v5 | 查询偏移量 |
| Metadata | v0-v8 | 获取集群元数据 |
| OffsetCommit | v0-v7 | 提交消费偏移量 |
| OffsetFetch | v0-v5 | 获取消费偏移量 |
| FindCoordinator | v0-v2 | 查找协调者 |
| JoinGroup | v0-v5 | 加入消费组 |
| Heartbeat | v0-v3 | 心跳 |
| LeaveGroup | v0-v3 | 离开消费组 |
| SyncGroup | v0-v3 | 同步消费组 |
| DescribeGroups | v0-v4 | 描述消费组 |
| ListGroups | v0-v2 | 列出消费组 |
| SaslHandshake | v0-v1 | SASL 握手 |
| ApiVersions | v0-v3 | API 版本协商 |
| CreateTopics | v0-v4 | 创建主题 |
| DeleteTopics | v0-v3 | 删除主题 |
| InitProducerId | v0-v3 | 初始化生产者 ID |
| SaslAuthenticate | v0-v1 | SASL 认证 |

### 版本协商机制

当 Kafka 客户端连接到代理时，会首先发送 ApiVersions 请求来了解服务端支持的 API 版本。代理会：

1. **本地处理 ApiVersions 请求**: 直接返回代理支持的版本列表，不转发到上游
2. **版本兼容**: 客户端根据返回的版本范围选择合适的协议版本进行通信
3. **透明转发**: 其他请求按照协商后的版本格式解析并转发到上游 Kafka

### Broker 地址重写

当客户端请求 Metadata 时，上游 Kafka 会返回集群中所有 Broker 的真实地址。代理会：

1. 解析 Metadata 响应中的 Broker 列表
2. 将所有 Broker 的 host:port 替换为代理的 advertised 地址
3. 客户端后续请求都会发送到代理，由代理转发到上游

### 监控统计

代理每分钟输出统计信息：
- 总请求数/响应数
- Produce/Fetch/Metadata 请求数
- 客户端入站/出站字节数

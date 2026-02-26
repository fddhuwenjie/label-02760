# Kafka Relay API 文档

本文档详细说明 Kafka Relay 代理服务的 API 接口和协议实现。

## 目录

- [概述](#概述)
- [协议基础](#协议基础)
- [请求/响应格式](#请求响应格式)
- [支持的 API](#支持的-api)
- [版本协商机制](#版本协商机制)
- [Broker 地址重写](#broker-地址重写)

## 概述

Kafka Relay 是一个 Kafka 协议代理，它：
1. 监听客户端连接，伪装成 Kafka Broker
2. 解析 Kafka 协议请求，记录详细信息
3. 转发请求到上游 Kafka Broker
4. 处理响应，必要时重写 Broker 地址
5. 返回响应给客户端

## 协议基础

### 消息帧格式

所有 Kafka 消息都使用以下帧格式：

```
+----------------+------------------+
| Message Size   | Message Content  |
| (4 bytes, BE)  | (variable)       |
+----------------+------------------+
```

- **Message Size**: 32位大端整数，表示后续内容的字节数（不包含自身）
- **Message Content**: 请求或响应的实际内容

### 数据类型

| 类型 | 描述 | 编码方式 |
|------|------|----------|
| INT8 | 8位有符号整数 | 1字节 |
| INT16 | 16位有符号整数 | 2字节，大端 |
| INT32 | 32位有符号整数 | 4字节，大端 |
| INT64 | 64位有符号整数 | 8字节，大端 |
| STRING | 字符串 | INT16长度 + UTF-8字节 |
| NULLABLE_STRING | 可空字符串 | 长度-1表示null |
| BYTES | 字节数组 | INT32长度 + 字节 |
| ARRAY | 数组 | INT32长度 + 元素 |
| COMPACT_STRING | 紧凑字符串 | VARINT长度+1 + UTF-8字节 |
| COMPACT_ARRAY | 紧凑数组 | VARINT长度+1 + 元素 |
| VARINT | 变长整数 | 每字节7位数据，最高位为继续标志 |
| TAGGED_FIELDS | 标签字段 | VARINT数量 + (tag + size + data)* |

### 请求头格式

请求头有三个版本：

**Header v0** (仅用于 ApiVersions):
```
+----------+-------------+----------------+
| api_key  | api_version | correlation_id |
| (INT16)  | (INT16)     | (INT32)        |
+----------+-------------+----------------+
```

**Header v1** (传统格式):
```
+----------+-------------+----------------+-----------+
| api_key  | api_version | correlation_id | client_id |
| (INT16)  | (INT16)     | (INT32)        | (STRING)  |
+----------+-------------+----------------+-----------+
```

**Header v2** (灵活格式，用于较新的 API 版本):
```
+----------+-------------+----------------+-----------------+---------------+
| api_key  | api_version | correlation_id | client_id       | tagged_fields |
| (INT16)  | (INT16)     | (INT32)        | (COMPACT_STRING)| (TAGGED)      |
+----------+-------------+----------------+-----------------+---------------+
```

### 响应头格式

**Header v0**:
```
+----------------+
| correlation_id |
| (INT32)        |
+----------------+
```

**Header v1** (灵活格式):
```
+----------------+---------------+
| correlation_id | tagged_fields |
| (INT32)        | (TAGGED)      |
+----------------+---------------+
```

## 请求响应格式

### 请求处理流程

```
Client                    Kafka Relay                 Upstream Kafka
  |                            |                            |
  |--- Request (size+data) --->|                            |
  |                            |-- Parse & Log Request -----|
  |                            |                            |
  |                            |--- Forward Request ------->|
  |                            |                            |
  |                            |<-- Response ---------------|
  |                            |                            |
  |                            |-- Process Response --------|
  |                            |   (rewrite if needed)      |
  |<-- Response ---------------|                            |
```

### 特殊处理

1. **ApiVersions 请求**: 本地处理，不转发到上游
2. **Metadata 响应**: 重写 Broker 地址为代理地址

## 支持的 API

### API 版本支持表

| API Key | API 名称 | 支持版本 | 说明 |
|---------|----------|----------|------|
| 0 | Produce | v0-v8 | 生产消息到 Topic |
| 1 | Fetch | v0-v11 | 从 Topic 消费消息 |
| 2 | ListOffsets | v0-v5 | 查询分区偏移量 |
| 3 | Metadata | v0-v8 | 获取集群元数据 |
| 8 | OffsetCommit | v0-v7 | 提交消费者偏移量 |
| 9 | OffsetFetch | v0-v5 | 获取已提交的偏移量 |
| 10 | FindCoordinator | v0-v2 | 查找组协调者 |
| 11 | JoinGroup | v0-v5 | 加入消费者组 |
| 12 | Heartbeat | v0-v3 | 消费者组心跳 |
| 13 | LeaveGroup | v0-v3 | 离开消费者组 |
| 14 | SyncGroup | v0-v3 | 同步消费者组状态 |
| 15 | DescribeGroups | v0-v4 | 描述消费者组 |
| 16 | ListGroups | v0-v2 | 列出所有消费者组 |
| 17 | SaslHandshake | v0-v1 | SASL 握手 |
| 18 | ApiVersions | v0-v3 | API 版本协商 |
| 19 | CreateTopics | v0-v4 | 创建 Topic |
| 20 | DeleteTopics | v0-v3 | 删除 Topic |
| 22 | InitProducerId | v0-v3 | 初始化生产者 ID |
| 36 | SaslAuthenticate | v0-v1 | SASL 认证 |

### 灵活版本起始点

不同 API 从特定版本开始使用灵活格式（Header v2）：

| API | 灵活版本起始 |
|-----|-------------|
| Produce | v9 |
| Fetch | v12 |
| Metadata | v9 |
| OffsetCommit | v8 |
| OffsetFetch | v6 |
| FindCoordinator | v3 |
| JoinGroup | v6 |
| Heartbeat | v4 |
| LeaveGroup | v4 |
| SyncGroup | v4 |
| DescribeGroups | v5 |
| ListGroups | v3 |
| CreateTopics | v5 |
| DeleteTopics | v4 |
| SaslAuthenticate | v2 |
| ListOffsets | v6 |
| SaslHandshake | 永不使用灵活格式 |

## 版本协商机制

### 协商流程

```
Client                         Kafka Relay
  |                                 |
  |--- ApiVersions Request -------->|
  |                                 |
  |<-- ApiVersions Response --------|
  |    (supported API versions)     |
  |                                 |
  |--- Other Requests ------------->|
  |    (using negotiated versions)  |
```

### ApiVersions 响应格式

**v0**:
```
+------------+------------------+
| error_code | api_versions[]   |
| (INT16)    | (ARRAY)          |
+------------+------------------+

api_version 元素:
+----------+-------------+-------------+
| api_key  | min_version | max_version |
| (INT16)  | (INT16)     | (INT16)     |
+----------+-------------+-------------+
```

**v1-v2**:
```
+------------+------------------+------------------+
| error_code | api_versions[]   | throttle_time_ms |
| (INT16)    | (ARRAY)          | (INT32)          |
+------------+------------------+------------------+
```

**v3** (灵活格式):
```
+------------+----------------------+------------------+---------------+
| error_code | api_versions[]       | throttle_time_ms | tagged_fields |
| (INT16)    | (COMPACT_ARRAY)      | (INT32)          | (TAGGED)      |
+------------+----------------------+------------------+---------------+

api_version 元素 (v3):
+----------+-------------+-------------+---------------+
| api_key  | min_version | max_version | tagged_fields |
| (INT16)  | (INT16)     | (INT16)     | (TAGGED)      |
+----------+-------------+-------------+---------------+
```

## Broker 地址重写

### 重写原理

当客户端请求 Metadata 时，上游 Kafka 返回的响应包含所有 Broker 的真实地址。代理需要将这些地址重写为自己的地址，确保客户端后续请求都发送到代理。

### Metadata 响应结构

**v0-v8** (传统格式):
```
+------------------+------------+---------------+-------------+--------+
| throttle_time_ms | brokers[]  | cluster_id    | controller  | topics |
| (v3+, INT32)     | (ARRAY)    | (v2+, STRING) | (v1+, INT32)| (ARRAY)|
+------------------+------------+---------------+-------------+--------+

broker 元素:
+---------+--------+------+------+
| node_id | host   | port | rack |
| (INT32) | (STR)  |(INT32)|(v1+) |
+---------+--------+------+------+
```

**v9+** (灵活格式):
```
+------------------+------------------+-------------------+-------------+------------------+
| throttle_time_ms | brokers[]        | cluster_id        | controller  | topics[]         |
| (INT32)          | (COMPACT_ARRAY)  | (COMPACT_STRING)  | (INT32)     | (COMPACT_ARRAY)  |
+------------------+------------------+-------------------+-------------+------------------+
```

### 重写示例

原始响应:
```
brokers: [
  { node_id: 1, host: "kafka-1.internal", port: 9092 },
  { node_id: 2, host: "kafka-2.internal", port: 9092 }
]
```

重写后:
```
brokers: [
  { node_id: 1, host: "proxy.example.com", port: 9092 },
  { node_id: 2, host: "proxy.example.com", port: 9092 }
]
```

## 详细 API 说明

### Produce API (Key=0)

生产消息到指定 Topic 的分区。

**请求格式 (v0-v8)**:
```
+------------------+------+------------+----------+
| transactional_id | acks | timeout_ms | topics[] |
| (v3+, STRING)    |(INT16)| (INT32)   | (ARRAY)  |
+------------------+------+------------+----------+

topic 元素:
+------+-------------+
| name | partitions[]|
| (STR)| (ARRAY)     |
+------+-------------+

partition 元素:
+-------+---------+
| index | records |
|(INT32)| (BYTES) |
+-------+---------+
```

**参数说明**:
- `transactional_id`: 事务 ID（v3+，用于事务性生产者）
- `acks`: 确认级别（0=不等待，1=Leader确认，-1=所有ISR确认）
- `timeout_ms`: 请求超时时间
- `topics`: 要写入的 Topic 列表
- `records`: 消息记录批次（RecordBatch 格式）

### Fetch API (Key=1)

从指定 Topic 分区消费消息。

**请求格式 (v0-v11)**:
```
+------------+-------------+-----------+-----------+-----------------+
| replica_id | max_wait_ms | min_bytes | max_bytes | isolation_level |
| (INT32)    | (INT32)     | (INT32)   | (v3+)     | (v4+, INT8)     |
+------------+-------------+-----------+-----------+-----------------+
+------------+---------------+----------+------------------+----------+
| session_id | session_epoch | topics[] | forgotten_topics | rack_id  |
| (v7+)      | (v7+)         | (ARRAY)  | (v7+, ARRAY)     | (v11+)   |
+------------+---------------+----------+------------------+----------+

topic 元素:
+------+-------------+
| name | partitions[]|
| (STR)| (ARRAY)     |
+------+-------------+

partition 元素:
+-----------+--------------------+--------------+------------------+---------------------+
| partition | current_leader_epoch| fetch_offset | log_start_offset | partition_max_bytes |
| (INT32)   | (v9+, INT32)       | (INT64)      | (v5+, INT64)     | (INT32)             |
+-----------+--------------------+--------------+------------------+---------------------+
```

**参数说明**:
- `replica_id`: 副本 ID（-1 表示普通消费者）
- `max_wait_ms`: 最大等待时间
- `min_bytes`: 最小返回字节数
- `max_bytes`: 最大返回字节数
- `isolation_level`: 隔离级别（0=READ_UNCOMMITTED, 1=READ_COMMITTED）
- `session_id/session_epoch`: 增量 Fetch 会话（v7+）
- `fetch_offset`: 开始消费的偏移量

### Metadata API (Key=3)

获取集群元数据信息。

**请求格式**:
```
+----------+
| topics[] |
| (ARRAY)  |
+----------+
```

空数组表示请求所有 Topic 的元数据。

**响应格式** (简化):
```
+------------------+------------+------------+-------------+----------+
| throttle_time_ms | brokers[]  | cluster_id | controller  | topics[] |
+------------------+------------+------------+-------------+----------+
```

## 错误码

| 错误码 | 名称 | 说明 |
|--------|------|------|
| 0 | NONE | 无错误 |
| 1 | OFFSET_OUT_OF_RANGE | 偏移量超出范围 |
| 2 | CORRUPT_MESSAGE | 消息损坏 |
| 3 | UNKNOWN_TOPIC_OR_PARTITION | 未知 Topic 或分区 |
| 5 | LEADER_NOT_AVAILABLE | Leader 不可用 |
| 6 | NOT_LEADER_FOR_PARTITION | 不是分区的 Leader |
| 7 | REQUEST_TIMED_OUT | 请求超时 |
| 35 | UNSUPPORTED_VERSION | 不支持的版本 |

## 参考资料

- [Kafka Protocol Guide](https://kafka.apache.org/protocol)
- [KIP-482: Flexible Versions](https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields)

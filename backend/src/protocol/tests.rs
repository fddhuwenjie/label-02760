#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use std::io::Cursor;
    use crate::protocol::types::*;
    use crate::protocol::api_versions::*;
    use crate::protocol::metadata::*;
    use crate::protocol::request::*;

    #[test]
    fn test_read_write_i32() {
        let mut buf = BytesMut::new();
        write_i32(&mut buf, 12345);
        write_i32(&mut buf, -9999);
        
        let mut cursor = Cursor::new(buf.as_ref());
        assert_eq!(read_i32(&mut cursor).unwrap(), 12345);
        assert_eq!(read_i32(&mut cursor).unwrap(), -9999);
    }

    #[test]
    fn test_read_write_string() {
        let mut buf = BytesMut::new();
        write_string(&mut buf, "hello");
        write_nullable_string(&mut buf, None);
        write_nullable_string(&mut buf, Some("world"));
        
        let mut cursor = Cursor::new(buf.as_ref());
        assert_eq!(read_string(&mut cursor).unwrap(), "hello");
        assert_eq!(read_nullable_string(&mut cursor).unwrap(), None);
        assert_eq!(read_nullable_string(&mut cursor).unwrap(), Some("world".to_string()));
    }

    #[test]
    fn test_read_write_varint() {
        let mut buf = BytesMut::new();
        write_unsigned_varint(&mut buf, 0);
        write_unsigned_varint(&mut buf, 127);
        write_unsigned_varint(&mut buf, 128);
        write_unsigned_varint(&mut buf, 16383);
        write_unsigned_varint(&mut buf, 16384);
        
        let mut cursor = Cursor::new(buf.as_ref());
        assert_eq!(read_unsigned_varint(&mut cursor).unwrap(), 0);
        assert_eq!(read_unsigned_varint(&mut cursor).unwrap(), 127);
        assert_eq!(read_unsigned_varint(&mut cursor).unwrap(), 128);
        assert_eq!(read_unsigned_varint(&mut cursor).unwrap(), 16383);
        assert_eq!(read_unsigned_varint(&mut cursor).unwrap(), 16384);
    }

    #[test]
    fn test_read_write_compact_string() {
        let mut buf = BytesMut::new();
        write_compact_string(&mut buf, "test");
        write_compact_nullable_string(&mut buf, None);
        write_compact_nullable_string(&mut buf, Some("kafka"));
        
        let mut cursor = Cursor::new(buf.as_ref());
        assert_eq!(read_compact_string(&mut cursor).unwrap(), "test");
        assert_eq!(read_compact_nullable_string(&mut cursor).unwrap(), None);
        assert_eq!(read_compact_nullable_string(&mut cursor).unwrap(), Some("kafka".to_string()));
    }

    #[test]
    fn test_read_write_array() {
        let mut buf = BytesMut::new();
        let items = vec![1i32, 2, 3, 4, 5];
        write_array(&mut buf, &items, |buf, &item| write_i32(buf, item));
        
        let mut cursor = Cursor::new(buf.as_ref());
        let result = read_array(&mut cursor, |buf| read_i32(buf)).unwrap();
        assert_eq!(result, items);
    }

    #[test]
    fn test_read_write_compact_array() {
        let mut buf = BytesMut::new();
        let items = vec![10i32, 20, 30];
        write_compact_array(&mut buf, &items, |buf, &item| write_i32(buf, item));
        
        let mut cursor = Cursor::new(buf.as_ref());
        let result = read_compact_array(&mut cursor, |buf| read_i32(buf)).unwrap();
        assert_eq!(result, items);
    }

    #[test]
    fn test_skip_tagged_fields() {
        let mut buf = BytesMut::new();
        write_empty_tagged_fields(&mut buf);
        
        let mut cursor = Cursor::new(buf.as_ref());
        assert!(skip_tagged_fields(&mut cursor).is_ok());
        assert_eq!(cursor.position(), 1); // 1 byte for varint 0
    }

    #[test]
    fn test_api_versions_response_v0() {
        let response = build_api_versions_response(0);
        assert!(!response.is_empty());
        
        let mut cursor = Cursor::new(response.as_ref());
        let error_code = read_i16(&mut cursor).unwrap();
        assert_eq!(error_code, 0);
    }

    #[test]
    fn test_api_versions_response_v3() {
        let response = build_api_versions_response_v3(0, 0);
        assert!(!response.is_empty());
        
        let mut cursor = Cursor::new(response.as_ref());
        let error_code = read_i16(&mut cursor).unwrap();
        assert_eq!(error_code, 0);
    }

    #[test]
    fn test_parse_request_header() {
        // Build a simple request: api_key=3 (Metadata), version=8, correlation_id=1, client_id="test"
        let mut buf = BytesMut::new();
        write_i16(&mut buf, 3);  // api_key
        write_i16(&mut buf, 8);  // api_version
        write_i32(&mut buf, 1);  // correlation_id
        write_nullable_string(&mut buf, Some("test-client"));  // client_id
        
        let request = KafkaRequest::parse(buf.as_ref()).unwrap();
        assert_eq!(request.header.api_key, 3);
        assert_eq!(request.header.api_version, 8);
        assert_eq!(request.header.correlation_id, 1);
        assert_eq!(request.header.client_id, Some("test-client".to_string()));
    }

    #[test]
    fn test_metadata_response_rewrite() {
        let response = MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![
                BrokerInfo {
                    node_id: 1,
                    host: "kafka-1".to_string(),
                    port: 9092,
                    rack: None,
                },
                BrokerInfo {
                    node_id: 2,
                    host: "kafka-2".to_string(),
                    port: 9092,
                    rack: Some("rack-1".to_string()),
                },
            ],
            cluster_id: Some("test-cluster".to_string()),
            controller_id: 1,
            topics: vec![],
            cluster_authorized_operations: -2147483648,
        };
        
        let rewritten = build_metadata_response_rewritten(8, &response, "proxy", 9092);
        assert!(!rewritten.is_empty());
        
        // Parse it back
        let mut cursor = Cursor::new(rewritten.as_ref());
        let parsed = parse_metadata_response(&mut cursor, 8).unwrap();
        
        // Verify brokers are rewritten
        assert_eq!(parsed.brokers.len(), 2);
        for broker in &parsed.brokers {
            assert_eq!(broker.host, "proxy");
            assert_eq!(broker.port, 9092);
        }
        assert_eq!(parsed.cluster_id, Some("test-cluster".to_string()));
    }

    #[test]
    fn test_metadata_response_v8_roundtrip() {
        let original = MetadataResponse {
            throttle_time_ms: 100,
            brokers: vec![BrokerInfo {
                node_id: 0,
                host: "localhost".to_string(),
                port: 9093,
                rack: None,
            }],
            cluster_id: Some("cluster-1".to_string()),
            controller_id: 0,
            topics: vec![TopicInfo {
                error_code: 0,
                name: "test-topic".to_string(),
                is_internal: false,
                partitions: vec![PartitionInfo {
                    error_code: 0,
                    partition_index: 0,
                    leader_id: 0,
                    leader_epoch: 1,
                    replica_nodes: vec![0],
                    isr_nodes: vec![0],
                    offline_replicas: vec![],
                }],
                topic_authorized_operations: -2147483648,
            }],
            cluster_authorized_operations: -2147483648,
        };
        
        let encoded = build_metadata_response_rewritten(8, &original, "localhost", 9093);
        let mut cursor = Cursor::new(encoded.as_ref());
        let decoded = parse_metadata_response(&mut cursor, 8).unwrap();
        
        assert_eq!(decoded.throttle_time_ms, original.throttle_time_ms);
        assert_eq!(decoded.brokers.len(), original.brokers.len());
        assert_eq!(decoded.topics.len(), original.topics.len());
        assert_eq!(decoded.topics[0].name, "test-topic");
        assert_eq!(decoded.topics[0].partitions.len(), 1);
    }
}

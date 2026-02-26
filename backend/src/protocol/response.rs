use bytes::{Bytes, BytesMut};
use std::io::{self, Cursor};
use super::types::*;

/// Kafka Response Header (v0)
#[derive(Debug, Clone)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}

impl ResponseHeader {
    pub fn parse(buf: &mut Cursor<&[u8]>) -> io::Result<Self> {
        let correlation_id = read_i32(buf)?;
        Ok(Self { correlation_id })
    }

    /// Encode header to bytes (kept for future response modification)
    #[allow(dead_code)]
    pub fn encode(&self, buf: &mut BytesMut) {
        write_i32(buf, self.correlation_id);
    }
}

/// A complete Kafka response with header and body
#[derive(Debug, Clone)]
pub struct KafkaResponse {
    pub header: ResponseHeader,
    pub body: Bytes,
}

impl KafkaResponse {
    /// Parse a response from raw bytes (excluding the 4-byte size prefix)
    pub fn parse(data: &[u8]) -> io::Result<Self> {
        let mut cursor = Cursor::new(data);
        let header = ResponseHeader::parse(&mut cursor)?;
        let pos = cursor.position() as usize;
        let body = Bytes::copy_from_slice(&data[pos..]);

        Ok(Self { header, body })
    }

    /// Encode the response back to bytes (kept for future response modification)
    #[allow(dead_code)]
    pub fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::new();
        self.header.encode(&mut buf);
        buf.extend_from_slice(&self.body);
        buf
    }

    /// Encode with size prefix (kept for future response modification)
    #[allow(dead_code)]
    pub fn encode_with_size(&self) -> BytesMut {
        let inner = self.encode();
        let mut buf = BytesMut::with_capacity(4 + inner.len());
        write_i32(&mut buf, inner.len() as i32);
        buf.extend_from_slice(&inner);
        buf
    }
}

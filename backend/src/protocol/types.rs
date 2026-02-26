use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::{self, Cursor};

/// Read a big-endian i8
pub fn read_i8(buf: &mut Cursor<&[u8]>) -> io::Result<i8> {
    if buf.remaining() < 1 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "not enough bytes for i8"));
    }
    Ok(buf.get_i8())
}

/// Read a big-endian i16
pub fn read_i16(buf: &mut Cursor<&[u8]>) -> io::Result<i16> {
    if buf.remaining() < 2 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "not enough bytes for i16"));
    }
    Ok(buf.get_i16())
}

/// Read a big-endian i32
pub fn read_i32(buf: &mut Cursor<&[u8]>) -> io::Result<i32> {
    if buf.remaining() < 4 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "not enough bytes for i32"));
    }
    Ok(buf.get_i32())
}

/// Read a big-endian i64
pub fn read_i64(buf: &mut Cursor<&[u8]>) -> io::Result<i64> {
    if buf.remaining() < 8 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "not enough bytes for i64"));
    }
    Ok(buf.get_i64())
}

/// Read a nullable string (length-prefixed with i16, -1 means null)
pub fn read_nullable_string(buf: &mut Cursor<&[u8]>) -> io::Result<Option<String>> {
    let len = read_i16(buf)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    if buf.remaining() < len {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "not enough bytes for string"));
    }
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Read a string (length-prefixed with i16)
pub fn read_string(buf: &mut Cursor<&[u8]>) -> io::Result<String> {
    read_nullable_string(buf)?.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unexpected null string"))
}

/// Read bytes (length-prefixed with i32)
pub fn read_bytes(buf: &mut Cursor<&[u8]>) -> io::Result<Option<Bytes>> {
    let len = read_i32(buf)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    if buf.remaining() < len {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "not enough bytes"));
    }
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    Ok(Some(Bytes::from(bytes)))
}

/// Read unsigned varint (used in flexible versions)
pub fn read_unsigned_varint(buf: &mut Cursor<&[u8]>) -> io::Result<u32> {
    let mut value: u32 = 0;
    let mut shift = 0;
    loop {
        if buf.remaining() < 1 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "not enough bytes for varint"));
        }
        let byte = buf.get_u8();
        value |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 28 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "varint too long"));
        }
    }
    Ok(value)
}

/// Write unsigned varint
pub fn write_unsigned_varint(buf: &mut BytesMut, mut value: u32) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.put_u8(byte);
        if value == 0 {
            break;
        }
    }
}

/// Read compact nullable string (flexible versions - varint length, 0 means null)
pub fn read_compact_nullable_string(buf: &mut Cursor<&[u8]>) -> io::Result<Option<String>> {
    let len = read_unsigned_varint(buf)?;
    if len == 0 {
        return Ok(None);
    }
    let len = (len - 1) as usize; // length is encoded as actual_length + 1
    if buf.remaining() < len {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "not enough bytes for compact string"));
    }
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Read compact string (flexible versions)
pub fn read_compact_string(buf: &mut Cursor<&[u8]>) -> io::Result<String> {
    read_compact_nullable_string(buf)?.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unexpected null compact string"))
}

/// Write compact nullable string
pub fn write_compact_nullable_string(buf: &mut BytesMut, value: Option<&str>) {
    match value {
        Some(s) => {
            write_unsigned_varint(buf, (s.len() + 1) as u32);
            buf.put_slice(s.as_bytes());
        }
        None => {
            write_unsigned_varint(buf, 0);
        }
    }
}

/// Write compact string
pub fn write_compact_string(buf: &mut BytesMut, value: &str) {
    write_compact_nullable_string(buf, Some(value));
}

/// Read compact array (flexible versions - varint length)
pub fn read_compact_array<T, F>(buf: &mut Cursor<&[u8]>, reader: F) -> io::Result<Vec<T>>
where
    F: Fn(&mut Cursor<&[u8]>) -> io::Result<T>,
{
    let len = read_unsigned_varint(buf)?;
    if len == 0 {
        return Ok(Vec::new());
    }
    let len = (len - 1) as usize; // length is encoded as actual_length + 1
    let mut result = Vec::with_capacity(len);
    for _ in 0..len {
        result.push(reader(buf)?);
    }
    Ok(result)
}

/// Write compact array
pub fn write_compact_array<T, F>(buf: &mut BytesMut, items: &[T], writer: F)
where
    F: Fn(&mut BytesMut, &T),
{
    write_unsigned_varint(buf, (items.len() + 1) as u32);
    for item in items {
        writer(buf, item);
    }
}

/// Skip tagged fields (flexible versions)
pub fn skip_tagged_fields(buf: &mut Cursor<&[u8]>) -> io::Result<()> {
    let num_fields = read_unsigned_varint(buf)?;
    for _ in 0..num_fields {
        let _tag = read_unsigned_varint(buf)?;
        let size = read_unsigned_varint(buf)?;
        if buf.remaining() < size as usize {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "not enough bytes for tagged field"));
        }
        buf.advance(size as usize);
    }
    Ok(())
}

/// Write empty tagged fields
pub fn write_empty_tagged_fields(buf: &mut BytesMut) {
    write_unsigned_varint(buf, 0);
}

/// Write a big-endian i16
pub fn write_i16(buf: &mut BytesMut, value: i16) {
    buf.put_i16(value);
}

/// Write a big-endian i32
pub fn write_i32(buf: &mut BytesMut, value: i32) {
    buf.put_i32(value);
}

/// Write a big-endian i64
pub fn write_i64(buf: &mut BytesMut, value: i64) {
    buf.put_i64(value);
}

/// Write a nullable string
pub fn write_nullable_string(buf: &mut BytesMut, value: Option<&str>) {
    match value {
        Some(s) => {
            write_i16(buf, s.len() as i16);
            buf.put_slice(s.as_bytes());
        }
        None => {
            write_i16(buf, -1);
        }
    }
}

/// Write a string
pub fn write_string(buf: &mut BytesMut, value: &str) {
    write_nullable_string(buf, Some(value));
}

/// Write bytes
pub fn write_bytes(buf: &mut BytesMut, value: Option<&Bytes>) {
    match value {
        Some(b) => {
            write_i32(buf, b.len() as i32);
            buf.put_slice(b);
        }
        None => {
            write_i32(buf, -1);
        }
    }
}

/// Read an array with a custom reader function
pub fn read_array<T, F>(buf: &mut Cursor<&[u8]>, reader: F) -> io::Result<Vec<T>>
where
    F: Fn(&mut Cursor<&[u8]>) -> io::Result<T>,
{
    let len = read_i32(buf)?;
    if len < 0 {
        return Ok(Vec::new());
    }
    let len = len as usize;
    let mut result = Vec::with_capacity(len);
    for _ in 0..len {
        result.push(reader(buf)?);
    }
    Ok(result)
}

/// Write an array with a custom writer function
pub fn write_array<T, F>(buf: &mut BytesMut, items: &[T], writer: F)
where
    F: Fn(&mut BytesMut, &T),
{
    write_i32(buf, items.len() as i32);
    for item in items {
        writer(buf, item);
    }
}

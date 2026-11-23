//! Protocol v2 Consumer stream payloads.
//!
//! Every [`ConsumeMessage`] encodes exactly one Kafka record and carries an
//! epoch-scoped delivery identity. All counts are
//! validated before allocation; byte and message credits are explicit.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::payload::{MessageHeader, PayloadError, MAX_MESSAGE_HEADERS};

pub const RESUME_TOKEN_LEN: usize = 32;
const MAX_TOPICS: usize = 1024;
const MAX_CONFIG_ENTRIES: usize = 512;
const MAX_COMMITTED_OFFSETS: usize = 65_536;
const MAX_PARTITIONS: usize = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResumeToken(pub [u8; RESUME_TOKEN_LEN]);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumeOpen {
    pub cluster: String,
    pub group_id: String,
    pub topics: Vec<String>,
    pub config: Vec<(String, String)>,
    pub initial_message_credit: u32,
    pub initial_byte_credit: u64,
    pub max_in_flight_messages: u32,
    pub max_in_flight_bytes: u64,
    pub client_id: String,
}

impl ConsumeOpen {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        write_str(&self.cluster, buf)?;
        write_str(&self.group_id, buf)?;
        write_string_list(&self.topics, MAX_TOPICS, buf)?;
        write_pairs(&self.config, MAX_CONFIG_ENTRIES, buf)?;
        buf.put_u32(self.initial_message_credit);
        buf.put_u64(self.initial_byte_credit);
        buf.put_u32(self.max_in_flight_messages);
        buf.put_u64(self.max_in_flight_bytes);
        write_str(&self.client_id, buf)
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        let cluster = read_str(&mut buf, "cluster")?;
        let group_id = read_str(&mut buf, "group_id")?;
        let topics = read_string_list(&mut buf, MAX_TOPICS, "topics")?;
        let config = read_pairs(&mut buf, MAX_CONFIG_ENTRIES, "config")?;
        require(&buf, 4 + 8 + 4 + 8)?;
        let initial_message_credit = buf.get_u32();
        let initial_byte_credit = buf.get_u64();
        let max_in_flight_messages = buf.get_u32();
        let max_in_flight_bytes = buf.get_u64();
        let client_id = read_str(&mut buf, "client_id")?;
        Ok(Self {
            cluster,
            group_id,
            topics,
            config,
            initial_message_credit,
            initial_byte_credit,
            max_in_flight_messages,
            max_in_flight_bytes,
            client_id,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumeReady {
    pub subscription_id: u64,
    pub subscription_epoch: u64,
    pub resume_token: ResumeToken,
    pub max_in_flight_messages: u32,
    pub max_in_flight_bytes: u64,
}

impl ConsumeReady {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        buf.put_u64(self.subscription_id);
        buf.put_u64(self.subscription_epoch);
        buf.put_slice(&self.resume_token.0);
        buf.put_u32(self.max_in_flight_messages);
        buf.put_u64(self.max_in_flight_bytes);
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        require(&buf, 8 + 8 + RESUME_TOKEN_LEN + 4 + 8)?;
        let subscription_id = buf.get_u64();
        let subscription_epoch = buf.get_u64();
        let mut token = [0; RESUME_TOKEN_LEN];
        buf.copy_to_slice(&mut token);
        Ok(Self {
            subscription_id,
            subscription_epoch,
            resume_token: ResumeToken(token),
            max_in_flight_messages: buf.get_u32(),
            max_in_flight_bytes: buf.get_u64(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumeResume {
    pub subscription_id: u64,
    pub previous_epoch: u64,
    pub resume_token: ResumeToken,
    pub initial_message_credit: u32,
    pub initial_byte_credit: u64,
}

impl ConsumeResume {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        buf.put_u64(self.subscription_id);
        buf.put_u64(self.previous_epoch);
        buf.put_slice(&self.resume_token.0);
        buf.put_u32(self.initial_message_credit);
        buf.put_u64(self.initial_byte_credit);
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        require(&buf, 8 + 8 + RESUME_TOKEN_LEN + 4 + 8)?;
        let subscription_id = buf.get_u64();
        let previous_epoch = buf.get_u64();
        let mut token = [0; RESUME_TOKEN_LEN];
        buf.copy_to_slice(&mut token);
        Ok(Self {
            subscription_id,
            previous_epoch,
            resume_token: ResumeToken(token),
            initial_message_credit: buf.get_u32(),
            initial_byte_credit: buf.get_u64(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumeMessage {
    pub subscription_id: u64,
    pub subscription_epoch: u64,
    pub delivery_id: u64,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: i64,
    /// Kafka keys and payloads are nullable. `None` must remain distinguishable
    /// from an empty binary value (notably for compacted-topic tombstones).
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<MessageHeader>,
    pub wire_size: u64,
}

impl ConsumeMessage {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        buf.put_u64(self.subscription_id);
        buf.put_u64(self.subscription_epoch);
        buf.put_u64(self.delivery_id);
        write_str(&self.topic, buf)?;
        buf.put_i32(self.partition);
        buf.put_i64(self.offset);
        buf.put_i64(self.timestamp_ms);
        write_nullable_bytes(self.key.as_deref(), buf)?;
        write_nullable_bytes(self.value.as_deref(), buf)?;
        write_message_headers(&self.headers, buf)?;
        buf.put_u64(self.wire_size);
        Ok(())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, PayloadError> {
        Self::decode_buf(buf)
    }

    /// 从任意 [`Buf`] 解码。传入 [`Bytes`] 时 key / value / header 值是对整帧缓冲的
    /// 零拷贝切片：消费热路径上一条消息只保留一份 payload。
    pub fn decode_buf<B: Buf>(mut buf: B) -> Result<Self, PayloadError> {
        require(&buf, 24)?;
        let subscription_id = buf.get_u64();
        let subscription_epoch = buf.get_u64();
        let delivery_id = buf.get_u64();
        let topic = read_str(&mut buf, "topic")?;
        require(&buf, 4 + 8 + 8)?;
        let partition = buf.get_i32();
        let offset = buf.get_i64();
        let timestamp_ms = buf.get_i64();
        let key = read_nullable_bytes(&mut buf)?;
        let value = read_nullable_bytes(&mut buf)?;
        let headers = read_message_headers(&mut buf)?;
        require(&buf, 8)?;
        let wire_size = buf.get_u64();
        Ok(Self {
            subscription_id,
            subscription_epoch,
            delivery_id,
            topic,
            partition,
            offset,
            timestamp_ms,
            key,
            value,
            headers,
            wire_size,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumeAck {
    pub subscription_id: u64,
    pub subscription_epoch: u64,
    pub delivery_id: u64,
    pub message_credit: u32,
    pub byte_credit: u64,
}

impl ConsumeAck {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        put_identity(self.subscription_id, self.subscription_epoch, buf);
        buf.put_u64(self.delivery_id);
        buf.put_u32(self.message_credit);
        buf.put_u64(self.byte_credit);
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        require(&buf, 8 + 8 + 8 + 4 + 8)?;
        Ok(Self {
            subscription_id: buf.get_u64(),
            subscription_epoch: buf.get_u64(),
            delivery_id: buf.get_u64(),
            message_credit: buf.get_u32(),
            byte_credit: buf.get_u64(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumeNack {
    pub subscription_id: u64,
    pub subscription_epoch: u64,
    pub delivery_id: u64,
    pub reason: String,
}

impl ConsumeNack {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        put_identity(self.subscription_id, self.subscription_epoch, buf);
        buf.put_u64(self.delivery_id);
        write_str(&self.reason, buf)
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        require(&buf, 24)?;
        Ok(Self {
            subscription_id: buf.get_u64(),
            subscription_epoch: buf.get_u64(),
            delivery_id: buf.get_u64(),
            reason: read_str(&mut buf, "nack_reason")?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowControl {
    pub subscription_id: u64,
    pub subscription_epoch: u64,
    pub message_credit: u32,
    pub byte_credit: u64,
}

impl FlowControl {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        put_identity(self.subscription_id, self.subscription_epoch, buf);
        buf.put_u32(self.message_credit);
        buf.put_u64(self.byte_credit);
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        require(&buf, 8 + 8 + 4 + 8)?;
        Ok(Self {
            subscription_id: buf.get_u64(),
            subscription_epoch: buf.get_u64(),
            message_credit: buf.get_u32(),
            byte_credit: buf.get_u64(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumeCommit {
    pub subscription_id: u64,
    pub subscription_epoch: u64,
}

impl ConsumeCommit {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        put_identity(self.subscription_id, self.subscription_epoch, buf);
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        let (subscription_id, subscription_epoch) = read_identity(&mut buf)?;
        Ok(Self {
            subscription_id,
            subscription_epoch,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedOffset {
    pub topic: String,
    pub partition: i32,
    pub next_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumeCommitted {
    pub subscription_id: u64,
    pub subscription_epoch: u64,
    pub offsets: Vec<CommittedOffset>,
}

impl ConsumeCommitted {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        put_identity(self.subscription_id, self.subscription_epoch, buf);
        write_count(self.offsets.len(), MAX_COMMITTED_OFFSETS, buf)?;
        for offset in &self.offsets {
            write_str(&offset.topic, buf)?;
            buf.put_i32(offset.partition);
            buf.put_i64(offset.next_offset);
        }
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        let (subscription_id, subscription_epoch) = read_identity(&mut buf)?;
        let count = read_count(&mut buf, MAX_COMMITTED_OFFSETS)?;
        let mut offsets = Vec::with_capacity(count);
        for _ in 0..count {
            let topic = read_str(&mut buf, "topic")?;
            require(&buf, 12)?;
            offsets.push(CommittedOffset {
                topic,
                partition: buf.get_i32(),
                next_offset: buf.get_i64(),
            });
        }
        Ok(Self {
            subscription_id,
            subscription_epoch,
            offsets,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumeClose {
    pub subscription_id: u64,
    pub subscription_epoch: u64,
}

impl ConsumeClose {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        put_identity(self.subscription_id, self.subscription_epoch, buf);
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        let (subscription_id, subscription_epoch) = read_identity(&mut buf)?;
        Ok(Self {
            subscription_id,
            subscription_epoch,
        })
    }
}

/// Explicit partition control on the dedicated consumer stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerPartitions {
    pub subscription_id: u64,
    pub subscription_epoch: u64,
    pub partitions: Vec<(String, i32)>,
}

impl ConsumerPartitions {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        put_identity(self.subscription_id, self.subscription_epoch, buf);
        write_count(self.partitions.len(), MAX_PARTITIONS, buf)?;
        for (topic, partition) in &self.partitions {
            write_str(topic, buf)?;
            buf.put_i32(*partition);
        }
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        let (subscription_id, subscription_epoch) = read_identity(&mut buf)?;
        let count = read_count(&mut buf, MAX_PARTITIONS)?;
        let mut partitions = Vec::with_capacity(count);
        for _ in 0..count {
            let topic = read_str(&mut buf, "topic")?;
            require(&buf, 4)?;
            partitions.push((topic, buf.get_i32()));
        }
        Ok(Self {
            subscription_id,
            subscription_epoch,
            partitions,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebalanceNotice {
    pub subscription_id: u64,
    pub subscription_epoch: u64,
    pub partitions: Vec<(String, i32)>,
}

impl RebalanceNotice {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        put_identity(self.subscription_id, self.subscription_epoch, buf);
        write_count(self.partitions.len(), MAX_PARTITIONS, buf)?;
        for (topic, partition) in &self.partitions {
            write_str(topic, buf)?;
            buf.put_i32(*partition);
        }
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        let (subscription_id, subscription_epoch) = read_identity(&mut buf)?;
        let count = read_count(&mut buf, MAX_PARTITIONS)?;
        let mut partitions = Vec::with_capacity(count);
        for _ in 0..count {
            let topic = read_str(&mut buf, "topic")?;
            require(&buf, 4)?;
            partitions.push((topic, buf.get_i32()));
        }
        Ok(Self {
            subscription_id,
            subscription_epoch,
            partitions,
        })
    }
}

fn put_identity(subscription_id: u64, epoch: u64, buf: &mut BytesMut) {
    buf.put_u64(subscription_id);
    buf.put_u64(epoch);
}

fn read_identity(buf: &mut &[u8]) -> Result<(u64, u64), PayloadError> {
    require(buf, 16)?;
    Ok((buf.get_u64(), buf.get_u64()))
}

fn write_count(count: usize, max: usize, buf: &mut BytesMut) -> Result<(), PayloadError> {
    if count > max || count > u32::MAX as usize {
        return Err(PayloadError::FieldTooLarge(count));
    }
    buf.put_u32(count as u32);
    Ok(())
}

fn read_count<B: Buf>(buf: &mut B, max: usize) -> Result<usize, PayloadError> {
    require(buf, 4)?;
    let count = buf.get_u32() as usize;
    if count > max {
        return Err(PayloadError::FieldTooLarge(count));
    }
    Ok(count)
}

fn write_string_list(
    values: &[String],
    max: usize,
    buf: &mut BytesMut,
) -> Result<(), PayloadError> {
    write_count(values.len(), max, buf)?;
    for value in values {
        write_str(value, buf)?;
    }
    Ok(())
}

fn read_string_list(
    buf: &mut &[u8],
    max: usize,
    field: &'static str,
) -> Result<Vec<String>, PayloadError> {
    let count = read_count(buf, max)?;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(read_str(buf, field)?);
    }
    Ok(values)
}

fn write_pairs(
    values: &[(String, String)],
    max: usize,
    buf: &mut BytesMut,
) -> Result<(), PayloadError> {
    write_count(values.len(), max, buf)?;
    for (key, value) in values {
        write_str(key, buf)?;
        write_str(value, buf)?;
    }
    Ok(())
}

fn read_pairs(
    buf: &mut &[u8],
    max: usize,
    field: &'static str,
) -> Result<Vec<(String, String)>, PayloadError> {
    let count = read_count(buf, max)?;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push((read_str(buf, field)?, read_str(buf, field)?));
    }
    Ok(values)
}

fn write_str(value: &str, buf: &mut BytesMut) -> Result<(), PayloadError> {
    if value.len() > u16::MAX as usize {
        return Err(PayloadError::FieldTooLarge(value.len()));
    }
    buf.put_u16(value.len() as u16);
    buf.put_slice(value.as_bytes());
    Ok(())
}

fn read_str<B: Buf>(buf: &mut B, field: &'static str) -> Result<String, PayloadError> {
    require(buf, 2)?;
    let len = buf.get_u16() as usize;
    require(buf, len)?;
    let mut bytes = vec![0; len];
    buf.copy_to_slice(&mut bytes);
    String::from_utf8(bytes).map_err(|_| PayloadError::InvalidUtf8 { field })
}

fn write_bytes(value: &[u8], buf: &mut BytesMut) -> Result<(), PayloadError> {
    if value.len() >= u32::MAX as usize {
        return Err(PayloadError::FieldTooLarge(value.len()));
    }
    buf.put_u32(value.len() as u32);
    buf.put_slice(value);
    Ok(())
}

fn write_nullable_bytes(value: Option<&[u8]>, buf: &mut BytesMut) -> Result<(), PayloadError> {
    match value {
        Some(value) => write_bytes(value, buf),
        None => {
            buf.put_u32(u32::MAX);
            Ok(())
        }
    }
}

fn write_message_headers(
    headers: &[MessageHeader],
    buf: &mut BytesMut,
) -> Result<(), PayloadError> {
    write_count(headers.len(), MAX_MESSAGE_HEADERS, buf)?;
    for (name, value) in headers {
        write_str(name, buf)?;
        write_bytes(value, buf)?;
    }
    Ok(())
}

fn read_message_headers<B: Buf>(buf: &mut B) -> Result<Vec<MessageHeader>, PayloadError> {
    let count = read_count(buf, MAX_MESSAGE_HEADERS)?;
    let mut headers = Vec::with_capacity(count);
    for _ in 0..count {
        headers.push((read_str(buf, "header_name")?, read_bytes(buf)?));
    }
    Ok(headers)
}

fn read_bytes<B: Buf>(buf: &mut B) -> Result<Bytes, PayloadError> {
    require(buf, 4)?;
    let len = buf.get_u32() as usize;
    require(buf, len)?;
    Ok(buf.copy_to_bytes(len))
}

fn read_nullable_bytes<B: Buf>(buf: &mut B) -> Result<Option<Bytes>, PayloadError> {
    require(buf, 4)?;
    let len = buf.get_u32();
    if len == u32::MAX {
        return Ok(None);
    }
    let len = len as usize;
    require(buf, len)?;
    Ok(Some(buf.copy_to_bytes(len)))
}

fn require<B: Buf>(buf: &B, len: usize) -> Result<(), PayloadError> {
    if buf.remaining() < len {
        return Err(PayloadError::Truncated);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consume_message_decoded_from_bytes_borrows_the_frame_buffer() {
        let message = ConsumeMessage {
            subscription_id: 1,
            subscription_epoch: 2,
            delivery_id: 3,
            topic: "t".into(),
            partition: 0,
            offset: 9,
            timestamp_ms: 0,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from(vec![7u8; 4096])),
            headers: vec![("h".into(), Bytes::from_static(b"v"))],
            wire_size: 4200,
        };
        let mut buf = BytesMut::new();
        message.encode(&mut buf).unwrap();
        let frame = buf.freeze();
        let range = frame.as_ptr() as usize..frame.as_ptr() as usize + frame.len();

        let decoded = ConsumeMessage::decode_buf(frame.clone()).unwrap();

        assert_eq!(decoded, message);
        let value = decoded.value.unwrap();
        assert!(
            range.contains(&(value.as_ptr() as usize)),
            "value must be a zero-copy slice"
        );
        assert!(range.contains(&(decoded.headers[0].1.as_ptr() as usize)));
    }

    fn open() -> ConsumeOpen {
        ConsumeOpen {
            cluster: "default".into(),
            group_id: "workers".into(),
            topics: vec!["events".into()],
            config: vec![("auto.offset.reset".into(), "earliest".into())],
            initial_message_credit: 8,
            initial_byte_credit: 1024 * 1024,
            max_in_flight_messages: 32,
            max_in_flight_bytes: 4 * 1024 * 1024,
            client_id: "php-123".into(),
        }
    }

    #[test]
    fn consume_open_roundtrip() {
        let value = open();
        let mut buf = BytesMut::new();
        value.encode(&mut buf).unwrap();
        assert_eq!(ConsumeOpen::decode(&buf).unwrap(), value);
    }

    #[test]
    fn consume_ready_and_resume_roundtrip() {
        let ready = ConsumeReady {
            subscription_id: 7,
            subscription_epoch: 3,
            resume_token: ResumeToken([9; RESUME_TOKEN_LEN]),
            max_in_flight_messages: 16,
            max_in_flight_bytes: 4096,
        };
        let mut buf = BytesMut::new();
        ready.encode(&mut buf).unwrap();
        assert_eq!(ConsumeReady::decode(&buf).unwrap(), ready);

        let resume = ConsumeResume {
            subscription_id: 7,
            previous_epoch: 3,
            resume_token: ready.resume_token,
            initial_message_credit: 4,
            initial_byte_credit: 2048,
        };
        let mut buf = BytesMut::new();
        resume.encode(&mut buf).unwrap();
        assert_eq!(ConsumeResume::decode(&buf).unwrap(), resume);
    }

    #[test]
    fn single_message_roundtrip() {
        let value = ConsumeMessage {
            subscription_id: 7,
            subscription_epoch: 3,
            delivery_id: 99,
            topic: "events".into(),
            partition: 2,
            offset: 1234,
            timestamp_ms: 1_700_000_000_000,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from_static(b"value")),
            headers: vec![("traceparent".into(), Bytes::from_static(b"trace"))],
            wire_size: 128,
        };
        let mut buf = BytesMut::new();
        value.encode(&mut buf).unwrap();
        assert_eq!(ConsumeMessage::decode(&buf).unwrap(), value);
    }

    #[test]
    fn nullable_message_fields_distinguish_null_from_empty() {
        let tombstone = ConsumeMessage {
            subscription_id: 7,
            subscription_epoch: 3,
            delivery_id: 100,
            topic: "events".into(),
            partition: 2,
            offset: 1235,
            timestamp_ms: 1_700_000_000_001,
            key: Some(Bytes::new()),
            value: None,
            headers: Vec::new(),
            wire_size: 80,
        };
        let mut buf = BytesMut::new();
        tombstone.encode(&mut buf).unwrap();
        let decoded = ConsumeMessage::decode(&buf).unwrap();
        assert_eq!(decoded.key, Some(Bytes::new()));
        assert_eq!(decoded.value, None);
    }

    #[test]
    fn control_frames_roundtrip() {
        let ack = ConsumeAck {
            subscription_id: 7,
            subscription_epoch: 3,
            delivery_id: 99,
            message_credit: 1,
            byte_credit: 128,
        };
        let mut buf = BytesMut::new();
        ack.encode(&mut buf).unwrap();
        assert_eq!(ConsumeAck::decode(&buf).unwrap(), ack);

        let flow = FlowControl {
            subscription_id: 7,
            subscription_epoch: 3,
            message_credit: 4,
            byte_credit: 4096,
        };
        let mut buf = BytesMut::new();
        flow.encode(&mut buf).unwrap();
        assert_eq!(FlowControl::decode(&buf).unwrap(), flow);

        let nack = ConsumeNack {
            subscription_id: 7,
            subscription_epoch: 3,
            delivery_id: 100,
            reason: "handler failed".into(),
        };
        let mut buf = BytesMut::new();
        nack.encode(&mut buf).unwrap();
        assert_eq!(ConsumeNack::decode(&buf).unwrap(), nack);

        let partitions = ConsumerPartitions {
            subscription_id: 7,
            subscription_epoch: 3,
            partitions: vec![("events".into(), 2)],
        };
        let mut buf = BytesMut::new();
        partitions.encode(&mut buf).unwrap();
        assert_eq!(ConsumerPartitions::decode(&buf).unwrap(), partitions);
    }

    #[test]
    fn committed_and_rebalance_roundtrip() {
        let committed = ConsumeCommitted {
            subscription_id: 7,
            subscription_epoch: 3,
            offsets: vec![CommittedOffset {
                topic: "events".into(),
                partition: 2,
                next_offset: 1235,
            }],
        };
        let mut buf = BytesMut::new();
        committed.encode(&mut buf).unwrap();
        assert_eq!(ConsumeCommitted::decode(&buf).unwrap(), committed);

        let notice = RebalanceNotice {
            subscription_id: 7,
            subscription_epoch: 4,
            partitions: vec![("events".into(), 2)],
        };
        let mut buf = BytesMut::new();
        notice.encode(&mut buf).unwrap();
        assert_eq!(RebalanceNotice::decode(&buf).unwrap(), notice);
    }

    #[test]
    fn rejects_count_before_allocating() {
        let mut buf = BytesMut::new();
        write_str("default", &mut buf).unwrap();
        write_str("group", &mut buf).unwrap();
        buf.put_u32((MAX_TOPICS + 1) as u32);
        assert!(matches!(
            ConsumeOpen::decode(&buf),
            Err(PayloadError::FieldTooLarge(_))
        ));
    }
}

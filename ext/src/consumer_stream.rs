//! Blocking PHP ConsumerStream and ConsumerRecord objects.
//!
//! The object owns one dedicated UDS connection.  It exposes one message at a
//! time and never participates in the RPC pool.

#![allow(non_snake_case)]

use crate::{ipc, protocol};
use bytes::BytesMut;
use ext_php_rs::boxed::ZBox;
use ext_php_rs::convert::IntoZval;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendClassObject, ZendHashTable, ZendObject, Zval};
use hi_kafka_proto::{
    codec, encode_frame, ConnectionRole, ConsumeAck, ConsumeClose, ConsumeCommit, ConsumeCommitted,
    ConsumeMessage, ConsumeNack, ConsumeOpen, ConsumeReady, ConsumeResume, ConsumerPartitions,
    ErrorKind, ErrorResp, FlowControl, FrameType, HelloResp, ResumeToken, HEADER_LEN,
};
use std::io::{Read, Write};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::sync::Mutex;
use std::time::Duration;

const READ_CHUNK_BYTES: usize = 64 * 1024;

#[php_class]
#[php(name = "Hi\\Kafka\\ConsumerRecord")]
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    subscription_id: u64,
    subscription_epoch: u64,
    delivery_id: u64,
    topic: String,
    partition: i32,
    offset: i64,
    timestamp_ms: i64,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Vec<(String, Vec<u8>)>,
    wire_size: u64,
}

#[php_impl]
impl ConsumerRecord {
    /// 与用户态 `ConsumerRecord` 相同的构造签名，供测试和伪 driver 使用。
    ///
    /// 手工构造的 record 不能用来伪造 ACK：worker 会逐帧校验 subscription、
    /// epoch 与 delivery id，只接受自己真正投递过的身份。
    #[allow(clippy::too_many_arguments)]
    #[php(optional = headers, defaults(headers = None))]
    pub fn __construct(
        subscriptionId: i64,
        subscriptionEpoch: i64,
        deliveryId: i64,
        topic: String,
        partition: i64,
        offset: i64,
        timestampMs: i64,
        wireSize: i64,
        key: Option<ext_php_rs::binary::Binary<u8>>,
        value: Option<ext_php_rs::binary::Binary<u8>>,
        headers: Option<&ZendHashTable>,
    ) -> PhpResult<Self> {
        let unsigned = |name: &str, value: i64| {
            u64::try_from(value)
                .map_err(|_| PhpException::default(format!("{name} must not be negative")))
        };
        let partition = i32::try_from(partition)
            .map_err(|_| PhpException::default("partition is out of range".to_string()))?;
        let mut parsed_headers = Vec::new();
        if let Some(headers) = headers {
            for (_, header) in headers {
                let header = header.array().ok_or_else(|| {
                    PhpException::default("each header must be array{name, value}".to_string())
                })?;
                let name = header.get("name").and_then(Zval::str).ok_or_else(|| {
                    PhpException::default("header name must be a string".to_string())
                })?;
                let value = header
                    .get("value")
                    .and_then(Zval::zend_str)
                    .map(|value| value.as_bytes().to_vec())
                    .ok_or_else(|| {
                        PhpException::default("header value must be a string".to_string())
                    })?;
                parsed_headers.push((name.to_string(), value));
            }
        }
        Ok(Self {
            subscription_id: unsigned("subscriptionId", subscriptionId)?,
            subscription_epoch: unsigned("subscriptionEpoch", subscriptionEpoch)?,
            delivery_id: unsigned("deliveryId", deliveryId)?,
            topic,
            partition,
            offset,
            timestamp_ms: timestampMs,
            key: key.map(|value| value.into()),
            value: value.map(|value| value.into()),
            headers: parsed_headers,
            wire_size: unsigned("wireSize", wireSize)?,
        })
    }

    pub fn subscription_id(&self) -> i64 {
        self.subscription_id as i64
    }
    pub fn subscription_epoch(&self) -> i64 {
        self.subscription_epoch as i64
    }
    pub fn delivery_id(&self) -> i64 {
        self.delivery_id as i64
    }
    pub fn topic(&self) -> String {
        self.topic.clone()
    }
    pub fn partition(&self) -> i64 {
        self.partition as i64
    }
    pub fn offset(&self) -> i64 {
        self.offset
    }
    pub fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }
    pub fn wire_size(&self) -> i64 {
        self.wire_size as i64
    }
    pub fn key(&self) -> Option<ext_php_rs::binary::Binary<u8>> {
        self.key.clone().map(Into::into)
    }
    pub fn value(&self) -> Option<ext_php_rs::binary::Binary<u8>> {
        self.value.clone().map(Into::into)
    }
    pub fn headers(&self) -> PhpResult<Zval> {
        let mut values = ZendHashTable::new();
        for (name, value) in &self.headers {
            let mut header = ZendHashTable::new();
            header
                .insert("name", name.as_str())
                .map_err(|e| PhpException::default(format!("header name: {e}")))?;
            header
                .insert("value", ext_php_rs::binary::Binary::from(value.clone()))
                .map_err(|e| PhpException::default(format!("header value: {e}")))?;
            values
                .push(header)
                .map_err(|e| PhpException::default(format!("header: {e}")))?;
        }
        values
            .into_zval(false)
            .map_err(|e| PhpException::default(e.to_string()))
    }
}

impl From<ConsumeMessage> for ConsumerRecord {
    fn from(value: ConsumeMessage) -> Self {
        Self {
            subscription_id: value.subscription_id,
            subscription_epoch: value.subscription_epoch,
            delivery_id: value.delivery_id,
            topic: value.topic,
            partition: value.partition,
            offset: value.offset,
            timestamp_ms: value.timestamp_ms,
            key: value.key.map(|bytes| bytes.to_vec()),
            value: value.value.map(|bytes| bytes.to_vec()),
            headers: value
                .headers
                .into_iter()
                .map(|(k, v)| (k, v.to_vec()))
                .collect(),
            wire_size: value.wire_size,
        }
    }
}

#[derive(Debug)]
struct State {
    socket: String,
    stream: UnixStream,
    open: ConsumeOpen,
    subscription_id: u64,
    epoch: u64,
    resume_token: ResumeToken,
    max_message_credit: u32,
    max_byte_credit: u64,
    pending_message_credit: u32,
    pending_byte_credit: u64,
    outstanding_delivery_id: Option<u64>,
    pending_ack_delivery_id: Option<u64>,
    pending_ack_epoch: u64,
    pending_ack_wire_size: u64,
    read_buffer: BytesMut,
    closed: bool,
}

#[php_class]
#[php(name = "Hi\\Kafka\\ConsumerStream")]
#[php(implements(
    ce = crate::consumer_stream_interface::get_ce,
    stub = "Hi\\Kafka\\ConsumerStreamInterface"
))]
#[derive(Debug)]
pub struct ConsumerStream {
    state: Mutex<State>,
}

impl ConsumerStream {
    pub fn connect(
        socket: &str,
        cluster: &str,
        group_id: &str,
        topics: Vec<String>,
        config: Vec<(String, String)>,
        timeout: Duration,
    ) -> Result<Self, ipc::IpcError> {
        let open = ConsumeOpen {
            cluster: cluster.to_owned(),
            group_id: group_id.to_owned(),
            topics,
            config,
            initial_message_credit: 0,
            initial_byte_credit: 0,
            max_in_flight_messages: 1,
            max_in_flight_bytes: 16 * 1024 * 1024,
            client_id: format!("php-{}", std::process::id()),
        };
        ipc::ensure(socket)?;
        let mut stream = connect_hello(socket, timeout)?;
        let ready = open_stream(&mut stream, &open)?;
        Ok(Self {
            state: Mutex::new(State {
                socket: socket.to_owned(),
                stream,
                open,
                subscription_id: ready.subscription_id,
                epoch: ready.subscription_epoch,
                resume_token: ready.resume_token,
                max_message_credit: ready.max_in_flight_messages,
                max_byte_credit: ready.max_in_flight_bytes,
                pending_message_credit: ready.max_in_flight_messages,
                pending_byte_credit: ready.max_in_flight_bytes,
                outstanding_delivery_id: None,
                pending_ack_delivery_id: None,
                pending_ack_epoch: 0,
                pending_ack_wire_size: 0,
                read_buffer: BytesMut::new(),
                closed: false,
            }),
        })
    }
}

#[php_impl]
impl ConsumerStream {
    #[php(defaults(timeoutMs = None))]
    pub fn next(
        &self,
        timeoutMs: Option<i64>,
    ) -> PhpResult<Option<ZBox<ZendClassObject<ConsumerRecord>>>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| PhpException::default("consumer stream lock poisoned".into()))?;
        ensure_open(&state)?;
        if state.outstanding_delivery_id.is_some() {
            return Err(PhpException::default(
                "ack or nack the current message before calling next".into(),
            ));
        }
        let timeout = Duration::from_millis(timeoutMs.unwrap_or(30_000).max(1) as u64);
        state
            .stream
            .set_read_timeout(Some(timeout))
            .map_err(io_php)?;
        loop {
            if state.pending_ack_delivery_id.is_some() {
                let message_credit = state.max_message_credit;
                let byte_credit = state.pending_ack_wire_size;
                if let Err(error) = flush_pending_ack(&mut state, message_credit, byte_credit) {
                    if reconnectable(&error) {
                        reconnect(&mut state, timeout).map_err(crate::ipc_err_to_php)?;
                        continue;
                    }
                    return Err(crate::ipc_err_to_php(error));
                }
            } else if state.pending_message_credit > 0 || state.pending_byte_credit > 0 {
                let flow = FlowControl {
                    subscription_id: state.subscription_id,
                    subscription_epoch: state.epoch,
                    message_credit: state.pending_message_credit,
                    byte_credit: state.pending_byte_credit,
                };
                if let Err(error) =
                    send_payload(&mut state.stream, FrameType::FlowControl, 0, |b| {
                        flow.encode(b)
                    })
                {
                    if reconnectable(&error) {
                        reconnect(&mut state, timeout).map_err(crate::ipc_err_to_php)?;
                        continue;
                    }
                    return Err(crate::ipc_err_to_php(error));
                }
                state.pending_message_credit = 0;
                state.pending_byte_credit = 0;
            }
            let frame = {
                let state = &mut *state;
                read_frame_buffered(&mut state.stream, &mut state.read_buffer)
            };
            let (kind, _, payload) = match frame {
                Ok(frame) => frame,
                Err(error) if timed_out(&error) => return Ok(None),
                Err(error) if reconnectable(&error) => {
                    reconnect(&mut state, timeout).map_err(crate::ipc_err_to_php)?;
                    continue;
                }
                Err(error) => return Err(crate::ipc_err_to_php(error)),
            };
            match kind {
                FrameType::ConsumeMessage => {
                    let message = ConsumeMessage::decode(&payload)
                        .map_err(|e| PhpException::default(e.to_string()))?;
                    if message.subscription_id != state.subscription_id
                        || message.subscription_epoch != state.epoch
                    {
                        return Err(PhpException::default(
                            "stale ConsumeMessage identity".into(),
                        ));
                    }
                    if state.outstanding_delivery_id.is_some() {
                        return Err(PhpException::default(
                            "consumer stream received more than one outstanding message".into(),
                        ));
                    }
                    state.outstanding_delivery_id = Some(message.delivery_id);
                    return Ok(Some(ZendClassObject::new(message.into())));
                }
                FrameType::RebalanceAssign | FrameType::RebalanceRevoke => {
                    let notice = hi_kafka_proto::RebalanceNotice::decode(&payload)
                        .map_err(|e| PhpException::default(e.to_string()))?;
                    if notice.subscription_id != state.subscription_id {
                        return Err(PhpException::default(
                            "rebalance notice belongs to another stream".into(),
                        ));
                    }
                    let epoch_changed = state.epoch != notice.subscription_epoch;
                    state.epoch = notice.subscription_epoch;
                    if epoch_changed {
                        state.outstanding_delivery_id = None;
                        clear_pending_ack(&mut state);
                        state.pending_message_credit = state.max_message_credit;
                        state.pending_byte_credit = state.max_byte_credit;
                    }
                    continue;
                }
                FrameType::Error => {
                    return Err(crate::ipc_err_to_php(
                        decode_worker_error(&payload).map_err(crate::ipc_err_to_php)?,
                    ))
                }
                FrameType::ConsumeClose => {
                    state.closed = true;
                    return Err(PhpException::default(
                        "consumer stream closed by worker".into(),
                    ));
                }
                other => {
                    return Err(PhpException::default(format!(
                        "unexpected consumer frame {other:?}"
                    )))
                }
            }
        }
    }

    pub fn ack(&self, message: &ZendClassObject<ConsumerRecord>) -> PhpResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| PhpException::default("consumer stream lock poisoned".into()))?;
        ensure_message(&state, message)?;
        ensure_outstanding(&state, message.delivery_id)?;
        state.pending_ack_delivery_id = Some(message.delivery_id);
        state.pending_ack_epoch = message.subscription_epoch;
        state.pending_ack_wire_size = message.wire_size;
        state.outstanding_delivery_id = None;
        Ok(())
    }

    #[php(defaults(reason = None))]
    pub fn nack(
        &self,
        message: &ZendClassObject<ConsumerRecord>,
        reason: Option<&ZendObject>,
    ) -> PhpResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| PhpException::default("consumer stream lock poisoned".into()))?;
        ensure_message(&state, message)?;
        ensure_outstanding(&state, message.delivery_id)?;
        let nack = ConsumeNack {
            subscription_id: state.subscription_id,
            subscription_epoch: state.epoch,
            delivery_id: message.delivery_id,
            reason: throwable_message(reason),
        };
        send_payload(&mut state.stream, FrameType::ConsumeNack, 0, |b| {
            nack.encode(b)
        })
        .map_err(crate::ipc_err_to_php)?;
        state.outstanding_delivery_id = None;
        state.closed = true;
        let _ = state.stream.shutdown(Shutdown::Both);
        Ok(())
    }

    #[php(defaults(timeoutMs = None))]
    pub fn commit(&self, timeoutMs: Option<i64>) -> PhpResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| PhpException::default("consumer stream lock poisoned".into()))?;
        ensure_open(&state)?;
        state
            .stream
            .set_read_timeout(Some(Duration::from_millis(
                timeoutMs.unwrap_or(5000).max(1) as u64,
            )))
            .map_err(io_php)?;
        flush_pending_ack_for_control(&mut state).map_err(crate::ipc_err_to_php)?;
        let cid = ipc::next_cid();
        let commit = ConsumeCommit {
            subscription_id: state.subscription_id,
            subscription_epoch: state.epoch,
        };
        send_payload(&mut state.stream, FrameType::ConsumeCommit, cid, |b| {
            commit.encode(b)
        })
        .map_err(crate::ipc_err_to_php)?;
        loop {
            let (kind, got_cid, payload) = {
                let state = &mut *state;
                read_frame_buffered(&mut state.stream, &mut state.read_buffer)
            }
            .map_err(crate::ipc_err_to_php)?;
            match kind {
                FrameType::ConsumeCommitted if got_cid == cid => {
                    ConsumeCommitted::decode(&payload)
                        .map_err(|e| PhpException::default(e.to_string()))?;
                    return Ok(());
                }
                FrameType::RebalanceAssign | FrameType::RebalanceRevoke => {
                    let notice = hi_kafka_proto::RebalanceNotice::decode(&payload)
                        .map_err(|e| PhpException::default(e.to_string()))?;
                    if notice.subscription_id != state.subscription_id {
                        return Err(PhpException::default(
                            "rebalance notice belongs to another stream".into(),
                        ));
                    }
                    let changed = state.epoch != notice.subscription_epoch;
                    state.epoch = notice.subscription_epoch;
                    if changed {
                        state.outstanding_delivery_id = None;
                        clear_pending_ack(&mut state);
                        state.pending_message_credit = state.max_message_credit;
                        state.pending_byte_credit = state.max_byte_credit;
                    }
                }
                FrameType::Error => {
                    return Err(crate::ipc_err_to_php(
                        decode_worker_error(&payload).map_err(crate::ipc_err_to_php)?,
                    ))
                }
                FrameType::ConsumeCommitted => {
                    return Err(PhpException::default(
                        "commit correlation id mismatch".into(),
                    ))
                }
                other => {
                    return Err(PhpException::default(format!(
                        "unexpected frame while committing: {other:?}"
                    )))
                }
            }
        }
    }

    /// `$partitions` 为空或省略表示当前完整 assignment。
    #[php(defaults(partitions = None))]
    pub fn pause(&self, partitions: Option<&ZendHashTable>) -> PhpResult<()> {
        self.control_partitions(
            FrameType::ConsumePause,
            parse_optional_partitions(partitions)?,
        )
    }

    /// `$partitions` 为空或省略表示当前完整 assignment。
    #[php(defaults(partitions = None))]
    pub fn resume(&self, partitions: Option<&ZendHashTable>) -> PhpResult<()> {
        self.control_partitions(
            FrameType::ConsumePartitionResume,
            parse_optional_partitions(partitions)?,
        )
    }

    pub fn close(&self) -> PhpResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| PhpException::default("consumer stream lock poisoned".into()))?;
        if state.closed {
            return Ok(());
        }
        let result = match flush_pending_ack_for_control(&mut state) {
            Ok(()) => {
                let close = ConsumeClose {
                    subscription_id: state.subscription_id,
                    subscription_epoch: state.epoch,
                };
                send_payload(&mut state.stream, FrameType::ConsumeClose, 0, |b| {
                    close.encode(b)
                })
            }
            Err(error) => Err(error),
        };
        state.closed = true;
        let _ = state.stream.shutdown(Shutdown::Both);
        result.map_err(crate::ipc_err_to_php)
    }

    pub fn subscription_id(&self) -> PhpResult<i64> {
        Ok(self
            .state
            .lock()
            .map_err(|_| PhpException::default("consumer stream lock poisoned".into()))?
            .subscription_id as i64)
    }
}

fn parse_optional_partitions(partitions: Option<&ZendHashTable>) -> PhpResult<Vec<(String, i32)>> {
    partitions.map_or_else(|| Ok(Vec::new()), parse_partitions)
}

fn throwable_message(reason: Option<&ZendObject>) -> String {
    reason
        .and_then(|reason| reason.try_call_method("getMessage", Vec::new()).ok())
        .and_then(|message| message.string())
        .unwrap_or_default()
}

impl ConsumerStream {
    fn control_partitions(&self, kind: FrameType, partitions: Vec<(String, i32)>) -> PhpResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| PhpException::default("consumer stream lock poisoned".into()))?;
        ensure_open(&state)?;
        flush_pending_ack_for_control(&mut state).map_err(crate::ipc_err_to_php)?;
        let value = ConsumerPartitions {
            subscription_id: state.subscription_id,
            subscription_epoch: state.epoch,
            partitions,
        };
        send_payload(&mut state.stream, kind, 0, |b| value.encode(b)).map_err(crate::ipc_err_to_php)
    }
}

pub(crate) fn parse_partitions(values: &ZendHashTable) -> PhpResult<Vec<(String, i32)>> {
    let mut partitions = Vec::with_capacity(values.len());
    for (_, value) in values.iter() {
        let item = value.array().ok_or_else(|| {
            PhpException::default(
                "each partition must be ['topic' => string, 'partition' => int]".into(),
            )
        })?;
        let topic = item
            .get("topic")
            .and_then(Zval::string)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                PhpException::default("partition topic must be a non-empty string".into())
            })?;
        let partition = item
            .get("partition")
            .and_then(Zval::long)
            .ok_or_else(|| PhpException::default("partition number must be an integer".into()))?;
        partitions.push((topic, crate::require_i32("partition", partition)?));
    }
    Ok(partitions)
}

impl Drop for ConsumerStream {
    fn drop(&mut self) {
        if let Ok(state) = self.state.get_mut() {
            if !state.closed {
                let _ = flush_pending_ack_for_control(state);
                let close = ConsumeClose {
                    subscription_id: state.subscription_id,
                    subscription_epoch: state.epoch,
                };
                let _ = send_payload(&mut state.stream, FrameType::ConsumeClose, 0, |b| {
                    close.encode(b)
                });
            }
        }
    }
}

fn ensure_open(state: &State) -> PhpResult<()> {
    if state.closed {
        Err(PhpException::default("consumer stream is closed".into()))
    } else {
        Ok(())
    }
}

fn ensure_message(state: &State, message: &ConsumerRecord) -> PhpResult<()> {
    ensure_open(state)?;
    if message.subscription_id != state.subscription_id || message.subscription_epoch != state.epoch
    {
        return Err(PhpException::default(
            "message does not belong to this stream epoch".into(),
        ));
    }
    Ok(())
}

fn ensure_outstanding(state: &State, delivery_id: u64) -> PhpResult<()> {
    if state.outstanding_delivery_id != Some(delivery_id) {
        return Err(PhpException::default(
            "message is not the current outstanding delivery".into(),
        ));
    }
    Ok(())
}

fn flush_pending_ack(
    state: &mut State,
    message_credit: u32,
    byte_credit: u64,
) -> Result<(), ipc::IpcError> {
    let Some(delivery_id) = state.pending_ack_delivery_id else {
        return Ok(());
    };
    let ack = ConsumeAck {
        subscription_id: state.subscription_id,
        subscription_epoch: state.pending_ack_epoch,
        delivery_id,
        message_credit,
        byte_credit,
    };
    send_payload(&mut state.stream, FrameType::ConsumeAck, 0, |b| {
        ack.encode(b)
    })?;
    clear_pending_ack(state);
    Ok(())
}

fn flush_pending_ack_for_control(state: &mut State) -> Result<(), ipc::IpcError> {
    if state.pending_ack_delivery_id.is_none() {
        return Ok(());
    }
    let wire_size = state.pending_ack_wire_size;
    flush_pending_ack(state, 0, 0)?;
    state.pending_message_credit =
        state.pending_message_credit.checked_add(1).ok_or_else(|| {
            ipc::IpcError::Encode("message credit overflow after deferred ACK".into())
        })?;
    state.pending_byte_credit = state
        .pending_byte_credit
        .checked_add(wire_size)
        .ok_or_else(|| ipc::IpcError::Encode("byte credit overflow after deferred ACK".into()))?;
    Ok(())
}

fn clear_pending_ack(state: &mut State) {
    state.pending_ack_delivery_id = None;
    state.pending_ack_epoch = 0;
    state.pending_ack_wire_size = 0;
}

fn send_payload<T>(
    stream: &mut UnixStream,
    kind: FrameType,
    cid: u64,
    encode: impl FnOnce(&mut BytesMut) -> Result<T, hi_kafka_proto::PayloadError>,
) -> Result<(), ipc::IpcError> {
    let mut payload = BytesMut::new();
    encode(&mut payload).map_err(|e| ipc::IpcError::Encode(e.to_string()))?;
    let mut frame = BytesMut::new();
    encode_frame(kind, cid, &payload, &mut frame)
        .map_err(|e| ipc::IpcError::Encode(e.to_string()))?;
    stream.write_all(&frame)?;
    Ok(())
}

fn read_frame(stream: &mut UnixStream) -> Result<(FrameType, u64, Vec<u8>), ipc::IpcError> {
    let mut header = [0u8; HEADER_LEN];
    stream.read_exact(&mut header)?;
    let header =
        codec::decode_header(&header).map_err(|e| ipc::IpcError::DecodeResp(e.to_string()))?;
    let mut payload = vec![0; header.payload_len as usize];
    stream.read_exact(&mut payload)?;
    Ok((header.kind, header.cid, payload))
}

fn read_frame_buffered(
    stream: &mut UnixStream,
    buffer: &mut BytesMut,
) -> Result<(FrameType, u64, Vec<u8>), ipc::IpcError> {
    fill_read_buffer(stream, buffer, HEADER_LEN)?;
    let header = codec::decode_header(&buffer[..HEADER_LEN])
        .map_err(|e| ipc::IpcError::DecodeResp(e.to_string()))?;
    let frame_len = HEADER_LEN + header.payload_len as usize;
    fill_read_buffer(stream, buffer, frame_len)?;
    let frame = buffer.split_to(frame_len);
    Ok((header.kind, header.cid, frame[HEADER_LEN..].to_vec()))
}

fn fill_read_buffer(
    stream: &mut UnixStream,
    buffer: &mut BytesMut,
    required: usize,
) -> Result<(), ipc::IpcError> {
    let mut chunk = [0u8; READ_CHUNK_BYTES];
    while buffer.len() < required {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
    Ok(())
}

fn connect_hello(socket: &str, timeout: Duration) -> Result<UnixStream, ipc::IpcError> {
    let mut stream = UnixStream::connect(socket)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    stream.write_all(
        &protocol::build_hello_frame_for(ConnectionRole::ConsumerStream)
            .map_err(|e| ipc::IpcError::Encode(e.to_string()))?,
    )?;
    let (kind, _, payload) = read_frame(&mut stream)?;
    if kind == FrameType::Error {
        return Err(decode_worker_error(&payload)?);
    }
    if kind != FrameType::Hello {
        return Err(ipc::IpcError::UnexpectedKind(kind));
    }
    let hello =
        HelloResp::decode(&payload).map_err(|e| ipc::IpcError::DecodeResp(e.to_string()))?;
    if hello.major != hi_kafka_proto::PROTOCOL_MAJOR {
        return Err(ipc::IpcError::DecodeResp(
            "worker protocol major mismatch".into(),
        ));
    }
    Ok(stream)
}

fn open_stream(stream: &mut UnixStream, open: &ConsumeOpen) -> Result<ConsumeReady, ipc::IpcError> {
    let cid = ipc::next_cid();
    send_payload(stream, FrameType::ConsumeOpen, cid, |b| open.encode(b))?;
    read_ready(stream, cid)
}

fn resume_stream(
    stream: &mut UnixStream,
    subscription_id: u64,
    previous_epoch: u64,
    resume_token: ResumeToken,
    message_credit: u32,
    byte_credit: u64,
) -> Result<ConsumeReady, ipc::IpcError> {
    let cid = ipc::next_cid();
    let resume = ConsumeResume {
        subscription_id,
        previous_epoch,
        resume_token,
        initial_message_credit: message_credit,
        initial_byte_credit: byte_credit,
    };
    send_payload(stream, FrameType::ConsumeResume, cid, |b| resume.encode(b))?;
    read_ready(stream, cid)
}

fn read_ready(stream: &mut UnixStream, cid: u64) -> Result<ConsumeReady, ipc::IpcError> {
    let (kind, got_cid, payload) = read_frame(stream)?;
    if kind == FrameType::Error {
        return Err(decode_worker_error(&payload)?);
    }
    if kind != FrameType::ConsumeReady {
        return Err(ipc::IpcError::UnexpectedKind(kind));
    }
    if got_cid != cid {
        return Err(ipc::IpcError::CidMismatch {
            sent: cid,
            got: got_cid,
        });
    }
    ConsumeReady::decode(&payload).map_err(|e| ipc::IpcError::DecodeResp(e.to_string()))
}

fn reconnect(state: &mut State, timeout: Duration) -> Result<(), ipc::IpcError> {
    crate::worker_health::invalidate(&state.socket);
    let grace = Duration::from_millis(
        std::env::var("HI_KAFKA_CONSUMER_RESUME_GRACE_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(10_000),
    );
    let deadline = std::time::Instant::now() + grace;
    loop {
        ipc::ensure(&state.socket)?;
        match connect_hello(&state.socket, timeout).and_then(|mut stream| {
            let ready = resume_stream(
                &mut stream,
                state.subscription_id,
                state.epoch,
                state.resume_token,
                0,
                0,
            )?;
            Ok((stream, ready))
        }) {
            Ok((stream, ready)) => {
                install_ready(state, stream, ready);
                return Ok(());
            }
            Err(ipc::IpcError::Worker {
                kind: ErrorKind::SubscriptionNotFound,
                ..
            })
            | Err(ipc::IpcError::Worker {
                kind: ErrorKind::WorkerUnavailable,
                ..
            }) => {
                return reopen(state, timeout);
            }
            Err(ipc::IpcError::Worker {
                kind: ErrorKind::StaleEpoch,
                ..
            }) if std::time::Instant::now() < deadline => {}
            Err(error) if reconnectable(&error) && std::time::Instant::now() < deadline => {
                crate::worker_health::invalidate(&state.socket);
            }
            Err(error) => return Err(error),
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn reopen(state: &mut State, timeout: Duration) -> Result<(), ipc::IpcError> {
    crate::worker_health::invalidate(&state.socket);
    ipc::ensure(&state.socket)?;
    let mut stream = connect_hello(&state.socket, timeout)?;
    let ready = open_stream(&mut stream, &state.open)?;
    install_ready(state, stream, ready);
    Ok(())
}

fn install_ready(state: &mut State, stream: UnixStream, ready: ConsumeReady) {
    state.stream = stream;
    state.subscription_id = ready.subscription_id;
    state.epoch = ready.subscription_epoch;
    state.resume_token = ready.resume_token;
    state.max_message_credit = ready.max_in_flight_messages;
    state.max_byte_credit = ready.max_in_flight_bytes;
    state.pending_message_credit = ready.max_in_flight_messages;
    state.pending_byte_credit = ready.max_in_flight_bytes;
    state.outstanding_delivery_id = None;
    state.read_buffer.clear();
    clear_pending_ack(state);
}

fn reconnectable(error: &ipc::IpcError) -> bool {
    matches!(error, ipc::IpcError::Io(io) if matches!(
        io.kind(),
        std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::NotConnected
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::NotFound
    ))
}

fn timed_out(error: &ipc::IpcError) -> bool {
    matches!(error, ipc::IpcError::Io(io) if matches!(
        io.kind(),
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    ))
}

fn decode_worker_error(payload: &[u8]) -> Result<ipc::IpcError, ipc::IpcError> {
    let error = ErrorResp::decode(payload).map_err(|e| ipc::IpcError::DecodeResp(e.to_string()))?;
    Ok(ipc::IpcError::Worker {
        kind: error.kind,
        retryable: error.retryable,
        outcome: error.outcome,
        native_code: error.native_code,
        message: error.message,
    })
}

fn io_php(error: std::io::Error) -> PhpException {
    crate::ipc_err_to_php(error.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffered_reader_preserves_the_next_coalesced_frame() {
        let (mut writer, mut reader) = UnixStream::pair().expect("unix stream pair");
        let mut wire = BytesMut::new();
        encode_frame(FrameType::Ping, 11, &[], &mut wire).expect("first frame");
        encode_frame(FrameType::Pong, 12, b"second", &mut wire).expect("second frame");
        writer.write_all(&wire).expect("coalesced write");

        let mut buffer = BytesMut::new();
        let first = read_frame_buffered(&mut reader, &mut buffer).expect("read first");
        assert_eq!(first, (FrameType::Ping, 11, Vec::new()));
        assert!(!buffer.is_empty());

        let second = read_frame_buffered(&mut reader, &mut buffer).expect("read second");
        assert_eq!(second, (FrameType::Pong, 12, b"second".to_vec()));
        assert!(buffer.is_empty());
    }

    #[test]
    fn socket_timeout_is_idle_not_disconnect() {
        for kind in [std::io::ErrorKind::TimedOut, std::io::ErrorKind::WouldBlock] {
            let error = ipc::IpcError::Io(std::io::Error::from(kind));
            assert!(timed_out(&error));
            assert!(!reconnectable(&error));
        }
    }
}

//! Shared protocol-v2 codecs used by the native and coroutine clients.

use bytes::BytesMut;
use hi_kafka_proto::{
    codec, encode_frame, ConnectionRole, ConsumeAck, ConsumeClose, ConsumeCommit, ConsumeCommitted,
    ConsumeMessage, ConsumeNack, ConsumeOpen, ConsumeReady, ConsumeResume, ConsumerPartitions,
    ErrorResp, FlowControl, FrameType, HelloReq, HelloResp, ProduceFnf, ProduceReq, ProduceResp,
    RebalanceNotice, RegisterClusterReq, RegisterClusterResp, ResumeToken, SetOAuthBearerTokenReq,
    SetOAuthBearerTokenResp, CAP_ALL, HEADER_LEN, PROTOCOL_MAJOR, RESUME_TOKEN_LEN,
};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_CID: AtomicU64 = AtomicU64::new(1);
pub fn next_cid() -> u64 {
    NEXT_CID.fetch_add(1, Ordering::Relaxed)
}
pub fn header_len() -> usize {
    HEADER_LEN
}

pub struct ParsedError {
    pub kind: u16,
    pub kind_name: &'static str,
    pub retryable: bool,
    pub outcome: hi_kafka_proto::OperationOutcome,
    pub native_code: i32,
    pub message: String,
}

pub fn parse_error_frame(bytes: &[u8]) -> anyhow::Result<ParsedError> {
    let (header, payload) = split_frame(bytes)?;
    if header.kind != FrameType::Error {
        anyhow::bail!("expected Error, got {:?}", header.kind);
    }
    let value = ErrorResp::decode(payload).map_err(|e| anyhow::anyhow!(e))?;
    Ok(ParsedError {
        kind: value.kind.as_u16(),
        kind_name: value.kind.as_str(),
        retryable: value.retryable,
        outcome: value.outcome,
        native_code: value.native_code,
        message: value.message,
    })
}

pub fn build_hello_frame() -> anyhow::Result<Vec<u8>> {
    build_hello_frame_for(ConnectionRole::Rpc)
}
pub fn build_hello_frame_for(role: ConnectionRole) -> anyhow::Result<Vec<u8>> {
    encode(FrameType::Hello, 0, |payload| {
        HelloReq {
            major: PROTOCOL_MAJOR,
            role,
            capabilities: CAP_ALL,
        }
        .encode(payload)
    })
}
pub fn parse_hello_resp(bytes: &[u8]) -> anyhow::Result<()> {
    let (header, payload) = split_frame(bytes)?;
    if header.kind != FrameType::Hello {
        anyhow::bail!("expected Hello, got {:?}", header.kind);
    }
    let value = HelloResp::decode(payload).map_err(|e| anyhow::anyhow!(e))?;
    if value.major != PROTOCOL_MAJOR {
        anyhow::bail!(
            "protocol major mismatch: {} != {}",
            value.major,
            PROTOCOL_MAJOR
        );
    }
    if value.capabilities & CAP_ALL != CAP_ALL {
        anyhow::bail!("worker lacks required v2 capabilities");
    }
    Ok(())
}

pub fn build_fnf_frame(
    cluster: &str,
    topic: &str,
    key: &[u8],
    value: &[u8],
    headers: Vec<(String, bytes::Bytes)>,
    partition: i32,
    timestamp_ms: i64,
) -> anyhow::Result<Vec<u8>> {
    let message = produce(cluster, topic, key, value, headers, partition, timestamp_ms);
    encode(FrameType::ProduceFnf, 0, |payload| message.encode(payload))
}
pub fn build_req_frame(
    cluster: &str,
    topic: &str,
    key: &[u8],
    value: &[u8],
    headers: Vec<(String, bytes::Bytes)>,
    partition: i32,
    timestamp_ms: i64,
    timeout_ms: u32,
) -> anyhow::Result<(u64, Vec<u8>)> {
    let cid = next_cid();
    let request = ProduceReq {
        msg: produce(cluster, topic, key, value, headers, partition, timestamp_ms),
        timeout_ms: timeout_ms.max(1),
    };
    Ok((
        cid,
        encode(FrameType::ProduceReq, cid, |payload| {
            request.encode(payload)
        })?,
    ))
}
fn produce(
    cluster: &str,
    topic: &str,
    key: &[u8],
    value: &[u8],
    headers: Vec<(String, bytes::Bytes)>,
    partition: i32,
    timestamp_ms: i64,
) -> ProduceFnf {
    ProduceFnf {
        cluster: cluster.into(),
        topic: topic.into(),
        key: bytes::Bytes::copy_from_slice(key),
        value: bytes::Bytes::copy_from_slice(value),
        partition,
        timestamp_ms,
        headers,
    }
}

#[derive(Debug)]
pub enum ParsedFrame {
    Resp { cid: u64, resp: ProduceResp },
    Other { kind: FrameType, cid: u64 },
}
pub fn parse_resp_frame(bytes: &[u8]) -> anyhow::Result<ParsedFrame> {
    let (header, payload) = split_frame(bytes)?;
    if header.kind == FrameType::ProduceResp {
        Ok(ParsedFrame::Resp {
            cid: header.cid,
            resp: ProduceResp::decode(payload).map_err(|e| anyhow::anyhow!(e))?,
        })
    } else {
        Ok(ParsedFrame::Other {
            kind: header.kind,
            cid: header.cid,
        })
    }
}

pub struct ParsedHeader {
    pub kind_byte: u8,
    pub cid: u64,
    pub payload_len: u32,
}
pub fn parse_header_only(bytes: &[u8]) -> anyhow::Result<ParsedHeader> {
    if bytes.len() < HEADER_LEN {
        anyhow::bail!("header truncated");
    }
    let value = codec::decode_header(&bytes[..HEADER_LEN]).map_err(|e| anyhow::anyhow!(e))?;
    Ok(ParsedHeader {
        kind_byte: value.kind as u8,
        cid: value.cid,
        payload_len: value.payload_len,
    })
}

pub fn build_goodbye_frame() -> anyhow::Result<Vec<u8>> {
    encode_raw(FrameType::Goodbye, 0, &[])
}
pub fn build_register_cluster_frame(
    cluster: &str,
    config: Vec<(String, String)>,
) -> anyhow::Result<(u64, Vec<u8>)> {
    let cid = next_cid();
    let value = RegisterClusterReq {
        cluster: cluster.into(),
        config,
    };
    Ok((
        cid,
        encode(FrameType::RegisterClusterReq, cid, |payload| {
            value.encode(payload)
        })?,
    ))
}
pub fn build_set_oauth_token_frame(
    cluster: &str,
    token_value: &str,
    lifetime_ms: i64,
    principal_name: &str,
    extensions: Vec<(String, String)>,
) -> anyhow::Result<(u64, Vec<u8>)> {
    let cid = next_cid();
    let value = SetOAuthBearerTokenReq {
        cluster: cluster.into(),
        token_value: token_value.into(),
        lifetime_ms,
        principal_name: principal_name.into(),
        extensions,
    };
    Ok((
        cid,
        encode(FrameType::SetOAuthBearerTokenReq, cid, |payload| {
            value.encode(payload)
        })?,
    ))
}

#[derive(Debug)]
pub enum ControlResp {
    RegisterCluster {
        cid: u64,
        ok: bool,
        message: Option<String>,
    },
    OAuth {
        cid: u64,
        ok: bool,
        message: Option<String>,
    },
}
pub fn parse_control_resp_frame(bytes: &[u8]) -> anyhow::Result<ControlResp> {
    let (header, payload) = split_frame(bytes)?;
    match header.kind {
        FrameType::RegisterClusterResp => {
            match RegisterClusterResp::decode(payload).map_err(|e| anyhow::anyhow!(e))? {
                RegisterClusterResp::Ok => Ok(ControlResp::RegisterCluster {
                    cid: header.cid,
                    ok: true,
                    message: None,
                }),
                RegisterClusterResp::Err { message } => Ok(ControlResp::RegisterCluster {
                    cid: header.cid,
                    ok: false,
                    message: Some(message),
                }),
            }
        }
        FrameType::SetOAuthBearerTokenResp => {
            match SetOAuthBearerTokenResp::decode(payload).map_err(|e| anyhow::anyhow!(e))? {
                SetOAuthBearerTokenResp::Ok => Ok(ControlResp::OAuth {
                    cid: header.cid,
                    ok: true,
                    message: None,
                }),
                SetOAuthBearerTokenResp::Err { message } => Ok(ControlResp::OAuth {
                    cid: header.cid,
                    ok: false,
                    message: Some(message),
                }),
            }
        }
        kind => anyhow::bail!("unexpected control response {kind:?}"),
    }
}

pub fn build_consume_open_frame(
    cluster: &str,
    group_id: &str,
    topics: Vec<String>,
    config: Vec<(String, String)>,
    initial_message_credit: u32,
    initial_byte_credit: u64,
    max_in_flight_messages: u32,
    max_in_flight_bytes: u64,
) -> anyhow::Result<(u64, Vec<u8>)> {
    let cid = next_cid();
    let value = ConsumeOpen {
        cluster: cluster.into(),
        group_id: group_id.into(),
        topics,
        config,
        initial_message_credit,
        initial_byte_credit,
        max_in_flight_messages,
        max_in_flight_bytes,
        client_id: format!("php-{}", std::process::id()),
    };
    Ok((
        cid,
        encode(FrameType::ConsumeOpen, cid, |payload| value.encode(payload))?,
    ))
}
pub fn build_consume_resume_frame(
    subscription_id: u64,
    previous_epoch: u64,
    token: &[u8],
    message_credit: u32,
    byte_credit: u64,
) -> anyhow::Result<(u64, Vec<u8>)> {
    if token.len() != RESUME_TOKEN_LEN {
        anyhow::bail!("resume token must be {RESUME_TOKEN_LEN} bytes");
    }
    let mut bytes = [0; RESUME_TOKEN_LEN];
    bytes.copy_from_slice(token);
    let cid = next_cid();
    let value = ConsumeResume {
        subscription_id,
        previous_epoch,
        resume_token: ResumeToken(bytes),
        initial_message_credit: message_credit,
        initial_byte_credit: byte_credit,
    };
    Ok((
        cid,
        encode(FrameType::ConsumeResume, cid, |payload| {
            value.encode(payload)
        })?,
    ))
}
pub fn build_consume_ack_frame(
    subscription_id: u64,
    epoch: u64,
    delivery_id: u64,
    message_credit: u32,
    byte_credit: u64,
) -> anyhow::Result<Vec<u8>> {
    let value = ConsumeAck {
        subscription_id,
        subscription_epoch: epoch,
        delivery_id,
        message_credit,
        byte_credit,
    };
    encode(FrameType::ConsumeAck, 0, |payload| value.encode(payload))
}
pub fn build_consume_nack_frame(
    subscription_id: u64,
    epoch: u64,
    delivery_id: u64,
    reason: String,
) -> anyhow::Result<Vec<u8>> {
    let value = ConsumeNack {
        subscription_id,
        subscription_epoch: epoch,
        delivery_id,
        reason,
    };
    encode(FrameType::ConsumeNack, 0, |payload| value.encode(payload))
}
pub fn build_flow_frame(
    subscription_id: u64,
    epoch: u64,
    message_credit: u32,
    byte_credit: u64,
) -> anyhow::Result<Vec<u8>> {
    let value = FlowControl {
        subscription_id,
        subscription_epoch: epoch,
        message_credit,
        byte_credit,
    };
    encode(FrameType::FlowControl, 0, |payload| value.encode(payload))
}
pub fn build_consume_commit_frame(
    subscription_id: u64,
    epoch: u64,
) -> anyhow::Result<(u64, Vec<u8>)> {
    let cid = next_cid();
    let value = ConsumeCommit {
        subscription_id,
        subscription_epoch: epoch,
    };
    Ok((
        cid,
        encode(FrameType::ConsumeCommit, cid, |payload| {
            value.encode(payload)
        })?,
    ))
}
pub fn build_consume_close_frame(subscription_id: u64, epoch: u64) -> anyhow::Result<Vec<u8>> {
    let value = ConsumeClose {
        subscription_id,
        subscription_epoch: epoch,
    };
    encode(FrameType::ConsumeClose, 0, |payload| value.encode(payload))
}
pub fn build_consume_partition_frame(
    kind: FrameType,
    subscription_id: u64,
    epoch: u64,
    partitions: Vec<(String, i32)>,
) -> anyhow::Result<Vec<u8>> {
    if !matches!(
        kind,
        FrameType::ConsumePause | FrameType::ConsumePartitionResume
    ) {
        anyhow::bail!("invalid consumer partition control frame {kind:?}");
    }
    let value = ConsumerPartitions {
        subscription_id,
        subscription_epoch: epoch,
        partitions,
    };
    encode(kind, 0, |payload| value.encode(payload))
}

#[derive(Debug)]
pub enum StreamFrame {
    Ready { cid: u64, value: ConsumeReady },
    Message(ConsumeMessage),
    Committed { cid: u64, value: ConsumeCommitted },
    Assign(RebalanceNotice),
    Revoke(RebalanceNotice),
    Close(ConsumeClose),
}
pub fn parse_stream_frame(bytes: &[u8]) -> anyhow::Result<StreamFrame> {
    let (header, payload) = split_frame(bytes)?;
    match header.kind {
        FrameType::ConsumeReady => Ok(StreamFrame::Ready {
            cid: header.cid,
            value: ConsumeReady::decode(payload).map_err(|e| anyhow::anyhow!(e))?,
        }),
        FrameType::ConsumeMessage => Ok(StreamFrame::Message(
            ConsumeMessage::decode(payload).map_err(|e| anyhow::anyhow!(e))?,
        )),
        FrameType::ConsumeCommitted => Ok(StreamFrame::Committed {
            cid: header.cid,
            value: ConsumeCommitted::decode(payload).map_err(|e| anyhow::anyhow!(e))?,
        }),
        FrameType::RebalanceAssign => Ok(StreamFrame::Assign(
            RebalanceNotice::decode(payload).map_err(|e| anyhow::anyhow!(e))?,
        )),
        FrameType::RebalanceRevoke => Ok(StreamFrame::Revoke(
            RebalanceNotice::decode(payload).map_err(|e| anyhow::anyhow!(e))?,
        )),
        FrameType::ConsumeClose => Ok(StreamFrame::Close(
            ConsumeClose::decode(payload).map_err(|e| anyhow::anyhow!(e))?,
        )),
        kind => anyhow::bail!("unexpected stream frame {kind:?}"),
    }
}

fn encode<T>(
    kind: FrameType,
    cid: u64,
    write: impl FnOnce(&mut BytesMut) -> Result<T, hi_kafka_proto::PayloadError>,
) -> anyhow::Result<Vec<u8>> {
    let mut payload = BytesMut::new();
    write(&mut payload).map_err(|e| anyhow::anyhow!(e))?;
    encode_raw(kind, cid, &payload)
}
fn encode_raw(kind: FrameType, cid: u64, payload: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut frame = BytesMut::new();
    encode_frame(kind, cid, payload, &mut frame).map_err(|e| anyhow::anyhow!(e))?;
    Ok(frame.to_vec())
}
fn split_frame(bytes: &[u8]) -> anyhow::Result<(hi_kafka_proto::Header, &[u8])> {
    if bytes.len() < HEADER_LEN {
        anyhow::bail!("frame header truncated");
    }
    let header = codec::decode_header(&bytes[..HEADER_LEN]).map_err(|e| anyhow::anyhow!(e))?;
    let end = HEADER_LEN
        .checked_add(header.payload_len as usize)
        .ok_or_else(|| anyhow::anyhow!("frame length overflow"))?;
    if bytes.len() != end {
        anyhow::bail!("frame length mismatch: {} != {end}", bytes.len());
    }
    Ok((header, &bytes[HEADER_LEN..end]))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn consumer_open_is_v2_stream_frame() {
        let (_, bytes) =
            build_consume_open_frame("c", "g", vec!["t".into()], vec![], 0, 0, 8, 1024).unwrap();
        assert_eq!(
            codec::decode_header(&bytes[..HEADER_LEN]).unwrap().kind,
            FrameType::ConsumeOpen
        );
    }

    #[test]
    fn consumer_resume_accepts_zero_initial_credit() {
        let (_, bytes) = build_consume_resume_frame(7, 3, &[9; RESUME_TOKEN_LEN], 0, 0).unwrap();
        let (header, payload) = split_frame(&bytes).unwrap();
        assert_eq!(header.kind, FrameType::ConsumeResume);
        let resume = ConsumeResume::decode(payload).unwrap();
        assert_eq!(resume.initial_message_credit, 0);
        assert_eq!(resume.initial_byte_credit, 0);
    }
}

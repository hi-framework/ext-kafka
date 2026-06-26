//! 协议编解码原语（非 PHP 入口）。
//!
//! 把字节级编/解码与 `#[php_function]` 入口解耦：本文件提供纯 Rust 函数，
//! lib.rs 里的 `#[php_function]` 调用它们做翻译。

use bytes::BytesMut;
use hi_kafka_proto::{
    CommitReq, CommitResp, ConsumerMessage, FrameType, HEADER_LEN, PayloadError, PollReq, PollResp,
    ProduceFnf, ProduceResp, RegisterClusterReq, RegisterClusterResp, SubscribeReq, SubscribeResp,
    UnsubscribeReq, codec, encode_frame,
};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_CID: AtomicU64 = AtomicU64::new(1);

pub fn next_cid() -> u64 {
    NEXT_CID.fetch_add(1, Ordering::Relaxed)
}

pub fn header_len() -> usize {
    HEADER_LEN
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
    let payload = build_payload(cluster, topic, key, value, headers, partition, timestamp_ms)?;
    let mut buf = BytesMut::new();
    encode_frame(FrameType::ProduceFnf, 0, &payload, &mut buf)
        .map_err(|e| anyhow::anyhow!("encode frame: {e}"))?;
    Ok(buf.to_vec())
}

pub fn build_req_frame(
    cluster: &str,
    topic: &str,
    key: &[u8],
    value: &[u8],
    headers: Vec<(String, bytes::Bytes)>,
    partition: i32,
    timestamp_ms: i64,
) -> anyhow::Result<(u64, Vec<u8>)> {
    let payload = build_payload(cluster, topic, key, value, headers, partition, timestamp_ms)?;
    let cid = next_cid();
    let mut buf = BytesMut::new();
    encode_frame(FrameType::ProduceReq, cid, &payload, &mut buf)
        .map_err(|e| anyhow::anyhow!("encode frame: {e}"))?;
    Ok((cid, buf.to_vec()))
}

#[derive(Debug)]
pub enum ParsedFrame {
    Resp {
        cid: u64,
        resp: ProduceResp,
    },
    Other {
        #[allow(dead_code)]
        kind: FrameType,
        #[allow(dead_code)]
        cid: u64,
        #[allow(dead_code)]
        payload_len: u32,
    },
}

pub fn parse_resp_frame(bytes: &[u8]) -> anyhow::Result<ParsedFrame> {
    if bytes.len() < HEADER_LEN {
        anyhow::bail!("frame too short: {} < {}", bytes.len(), HEADER_LEN);
    }
    let header = codec::decode_header(&bytes[..HEADER_LEN])
        .map_err(|e| anyhow::anyhow!("decode header: {e}"))?;
    let need = HEADER_LEN + header.payload_len as usize;
    if bytes.len() < need {
        anyhow::bail!("payload truncated: {} < {}", bytes.len(), need);
    }
    let payload = &bytes[HEADER_LEN..need];
    match header.kind {
        FrameType::ProduceResp => {
            let resp = ProduceResp::decode(payload)
                .map_err(|e| anyhow::anyhow!("decode resp: {e}"))?;
            Ok(ParsedFrame::Resp {
                cid: header.cid,
                resp,
            })
        }
        other => Ok(ParsedFrame::Other {
            kind: other,
            cid: header.cid,
            payload_len: header.payload_len,
        }),
    }
}

pub struct ParsedHeader {
    pub kind_byte: u8,
    pub cid: u64,
    pub payload_len: u32,
}

pub fn parse_header_only(bytes: &[u8]) -> anyhow::Result<ParsedHeader> {
    if bytes.len() < HEADER_LEN {
        anyhow::bail!("header too short: {} < {}", bytes.len(), HEADER_LEN);
    }
    let h = codec::decode_header(&bytes[..HEADER_LEN])
        .map_err(|e| anyhow::anyhow!("decode header: {e}"))?;
    Ok(ParsedHeader {
        kind_byte: h.kind as u8,
        cid: h.cid,
        payload_len: h.payload_len,
    })
}

fn build_payload(
    cluster: &str,
    topic: &str,
    key: &[u8],
    value: &[u8],
    headers: Vec<(String, bytes::Bytes)>,
    partition: i32,
    timestamp_ms: i64,
) -> Result<BytesMut, PayloadError> {
    let msg = ProduceFnf {
        cluster: cluster.to_string(),
        topic: topic.to_string(),
        key: bytes::Bytes::copy_from_slice(key),
        value: bytes::Bytes::copy_from_slice(value),
        partition,
        timestamp_ms,
        headers,
    };
    let mut buf = BytesMut::new();
    msg.encode(&mut buf)?;
    Ok(buf)
}

// ============================================================================
// Consumer 协议原语
// ============================================================================

pub fn build_subscribe_frame(
    cluster: &str,
    group_id: &str,
    topics: Vec<String>,
    config: Vec<(String, String)>,
) -> anyhow::Result<(u64, Vec<u8>)> {
    let req = SubscribeReq {
        cluster: cluster.to_string(),
        group_id: group_id.to_string(),
        topics,
        config,
    };
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| anyhow::anyhow!("encode SubscribeReq: {e}"))?;
    let cid = next_cid();
    let mut frame = BytesMut::new();
    encode_frame(FrameType::SubscribeReq, cid, &payload, &mut frame)
        .map_err(|e| anyhow::anyhow!("encode frame: {e}"))?;
    Ok((cid, frame.to_vec()))
}

pub fn build_poll_frame(
    subscription_id: u64,
    max_messages: u32,
    timeout_ms: u32,
) -> anyhow::Result<(u64, Vec<u8>)> {
    let req = PollReq {
        subscription_id,
        max_messages,
        timeout_ms,
    };
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| anyhow::anyhow!("encode PollReq: {e}"))?;
    let cid = next_cid();
    let mut frame = BytesMut::new();
    encode_frame(FrameType::PollReq, cid, &payload, &mut frame)
        .map_err(|e| anyhow::anyhow!("encode frame: {e}"))?;
    Ok((cid, frame.to_vec()))
}

pub fn build_commit_frame(subscription_id: u64) -> anyhow::Result<(u64, Vec<u8>)> {
    let req = CommitReq { subscription_id };
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| anyhow::anyhow!("encode CommitReq: {e}"))?;
    let cid = next_cid();
    let mut frame = BytesMut::new();
    encode_frame(FrameType::CommitReq, cid, &payload, &mut frame)
        .map_err(|e| anyhow::anyhow!("encode frame: {e}"))?;
    Ok((cid, frame.to_vec()))
}

/// Unsubscribe 是 fire-and-forget（无 RESP），cid 固定为 0。
pub fn build_unsubscribe_frame(subscription_id: u64) -> anyhow::Result<Vec<u8>> {
    let req = UnsubscribeReq { subscription_id };
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| anyhow::anyhow!("encode UnsubscribeReq: {e}"))?;
    let mut frame = BytesMut::new();
    encode_frame(FrameType::Unsubscribe, 0, &payload, &mut frame)
        .map_err(|e| anyhow::anyhow!("encode frame: {e}"))?;
    Ok(frame.to_vec())
}

pub fn build_register_cluster_frame(
    cluster: &str,
    config: Vec<(String, String)>,
) -> anyhow::Result<(u64, Vec<u8>)> {
    let req = RegisterClusterReq {
        cluster: cluster.to_string(),
        config,
    };
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| anyhow::anyhow!("encode RegisterClusterReq: {e}"))?;
    let cid = next_cid();
    let mut frame = BytesMut::new();
    encode_frame(FrameType::RegisterClusterReq, cid, &payload, &mut frame)
        .map_err(|e| anyhow::anyhow!("encode frame: {e}"))?;
    Ok((cid, frame.to_vec()))
}

#[derive(Debug)]
pub enum ConsumerResp {
    SubscribeOk { cid: u64, subscription_id: u64 },
    SubscribeErr { cid: u64, message: String },
    PollOk { cid: u64, messages: Vec<ConsumerMessage> },
    PollErr { cid: u64, message: String },
    CommitOk { cid: u64 },
    CommitErr { cid: u64, message: String },
    RegisterClusterOk { cid: u64 },
    RegisterClusterErr { cid: u64, message: String },
}

pub fn parse_consumer_resp_frame(bytes: &[u8]) -> anyhow::Result<ConsumerResp> {
    if bytes.len() < HEADER_LEN {
        anyhow::bail!("frame too short: {} < {}", bytes.len(), HEADER_LEN);
    }
    let header = codec::decode_header(&bytes[..HEADER_LEN])
        .map_err(|e| anyhow::anyhow!("decode header: {e}"))?;
    let need = HEADER_LEN + header.payload_len as usize;
    if bytes.len() < need {
        anyhow::bail!("payload truncated: {} < {}", bytes.len(), need);
    }
    let payload = &bytes[HEADER_LEN..need];

    Ok(match header.kind {
        FrameType::SubscribeResp => match SubscribeResp::decode(payload)
            .map_err(|e| anyhow::anyhow!("decode SubscribeResp: {e}"))?
        {
            SubscribeResp::Ok { subscription_id } => ConsumerResp::SubscribeOk {
                cid: header.cid,
                subscription_id,
            },
            SubscribeResp::Err { message } => ConsumerResp::SubscribeErr {
                cid: header.cid,
                message,
            },
        },
        FrameType::PollResp => match PollResp::decode(payload)
            .map_err(|e| anyhow::anyhow!("decode PollResp: {e}"))?
        {
            PollResp::Ok { messages } => ConsumerResp::PollOk {
                cid: header.cid,
                messages,
            },
            PollResp::Err { message } => ConsumerResp::PollErr {
                cid: header.cid,
                message,
            },
        },
        FrameType::CommitResp => match CommitResp::decode(payload)
            .map_err(|e| anyhow::anyhow!("decode CommitResp: {e}"))?
        {
            CommitResp::Ok => ConsumerResp::CommitOk { cid: header.cid },
            CommitResp::Err { message } => ConsumerResp::CommitErr {
                cid: header.cid,
                message,
            },
        },
        FrameType::RegisterClusterResp => match RegisterClusterResp::decode(payload)
            .map_err(|e| anyhow::anyhow!("decode RegisterClusterResp: {e}"))?
        {
            RegisterClusterResp::Ok => ConsumerResp::RegisterClusterOk { cid: header.cid },
            RegisterClusterResp::Err { message } => ConsumerResp::RegisterClusterErr {
                cid: header.cid,
                message,
            },
        },
        other => anyhow::bail!("unexpected consumer frame kind: {other:?}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fnf_frame_roundtrip_via_parse_header() {
        let bytes = build_fnf_frame("c", "t", b"k", b"v", vec![], -1, -1).unwrap();
        let h = parse_header_only(&bytes).unwrap();
        assert_eq!(h.kind_byte, FrameType::ProduceFnf as u8);
        assert_eq!(h.cid, 0);
    }

    #[test]
    fn test_req_frame_assigns_monotonic_cid() {
        let (cid1, _) = build_req_frame("c", "t", b"k", b"v", vec![], -1, -1).unwrap();
        let (cid2, _) = build_req_frame("c", "t", b"k", b"v", vec![], -1, -1).unwrap();
        assert!(cid2 > cid1);
    }
}

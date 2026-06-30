//! 统一错误响应。
//!
//! worker 任何 handler 失败时回 [`FrameType::Error`](crate::FrameType::Error) 帧，
//! 载荷为 [`ErrorResp`]——让 PHP 侧拿到**机器可读**的 [`ErrorKind`] + retryable +
//! 原生 librdkafka 码，而不是只能 `str_contains(message)` 猜。
//!
//! 线格式：
//! ```text
//! [u16 kind][u8 retryable][i32 native_code][u16 msg_len][msg]
//! ```

use crate::payload::PayloadError;
use bytes::{Buf, BufMut, BytesMut};

/// 统一错误大类，贯穿 worker → ext → PHP。
///
/// u16 线编码；**未知值解码为 `Internal`**（前向兼容：新端新增 kind，旧端不致解码失败）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum ErrorKind {
    /// 未分类 / worker 内部错误（含 panic 回退）
    Internal = 0,
    /// worker 起不来 / 连不上 / 自动重试耗尽
    WorkerUnavailable = 1,
    /// IPC 或 broker 操作超时
    Timeout = 2,
    /// worker 正在停机，拒绝新请求
    WorkerDraining = 3,
    /// 协议错乱（版本不匹配 / 帧类型意外 / cid 不匹配 / 解码失败）
    Protocol = 4,
    /// 集群未注册（需先 registerCluster）
    ClusterNotRegistered = 5,
    /// 调用参数非法（平行数组长度不一致 / 缺 bootstrap.servers / 非法 op 等）
    InvalidArgument = 6,
    /// 集群无 transactional.id，无法做事务操作
    NotTransactional = 7,
    /// subscription 不存在（通常被扩展端 virtual_id 自愈吸收）
    SubscriptionNotFound = 8,
    /// broker 可重试错（QueueFull / Leader 选举 / 网络 / 超时 等）
    BrokerRetryable = 9,
    /// 消息超过 broker / 协议大小上限
    MessageTooLarge = 10,
    /// SASL 认证 / ACL 授权失败（致命，重试无意义）
    AuthnAuthz = 11,
    /// offset / seek 目标非法（越界 / 无对应时间戳）
    OffsetInvalid = 12,
    /// 事务状态错（未 begin / 并发事务 / fenced 等）
    TxnState = 13,
    /// 未知 topic 或 partition
    UnknownTopicOrPartition = 14,
}

impl ErrorKind {
    pub fn as_u16(self) -> u16 {
        self as u16
    }

    /// 未知值映射为 `Internal`，保证前向兼容。
    pub fn from_u16(v: u16) -> Self {
        match v {
            1 => Self::WorkerUnavailable,
            2 => Self::Timeout,
            3 => Self::WorkerDraining,
            4 => Self::Protocol,
            5 => Self::ClusterNotRegistered,
            6 => Self::InvalidArgument,
            7 => Self::NotTransactional,
            8 => Self::SubscriptionNotFound,
            9 => Self::BrokerRetryable,
            10 => Self::MessageTooLarge,
            11 => Self::AuthnAuthz,
            12 => Self::OffsetInvalid,
            13 => Self::TxnState,
            14 => Self::UnknownTopicOrPartition,
            _ => Self::Internal,
        }
    }

    /// 该类错误默认是否值得重试（worker / ext 构造时可显式覆盖）。
    pub fn default_retryable(self) -> bool {
        matches!(
            self,
            Self::WorkerUnavailable | Self::Timeout | Self::WorkerDraining | Self::BrokerRetryable
        )
    }

    /// 稳定的字符串名（给 PHP `getKindName()` / 日志用）。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Internal => "INTERNAL",
            Self::WorkerUnavailable => "WORKER_UNAVAILABLE",
            Self::Timeout => "TIMEOUT",
            Self::WorkerDraining => "WORKER_DRAINING",
            Self::Protocol => "PROTOCOL",
            Self::ClusterNotRegistered => "CLUSTER_NOT_REGISTERED",
            Self::InvalidArgument => "INVALID_ARGUMENT",
            Self::NotTransactional => "NOT_TRANSACTIONAL",
            Self::SubscriptionNotFound => "SUBSCRIPTION_NOT_FOUND",
            Self::BrokerRetryable => "BROKER_RETRYABLE",
            Self::MessageTooLarge => "MESSAGE_TOO_LARGE",
            Self::AuthnAuthz => "AUTHN_AUTHZ",
            Self::OffsetInvalid => "OFFSET_INVALID",
            Self::TxnState => "TXN_STATE",
            Self::UnknownTopicOrPartition => "UNKNOWN_TOPIC_OR_PARTITION",
        }
    }
}

/// [`FrameType::Error`](crate::FrameType::Error) 帧的载荷。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorResp {
    pub kind: ErrorKind,
    pub retryable: bool,
    /// 原生 librdkafka `rd_kafka_resp_err_t`（producer / consumer broker 错误）；无则 0。
    pub native_code: i32,
    pub message: String,
}

impl ErrorResp {
    /// 用 kind 的默认 retryable + 无 native_code 构造。
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            retryable: kind.default_retryable(),
            native_code: 0,
            message: message.into(),
        }
    }

    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        buf.put_u16(self.kind.as_u16());
        buf.put_u8(self.retryable as u8);
        buf.put_i32(self.native_code);
        write_str_u16(&self.message, buf)?;
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        // kind(2) + retryable(1) + native_code(4)
        if buf.remaining() < 7 {
            return Err(PayloadError::Truncated);
        }
        let kind = ErrorKind::from_u16(buf.get_u16());
        let retryable = buf.get_u8() != 0;
        let native_code = buf.get_i32();
        let message = read_str_u16(&mut buf, "error_message")?;
        Ok(Self {
            kind,
            retryable,
            native_code,
            message,
        })
    }
}

// 内部 helpers —— 与其它模块一致，避免跨模块 pub
fn write_str_u16(s: &str, buf: &mut BytesMut) -> Result<(), PayloadError> {
    if s.len() > u16::MAX as usize {
        return Err(PayloadError::FieldTooLarge(s.len()));
    }
    buf.put_u16(s.len() as u16);
    buf.put_slice(s.as_bytes());
    Ok(())
}

fn read_str_u16(buf: &mut &[u8], field: &'static str) -> Result<String, PayloadError> {
    if buf.remaining() < 2 {
        return Err(PayloadError::Truncated);
    }
    let len = buf.get_u16() as usize;
    if buf.remaining() < len {
        return Err(PayloadError::Truncated);
    }
    let bytes = buf.copy_to_bytes(len).to_vec();
    String::from_utf8(bytes).map_err(|_| PayloadError::InvalidUtf8 { field })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_resp_roundtrip() {
        let e = ErrorResp {
            kind: ErrorKind::AuthnAuthz,
            retryable: false,
            native_code: -167,
            message: "sasl auth failed".into(),
        };
        let mut buf = BytesMut::new();
        e.encode(&mut buf).unwrap();
        assert_eq!(ErrorResp::decode(&buf).unwrap(), e);
    }

    #[test]
    fn test_unknown_kind_maps_to_internal() {
        let mut buf = BytesMut::new();
        buf.put_u16(60000); // 未知 kind
        buf.put_u8(0);
        buf.put_i32(0);
        buf.put_u16(0); // 空 message
        assert_eq!(ErrorResp::decode(&buf).unwrap().kind, ErrorKind::Internal);
    }

    #[test]
    fn test_kind_u16_roundtrip() {
        for k in [
            ErrorKind::Internal,
            ErrorKind::WorkerUnavailable,
            ErrorKind::BrokerRetryable,
            ErrorKind::TxnState,
            ErrorKind::UnknownTopicOrPartition,
        ] {
            assert_eq!(ErrorKind::from_u16(k.as_u16()), k);
        }
    }

    #[test]
    fn test_default_retryable() {
        assert!(ErrorKind::Timeout.default_retryable());
        assert!(ErrorKind::WorkerUnavailable.default_retryable());
        assert!(!ErrorKind::AuthnAuthz.default_retryable());
        assert!(!ErrorKind::InvalidArgument.default_retryable());
    }

    #[test]
    fn test_truncated() {
        let buf = [0u8; 3]; // < 7
        assert!(matches!(
            ErrorResp::decode(&buf),
            Err(PayloadError::Truncated)
        ));
    }
}

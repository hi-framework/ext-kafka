//! 统一错误响应。
//!
//! worker 任何 handler 失败时回 [`FrameType::Error`](crate::FrameType::Error) 帧，
//! 载荷为 [`ErrorResp`]——让 PHP 侧拿到**机器可读**的 [`ErrorKind`] + retryable +
//! 原生 librdkafka 码，而不是只能 `str_contains(message)` 猜。
//!
//! v2 线格式：
//! ```text
//! [u16 kind][u8 retryable][u8 outcome][i32 native_code][u16 msg_len][msg]
//! ```

use crate::payload::PayloadError;
use bytes::{Buf, BufMut, BytesMut};

/// 统一错误大类，贯穿 worker → ext → PHP。
///
/// u16 线编码；**未知值解码为 `Internal`**（前向兼容：新端新增 kind，旧端不致解码失败）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum ErrorKind {
    Internal = 0,
    InvalidArgument = 1,
    Protocol = 2,
    FrameTooLarge = 3,
    WorkerDraining = 4,
    ClusterNotRegistered = 5,
    SubscriptionNotFound = 6,
    StaleEpoch = 7,
    StaleDelivery = 8,
    CreditExhausted = 9,
    RecordTooLarge = 10,
    Authentication = 11,
    Authorization = 12,
    Rebalance = 13,
    Commit = 14,
    ProducerQueueFull = 15,
    Timeout = 16,
    TransactionState = 17,
    TransactionOwner = 18,
    WorkerUnavailable = 19,
    BrokerRetryable = 20,
    UnknownTopicOrPartition = 21,
    OffsetInvalid = 22,
    NotTransactional = 23,
}

impl ErrorKind {
    pub fn as_u16(self) -> u16 {
        self as u16
    }

    /// 未知值映射为 `Internal`，保证前向兼容。
    pub fn from_u16(v: u16) -> Self {
        match v {
            1 => Self::InvalidArgument,
            2 => Self::Protocol,
            3 => Self::FrameTooLarge,
            4 => Self::WorkerDraining,
            5 => Self::ClusterNotRegistered,
            6 => Self::SubscriptionNotFound,
            7 => Self::StaleEpoch,
            8 => Self::StaleDelivery,
            9 => Self::CreditExhausted,
            10 => Self::RecordTooLarge,
            11 => Self::Authentication,
            12 => Self::Authorization,
            13 => Self::Rebalance,
            14 => Self::Commit,
            15 => Self::ProducerQueueFull,
            16 => Self::Timeout,
            17 => Self::TransactionState,
            18 => Self::TransactionOwner,
            19 => Self::WorkerUnavailable,
            20 => Self::BrokerRetryable,
            21 => Self::UnknownTopicOrPartition,
            22 => Self::OffsetInvalid,
            23 => Self::NotTransactional,
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
            Self::InvalidArgument => "INVALID_ARGUMENT",
            Self::Protocol => "PROTOCOL",
            Self::FrameTooLarge => "FRAME_TOO_LARGE",
            Self::WorkerDraining => "WORKER_DRAINING",
            Self::ClusterNotRegistered => "CLUSTER_NOT_REGISTERED",
            Self::SubscriptionNotFound => "SUBSCRIPTION_NOT_FOUND",
            Self::StaleEpoch => "STALE_EPOCH",
            Self::StaleDelivery => "STALE_DELIVERY",
            Self::CreditExhausted => "CREDIT_EXHAUSTED",
            Self::RecordTooLarge => "RECORD_TOO_LARGE",
            Self::Authentication => "AUTHENTICATION",
            Self::Authorization => "AUTHORIZATION",
            Self::Rebalance => "REBALANCE",
            Self::Commit => "COMMIT",
            Self::ProducerQueueFull => "PRODUCER_QUEUE_FULL",
            Self::Timeout => "TIMEOUT",
            Self::TransactionState => "TRANSACTION_STATE",
            Self::TransactionOwner => "TRANSACTION_OWNER",
            Self::WorkerUnavailable => "WORKER_UNAVAILABLE",
            Self::BrokerRetryable => "BROKER_RETRYABLE",
            Self::UnknownTopicOrPartition => "UNKNOWN_TOPIC_OR_PARTITION",
            Self::OffsetInvalid => "OFFSET_INVALID",
            Self::NotTransactional => "NOT_TRANSACTIONAL",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OperationOutcome {
    NotApplied = 0,
    Applied = 1,
    Unknown = 2,
}

impl OperationOutcome {
    fn decode(tag: u8) -> Result<Self, PayloadError> {
        match tag {
            0 => Ok(Self::NotApplied),
            1 => Ok(Self::Applied),
            2 => Ok(Self::Unknown),
            tag => Err(PayloadError::InvalidTag {
                tag,
                field: "operation_outcome",
            }),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotApplied => "not_applied",
            Self::Applied => "applied",
            Self::Unknown => "unknown",
        }
    }
}

/// [`FrameType::Error`](crate::FrameType::Error) 帧的载荷。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorResp {
    pub kind: ErrorKind,
    pub retryable: bool,
    pub outcome: OperationOutcome,
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
            outcome: OperationOutcome::NotApplied,
            native_code: 0,
            message: message.into(),
        }
    }

    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        buf.put_u16(self.kind.as_u16());
        buf.put_u8(self.retryable as u8);
        buf.put_u8(self.outcome as u8);
        buf.put_i32(self.native_code);
        write_str_u16(&self.message, buf)?;
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        // kind(2) + retryable(1) + outcome(1) + native_code(4)
        if buf.remaining() < 8 {
            return Err(PayloadError::Truncated);
        }
        let kind = ErrorKind::from_u16(buf.get_u16());
        let retryable = buf.get_u8() != 0;
        let outcome = OperationOutcome::decode(buf.get_u8())?;
        let native_code = buf.get_i32();
        let message = read_str_u16(&mut buf, "error_message")?;
        Ok(Self {
            kind,
            retryable,
            outcome,
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
            kind: ErrorKind::Authentication,
            retryable: false,
            outcome: OperationOutcome::NotApplied,
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
            ErrorKind::TransactionState,
            ErrorKind::UnknownTopicOrPartition,
        ] {
            assert_eq!(ErrorKind::from_u16(k.as_u16()), k);
        }
    }

    #[test]
    fn test_default_retryable() {
        assert!(ErrorKind::Timeout.default_retryable());
        assert!(ErrorKind::WorkerUnavailable.default_retryable());
        assert!(!ErrorKind::Authentication.default_retryable());
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

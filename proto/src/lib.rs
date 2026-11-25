//! Hi-Kafka IPC protocol v2.
//!
//! 帧格式：
//! ```text
//! 0       4       5      13                       N
//! +-------+-------+-------+------------------------+
//! | len   | type  | cid   |    payload            |
//! | (u32) | (u8)  | (u64) |                       |
//! +-------+-------+-------+------------------------+
//! ```
//!
//! - `len`: payload 长度（不含 header 自身的 13 字节），最大 16 MB
//! - `type`: 帧类型（[`FrameType`]）
//! - `cid`: correlation id，0 表示单向无应答帧
//! - `payload`: 帧体（按 type 解释）

pub mod cluster;
pub mod codec;
pub mod error;
pub mod frame;
pub mod oauth;
pub mod payload;
pub mod stream;

pub use cluster::{RegisterClusterReq, RegisterClusterResp};
pub use codec::{decode_header, encode_frame, CodecError, Header};
pub use error::{ErrorKind, ErrorResp, OperationOutcome};
pub use frame::{
    Frame, FrameType, DEFAULT_PRODUCE_TIMEOUT_MS, HEADER_LEN, MAX_PAYLOAD_LEN, PROTOCOL_MAJOR,
};
pub use oauth::{SetOAuthBearerTokenReq, SetOAuthBearerTokenResp};
pub use payload::{
    ConnectionRole, DeliveryAck, DeliveryErr, Goodbye, GoodbyeResp, HelloReq, HelloResp,
    MessageHeader, PayloadError, ProduceFields, ProduceFnf, ProduceReq, ProduceResp,
    AUTO_PARTITION, AUTO_TIMESTAMP, CAP_ALL, CAP_CONSUMER_STREAM, CAP_GOODBYE_FLUSH, CAP_PRODUCER,
    MAX_MESSAGE_HEADERS,
};
pub use stream::{
    CommittedOffset, ConsumeAck, ConsumeClose, ConsumeCommit, ConsumeCommitted, ConsumeMessage,
    ConsumeNack, ConsumeOpen, ConsumeReady, ConsumeResume, ConsumerPartitions, FlowControl,
    RebalanceNotice, ResumeToken, RESUME_TOKEN_LEN,
};

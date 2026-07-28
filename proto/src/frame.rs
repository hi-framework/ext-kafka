use bytes::Bytes;

/// 帧头长度：4 (len) + 1 (type) + 8 (cid)
pub const HEADER_LEN: usize = 13;

/// 单帧 payload 最大长度：16 MiB
pub const MAX_PAYLOAD_LEN: usize = 16 * 1024 * 1024;

/// Protocol v2 is intentionally incompatible with the legacy batch consumer.
/// A v1 worker/client pair must use its old socket and is never accepted here.
pub const PROTOCOL_MAJOR: u8 = 2;

/// Default synchronous produce timeout used by public APIs before encoding.
pub const DEFAULT_PRODUCE_TIMEOUT_MS: u32 = 5000;

/// 帧类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    /// 握手帧。首次连接必须由 PHP 端发出。
    Hello = 0x01,
    /// 心跳请求
    Ping = 0x02,
    /// 心跳响应
    Pong = 0x03,
    /// Producer fire-and-forget（无应答）
    ProduceFnf = 0x10,
    /// Producer 异步带 ACK 请求
    ProduceReq = 0x11,
    /// Producer ACK 响应
    ProduceResp = 0x12,
    /// 注册/覆盖集群（cluster name + librdkafka 配置）
    RegisterClusterReq = 0x27,
    /// 注册响应
    RegisterClusterResp = 0x28,
    /// 推送 SASL/OAUTHBEARER token 给指定 cluster
    SetOAuthBearerTokenReq = 0x35,
    /// SetOAuthBearerToken 响应
    SetOAuthBearerTokenResp = 0x36,
    /// 流控（credit 续费）
    FlowControl = 0x30,
    /// 错误响应
    Error = 0x40,
    /// worker 即将停机通知
    ShutdownNotice = 0x41,
    /// 客户端告别（PHP 进程退出时主动发）：fire-and-forget，无 payload。
    /// worker 收到后若此连接关闭即成最后一个连接且无活跃订阅，则立即自退，
    /// 不必等 idle 超时。发送失败 / worker 已死 → 退化回 idle 自退。
    Goodbye = 0x42,
    /// v2 Consumer：建立专用 stream。
    ConsumeOpen = 0x50,
    /// v2 Consumer：stream 建立/恢复成功。
    ConsumeReady = 0x51,
    /// v2 Consumer：在恢复租约内重建 stream。
    ConsumeResume = 0x52,
    /// v2 Consumer：单条 record delivery。
    ConsumeMessage = 0x53,
    /// v2 Consumer：应用成功处理 delivery。
    ConsumeAck = 0x54,
    /// v2 Consumer：应用处理 delivery 失败。
    ConsumeNack = 0x55,
    /// v2 Consumer：提交连续 ACK watermark。
    ConsumeCommit = 0x56,
    /// v2 Consumer：返回实际提交的 next offsets。
    ConsumeCommitted = 0x57,
    /// v2 Consumer：assignment 生效通知。
    RebalanceAssign = 0x58,
    /// v2 Consumer：partition revoke 通知。
    RebalanceRevoke = 0x59,
    /// v2 Consumer：正常或异常关闭 stream。
    ConsumeClose = 0x5A,
    /// v2 Consumer：暂停指定 assignment partitions。
    ConsumePause = 0x5B,
    /// v2 Consumer：恢复指定 assignment partitions。
    ConsumePartitionResume = 0x5C,
}

impl FrameType {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0x01 => Self::Hello,
            0x02 => Self::Ping,
            0x03 => Self::Pong,
            0x10 => Self::ProduceFnf,
            0x11 => Self::ProduceReq,
            0x12 => Self::ProduceResp,
            0x27 => Self::RegisterClusterReq,
            0x28 => Self::RegisterClusterResp,
            0x35 => Self::SetOAuthBearerTokenReq,
            0x36 => Self::SetOAuthBearerTokenResp,
            0x30 => Self::FlowControl,
            0x40 => Self::Error,
            0x41 => Self::ShutdownNotice,
            0x42 => Self::Goodbye,
            0x50 => Self::ConsumeOpen,
            0x51 => Self::ConsumeReady,
            0x52 => Self::ConsumeResume,
            0x53 => Self::ConsumeMessage,
            0x54 => Self::ConsumeAck,
            0x55 => Self::ConsumeNack,
            0x56 => Self::ConsumeCommit,
            0x57 => Self::ConsumeCommitted,
            0x58 => Self::RebalanceAssign,
            0x59 => Self::RebalanceRevoke,
            0x5A => Self::ConsumeClose,
            0x5B => Self::ConsumePause,
            0x5C => Self::ConsumePartitionResume,
            _ => return None,
        })
    }
}

/// 已解码的帧
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub kind: FrameType,
    pub cid: u64,
    pub payload: Bytes,
}

impl Frame {
    pub fn new(kind: FrameType, cid: u64, payload: impl Into<Bytes>) -> Self {
        Self {
            kind,
            cid,
            payload: payload.into(),
        }
    }
}

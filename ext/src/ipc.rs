//! IPC 客户端：编帧、连 worker、读写。
//!
//! 把 `lib.rs` 和 `client.rs` 共享的 IPC 逻辑抽出来，避免重复。
//! Phase 2.9 起所有出向连接走 [`pool`] 复用。
//! Phase 2.12 起 worker 就绪状态由 [`worker_health`] 缓存。

use crate::cluster_replay::{self, OAuthToken};
use crate::pool;
use crate::protocol;
use crate::spawn;
use crate::worker_health::{self, EnsureOutcome};
use bytes::BytesMut;
use hi_kafka_proto::{
    codec, encode_frame, ErrorKind, ErrorResp, FrameType, OperationOutcome, ProduceResp,
    RegisterClusterReq, RegisterClusterResp, SetOAuthBearerTokenReq, SetOAuthBearerTokenResp,
    HEADER_LEN,
};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

// === 重试统计（供 hi_kafka_retry_stats 暴露） ============================

static RETRY_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
static RETRY_SUCCESSES: AtomicU64 = AtomicU64::new(0);
static RETRY_FAILURES: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy)]
pub struct RetryStats {
    pub attempts: u64,
    pub successes: u64,
    pub failures: u64,
}

pub fn retry_stats() -> RetryStats {
    RetryStats {
        attempts: RETRY_ATTEMPTS.load(Ordering::Relaxed),
        successes: RETRY_SUCCESSES.load(Ordering::Relaxed),
        failures: RETRY_FAILURES.load(Ordering::Relaxed),
    }
}

static NEXT_CID: AtomicU64 = AtomicU64::new(1);

pub fn next_cid() -> u64 {
    NEXT_CID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, thiserror::Error)]
pub enum IpcError {
    #[error("ensure_worker: {0}")]
    EnsureWorker(spawn::SpawnError),

    #[error("encode: {0}")]
    Encode(String),

    #[error("pool: {0}")]
    Pool(#[from] pool::PoolError),

    #[error("io: {0}")]
    Io(std::io::Error),

    #[error("decode resp: {0}")]
    DecodeResp(String),

    #[error("unexpected resp frame type: {0:?}")]
    UnexpectedKind(FrameType),

    #[error("cid mismatch: sent {sent}, got {got}")]
    CidMismatch { sent: u64, got: u64 },

    #[error("server error: {0}")]
    Server(String),

    /// worker 回的结构化错误（Error 帧）：携带机器可读 kind / retryable / 原生码。
    #[error("[{}] {message}", .kind.as_str())]
    Worker {
        kind: ErrorKind,
        retryable: bool,
        outcome: OperationOutcome,
        native_code: i32,
        message: String,
    },
}

impl From<std::io::Error> for IpcError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// 公共前置：保证 worker 就绪。命中缓存时零开销；
/// L: 若本次是新 spawn 出的 worker（cache miss），自动 replay 已知 cluster + token
/// 让"业务调用 → worker 死 → 透明重连"链路覆盖完整状态。
///
/// pub(crate) 以便 PHP `ensureWorker()` 入口（lib.rs / client.rs）也走这条路径——
/// 业务上"显式 ensureWorker" 等价于"worker 一定 alive 且所有 cluster 已注册"。
pub(crate) fn ensure(socket: &str) -> Result<(), IpcError> {
    let outcome = worker_health::ensure(socket).map_err(IpcError::EnsureWorker)?;
    if outcome == EnsureOutcome::JustSpawned {
        replay_clusters(socket)?;
    }
    Ok(())
}

/// 把 cluster_replay 里的所有 cluster 注册重发到（可能是新起的）worker。
/// 失败会立即返回——因为重放发生在 ensure 调用栈内，让上层拿到清晰错误。
pub(crate) fn replay_clusters(socket: &str) -> Result<(), IpcError> {
    let snap = cluster_replay::snapshot(socket);
    if snap.is_empty() {
        return Ok(());
    }
    tracing::debug!(
        socket,
        count = snap.len(),
        "replaying cluster registrations to fresh worker"
    );
    let timeout = Duration::from_secs(5);
    for (cluster, entry) in snap {
        if !entry.config.is_empty() {
            register_cluster_once(socket, &cluster, entry.config, timeout)?;
        }
        if let Some(t) = entry.oauth_token {
            set_oauth_bearer_token_once(
                socket,
                &cluster,
                &t.token_value,
                t.lifetime_ms,
                &t.principal_name,
                t.extensions,
                timeout,
            )?;
        }
    }
    Ok(())
}

/// 判定一个错误是否暗示 worker 已死，需要让缓存失效以触发下次重启。
///
/// - `Pool(connect refused)` / `Io(BrokenPipe/ConnectionReset)`：worker 八成挂了
/// - `Encode` / `DecodeResp` / `CidMismatch`：协议层问题，worker 可能还活着
fn should_invalidate(e: &IpcError) -> bool {
    use std::io::ErrorKind;
    match e {
        IpcError::EnsureWorker(_) => false, // ensure 本身失败：缓存还没建立
        IpcError::Pool(_) => true,          // 拿不到连接，多半 worker 挂了
        IpcError::Io(io_err) => matches!(
            io_err.kind(),
            ErrorKind::BrokenPipe
                | ErrorKind::ConnectionReset
                | ErrorKind::ConnectionRefused
                | ErrorKind::ConnectionAborted
                | ErrorKind::UnexpectedEof
        ),
        IpcError::Encode(_) | IpcError::DecodeResp(_) => false,
        IpcError::UnexpectedKind(_) | IpcError::CidMismatch { .. } => false,
        IpcError::Server(_) => false,     // 业务层错误，worker 还活着
        IpcError::Worker { .. } => false, // worker 回的结构化业务错误，worker 还活着
    }
}

/// 通用自动恢复包装。两类恢复各自最多一次，避免故障时形成重试风暴：
///
/// - worker 已死（[`should_invalidate`]）：让 health 缓存失效，重试时 `ensure` 会重新
///   探测，必要时重新 spawn 并重放本进程记录的集群；
/// - `CLUSTER_NOT_REGISTERED`：worker 被其它进程重启过，本进程的集群不在新 worker
///   中。重放后重试；该错误在执行前判定，重试不会产生重复消息。
///
/// worker 被其它进程重启时两者会先后出现：旧连接先 EOF，重试后才遇到未注册。
fn with_retry<F, T>(socket: &str, mut op: F) -> Result<T, IpcError>
where
    F: FnMut() -> Result<T, IpcError>,
{
    let mut reconnected = false;
    let mut replayed = false;
    loop {
        let error = match op() {
            Ok(value) => {
                if reconnected || replayed {
                    RETRY_SUCCESSES.fetch_add(1, Ordering::Relaxed);
                }
                return Ok(value);
            }
            Err(error) => error,
        };
        let recover_connection = !reconnected && should_invalidate(&error);
        let recover_registration =
            !replayed && is_cluster_not_registered(&error) && cluster_replay::count(socket) > 0;
        if !recover_connection && !recover_registration {
            if reconnected || replayed {
                RETRY_FAILURES.fetch_add(1, Ordering::Relaxed);
            }
            if should_invalidate(&error) {
                worker_health::invalidate(socket);
            }
            return Err(error);
        }
        RETRY_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
        if recover_connection {
            reconnected = true;
            worker_health::invalidate(socket);
        } else {
            replayed = true;
            replay_clusters(socket)?;
        }
    }
}

fn is_cluster_not_registered(e: &IpcError) -> bool {
    matches!(
        e,
        IpcError::Worker {
            kind: ErrorKind::ClusterNotRegistered,
            ..
        }
    )
}

/// 把本进程记录的全部集群注册与 OAuth token 重放到 worker。
///
/// 供协程 driver 在收到 `CLUSTER_NOT_REGISTERED` 时调用：worker 可能被其它进程
/// 重启过，本进程的注册只存在于扩展的重放缓存里。
pub fn replay(socket: &str) -> Result<(), IpcError> {
    ensure(socket)?;
    replay_clusters(socket)
}

/// 通用：write 帧，遇 IO 错就 poison 连接，让池下次重建。
fn write_frame(
    conn: &mut pool::PooledConn,
    kind: FrameType,
    cid: u64,
    payload: &[u8],
    write_timeout: Option<Duration>,
) -> Result<(), IpcError> {
    let mut frame = BytesMut::new();
    encode_frame(kind, cid, payload, &mut frame).map_err(|e| IpcError::Encode(e.to_string()))?;
    write_encoded(conn, &frame, write_timeout)
}

/// 写一帧已编码好的完整字节，遇 IO 错就 poison 连接。
fn write_encoded(
    conn: &mut pool::PooledConn,
    frame: &[u8],
    write_timeout: Option<Duration>,
) -> Result<(), IpcError> {
    let stream = conn.stream_mut();
    stream.set_write_timeout(write_timeout)?;
    if let Err(e) = stream.write_all(frame).and_then(|_| stream.flush()) {
        conn.poison();
        return Err(e.into());
    }
    Ok(())
}

/// 在复用缓冲里编码生产帧并直接写出，不经过中间 payload 与 frame 缓冲。
fn write_produce_frame(
    conn: &mut pool::PooledConn,
    input: &protocol::ProduceInput<'_>,
    cid: u64,
    timeout_ms: Option<u32>,
    write_timeout: Duration,
) -> Result<(), IpcError> {
    protocol::with_produce_frame(input, cid, timeout_ms, |frame| {
        write_encoded(conn, frame, Some(write_timeout))
    })
    .map_err(|e| IpcError::Encode(e.to_string()))?
}

#[derive(Debug, Clone)]
pub struct ProduceOptions {
    pub headers: Vec<(String, bytes::Bytes)>,
    /// `-1` = 由 librdkafka 的 partitioner（key hash）决定。
    /// 显式正整数 = 强制写入该分区。
    pub partition: i32,
    /// `-1` = librdkafka 用当前时间戳。显式正整数 = 消息时间戳（毫秒）。
    pub timestamp_ms: i64,
}
// 故意不 `derive(Default)`：默认 0 会被 rdkafka 当作「写第 0 分区 + 时间戳 0」
// 而不是「auto」，是语义陷阱。要构造就显式给三个字段。

fn produce_input<'a>(
    cluster: &'a str,
    topic: &'a str,
    key: &'a [u8],
    value: &'a [u8],
    opts: &'a ProduceOptions,
) -> protocol::ProduceInput<'a> {
    protocol::ProduceInput {
        cluster,
        topic,
        key,
        value,
        headers: &opts.headers,
        partition: opts.partition,
        timestamp_ms: opts.timestamp_ms,
    }
}

/// 生产一条消息；key / value 为任意字节（PHP 字符串是字节串，不要求 UTF-8）。
pub fn produce_fnf_bin(
    socket: &str,
    cluster: &str,
    topic: &str,
    key: &[u8],
    value: &[u8],
    opts: ProduceOptions,
) -> Result<(), IpcError> {
    with_retry(socket, || {
        produce_fnf_bin_inner(socket, cluster, topic, key, value, &opts)
    })
}

fn produce_fnf_bin_inner(
    socket: &str,
    cluster: &str,
    topic: &str,
    key: &[u8],
    value: &[u8],
    opts: &ProduceOptions,
) -> Result<(), IpcError> {
    ensure(socket)?;
    let input = produce_input(cluster, topic, key, value, opts);
    let cid = next_cid();
    let pool = pool::pool_for(Path::new(socket));
    let mut conn = pool.acquire()?;
    let timeout = Duration::from_secs(5);
    write_produce_frame(&mut conn, &input, cid, None, timeout)?;
    let (kind, _payload) = recv_resp(&mut conn, cid, timeout)?;
    if kind != FrameType::ProduceResp {
        conn.poison();
        return Err(IpcError::UnexpectedKind(kind));
    }
    Ok(())
}

pub fn produce_sync_bin(
    socket: &str,
    cluster: &str,
    topic: &str,
    key: &[u8],
    value: &[u8],
    opts: ProduceOptions,
    timeout: Duration,
) -> Result<ProduceResp, IpcError> {
    with_retry(socket, || {
        produce_sync_bin_inner(socket, cluster, topic, key, value, &opts, timeout)
    })
}

fn produce_sync_bin_inner(
    socket: &str,
    cluster: &str,
    topic: &str,
    key: &[u8],
    value: &[u8],
    opts: &ProduceOptions,
    timeout: Duration,
) -> Result<ProduceResp, IpcError> {
    ensure(socket)?;
    let input = produce_input(cluster, topic, key, value, opts);
    let timeout_ms = (timeout.as_millis().min(u32::MAX as u128) as u32).max(1);
    let cid = next_cid();
    let pool = pool::pool_for(Path::new(socket));
    let mut conn = pool.acquire()?;
    write_produce_frame(&mut conn, &input, cid, Some(timeout_ms), timeout)?;

    let io_timeout = timeout + Duration::from_secs(2);
    let (kind, resp_payload) = recv_resp(&mut conn, cid, io_timeout)?;
    if kind != FrameType::ProduceResp {
        conn.poison();
        return Err(IpcError::UnexpectedKind(kind));
    }
    ProduceResp::decode(&resp_payload).map_err(|e| IpcError::DecodeResp(e.to_string()))
}

// ============================================================================
// Shared control-plane response reader
// ============================================================================

/// round_trip 的单次执行体。retry-friendly：失败不副作用，全部清理由 PooledConn::Drop 处理。
/// 读一帧响应：13B header（校验 cid）+ payload。
/// 若是 worker 回的 `Error` 帧 → 解码成结构化 [`IpcError::Worker`]（不 poison 连接，
/// 连接健康，只是业务/前置错）。否则返回 `(frame_kind, payload)` 供调用方按
/// expected kind 处理。IO / 解码错会 poison 连接，让池下次重建。
fn recv_resp(
    conn: &mut pool::PooledConn,
    cid: u64,
    timeout: Duration,
) -> Result<(FrameType, Vec<u8>), IpcError> {
    let stream = conn.stream_mut();
    stream.set_read_timeout(Some(timeout))?;

    let mut header = [0u8; HEADER_LEN];
    if let Err(e) = stream.read_exact(&mut header) {
        conn.poison();
        return Err(e.into());
    }
    let h = match codec::decode_header(&header) {
        Ok(h) => h,
        Err(e) => {
            conn.poison();
            return Err(IpcError::DecodeResp(e.to_string()));
        }
    };
    if h.cid != cid {
        conn.poison();
        return Err(IpcError::CidMismatch {
            sent: cid,
            got: h.cid,
        });
    }
    let mut payload = vec![0u8; h.payload_len as usize];
    if let Err(e) = conn.stream_mut().read_exact(&mut payload) {
        conn.poison();
        return Err(e.into());
    }
    if h.kind == FrameType::Error {
        let er = ErrorResp::decode(&payload).map_err(|e| IpcError::DecodeResp(e.to_string()))?;
        return Err(IpcError::Worker {
            kind: er.kind,
            retryable: er.retryable,
            outcome: er.outcome,
            native_code: er.native_code,
            message: er.message,
        });
    }
    Ok((h.kind, payload))
}

fn round_trip_once(
    socket: &str,
    req_kind: FrameType,
    expected_resp_kind: FrameType,
    req_payload: &[u8],
    timeout: Duration,
) -> Result<Vec<u8>, IpcError> {
    ensure(socket)?;
    let cid = next_cid();
    let pool = pool::pool_for(Path::new(socket));
    let mut conn = pool.acquire()?;

    write_frame(&mut conn, req_kind, cid, req_payload, Some(timeout))?;

    let (kind, payload) = recv_resp(&mut conn, cid, timeout)?;
    if kind != expected_resp_kind {
        conn.poison();
        return Err(IpcError::UnexpectedKind(kind));
    }
    Ok(payload)
}

pub fn register_cluster(
    socket: &str,
    cluster: &str,
    config: Vec<(String, String)>,
    timeout: Duration,
) -> Result<(), IpcError> {
    // 修：with_retry 必须包业务入口——L 拆 once 版本时漏写
    with_retry(socket, || {
        register_cluster_once(socket, cluster, config.clone(), timeout)
    })?;
    // L: 成功后入扩展端 replay 缓存——下次 worker 死亡 + spawn 时透明重放
    cluster_replay::record_cluster(socket, cluster, config);
    Ok(())
}

/// register_cluster 的"裸"版本——不更新 replay 缓存，绕开 ensure 的 replay 死循环。
/// `ensure` 内部 replay 路径 和 `register_cluster` 公共入口都调它。
/// round_trip_once 不会触发 with_retry，避免和外层 retry 嵌套。
fn register_cluster_once(
    socket: &str,
    cluster: &str,
    config: Vec<(String, String)>,
    timeout: Duration,
) -> Result<(), IpcError> {
    let req = RegisterClusterReq {
        cluster: cluster.to_string(),
        config,
    };
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| IpcError::Encode(e.to_string()))?;
    let resp_bytes = round_trip_once(
        socket,
        FrameType::RegisterClusterReq,
        FrameType::RegisterClusterResp,
        &payload,
        timeout,
    )?;
    match RegisterClusterResp::decode(&resp_bytes)
        .map_err(|e| IpcError::DecodeResp(e.to_string()))?
    {
        RegisterClusterResp::Ok => Ok(()),
        RegisterClusterResp::Err { message } => Err(IpcError::Server(message)),
    }
}

pub fn set_oauth_bearer_token(
    socket: &str,
    cluster: &str,
    token_value: &str,
    lifetime_ms: i64,
    principal_name: &str,
    extensions: Vec<(String, String)>,
    timeout: Duration,
) -> Result<(), IpcError> {
    with_retry(socket, || {
        set_oauth_bearer_token_once(
            socket,
            cluster,
            token_value,
            lifetime_ms,
            principal_name,
            extensions.clone(),
            timeout,
        )
    })?;
    cluster_replay::record_oauth_token(
        socket,
        cluster,
        OAuthToken {
            token_value: token_value.to_string(),
            lifetime_ms,
            principal_name: principal_name.to_string(),
            extensions,
        },
    );
    Ok(())
}

fn set_oauth_bearer_token_once(
    socket: &str,
    cluster: &str,
    token_value: &str,
    lifetime_ms: i64,
    principal_name: &str,
    extensions: Vec<(String, String)>,
    timeout: Duration,
) -> Result<(), IpcError> {
    let req = SetOAuthBearerTokenReq {
        cluster: cluster.to_string(),
        token_value: token_value.to_string(),
        lifetime_ms,
        principal_name: principal_name.to_string(),
        extensions,
    };
    let mut payload = BytesMut::new();
    req.encode(&mut payload)
        .map_err(|e| IpcError::Encode(e.to_string()))?;
    let resp_bytes = round_trip_once(
        socket,
        FrameType::SetOAuthBearerTokenReq,
        FrameType::SetOAuthBearerTokenResp,
        &payload,
        timeout,
    )?;
    match SetOAuthBearerTokenResp::decode(&resp_bytes)
        .map_err(|e| IpcError::DecodeResp(e.to_string()))?
    {
        SetOAuthBearerTokenResp::Ok => Ok(()),
        SetOAuthBearerTokenResp::Err { message } => Err(IpcError::Server(message)),
    }
}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn io(k: std::io::ErrorKind) -> IpcError {
        IpcError::Io(std::io::Error::from(k))
    }

    // === should_invalidate 分类矩阵 =====================================

    #[test]
    fn test_should_invalidate_io_kinds_true() {
        use std::io::ErrorKind::*;
        for k in [
            BrokenPipe,
            ConnectionReset,
            ConnectionRefused,
            ConnectionAborted,
            UnexpectedEof,
        ] {
            assert!(should_invalidate(&io(k)), "{k:?} 应触发缓存失效");
        }
    }

    #[test]
    fn test_should_invalidate_io_kinds_false() {
        use std::io::ErrorKind::*;
        for k in [TimedOut, PermissionDenied, WouldBlock, Interrupted] {
            assert!(
                !should_invalidate(&io(k)),
                "{k:?} 不应触发缓存失效——不是 worker 死"
            );
        }
    }

    #[test]
    fn test_should_invalidate_worker_business_error() {
        // worker 回的结构化业务错，worker 还活着 → 不 invalidate
        let e = IpcError::Worker {
            kind: ErrorKind::InvalidArgument,
            retryable: false,
            outcome: OperationOutcome::NotApplied,
            native_code: -1,
            message: "bad arg".into(),
        };
        assert!(!should_invalidate(&e));
    }

    #[test]
    fn test_should_invalidate_protocol_errors_false() {
        // 协议层错误：worker 可能还活着，不 invalidate
        assert!(!should_invalidate(&IpcError::Encode("x".into())));
        assert!(!should_invalidate(&IpcError::DecodeResp("y".into())));
        assert!(!should_invalidate(&IpcError::UnexpectedKind(
            FrameType::Ping
        )));
        assert!(!should_invalidate(&IpcError::CidMismatch {
            sent: 1,
            got: 2
        }));
        assert!(!should_invalidate(&IpcError::Server("srv err".into())));
    }

    #[test]
    fn test_should_invalidate_pool_error_true() {
        // pool 拿不到连接大概率是 worker 挂了——两种 PoolError variant 都算
        let e = IpcError::Pool(pool::PoolError::Connect {
            socket: "/tmp/x.sock".into(),
            source: std::io::Error::from(std::io::ErrorKind::ConnectionRefused),
        });
        assert!(should_invalidate(&e));

        let e2 = IpcError::Pool(pool::PoolError::Handshake {
            socket: "/tmp/x.sock".into(),
            reason: "bad hello".into(),
        });
        assert!(should_invalidate(&e2));
    }

    // === with_retry 分支 ================================================

    fn snap() -> (u64, u64, u64) {
        (
            RETRY_ATTEMPTS.load(Ordering::Relaxed),
            RETRY_SUCCESSES.load(Ordering::Relaxed),
            RETRY_FAILURES.load(Ordering::Relaxed),
        )
    }

    fn delta(before: (u64, u64, u64), after: (u64, u64, u64)) -> (u64, u64, u64) {
        (after.0 - before.0, after.1 - before.1, after.2 - before.2)
    }

    /// RETRY_ATTEMPTS/SUCCESSES/FAILURES 是进程级全局；并发跑 with_retry 测试会互串。
    /// 用这个 Mutex 把它们串起来，才能对 delta 做精确断言。
    fn retry_lock() -> std::sync::MutexGuard<'static, ()> {
        static GUARD: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        GUARD
            .get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap_or_else(|p| p.into_inner())
    }

    #[test]
    fn test_with_retry_success_first_try_no_counter_change() {
        let _g = retry_lock();
        let before = snap();
        let ok: Result<i32, IpcError> = with_retry("/tmp/never-exists.sock", || Ok(42));
        assert_eq!(ok.unwrap(), 42);
        let d = delta(before, snap());
        assert_eq!(d, (0, 0, 0), "首发即成功，计数器不动");
    }

    #[test]
    fn test_with_retry_non_invalidatable_error_no_retry() {
        let _g = retry_lock();
        // Encode 错——非可 invalidate 类，op 只被调一次
        let calls = Cell::new(0);
        let before = snap();
        let r: Result<(), IpcError> = with_retry("/tmp/no.sock", || {
            calls.set(calls.get() + 1);
            Err(IpcError::Encode("bad".into()))
        });
        assert!(matches!(r, Err(IpcError::Encode(_))));
        assert_eq!(calls.get(), 1, "非可 invalidate 错误只调 op 一次");
        let d = delta(before, snap());
        assert_eq!(d, (0, 0, 0));
    }

    #[test]
    fn test_with_retry_invalidatable_then_success_bumps_successes() {
        let _g = retry_lock();
        // 第一次 BrokenPipe，第二次 Ok
        let calls = Cell::new(0);
        let before = snap();
        let r = with_retry("/tmp/ipc-test-retry-ok.sock", || {
            calls.set(calls.get() + 1);
            if calls.get() == 1 {
                Err(io(std::io::ErrorKind::BrokenPipe))
            } else {
                Ok(1)
            }
        });
        assert_eq!(r.unwrap(), 1);
        assert_eq!(calls.get(), 2, "invalidatable 错误后应重试一次");
        let d = delta(before, snap());
        assert_eq!(d, (1, 1, 0), "attempts+1, successes+1, failures 不动");
    }

    #[test]
    fn test_with_retry_invalidatable_twice_bumps_failures() {
        let _g = retry_lock();
        // 两次都失败——attempts+1, failures+1, successes 不动
        let calls = Cell::new(0);
        let before = snap();
        let r: Result<(), IpcError> = with_retry("/tmp/ipc-test-retry-fail.sock", || {
            calls.set(calls.get() + 1);
            Err(io(std::io::ErrorKind::ConnectionReset))
        });
        assert!(r.is_err());
        assert_eq!(calls.get(), 2, "第二次仍失败后不再重试");
        let d = delta(before, snap());
        assert_eq!(d, (1, 0, 1));
    }

    // === next_cid / retry_stats ==========================================

    #[test]
    fn test_next_cid_monotonic() {
        let a = next_cid();
        let b = next_cid();
        let c = next_cid();
        assert!(a < b && b < c);
    }

    #[test]
    fn test_retry_stats_snapshot() {
        // retry_stats() 只做原子读，值必须与直接读一致
        let direct = (
            RETRY_ATTEMPTS.load(Ordering::Relaxed),
            RETRY_SUCCESSES.load(Ordering::Relaxed),
            RETRY_FAILURES.load(Ordering::Relaxed),
        );
        let s = retry_stats();
        assert_eq!(s.attempts, direct.0);
        assert_eq!(s.successes, direct.1);
        assert_eq!(s.failures, direct.2);
    }

    // === produce frame encoding =========================================

    fn decode_frame(frame: &[u8]) -> (hi_kafka_proto::Header, &[u8]) {
        let header = codec::decode_header(&frame[..HEADER_LEN]).unwrap();
        assert_eq!(frame.len(), HEADER_LEN + header.payload_len as usize);
        (header, &frame[HEADER_LEN..])
    }

    #[test]
    fn test_produce_frame_roundtrips_binary_key_value_and_headers() {
        let opts = ProduceOptions {
            headers: vec![("k1".into(), bytes::Bytes::from_static(b"v\x00\xff"))],
            partition: 3,
            timestamp_ms: 12345,
        };
        let input = produce_input("cluster", "topic", b"k\x00ey", b"v\x00al\xff", &opts);
        let decoded = protocol::with_produce_frame(&input, 7, None, |frame| {
            let (header, payload) = decode_frame(frame);
            assert_eq!(header.kind, FrameType::ProduceFnf);
            assert_eq!(header.cid, 7);
            hi_kafka_proto::ProduceFnf::decode(payload).unwrap()
        })
        .unwrap();
        assert_eq!(decoded.cluster, "cluster");
        assert_eq!(decoded.topic, "topic");
        assert_eq!(decoded.key.as_ref(), b"k\x00ey");
        assert_eq!(decoded.value.as_ref(), b"v\x00al\xff");
        assert_eq!(decoded.partition, 3);
        assert_eq!(decoded.timestamp_ms, 12345);
        assert_eq!(decoded.headers, opts.headers);
    }

    #[test]
    fn test_produce_req_frame_carries_timeout_and_reuses_buffer() {
        let opts = ProduceOptions {
            headers: vec![],
            partition: -1,
            timestamp_ms: -1,
        };
        // 先编一条大消息再编小消息：复用缓冲必须被清空，不能残留上一帧的字节。
        let large = vec![1u8; 4096];
        let big = produce_input("c", "t", b"k", &large, &opts);
        protocol::with_produce_frame(&big, 1, None, |frame| frame.len()).unwrap();
        let small = produce_input("c", "t", b"k", b"v", &opts);
        let decoded = protocol::with_produce_frame(&small, 2, Some(1500), |frame| {
            let (header, payload) = decode_frame(frame);
            assert_eq!(header.kind, FrameType::ProduceReq);
            hi_kafka_proto::ProduceReq::decode(payload).unwrap()
        })
        .unwrap();
        assert_eq!(decoded.timeout_ms, 1500);
        assert_eq!(decoded.msg.value.as_ref(), b"v");
    }
}

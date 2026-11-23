use crate::cluster::{ClusterRegistryHandle, StoredOAuthToken};
use crate::consumer::{ConsumerError, ConsumerHandle, OutboundFrame};
use crate::error::WorkerError;
use crate::metrics::Metrics;
use crate::producer::ProducerHandle;
use crate::shutdown::ShutdownHandle;
use anyhow::Context;
use bytes::BytesMut;
use futures::FutureExt;
use hi_kafka_proto::{
    codec, encode_frame, ConnectionRole, ConsumeAck, ConsumeClose, ConsumeCommit, ConsumeNack,
    ConsumeOpen, ConsumeResume, ConsumerPartitions, DeliveryAck, ErrorKind, ErrorResp, FlowControl,
    FrameType, HelloReq, HelloResp, ProduceFnf, ProduceReq, ProduceResp, RegisterClusterReq,
    RegisterClusterResp, SetOAuthBearerTokenReq, SetOAuthBearerTokenResp, CAP_ALL,
    CAP_CONSUMER_STREAM, HEADER_LEN, MAX_PAYLOAD_LEN, PROTOCOL_MAJOR,
};
use std::io::IoSlice;
use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

pub struct Server {
    listener: UnixListener,
    producer: ProducerHandle,
    consumer: ConsumerHandle,
    registry: ClusterRegistryHandle,
    shutdown: ShutdownHandle,
    metrics: Arc<Metrics>,
    /// 无任何连接且持续空闲超过此时长 → worker 自动 drain 退出（解决主进程退出后
    /// worker 残留）。`Duration::ZERO` = 禁用（常驻，旧行为）。
    idle_timeout: Duration,
    socket_identity: SocketIdentity,
}

/// bind 时 socket 文件的 (dev, ino)。退出时据此判断路径上的文件是否仍是自己的。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SocketIdentity {
    dev: u64,
    ino: u64,
}

fn identity_of(path: &Path) -> Option<SocketIdentity> {
    use std::os::unix::fs::MetadataExt;
    std::fs::symlink_metadata(path)
        .ok()
        .map(|meta| SocketIdentity {
            dev: meta.dev(),
            ino: meta.ino(),
        })
}

/// 仅当路径上仍是本 worker bind 的 socket 文件时删除，返回是否删除。
///
/// 旧 worker drain 期间已停止 accept，客户端会拉起新 worker 并在同一路径重新 bind；
/// 旧 worker 退出时无条件删除会让新 worker 的 socket 消失、变得不可达。
pub fn remove_socket_if_owned(path: &Path, identity: SocketIdentity) -> bool {
    if identity_of(path) != Some(identity) {
        return false;
    }
    std::fs::remove_file(path).is_ok()
}

impl Server {
    pub async fn bind(socket: &Path) -> anyhow::Result<Self> {
        let registry = crate::cluster::ClusterRegistry::new();
        let shutdown = crate::shutdown::ShutdownState::new();
        let metrics = Metrics::new();
        Self::bind_with(
            socket,
            crate::producer::logging(),
            crate::consumer::logging(shutdown.clone(), metrics.clone()),
            registry,
            shutdown,
            metrics,
            Duration::ZERO,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn bind_with(
        socket: &Path,
        producer: ProducerHandle,
        consumer: ConsumerHandle,
        registry: ClusterRegistryHandle,
        shutdown: ShutdownHandle,
        metrics: Arc<Metrics>,
        idle_timeout: Duration,
    ) -> anyhow::Result<Self> {
        if socket.exists() {
            // 只清理无人监听的残留 socket。路径上已有活着的 worker 时绝不能抢占：
            // 删除后重新 bind 会让新老两个 worker 同时存活，已连接的客户端与新连接
            // 分别落在不同实例上（例如 spawn 探测因 accept 队列满而误判 worker 已死）。
            match std::os::unix::net::UnixStream::connect(socket) {
                Ok(_) => anyhow::bail!(
                    "socket {} is already served by a running worker",
                    socket.display()
                ),
                Err(_) => std::fs::remove_file(socket).context("remove stale socket")?,
            }
        }
        if let Some(parent) = socket.parent() {
            std::fs::create_dir_all(parent).context("create socket parent dir")?;
        }
        let listener = UnixListener::bind(socket).context("bind unix listener")?;
        let socket_identity = identity_of(socket).context("stat socket file after bind")?;
        info!(socket = %socket.display(), "listening");
        Ok(Self {
            listener,
            producer,
            consumer,
            registry,
            shutdown,
            metrics,
            idle_timeout,
            socket_identity,
        })
    }

    /// 本 worker bind 的 socket 文件标识，交给 [`remove_socket_if_owned`] 在退出时使用。
    pub fn socket_identity(&self) -> SocketIdentity {
        self.socket_identity
    }

    /// 主循环：accept → spawn connection task。task 句柄进 [`JoinSet`]，
    /// `drain_timeout` 决定 SIGTERM 后等已 ack 但尚未写回响应的连接的最大时间。
    ///
    /// 修复 P0 #1：原实现的 `tokio::select!{ server.run() vs signal }` 在
    /// 信号到来时直接 drop server future，detach 的 connection task 在 main
    /// 返回后被 runtime abort，导致已 ack 但未 PRODUCE_RESP 的 cid 永久丢失。
    /// 现在改为：信号方通过 `shutdown` 通知，server.run 自己 drain，等所有
    /// in-flight connection 把响应写回（超时强制 abort）才返回。
    pub async fn run(self, drain_timeout: Duration) -> anyhow::Result<()> {
        let mut tasks: JoinSet<()> = JoinSet::new();
        let idle_timeout = self.idle_timeout;
        let mut last_active = tokio::time::Instant::now();
        // 收到客户端 Goodbye（PHP 进程退出时主动告别）后置位；有新连接接入时复位。
        // 含义：「Goodbye 之后没有新使用者到来」。配合「最后一个连接关闭」判定，让
        // worker 在最后的使用者离开时立即自退，而不必等满 idle_timeout；Goodbye 之后
        // 接入、未告别就断开的使用者仍走 idle 超时。仅在启用 idle 自退时生效。
        let expedited = Arc::new(AtomicBool::new(false));
        // idle 检查间隔：idle_timeout 的 1/4，夹在 [1s, 30s]。禁用时给个无害的 30s。
        let check_interval = if idle_timeout.is_zero() {
            Duration::from_secs(30)
        } else {
            (idle_timeout / 4).clamp(Duration::from_secs(1), Duration::from_secs(30))
        };
        let mut idle_ticker = tokio::time::interval(check_interval);
        idle_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = self.shutdown.wait_draining() => {
                    info!(
                        in_flight = tasks.len(),
                        ?drain_timeout,
                        "server: drain signaled, waiting for in-flight connection tasks"
                    );
                    drain_tasks(&mut tasks, drain_timeout).await;
                    info!("server: drain complete");
                    return Ok(());
                }
                res = self.listener.accept() => {
                    last_active = tokio::time::Instant::now();
                    match res {
                        Ok((stream, _addr)) => {
                            debug!("client connected");
                            expedited.store(false, Ordering::Release);
                            Metrics::inc(&self.metrics.ipc_connections_total);
                            let producer = self.producer.clone();
                            let consumer = self.consumer.clone();
                            let registry = self.registry.clone();
                            let shutdown = self.shutdown.clone();
                            let metrics = self.metrics.clone();
                            let expedited = expedited.clone();
                            tasks.spawn(async move {
                                if let Err(e) = handle_connection(stream, producer, consumer, registry, shutdown, metrics, expedited).await {
                                    warn!(error = ?e, "connection ended with error");
                                }
                            });
                        }
                        Err(e) => {
                            error!(error = ?e, "accept failed");
                        }
                    }
                }
                Some(joined) = tasks.join_next(), if !tasks.is_empty() => {
                    if let Err(e) = joined {
                        warn!(error = ?e, "connection task ended with error");
                    }
                    // 只在这一处回收已结束的连接任务：其它分支提前回收会让「最后一个
                    // 连接关闭」的时刻被跳过，Goodbye 快速退出因此失效。
                    if !idle_timeout.is_zero()
                        && tasks.is_empty()
                        && expedited.load(Ordering::Acquire)
                        && self.consumer.active_subscriptions().await == 0
                    {
                        info!("client goodbye + last connection closed, self-terminating");
                        self.shutdown.start_draining();
                    }
                }
                _ = idle_ticker.tick(), if !idle_timeout.is_zero() => {
                    if tasks.is_empty() {
                        // Dedicated stream 断开后 actor 只保留到 resume grace；仍 attached
                        // 或仍在 grace 内的 subscription 是活跃资源，不能被 idle 退出误杀。
                        let has_active_subs = self.consumer.active_subscriptions().await > 0;
                        if !has_active_subs && last_active.elapsed() >= idle_timeout {
                            info!(
                                ?idle_timeout,
                                "worker idle (no connections, no active subscriptions), self-terminating"
                            );
                            self.shutdown.start_draining();
                        }
                    } else {
                        // 还有连接 → 不算空闲，刷新计时基准。
                        last_active = tokio::time::Instant::now();
                    }
                }
            }
        }
    }
}

/// drain：等所有 in-flight connection task 自然结束，超时则全部 abort
/// 避免 SIGTERM 之后 hi-kafka 客户端永远等不到已 ack 的 PRODUCE_RESP
async fn drain_tasks(tasks: &mut JoinSet<()>, drain_timeout: Duration) {
    if tasks.is_empty() {
        return;
    }
    let deadline = tokio::time::Instant::now() + drain_timeout;
    loop {
        match tokio::time::timeout_at(deadline, tasks.join_next()).await {
            Ok(Some(res)) => {
                if let Err(e) = res {
                    warn!(error = ?e, "connection task error during drain");
                }
            }
            Ok(None) => return, // 全部结束
            Err(_) => {
                warn!(
                    remaining = tasks.len(),
                    "drain timeout reached, aborting remaining connection tasks"
                );
                tasks.abort_all();
                // 等 abort 真正完成（一般立即）
                while tasks.join_next().await.is_some() {}
                return;
            }
        }
    }
}

/// P3 #8：按帧类型做"业务期望"的 payload 上限。比协议级 MAX_PAYLOAD_LEN
/// 严格，避免 16 MiB × N 连接 OOM。返回 `Err` 则连接立即关。
fn check_per_frame_limit(kind: FrameType, payload_len: u32) -> anyhow::Result<()> {
    // 单条业务消息（key + value + headers）上限：4 MiB
    // ——Kafka broker 默认 message.max.bytes 1 MiB，留 4× 余量给 headers / 大 value
    const PRODUCE_MAX: u32 = 4 * 1024 * 1024;
    // 控制帧（注册集群、订阅、提交、事务、OAuth token 等）：64 KiB 足矣
    const CONTROL_MAX: u32 = 64 * 1024;

    let limit = match kind {
        FrameType::ProduceFnf
        | FrameType::ProduceReq
        | FrameType::ProduceResp
        | FrameType::ConsumeMessage => PRODUCE_MAX,
        // 其余全部按控制帧
        _ => CONTROL_MAX,
    };
    if payload_len > limit {
        anyhow::bail!(
            "frame payload {} exceeds per-type limit {} for {:?}",
            payload_len,
            limit,
            kind
        );
    }
    Ok(())
}

async fn handle_connection(
    mut stream: UnixStream,
    producer: ProducerHandle,
    consumer: ConsumerHandle,
    registry: ClusterRegistryHandle,
    shutdown: ShutdownHandle,
    metrics: Arc<Metrics>,
    expedited: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    let mut header_buf = [0u8; HEADER_LEN];
    let mut payload_buf = BytesMut::new();
    // F: 第一帧必须是 HELLO 且 PROTOCOL_MAJOR 与 worker 自身一致；
    // 否则关连接（让客户端 read 端 EOF → pool 视为 connect 失败，重试）
    let mut handshaked = false;
    let mut connection_role = None;

    loop {
        // 帧与帧之间（连接空闲）才响应 drain：正在处理的请求一定会写完响应。
        // read_exact 被取消可能丢掉已读的半个 header，但此时连接本来就要关闭。
        let read = tokio::select! {
            // 已到达的请求优先读：它会得到明确的 WORKER_DRAINING，而不是被直接丢弃。
            biased;
            read = stream.read_exact(&mut header_buf) => read,
            _ = shutdown.wait_draining() => {
                debug!("closing idle connection for drain");
                return Ok(());
            }
        };
        if let Err(e) = read {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                debug!("client disconnected");
                return Ok(());
            }
            return Err(e.into());
        }

        let header = codec::decode_header(&header_buf).context("decode header")?;
        Metrics::inc(&metrics.ipc_frames_total);
        // P3 #8：codec::decode_header 已检 MAX_PAYLOAD_LEN（16 MiB 协议级硬上限）。
        // 这里再按 frame 类型加一层"实际业务上限"，让恶意/错配的客户端无法
        // 在没有恶意单帧的前提下 16 MiB × N 个连接打爆 worker 内存。
        // 业务侧 ProduceReq 消息单条 > 4 MiB 是反模式（Kafka 默认 message.max.bytes 1 MiB），
        // RegisterCluster/OAuth token 等控制帧更应该几 KiB 就够。
        if let Err(e) = check_per_frame_limit(header.kind, header.payload_len) {
            warn!(cid = header.cid, kind = ?header.kind, payload_len = header.payload_len,
                  "rejecting oversized frame: {e}");
            // 直接关连接：不必为了「对齐」再分配并读完一个超限 payload。
            return Err(e);
        }
        payload_buf.clear();
        payload_buf.resize(header.payload_len as usize, 0);
        stream
            .read_exact(&mut payload_buf)
            .await
            .context("read payload")?;

        // F: 握手——在 dispatch 业务帧前先完成
        if !handshaked {
            if header.kind != FrameType::Hello {
                warn!(
                    cid = header.cid,
                    kind = ?header.kind,
                    "first frame is not HELLO; closing connection"
                );
                anyhow::bail!("handshake required, got {:?}", header.kind);
            }
            let hello = HelloReq::decode(&payload_buf).context("decode HELLO payload")?;
            if hello.major != PROTOCOL_MAJOR {
                warn!(
                    client_major = hello.major,
                    server_major = PROTOCOL_MAJOR,
                    "HELLO major mismatch; closing connection"
                );
                anyhow::bail!(
                    "PROTOCOL_MAJOR mismatch: client {} vs server {}",
                    hello.major,
                    PROTOCOL_MAJOR
                );
            }
            if hello.role == ConnectionRole::ConsumerStream
                && hello.capabilities & CAP_CONSUMER_STREAM == 0
            {
                anyhow::bail!("consumer stream role without consumer-stream capability");
            }
            let resp = HelloResp {
                major: PROTOCOL_MAJOR,
                capabilities: CAP_ALL,
            };
            let mut resp_buf = BytesMut::new();
            resp.encode(&mut resp_buf).context("encode HELLO resp")?;
            write_frame(&mut stream, FrameType::Hello, header.cid, &resp_buf).await?;
            handshaked = true;
            connection_role = Some(hello.role);
            debug!(cid = header.cid, role = ?hello.role, "HELLO handshake ok");
            if hello.role == ConnectionRole::ConsumerStream {
                return handle_consumer_stream(stream, consumer, metrics).await;
            }
            continue;
        }

        if matches!(
            header.kind,
            FrameType::ConsumeOpen
                | FrameType::ConsumeResume
                | FrameType::ConsumeAck
                | FrameType::ConsumeNack
                | FrameType::ConsumeCommit
                | FrameType::ConsumeClose
                | FrameType::FlowControl
        ) && connection_role != Some(ConnectionRole::ConsumerStream)
        {
            anyhow::bail!(
                "consumer stream frame {:?} received on {:?} connection",
                header.kind,
                connection_role
            );
        }

        dispatch(
            &mut stream,
            header.kind,
            header.cid,
            &payload_buf,
            &producer,
            &consumer,
            &registry,
            &shutdown,
            &metrics,
            &expedited,
        )
        .await?;
    }
}

/// Dedicated, full-duplex Consumer v2 session.  It never enters the RPC
/// dispatcher and therefore cannot be head-of-line blocked by ProduceSync.
async fn handle_consumer_stream(
    stream: UnixStream,
    consumer: ConsumerHandle,
    metrics: Arc<Metrics>,
) -> anyhow::Result<()> {
    let (mut reader, mut writer) = stream.into_split();
    let mut inbound_payload = BytesMut::new();
    let first = read_wire_frame(&mut reader, &mut inbound_payload)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!("consumer stream closed before ConsumeOpen/ConsumeResume")
        })?;
    let (outbound_tx, mut outbound_rx) =
        mpsc::channel::<OutboundFrame>(consumer.outbound_capacity());

    let ready = match first.kind {
        FrameType::ConsumeOpen => match ConsumeOpen::decode(&inbound_payload) {
            Ok(request) => consumer.open(request, outbound_tx).await,
            Err(error) => Err(ConsumerError::InvalidArgument(error.to_string())),
        },
        FrameType::ConsumeResume => match ConsumeResume::decode(&inbound_payload) {
            Ok(request) => consumer.resume(request, outbound_tx).await,
            Err(error) => Err(ConsumerError::InvalidArgument(error.to_string())),
        },
        other => {
            write_error_half(
                &mut writer,
                first.cid,
                ErrorResp::new(
                    ErrorKind::Protocol,
                    format!(
                        "first consumer frame must be ConsumeOpen/ConsumeResume, got {other:?}"
                    ),
                ),
            )
            .await?;
            return Ok(());
        }
    };
    let attachment = match ready {
        Ok(value) => value,
        Err(error) => {
            write_error_half(&mut writer, first.cid, consumer_err_to_resp(&error)).await?;
            return Ok(());
        }
    };
    let ready = attachment.ready;
    let attachment_lease = attachment.lease;
    let session = attachment.session;
    let mut ready_payload = BytesMut::new();
    ready
        .encode(&mut ready_payload)
        .context("encode ConsumeReady")?;
    write_frame_half(
        &mut writer,
        FrameType::ConsumeReady,
        first.cid,
        &ready_payload,
    )
    .await?;
    let writer = Arc::new(Mutex::new(writer));
    let stream_writer = writer.clone();
    let mut writer_task = tokio::spawn(async move {
        while let Some(frame) = outbound_rx.recv().await {
            let mut output = stream_writer.lock().await;
            write_frame_half(&mut output, frame.kind, frame.cid, &frame.payload).await?;
        }
        Ok::<(), anyhow::Error>(())
    });

    let reader_result: anyhow::Result<()> = async {
        loop {
            tokio::select! {
            incoming = read_wire_frame(&mut reader, &mut inbound_payload) => {
                let Some(header) = incoming? else { break Ok(()); };
                Metrics::inc(&metrics.ipc_frames_total);
                let result = match header.kind {
                    FrameType::ConsumeAck => decode_consumer(&inbound_payload, ConsumeAck::decode).and_then_async(|value| session.ack(value)).await,
                    FrameType::ConsumeNack => decode_consumer(&inbound_payload, ConsumeNack::decode).and_then_async(|value| session.nack(value)).await,
                    FrameType::FlowControl => decode_consumer(&inbound_payload, FlowControl::decode).and_then_async(|value| session.flow(value)).await,
                    FrameType::ConsumeCommit => decode_consumer(&inbound_payload, ConsumeCommit::decode).and_then_async(|value| session.commit(value, header.cid)).await,
                    FrameType::ConsumePause => decode_consumer(&inbound_payload, ConsumerPartitions::decode).and_then_async(|value| session.pause(value)).await,
                    FrameType::ConsumePartitionResume => decode_consumer(&inbound_payload, ConsumerPartitions::decode).and_then_async(|value| session.resume_partitions(value)).await,
                    FrameType::ConsumeClose => decode_consumer(&inbound_payload, ConsumeClose::decode).and_then_async(|value| session.close(value)).await,
                    other => Err(ConsumerError::InvalidArgument(format!("invalid frame {other:?} on consumer stream"))),
                };
                if let Err(error) = result {
                    let mut output = writer.lock().await;
                    write_error_half(&mut output, header.cid, consumer_err_to_resp(&error)).await?;
                    if matches!(error, ConsumerError::InvalidArgument(_) | ConsumerError::StaleEpoch { .. }) { break Ok(()); }
                }
                if header.kind == FrameType::ConsumeClose { break Ok(()); }
            }
            written = &mut writer_task => {
                break match written {
                    Ok(result) => result,
                    Err(error) => Err(anyhow::anyhow!("consumer writer task failed: {error}")),
                };
            }
            }
        }
    }
    .await;
    writer_task.abort();
    session.disconnect(attachment_lease).await;
    reader_result
}

trait ConsumerResultExt<T> {
    async fn and_then_async<F, Fut>(self, operation: F) -> Result<(), ConsumerError>
    where
        F: FnOnce(T) -> Fut,
        Fut: std::future::Future<Output = Result<(), ConsumerError>>;
}

impl<T> ConsumerResultExt<T> for Result<T, ConsumerError> {
    async fn and_then_async<F, Fut>(self, operation: F) -> Result<(), ConsumerError>
    where
        F: FnOnce(T) -> Fut,
        Fut: std::future::Future<Output = Result<(), ConsumerError>>,
    {
        operation(self?).await
    }
}

fn decode_consumer<T>(
    payload: &[u8],
    decode: impl FnOnce(&[u8]) -> Result<T, hi_kafka_proto::PayloadError>,
) -> Result<T, ConsumerError> {
    decode(payload).map_err(|error| ConsumerError::InvalidArgument(error.to_string()))
}

async fn read_wire_frame(
    reader: &mut tokio::net::unix::OwnedReadHalf,
    payload: &mut BytesMut,
) -> anyhow::Result<Option<hi_kafka_proto::Header>> {
    let mut header_bytes = [0u8; HEADER_LEN];
    match reader.read_exact(&mut header_bytes).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let header = codec::decode_header(&header_bytes).context("decode consumer frame header")?;
    check_per_frame_limit(header.kind, header.payload_len)?;
    payload.clear();
    payload.resize(header.payload_len as usize, 0);
    reader
        .read_exact(payload)
        .await
        .context("read consumer frame payload")?;
    Ok(Some(header))
}

async fn write_frame_half(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    kind: FrameType,
    cid: u64,
    payload: &[u8],
) -> anyhow::Result<()> {
    if payload.len() > MAX_PAYLOAD_LEN {
        anyhow::bail!("consumer payload too large: {}", payload.len());
    }
    let mut header = [0u8; HEADER_LEN];
    header[..4].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    header[4] = kind as u8;
    header[5..].copy_from_slice(&cid.to_be_bytes());
    let slices = [IoSlice::new(&header), IoSlice::new(payload)];
    let written = writer
        .write_vectored(&slices)
        .await
        .context("write consumer frame")?;
    if written == 0 {
        return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into());
    }
    if written < HEADER_LEN {
        writer
            .write_all(&header[written..])
            .await
            .context("finish consumer header")?;
        writer
            .write_all(payload)
            .await
            .context("finish consumer payload")?;
    } else if written < HEADER_LEN + payload.len() {
        writer
            .write_all(&payload[written - HEADER_LEN..])
            .await
            .context("finish consumer payload")?;
    }
    Ok(())
}

async fn write_error_half(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    cid: u64,
    error: ErrorResp,
) -> anyhow::Result<()> {
    let mut payload = BytesMut::new();
    error
        .encode(&mut payload)
        .context("encode consumer error")?;
    write_frame_half(writer, FrameType::Error, cid, &payload).await
}

/// I: 外层 panic 兜底。每帧 dispatch 整体 catch_unwind，panic 时返回 Err
/// 让 `handle_connection` 关连接（客户端见 EOF → with_retry 触发自愈）。
///
/// `handle_produce_req` 内部已有更精细的 catch_unwind + fallback ProduceResp::Err
/// （retryable=true 配合 idempotence 安全去重），那层会先 catch，外层只对其它
/// 10 个 handler 的 panic 生效——这些路径上 panic 多发生在协议解码或 worker
/// 内部数据结构上，broker 状态不变；at-least-once 语义下客户端重试是安全的。
async fn dispatch(
    stream: &mut UnixStream,
    kind: FrameType,
    cid: u64,
    payload: &[u8],
    producer: &ProducerHandle,
    consumer: &ConsumerHandle,
    registry: &ClusterRegistryHandle,
    shutdown: &ShutdownHandle,
    metrics: &Arc<Metrics>,
    expedited: &Arc<AtomicBool>,
) -> anyhow::Result<()> {
    let result = AssertUnwindSafe(dispatch_inner(
        stream, kind, cid, payload, producer, consumer, registry, shutdown, metrics, expedited,
    ))
    .catch_unwind()
    .await;
    match result {
        Ok(r) => r,
        Err(panic) => {
            let msg = describe_panic(&panic);
            error!(cid, ?kind, panic = %msg, "dispatch panicked, closing connection");
            Err(anyhow::anyhow!("dispatch panic [{kind:?}]: {msg}"))
        }
    }
}

async fn dispatch_inner(
    stream: &mut UnixStream,
    kind: FrameType,
    cid: u64,
    payload: &[u8],
    producer: &ProducerHandle,
    _consumer: &ConsumerHandle,
    registry: &ClusterRegistryHandle,
    shutdown: &ShutdownHandle,
    metrics: &Arc<Metrics>,
    expedited: &Arc<AtomicBool>,
) -> anyhow::Result<()> {
    match kind {
        FrameType::Hello => {
            write_error_frame(
                stream,
                cid,
                ErrorResp::new(
                    ErrorKind::Protocol,
                    "HELLO is only valid as the first frame",
                ),
            )
            .await?;
            anyhow::bail!("repeated HELLO on established RPC connection");
        }
        FrameType::Ping => {
            debug!(cid, "PING received");
            write_frame(stream, FrameType::Pong, cid, &[]).await?;
        }
        FrameType::ProduceFnf => {
            handle_produce_fnf(stream, cid, payload, producer, shutdown, metrics).await?;
        }
        FrameType::ProduceReq => {
            handle_produce_req(stream, cid, payload, producer, shutdown, metrics).await?;
        }
        FrameType::RegisterClusterReq => {
            handle_register_cluster(stream, cid, payload, registry).await?;
        }
        FrameType::SetOAuthBearerTokenReq => {
            handle_set_oauth_token(stream, cid, payload, registry).await?;
        }
        FrameType::Goodbye => {
            // 客户端（PHP 进程）主动告别，fire-and-forget，无响应。置位 expedited：
            // 此连接关闭后若已无其它连接且无活跃订阅，主循环立即自退（见 run() 的
            // join_next 分支）。worker 是节点级共享单例，这里**不**直接 drain——
            // 必须等「最后一个连接关闭」的判定，避免误杀仍在使用的其它进程。
            debug!(cid, "client goodbye received; marking expedited shutdown");
            expedited.store(true, Ordering::Release);
        }
        other => {
            warn!(kind = ?other, cid, "invalid frame on RPC connection");
            write_error_frame(
                stream,
                cid,
                ErrorResp::new(ErrorKind::Protocol, format!("invalid RPC frame {other:?}")),
            )
            .await?;
            anyhow::bail!("invalid frame {other:?} on RPC connection");
        }
    }
    Ok(())
}

async fn handle_set_oauth_token(
    stream: &mut UnixStream,
    cid: u64,
    payload: &[u8],
    registry: &ClusterRegistryHandle,
) -> anyhow::Result<()> {
    let req = match SetOAuthBearerTokenReq::decode(payload) {
        Ok(r) => r,
        Err(e) => {
            return write_error_frame(
                stream,
                cid,
                ErrorResp::new(ErrorKind::Protocol, format!("payload decode: {e}")),
            )
            .await;
        }
    };
    let cluster = req.cluster.clone();
    let lifetime_ms = req.lifetime_ms;
    let token = StoredOAuthToken {
        token_value: req.token_value,
        lifetime_ms: req.lifetime_ms,
        principal_name: req.principal_name,
        extensions: req.extensions,
    };
    match registry.set_oauth_token(&cluster, token).await {
        Ok(()) => {
            info!(%cluster, lifetime_ms, "OAuth token updated");
            let mut buf = BytesMut::new();
            SetOAuthBearerTokenResp::Ok
                .encode(&mut buf)
                .context("encode set_oauth_token_resp")?;
            write_frame(stream, FrameType::SetOAuthBearerTokenResp, cid, &buf).await
        }
        Err(msg) => {
            warn!(%cluster, error = %msg, "set_oauth_token failed");
            // set_oauth_token 失败基本是目标 cluster 尚未注册
            write_error_frame(
                stream,
                cid,
                ErrorResp::new(ErrorKind::ClusterNotRegistered, msg),
            )
            .await
        }
    }
}

async fn handle_register_cluster(
    stream: &mut UnixStream,
    cid: u64,
    payload: &[u8],
    registry: &ClusterRegistryHandle,
) -> anyhow::Result<()> {
    let req = match RegisterClusterReq::decode(payload) {
        Ok(r) => r,
        Err(e) => {
            return write_error_frame(
                stream,
                cid,
                ErrorResp::new(ErrorKind::Protocol, format!("payload decode: {e}")),
            )
            .await;
        }
    };
    let cluster = req.cluster.clone();
    let config: std::collections::HashMap<_, _> = req.config.into_iter().collect();
    if !config.contains_key("bootstrap.servers") {
        return write_error_frame(
            stream,
            cid,
            ErrorResp::new(
                ErrorKind::InvalidArgument,
                format!("cluster '{cluster}': missing required key 'bootstrap.servers'"),
            ),
        )
        .await;
    }
    let outcome = registry.register(cluster.clone(), config).await;
    debug!(%cluster, ?outcome, "cluster registered");
    let mut buf = BytesMut::new();
    RegisterClusterResp::Ok
        .encode(&mut buf)
        .context("encode register_cluster_resp")?;
    write_frame(stream, FrameType::RegisterClusterResp, cid, &buf).await
}

/// P2 #9：panic guard 包裹 produce_ack + encode + write_frame。
///
/// 真实风险：produce_ack 已 ack broker（消息已 commit）后，在 encode/write 之间
/// 发生 panic → connection drop → PHP 客户端 EOF → 重试 → 重复 produce。
/// 我们 catch panic 后**尽力**写一个 server-error frame（retryable=true 让客户端
/// 重试，配合 librdkafka enable.idempotence=true 可去重）；
/// 写 frame 本身也可能 panic，那只能让客户端见 EOF 然后重试，
/// 至少日志里留下了完整 panic backtrace 便于事后定位。
async fn handle_produce_req(
    stream: &mut UnixStream,
    cid: u64,
    payload: &[u8],
    producer: &ProducerHandle,
    shutdown: &ShutdownHandle,
    metrics: &Arc<Metrics>,
) -> anyhow::Result<()> {
    let result = AssertUnwindSafe(handle_produce_req_inner(
        stream, cid, payload, producer, shutdown, metrics,
    ))
    .catch_unwind()
    .await;
    match result {
        Ok(r) => r,
        Err(panic) => {
            let msg = describe_panic(&panic);
            error!(cid, panic = %msg, "handle_produce_req panicked, attempting fallback error frame");
            Metrics::inc(&metrics.produce_resp_err_total);
            // retryable=true：配合 librdkafka enable.idempotence 安全去重
            let err_resp = ErrorResp {
                kind: ErrorKind::Internal,
                retryable: true,
                outcome: hi_kafka_proto::OperationOutcome::Unknown,
                native_code: 0,
                message: format!("server panic: {msg}"),
            };
            let _ = write_error_frame(stream, cid, err_resp).await;
            Err(anyhow::anyhow!("panic in handle_produce_req: {msg}"))
        }
    }
}

/// PRODUCE_FNF：fire-and-forget，但**分层**回错——cluster 不存在 / 参数非法 /
/// 本地 enqueue 失败等「同步可知」的前置错误回结构化 [`ErrorResp`] 给业务；
/// enqueue 成功则回轻量 ack（不等 broker delivery，故 partition/offset = -1）。
/// 真正的 broker 异步投递结果仍不保证。
async fn handle_produce_fnf(
    stream: &mut UnixStream,
    cid: u64,
    payload: &[u8],
    producer: &ProducerHandle,
    shutdown: &ShutdownHandle,
    metrics: &Arc<Metrics>,
) -> anyhow::Result<()> {
    Metrics::inc(&metrics.produce_fnf_total);
    if shutdown.is_draining() {
        Metrics::inc(&metrics.frames_dropped_draining_total);
        Metrics::inc(&metrics.produce_fnf_failed_total);
        return write_error_frame(
            stream,
            cid,
            ErrorResp::new(ErrorKind::WorkerDraining, "worker is draining"),
        )
        .await;
    }
    let msg = match ProduceFnf::decode(payload) {
        Ok(m) => m,
        Err(e) => {
            Metrics::inc(&metrics.produce_fnf_failed_total);
            warn!(cid, error = ?e, "PRODUCE_FNF payload decode failed");
            return write_error_frame(
                stream,
                cid,
                ErrorResp::new(ErrorKind::Protocol, format!("payload decode: {e}")),
            )
            .await;
        }
    };
    let cluster = msg.cluster.clone();
    let topic = msg.topic.clone();
    match producer.produce_fnf(msg).await {
        Ok(()) => {
            let resp = ProduceResp::Ok(DeliveryAck {
                partition: -1,
                offset: -1,
            });
            let mut buf = BytesMut::new();
            resp.encode(&mut buf).context("encode fnf ack")?;
            write_frame(stream, FrameType::ProduceResp, cid, &buf).await
        }
        Err(e) => {
            Metrics::inc(&metrics.produce_fnf_failed_total);
            warn!(cid, %cluster, %topic, error = ?e, "PRODUCE_FNF enqueue failed");
            write_error_frame(stream, cid, WorkerError::from_anyhow(&e)).await
        }
    }
}

fn describe_panic(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

async fn handle_produce_req_inner(
    stream: &mut UnixStream,
    cid: u64,
    payload: &[u8],
    producer: &ProducerHandle,
    shutdown: &ShutdownHandle,
    metrics: &Arc<Metrics>,
) -> anyhow::Result<()> {
    Metrics::inc(&metrics.produce_req_total);
    if shutdown.is_draining() {
        Metrics::inc(&metrics.frames_dropped_draining_total);
        Metrics::inc(&metrics.produce_resp_err_total);
        return write_error_frame(
            stream,
            cid,
            ErrorResp::new(ErrorKind::WorkerDraining, "worker is draining"),
        )
        .await;
    }
    let msg = match ProduceReq::decode(payload) {
        Ok(m) => m,
        Err(e) => {
            warn!(cid, error = ?e, "PRODUCE_REQ payload decode failed");
            Metrics::inc(&metrics.produce_resp_err_total);
            return write_error_frame(
                stream,
                cid,
                ErrorResp::new(ErrorKind::Protocol, format!("payload decode: {e}")),
            )
            .await;
        }
    };
    let timeout = Duration::from_millis(req_timeout_ms(msg.timeout_ms));
    let produce_msg = msg.msg;
    debug!(cid, cluster = %produce_msg.cluster, topic = %produce_msg.topic, ?timeout, "PRODUCE_REQ received");
    match producer.produce_ack(produce_msg, timeout).await {
        Ok(resp) => {
            Metrics::inc(&metrics.produce_resp_ok_total);
            let mut resp_payload = BytesMut::new();
            resp.encode(&mut resp_payload)
                .context("encode PRODUCE_RESP")?;
            write_frame(stream, FrameType::ProduceResp, cid, &resp_payload).await
        }
        Err(e) => {
            warn!(cid, error = ?e, "produce_ack failed");
            Metrics::inc(&metrics.produce_resp_err_total);
            write_error_frame(stream, cid, WorkerError::from_anyhow(&e)).await
        }
    }
}

/// Defense in depth for raw protocol clients: a zero timeout still gets a
/// deterministic 1 ms operation instead of an immediate scheduler timeout.
fn req_timeout_ms(timeout_ms: u32) -> u64 {
    timeout_ms.max(1) as u64
}

async fn write_frame(
    stream: &mut UnixStream,
    kind: FrameType,
    cid: u64,
    payload: &[u8],
) -> anyhow::Result<()> {
    let mut buf = BytesMut::new();
    encode_frame(kind, cid, payload, &mut buf).context("encode frame")?;
    stream.write_all(&buf).await.context("write frame")?;
    stream.flush().await.context("flush frame")?;
    Ok(())
}

/// 把一个 [`ErrorResp`] 编成 `FrameType::Error` 帧写回客户端——
/// 所有 handler 的失败统一走这里，让 PHP 侧拿到结构化 kind。
async fn write_error_frame(
    stream: &mut UnixStream,
    cid: u64,
    err: ErrorResp,
) -> anyhow::Result<()> {
    let mut buf = BytesMut::new();
    err.encode(&mut buf).context("encode error resp")?;
    write_frame(stream, FrameType::Error, cid, &buf).await
}

/// 把 [`ConsumerError`] 映射成结构化 [`ErrorResp`]。
/// 关键：`NotFound → SUBSCRIPTION_NOT_FOUND`，让扩展端自愈从字符串匹配升级为 kind 判定。
fn consumer_err_to_resp(e: &ConsumerError) -> ErrorResp {
    e.response()
}

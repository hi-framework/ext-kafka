//! Worker 在 .so 内的入口点。
//!
//! 正常路径：spawn 在 fork 出的孙进程里 exec 一个只加载本扩展的 PHP CLI
//! （`php -n -d extension=<本 .so>`），并通过 [`EXEC_SOCKET_ENV`] 传入 socket。新进程
//! 的 MINIT 看到该变量后调用 [`exec_config_from_env`] 与 [`run_in_child`]，不再返回。
//! exec 换掉了整个地址空间，fork 时其它线程持有的锁随之作废，因此在任何运行时、
//! 任何时刻懒加载拉起 worker 都是安全的。
//!
//! 回退路径：找不到 PHP CLI 或 exec 失败时，孙进程直接调 [`run_in_child`]，在 fork
//! 出的进程里运行 worker（旧行为）。
//!
//! 子进程做的事：
//! 0. 关闭继承的全部 fd，清空信号屏蔽字
//! 1. `setsid()` 脱离 PHP 会话
//! 2. 关 stdin/stdout/stderr（或重定向到日志文件）
//! 3. 重置 SIGTERM/SIGINT 默认行为
//! 4. 构造 tokio runtime
//! 5. 启动 `Server` 跑 IPC + producer + consumer
//! 6. 收到 SIGTERM 后 drain + flush + 退出
//!
//! **重要**：子进程**不要调任何 PHP 函数**——继承下来的 PHP 状态可能已经不一致。

use std::ffi::CString;
use std::fs::OpenOptions;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::time::Duration;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone)]
pub struct ChildConfig {
    pub socket: PathBuf,
    pub log_level: String,
    pub log_file: Option<PathBuf>,
    pub drain_timeout: Duration,
    pub metrics_addr: Option<std::net::SocketAddr>,
    pub launch: Launch,
}

/// worker 是怎样启动的，只用于启动日志，便于排查是否走了回退路径。
#[derive(Debug, Clone)]
pub enum Launch {
    /// fork 后 exec 了 PHP CLI，运行在全新的进程镜像中。
    Exec,
    /// 无法 exec（原因见字段），直接运行在 fork 出的进程中。
    InProcess { reason: String },
    /// exec 调用失败（errno），回退为运行在 fork 出的进程中。
    ExecFailed { errno: i32 },
}

/// exec 路径上由父进程设置、交给新进程 MINIT 的 socket 路径。存在即表示「本进程是
/// worker」。其余配置（日志、drain、metrics）由父进程解析后以各自的环境变量传入。
pub const EXEC_SOCKET_ENV: &str = "HI_KAFKA_INTERNAL_WORKER_SOCKET";

/// exec 出的 worker 进程在 MINIT 中调用：读取父进程传入的配置。非 worker 进程返回 `None`。
pub fn exec_config_from_env() -> Option<ChildConfig> {
    let socket = std::env::var_os(EXEC_SOCKET_ENV).filter(|v| !v.is_empty())?;
    let env = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    Some(ChildConfig {
        socket: PathBuf::from(socket),
        log_level: env("HI_KAFKA_LOG_LEVEL").unwrap_or_else(|| "info".to_string()),
        log_file: env("HI_KAFKA_LOG_FILE").map(PathBuf::from),
        drain_timeout: Duration::from_millis(
            env("HI_KAFKA_DRAIN_TIMEOUT_MS")
                .and_then(|v| v.parse().ok())
                .unwrap_or(crate::ini_config::DEFAULT_DRAIN_TIMEOUT_MS),
        ),
        metrics_addr: env("HI_KAFKA_METRICS_ADDR").and_then(|v| v.parse().ok()),
        launch: Launch::Exec,
    })
}

/// **不返回**——worker 在此处完成所有工作并 `_exit()`。
///
/// 两种调用语境：exec 出的新进程的 MINIT（正常路径），或 exec 不可用时 fork 出的
/// 孙进程（回退路径）。
///
/// # Safety
///
/// - 调用方负责确保处于上述两种语境之一：本函数会关闭全部继承的 fd 并永不返回
/// - 回退路径上调用方负责在 fork 后不要调用任何 PHP 函数
pub unsafe fn run_in_child(config: ChildConfig) -> ! {
    // 0. 关闭继承自 PHP 的全部 fd（监听端口、数据库/Redis 连接、spawn 锁……）。
    //    worker 是长驻进程：继承的监听 socket 会让 PHP 服务重启时端口被占用，继承的
    //    客户端连接永远不会关闭；继承的 spawn 锁会让锁在 worker 存活期间一直被持有。
    //    同时清空信号屏蔽字：调用线程可能屏蔽了 SIGTERM，worker 将无法优雅退出。
    unsafe {
        close_inherited_fds();
        let mut empty: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut empty);
        libc::pthread_sigmask(libc::SIG_SETMASK, &empty, std::ptr::null_mut());
    }

    // 1. 脱离 PHP 会话（double fork 的中间进程已经 setsid；这里对直接调用方兜底）
    unsafe { libc::setsid() };

    // 2. 改进程名让 pgrep/ps 能识别（最长 15 字符 + null）
    #[cfg(target_os = "linux")]
    unsafe {
        let name = b"hi-kafka-worker\0";
        libc::prctl(libc::PR_SET_NAME, name.as_ptr() as libc::c_ulong);
        // 不设置 PR_SET_PDEATHSIG —— worker 应在父死后继续活，
        // 由后续 PHP 进程通过 UDS 复用，不被 PID 1 杀掉
    }

    // Y1: macOS 上 `prctl` 不存在。
    //
    // 我们走 `pthread_setname_np` 改主线程名——Activity Monitor / Instruments /
    // 调试器（lldb）能识别。**但 `ps -ef` 不行**：macOS kernel 给每个进程维护
    // 独立的 procinfo commandline buffer，与 user-space 的 `_NSGetArgv()` 完全
    // 隔离。覆盖 user-space argv[0] 字符串内存对 `ps -ef` 显示无效（实测验证）。
    // Darwin 有 `proc_setname_np` 私有 SPI 但 Apple 不保证 ABI 稳定。
    //
    // 实际效果：macOS 上 `ps -ef` 看 worker 仍像 PHP 进程；运维定位时**用
    // `pgrep -f /tmp/.*\.sock`** 按 socket 路径找，或用 `scripts/cleanup-ghosts.sh`
    // 一键清理。
    #[cfg(target_os = "macos")]
    unsafe {
        let new_name = b"hi-kafka-worker\0";
        libc::pthread_setname_np(new_name.as_ptr() as *const _);
    }

    // 3. 重定向 stdio
    redirect_stdio(config.log_file.as_deref());

    // 4. 重置信号处理器，避免继承 PHP 的奇怪 handlers
    for sig in &[libc::SIGTERM, libc::SIGINT, libc::SIGHUP, libc::SIGPIPE] {
        unsafe { libc::signal(*sig, libc::SIG_DFL) };
    }

    // 5. 初始化日志
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_new(&config.log_level).unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(true)
        .try_init();

    info!(
        version = env!("CARGO_PKG_VERSION"),
        socket = %config.socket.display(),
        launch = ?config.launch,
        "hi-kafka worker (embedded in .so) starting"
    );
    const FORKED_COPY: &str = "worker runs in a forked copy of the PHP process; if that \
        process was multi-threaded the worker may deadlock and is then killed after the \
        ready timeout. Set hi_kafka.php_binary to a PHP CLI binary to enable exec.";
    match &config.launch {
        Launch::Exec => {}
        Launch::InProcess { reason } => warn!(exec_unavailable = %reason, "{FORKED_COPY}"),
        Launch::ExecFailed { errno } => warn!(
            exec_error = %std::io::Error::from_raw_os_error(*errno),
            "{FORKED_COPY}"
        ),
    }

    // 6. glibc 上限制 malloc arena 数，必须在创建 tokio / librdkafka 线程之前。
    hi_kafka_worker::allocator::limit_malloc_arenas();

    // 7. 构造 tokio runtime
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .thread_name("hi-kafka")
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            error!(error = ?e, "build tokio runtime failed");
            unsafe { libc::_exit(1) };
        }
    };

    // 8. 跑 server
    let result = rt.block_on(run_server(config));

    if let Err(e) = result {
        error!(error = ?e, "worker exited with error");
        unsafe { libc::_exit(1) };
    }

    // 用 _exit 跳过 atexit handlers（可能继承自 PHP）
    unsafe { libc::_exit(0) };
}

async fn run_server(config: ChildConfig) -> anyhow::Result<()> {
    use hi_kafka_worker::{
        cluster::ClusterRegistry, metrics, server, shutdown::ShutdownState, Metrics,
    };

    let pid_file = pid_file_for(&config.socket);

    let registry = ClusterRegistry::new();

    let producer = build_producer(registry.clone());
    let shutdown = ShutdownState::new();
    let m = Metrics::new();
    let consumer = build_consumer(registry.clone(), shutdown.clone(), m.clone());

    if let Some(addr) = config.metrics_addr {
        let m_clone = m.clone();
        tokio::spawn(async move {
            if let Err(e) = metrics::serve(addr, m_clone).await {
                error!(error = ?e, "metrics endpoint exited");
            }
        });
    }

    let mut sig_term = signal(SignalKind::terminate())?;
    let mut sig_int = signal(SignalKind::interrupt())?;

    // 内嵌 worker：无连接持续空闲超时则自退，解决主进程退出后 worker 残留。
    // 默认 5min；HI_KAFKA_IDLE_TIMEOUT_MS=0 可禁用（回到常驻）。
    let idle_timeout = std::env::var("HI_KAFKA_IDLE_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or_else(|| Duration::from_secs(300));
    let server = server::Server::bind_with(
        &config.socket,
        producer.clone(),
        consumer,
        registry,
        shutdown.clone(),
        m,
        idle_timeout,
    )
    .await?;
    // 只有 bind 成功后才写 pid 文件：bind 失败的 worker（例如路径上已有 worker 在服务、
    // 只是暂时无响应）若先写入，会把仍在服务的 worker 的 pid 覆盖成一个已退出的进程。
    let _ = write_pid_file(&pid_file);

    // P0 #1：同 main.rs，信号触发 shutdown，server.run 自己 drain。
    let shutdown_for_sig = shutdown.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = sig_term.recv() => info!("SIGTERM received, draining..."),
            _ = sig_int.recv() => info!("SIGINT received, draining..."),
        }
        shutdown_for_sig.start_draining();
    });

    let socket_identity = server.socket_identity();
    if let Err(e) = server.run(config.drain_timeout).await {
        error!(error = ?e, "server exited with error");
    }
    if let Err(e) = producer.flush(config.drain_timeout).await {
        error!(error = ?e, "producer flush during shutdown failed");
    }

    // drain 期间可能已有新 worker 在同一路径 bind：只删除自己的 socket 与 pid 文件。
    server::remove_socket_if_owned(&config.socket, &socket_identity);
    remove_pid_file_if_owned(&pid_file);
    info!("hi-kafka worker (embedded) stopped");
    Ok(())
}

/// 关闭 fd >= 3。优先用 Linux `close_range`，否则遍历到 fd 上限（封顶 65536）。
unsafe fn close_inherited_fds() {
    #[cfg(target_os = "linux")]
    {
        if unsafe { libc::syscall(libc::SYS_close_range, 3u32, u32::MAX, 0u32) } == 0 {
            return;
        }
    }
    let max = match unsafe { libc::sysconf(libc::_SC_OPEN_MAX) } {
        n if n > 0 => n.min(65_536) as libc::c_int,
        _ => 4096,
    };
    for fd in 3..max {
        unsafe { libc::close(fd) };
    }
}

fn pid_file_for(socket: &std::path::Path) -> std::path::PathBuf {
    let mut p = socket.to_path_buf();
    let stem = p.file_name().map(|s| s.to_os_string()).unwrap_or_default();
    let mut name = stem;
    name.push(".pid");
    p.set_file_name(name);
    p
}

fn remove_pid_file_if_owned(path: &std::path::Path) {
    let ours = std::fs::read_to_string(path)
        .ok()
        .and_then(|content| content.trim().parse::<u32>().ok())
        == Some(std::process::id());
    if ours {
        let _ = std::fs::remove_file(path);
    }
}

fn write_pid_file(path: &std::path::Path) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(path)?;
    writeln!(f, "{}", std::process::id())
}

// graceful_drain 已经废弃：server.run 现在自己负责 drain connection task，
// producer.flush 由调用方在 server.run 返回之后再做一次。

fn redirect_stdio(log_file: Option<&std::path::Path>) {
    // 关掉继承自 PHP 的 stdio
    let dev_null = match OpenOptions::new().read(true).write(true).open("/dev/null") {
        Ok(f) => f,
        Err(_) => return,
    };
    unsafe {
        libc::dup2(dev_null.as_raw_fd(), libc::STDIN_FILENO);
    }

    let out_fd = match log_file {
        Some(path) => match OpenOptions::new().create(true).append(true).open(path) {
            Ok(f) => Some(f),
            Err(_) => None,
        },
        None => None,
    };

    let target_fd = out_fd
        .as_ref()
        .map(|f| f.as_raw_fd())
        .unwrap_or_else(|| dev_null.as_raw_fd());

    unsafe {
        libc::dup2(target_fd, libc::STDOUT_FILENO);
        libc::dup2(target_fd, libc::STDERR_FILENO);
    }

    // 其它继承的 fd 已在 run_in_child 开头由 close_inherited_fds 关闭。
    drop(dev_null);
    drop(out_fd);
}

#[cfg(feature = "kafka")]
fn build_producer(
    registry: hi_kafka_worker::ClusterRegistryHandle,
) -> hi_kafka_worker::ProducerHandle {
    use hi_kafka_worker::producer::KafkaProducer;
    use std::sync::Arc;
    Arc::new(KafkaProducer::new(registry))
}

#[cfg(not(feature = "kafka"))]
fn build_producer(
    _registry: hi_kafka_worker::ClusterRegistryHandle,
) -> hi_kafka_worker::ProducerHandle {
    hi_kafka_worker::producer::logging()
}

#[cfg(feature = "kafka")]
fn build_consumer(
    registry: hi_kafka_worker::ClusterRegistryHandle,
    shutdown: hi_kafka_worker::ShutdownHandle,
    metrics: std::sync::Arc<hi_kafka_worker::Metrics>,
) -> hi_kafka_worker::ConsumerHandle {
    hi_kafka_worker::consumer::kafka(registry, shutdown, metrics)
}

#[cfg(not(feature = "kafka"))]
fn build_consumer(
    _registry: hi_kafka_worker::ClusterRegistryHandle,
    shutdown: hi_kafka_worker::ShutdownHandle,
    metrics: std::sync::Arc<hi_kafka_worker::Metrics>,
) -> hi_kafka_worker::ConsumerHandle {
    hi_kafka_worker::consumer::logging(shutdown, metrics)
}

// 抑制 CString import 警告（保留供未来使用）
#[allow(dead_code)]
fn _unused_cstring(_s: &str) -> Option<CString> {
    None
}

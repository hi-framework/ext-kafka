//! Worker 自启动：扩展首次需要 worker 时，**在 .so 内部**通过 `libc::fork()`
//! 拉起一个节点级守护进程。子进程直接调 [`crate::worker_entry::run_in_child`]，
//! **不 exec 外部二进制**——整个 worker 代码已经链接进 .so。
//!
//! 并发控制：`flock(LOCK_EX | LOCK_NB)` 锁住 `<socket>.spawn-lock` 文件。
//! 同 pod 内多个 PHP 进程并发到这一步时只有一个能拿到锁、负责 fork，
//! 其它等 socket 出现。
//!
//! 进程模型：double fork。中间进程只调用 async-signal-safe 的 `setsid` / `fork` /
//! `write` / `_exit`，经 pipe 回报孙进程 PID 后立即退出并被 spawner `waitpid` 回收；
//! worker（孙进程）从一开始就被 init 收养，不会成为 PHP-FPM / Swoole manager 的
//! 子进程（不留僵尸、不干扰它们的 SIGCHLD 处理）。worker 启动后先关闭全部继承的 fd。
//!
//! 就绪判定：HELLO 握手成功才算就绪（connect 成功不够：可能是旧版本、半启动或
//! 卡死的 worker）。超时未就绪的孙进程会被 SIGKILL：在多线程进程中 fork 时，子进程
//! 可能继承其它线程持有的锁而死锁，这里保证最坏情况是一次有界的启动失败，而不是
//! 一个永远不就绪、却占着 socket namespace 的僵死进程。

use crate::protocol;
use crate::worker_entry::{self, ChildConfig};
use hi_kafka_proto::HEADER_LEN;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    #[error("fork failed: {0}")]
    ForkFailed(std::io::Error),

    #[error("worker did not become ready in {timeout_ms}ms")]
    NotReady { timeout_ms: u64 },

    #[error("create spawn lock file: {0}")]
    CreateLockFile(std::io::Error),

    /// N: 进程已有 {count} 个线程；fork 后只剩调用线程，其它线程持有的 heap 锁
    /// 会在子进程里永久死锁。仅在 `HI_KAFKA_STRICT_FORK=1` 时启用此严格检查。
    /// 修复：在 Swoole/Swow reactor / Sentry SDK 起 thread 前调 ensureWorker()。
    #[error(
        "refusing fork in multi-threaded process (threads={count}, HI_KAFKA_STRICT_FORK=1). \
         Call Hi\\Kafka\\Client::ensureWorker() BEFORE starting Swoole/Swow reactor or \
         any SDK that spawns background threads"
    )]
    MultiThreadedFork { count: usize },
}

/// 启动配置
pub struct SpawnConfig {
    pub socket: PathBuf,
    pub log_level: String,
    pub log_file: Option<PathBuf>,
    /// worker 收到 SIGTERM 后排干 in-flight 连接的超时
    pub drain_timeout: Duration,
    /// 等待 socket 就绪的总超时
    pub ready_timeout: Duration,
}

impl SpawnConfig {
    /// 解析配置：先读 env / php.ini（env > ini > default），fork 前父进程一次性算出来。
    pub fn from_env(socket: PathBuf) -> Self {
        Self {
            socket,
            log_level: crate::ini_config::log_level(),
            log_file: crate::ini_config::log_file(),
            drain_timeout: Duration::from_millis(crate::ini_config::drain_timeout_ms()),
            ready_timeout: Duration::from_secs(5),
        }
    }

    fn to_child_config(&self) -> ChildConfig {
        ChildConfig {
            socket: self.socket.clone(),
            log_level: self.log_level.clone(),
            log_file: self.log_file.clone(),
            drain_timeout: self.drain_timeout,
            metrics_addr: crate::ini_config::metrics_addr(),
        }
    }
}

/// L: 区分 worker 已活 vs 本次刚 spawn，让上层决定是否要重放 cluster 注册。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpawnOutcome {
    AlreadyAlive,
    JustSpawned,
}

/// 单次探测的 IO 上限。worker 正常时 HELLO 是亚毫秒级。
const PROBE_TIMEOUT: Duration = Duration::from_millis(300);

/// 检测 worker 是否可用：connect 并完成 protocol HELLO。
pub fn worker_alive(socket: &Path) -> bool {
    handshake(socket, PROBE_TIMEOUT).is_some()
}

/// 直连 socket 并完成 HELLO 握手（RPC 角色）。任一步失败返回 `None`。
pub fn handshake(socket: &Path, timeout: Duration) -> Option<UnixStream> {
    let mut stream = crate::uds::connect(socket, timeout).ok()?;
    stream.set_read_timeout(Some(timeout)).ok()?;
    stream.set_write_timeout(Some(timeout)).ok()?;

    let hello = protocol::build_hello_frame().ok()?;
    stream.write_all(&hello).ok()?;

    let mut header = [0u8; HEADER_LEN];
    stream.read_exact(&mut header).ok()?;
    let payload_len = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
    // HELLO RESP payload 只有几个字节；异常大说明对端不是本协议的 worker。
    if payload_len > 64 {
        return None;
    }
    let mut full = vec![0u8; HEADER_LEN + payload_len];
    full[..HEADER_LEN].copy_from_slice(&header);
    stream.read_exact(&mut full[HEADER_LEN..]).ok()?;
    protocol::parse_hello_resp(&full).ok()?;
    Some(stream)
}

/// 主入口：保证 socket 后面有一个能响应的 worker。
pub fn ensure_worker(cfg: &SpawnConfig) -> Result<SpawnOutcome, SpawnError> {
    // Fast path
    if worker_alive(&cfg.socket) {
        return Ok(SpawnOutcome::AlreadyAlive);
    }

    // 准备 lock 文件路径
    let lock_path = lock_path_for(&cfg.socket);
    if let Some(parent) = lock_path.parent() {
        std::fs::create_dir_all(parent).map_err(SpawnError::CreateLockFile)?;
    }
    let lock_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(SpawnError::CreateLockFile)?;

    // 非阻塞独占锁
    let got_lock = try_flock_ex_nb(&lock_file);

    let deadline = Instant::now() + cfg.ready_timeout;
    if got_lock {
        // 拿锁后再探测一次：可能在我们等锁之前别的进程刚完成启动。
        if worker_alive(&cfg.socket) {
            return Ok(SpawnOutcome::AlreadyAlive);
        }
        let pid = spawn_worker_inproc(cfg)?;
        if let Err(error) = wait_for_socket(&cfg.socket, deadline) {
            // 未就绪的 worker 可能已经死锁；杀掉它，让下一次 ensure 干净地重试。
            if pid > 0 {
                unsafe { libc::kill(pid, libc::SIGKILL) };
            }
            return Err(error);
        }
    } else {
        // 别的进程在 spawn，等 socket 就绪
        wait_for_socket(&cfg.socket, deadline)?;
    }

    drop(lock_file);
    Ok(SpawnOutcome::JustSpawned)
}

fn lock_path_for(socket: &Path) -> PathBuf {
    let mut p = socket.to_path_buf();
    let stem = p.file_name().map(|s| s.to_os_string()).unwrap_or_default();
    let mut new_name = stem;
    new_name.push(".spawn-lock");
    p.set_file_name(new_name);
    p
}

fn try_flock_ex_nb(file: &File) -> bool {
    let fd = file.as_raw_fd();
    let rc = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
    rc == 0
}

/// 在 .so 内 double fork 出 worker，返回 worker（孙进程）的 PID；无法获知时返回 0。
///
/// 父：回收中间进程后返回，由调用方等待就绪
/// 孙：跳到 [`worker_entry::run_in_child`]，**永不返回**
fn spawn_worker_inproc(cfg: &SpawnConfig) -> Result<libc::pid_t, SpawnError> {
    // 多线程进程中 fork：子进程只剩调用线程，其它线程持有的锁（malloc 等）可能
    // 永久不释放。危险线程多来自第三方扩展（Sentry / OTel / Swoole 异步 IO 线程池）；
    // macOS 的 libdispatch 等系统线程不构成风险（PHP CLI 启动即有 3 个）。
    //
    // 默认允许并依赖「就绪超时即 SIGKILL」把最坏情况限定为一次有界失败——worker
    // 崩溃后的惰性恢复往往发生在已有线程池的 Swoole worker 里，拒绝 fork 会让 Kafka
    // 长期不可用。`HI_KAFKA_STRICT_FORK=1` 时严格拒绝。
    let n_threads = current_thread_count();
    if n_threads > 1 {
        let strict = std::env::var("HI_KAFKA_STRICT_FORK")
            .map(|v| v != "0" && !v.is_empty())
            .unwrap_or(false);
        if strict {
            return Err(SpawnError::MultiThreadedFork { count: n_threads });
        }
        tracing::warn!(
            threads = n_threads,
            "forking worker from a multi-threaded process; a worker that inherits a held lock \
             will be killed after the ready timeout. Call ensureWorker() before starting \
             reactors/threads to avoid this."
        );
    }

    // fork 之后的子进程不做任何分配：所有需要的数据在这里准备好。
    let child_config = cfg.to_child_config();
    let mut pipe_fds = [0 as libc::c_int; 2];
    if unsafe { libc::pipe(pipe_fds.as_mut_ptr()) } != 0 {
        return Err(SpawnError::ForkFailed(std::io::Error::last_os_error()));
    }
    let [read_fd, write_fd] = pipe_fds;

    let intermediate = unsafe { libc::fork() };
    if intermediate < 0 {
        let error = std::io::Error::last_os_error();
        unsafe {
            libc::close(read_fd);
            libc::close(write_fd);
        }
        return Err(SpawnError::ForkFailed(error));
    }

    if intermediate == 0 {
        // === 中间进程：只允许 async-signal-safe 调用 ===
        unsafe {
            libc::close(read_fd);
            libc::setsid();
            let worker = libc::fork();
            if worker == 0 {
                libc::close(write_fd);
                // 进入 worker，永不返回
                worker_entry::run_in_child(child_config);
            }
            let bytes = worker.to_ne_bytes();
            libc::write(write_fd, bytes.as_ptr().cast(), bytes.len());
            libc::_exit(if worker > 0 { 0 } else { 1 });
        }
    }

    // === 父进程 ===
    unsafe { libc::close(write_fd) };
    let mut status = 0;
    while unsafe { libc::waitpid(intermediate, &mut status, 0) } < 0
        && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
    {}
    let mut bytes = [0u8; std::mem::size_of::<libc::pid_t>()];
    let mut file = unsafe { <File as std::os::fd::FromRawFd>::from_raw_fd(read_fd) };
    let worker = match file.read_exact(&mut bytes) {
        Ok(()) => libc::pid_t::from_ne_bytes(bytes),
        Err(_) => 0,
    };
    if worker < 0 {
        return Err(SpawnError::ForkFailed(std::io::Error::other(
            "intermediate process failed to fork the worker",
        )));
    }
    Ok(worker)
}

/// 当前进程的线程数。各 OS 拿数据来源不同：
/// - Linux: `/proc/self/status` 的 `Threads:` 行
/// - macOS: mach `task_threads()` 取 thread port 数组长度
/// - 其他 / 失败：返回 1（按"单线程"处理，不阻塞）
fn current_thread_count() -> usize {
    #[cfg(target_os = "linux")]
    {
        if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
            for line in s.lines() {
                if let Some(rest) = line.strip_prefix("Threads:") {
                    if let Ok(n) = rest.trim().parse::<usize>() {
                        return n.max(1);
                    }
                }
            }
        }
        1
    }
    #[cfg(target_os = "macos")]
    {
        // 用 libproc.h::proc_pidinfo(PROC_PIDTASKINFO) 拿 pti_threadnum。
        // 这是 libSystem 公开符号，链接稳定，不依赖 mach 内部 ABI。
        unsafe extern "C" {
            fn proc_pidinfo(
                pid: libc::pid_t,
                flavor: libc::c_int,
                arg: u64,
                buffer: *mut libc::c_void,
                buffersize: libc::c_int,
            ) -> libc::c_int;
        }
        const PROC_PIDTASKINFO: libc::c_int = 4;
        #[repr(C)]
        #[derive(Default)]
        struct ProcTaskInfo {
            virtual_size: u64,
            resident_size: u64,
            total_user: u64,
            total_system: u64,
            threads_user: u64,
            threads_system: u64,
            policy: i32,
            faults: i32,
            pageins: i32,
            cow_faults: i32,
            messages_sent: i32,
            messages_received: i32,
            syscalls_mach: i32,
            syscalls_unix: i32,
            csw: i32,
            threadnum: i32,
            numrunning: i32,
            priority: i32,
        }
        let mut info = ProcTaskInfo::default();
        let size = std::mem::size_of::<ProcTaskInfo>() as libc::c_int;
        let r = unsafe {
            proc_pidinfo(
                libc::getpid(),
                PROC_PIDTASKINFO,
                0,
                &mut info as *mut _ as *mut libc::c_void,
                size,
            )
        };
        if r > 0 {
            (info.threadnum as usize).max(1)
        } else {
            1
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        1
    }
}

fn wait_for_socket(socket: &Path, deadline: Instant) -> Result<(), SpawnError> {
    while Instant::now() < deadline {
        if worker_alive(socket) {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(30));
    }
    Err(SpawnError::NotReady {
        timeout_ms: deadline.elapsed().as_millis() as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_path_for() {
        let p = lock_path_for(Path::new("/tmp/hi-kafka-v2.sock"));
        assert_eq!(p, PathBuf::from("/tmp/hi-kafka-v2.sock.spawn-lock"));
    }

    #[test]
    fn test_worker_alive_false_for_missing() {
        assert!(!worker_alive(Path::new(
            "/tmp/definitely-does-not-exist.sock"
        )));
    }
}

//! Worker 自启动：扩展首次需要 worker 时，**在 .so 内部**通过 `libc::fork()`
//! 拉起一个节点级守护进程。子进程直接调 [`crate::worker_entry::run_in_child`]，
//! **不 exec 外部二进制**——整个 worker 代码已经链接进 .so。
//!
//! 并发控制：`flock(LOCK_EX | LOCK_NB)` 锁住 `<socket>.spawn-lock` 文件。
//! 同 pod 内多个 PHP 进程并发到这一步时只有一个能拿到锁、负责 fork，
//! 其它等 socket 出现。
//!
//! 父子分离：子进程 `setsid()` 脱离 PHP 会话 + 关 stdio + 重置信号处理器。
//! PHP 父进程退出后，子进程被 init/PID1 收养，成为节点级守护。

use crate::worker_entry::{self, ChildConfig};
use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
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
}

/// 启动配置
pub struct SpawnConfig {
    pub socket: PathBuf,
    pub brokers: Option<String>,
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
            brokers: std::env::var("HI_KAFKA_BROKERS").ok(),
            log_level: crate::ini_config::log_level(),
            log_file: crate::ini_config::log_file(),
            drain_timeout: Duration::from_millis(crate::ini_config::drain_timeout_ms()),
            ready_timeout: Duration::from_secs(5),
        }
    }

    fn to_child_config(&self) -> ChildConfig {
        ChildConfig {
            socket: self.socket.clone(),
            brokers: self.brokers.clone(),
            log_level: self.log_level.clone(),
            log_file: self.log_file.clone(),
            drain_timeout: self.drain_timeout,
            metrics_addr: crate::ini_config::metrics_addr(),
        }
    }
}

/// 检测 worker 是否在跑：尝试 connect socket。
pub fn worker_alive(socket: &Path) -> bool {
    std::os::unix::net::UnixStream::connect(socket).is_ok()
}

/// 主入口：保证 socket 后面有一个能响应的 worker。
pub fn ensure_worker(cfg: &SpawnConfig) -> Result<(), SpawnError> {
    // Fast path
    if worker_alive(&cfg.socket) {
        return Ok(());
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
        // 拿到锁的进程负责 spawn
        spawn_worker_inproc(cfg)?;
        wait_for_socket(&cfg.socket, deadline)?;
    } else {
        // 别的进程在 spawn，等 socket 就绪
        wait_for_socket(&cfg.socket, deadline)?;
    }

    drop(lock_file);
    Ok(())
}

fn lock_path_for(socket: &Path) -> PathBuf {
    let mut p = socket.to_path_buf();
    let stem = p
        .file_name()
        .map(|s| s.to_os_string())
        .unwrap_or_default();
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

/// 在 .so 内 fork 出 worker 子进程。
///
/// 父：返回 Ok 让 PHP 继续
/// 子：跳到 [`worker_entry::run_in_child`]，**永不返回**
fn spawn_worker_inproc(cfg: &SpawnConfig) -> Result<(), SpawnError> {
    let child_config = cfg.to_child_config();

    // fork() 之前不持锁、不持文件 handle（除了 lock_file 由调用方持有）
    // 子进程会继承 lock_file fd，但我们不 close 它——父端 drop 后锁会自动释放
    let pid = unsafe { libc::fork() };

    if pid < 0 {
        return Err(SpawnError::ForkFailed(std::io::Error::last_os_error()));
    }

    if pid == 0 {
        // === 子进程 ===
        // 进入 worker，永不返回
        unsafe { worker_entry::run_in_child(child_config) };
        // unreachable
    }

    // === 父进程 ===
    // 不 wait —— 子进程独立活下去，等下面 wait_for_socket 检测就绪
    Ok(())
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
        let p = lock_path_for(Path::new("/tmp/hi-kafka.sock"));
        assert_eq!(p, PathBuf::from("/tmp/hi-kafka.sock.spawn-lock"));
    }

    #[test]
    fn test_worker_alive_false_for_missing() {
        assert!(!worker_alive(Path::new(
            "/tmp/definitely-does-not-exist.sock"
        )));
    }

}

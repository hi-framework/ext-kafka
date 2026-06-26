//! 扩展端 Unix Domain Socket 连接池。
//!
//! 设计要点：
//!
//! - **按 socket 路径全局共享**：用 `OnceLock<Mutex<HashMap>>` 维护
//!   socket→pool 的映射。同一进程内同一 socket 只有一个池。
//! - **RAII**：[`PooledConn`] Drop 时自动归还连接到池。若使用方调用
//!   [`PooledConn::poison`]，连接会被直接丢弃（用于 IO 出错的连接）。
//! - **半关闭检测**：acquire 时对取出的连接做 `set_nonblocking + peek`
//!   探测，若发现 EOF 或 RST，丢弃并新建。代价 ≤ 1 syscall。
//! - **MVP 阻塞 IO**：协程感知留给 Phase 3。这里默认 PHP-FPM / CLI 场景，
//!   在 Swoole/Swow 下也能用——因为单次 produce 的 IO 时间通常 < 5ms，
//!   不会显著影响协程调度。
//!
//! 与 SkyWalking PHP 的 `OnceCell<Mutex<UnixStream>>` 单连接相比：
//! 多协程并发时本池的等待时间更小（每协程独占一个连接，无 mutex 排队）。

use std::collections::HashMap;
use std::io::{ErrorKind, Read};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("connect {socket}: {source}")]
    Connect {
        socket: String,
        source: std::io::Error,
    },
}

const DEFAULT_MAX_IDLE: usize = 16;

pub struct ConnectionPool {
    socket: PathBuf,
    max_idle: usize,
    idle: Mutex<Vec<UnixStream>>,
    /// 累计统计
    stats: Mutex<PoolStats>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PoolStats {
    pub acquires_total: u64,
    pub hits_total: u64,
    pub misses_total: u64,
    pub closed_total: u64,
    pub poisoned_total: u64,
}

impl ConnectionPool {
    pub fn new(socket: PathBuf, max_idle: usize) -> Self {
        Self {
            socket,
            max_idle,
            idle: Mutex::new(Vec::new()),
            stats: Mutex::new(PoolStats::default()),
        }
    }

    pub fn stats(&self) -> PoolStats {
        *self.stats.lock().unwrap()
    }

    pub fn idle_count(&self) -> usize {
        self.idle.lock().unwrap().len()
    }

    /// 取一个连接出来。优先复用 idle 里的，否则新建。
    pub fn acquire(self: &Arc<Self>) -> Result<PooledConn, PoolError> {
        self.stats.lock().unwrap().acquires_total += 1;

        // 尝试从 idle 弹一个
        loop {
            let candidate = self.idle.lock().unwrap().pop();
            match candidate {
                Some(stream) => {
                    if is_alive(&stream) {
                        self.stats.lock().unwrap().hits_total += 1;
                        return Ok(PooledConn {
                            stream: Some(stream),
                            pool: Arc::clone(self),
                            poisoned: false,
                        });
                    } else {
                        // 半关闭/对端已断，丢弃后继续找
                        self.stats.lock().unwrap().closed_total += 1;
                        continue;
                    }
                }
                None => break,
            }
        }

        // 池空 → 新建
        self.stats.lock().unwrap().misses_total += 1;
        let stream = UnixStream::connect(&self.socket).map_err(|e| PoolError::Connect {
            socket: self.socket.display().to_string(),
            source: e,
        })?;
        Ok(PooledConn {
            stream: Some(stream),
            pool: Arc::clone(self),
            poisoned: false,
        })
    }

    fn release(&self, stream: UnixStream) {
        let mut idle = self.idle.lock().unwrap();
        if idle.len() < self.max_idle {
            idle.push(stream);
        }
        // 超过 max_idle 直接丢弃
    }
}

/// 探测连接是否还活着。
/// 用 nonblocking peek：返回 0 字节 → 对端关闭；EWOULDBLOCK → 还活着。
fn is_alive(stream: &UnixStream) -> bool {
    if stream.set_nonblocking(true).is_err() {
        return false;
    }
    let mut buf = [0u8; 1];
    let alive = match (&*stream).read(&mut buf) {
        Ok(0) => false, // EOF
        Ok(_) => false, // 不应有未读数据，半关闭
        Err(e) => matches!(e.kind(), ErrorKind::WouldBlock),
    };
    // 还原阻塞模式
    let _ = stream.set_nonblocking(false);
    alive
}

pub struct PooledConn {
    stream: Option<UnixStream>,
    pool: Arc<ConnectionPool>,
    poisoned: bool,
}

impl PooledConn {
    pub fn stream_mut(&mut self) -> &mut UnixStream {
        self.stream.as_mut().expect("connection already taken")
    }

    /// 标记此连接不可复用（IO 失败时调用）。Drop 时不归还池。
    pub fn poison(&mut self) {
        self.poisoned = true;
        self.pool.stats.lock().unwrap().poisoned_total += 1;
    }
}

impl Drop for PooledConn {
    fn drop(&mut self) {
        if self.poisoned {
            return;
        }
        if let Some(stream) = self.stream.take() {
            self.pool.release(stream);
        }
    }
}

static POOLS: OnceLock<Mutex<HashMap<PathBuf, Arc<ConnectionPool>>>> = OnceLock::new();

fn pools() -> &'static Mutex<HashMap<PathBuf, Arc<ConnectionPool>>> {
    POOLS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// 获取或创建给定 socket 的全局池。
pub fn pool_for(socket: &Path) -> Arc<ConnectionPool> {
    let mut map = pools().lock().unwrap();
    if let Some(p) = map.get(socket) {
        return p.clone();
    }
    let max_idle = std::env::var("HI_KAFKA_POOL_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|n: &usize| *n > 0)
        .unwrap_or(DEFAULT_MAX_IDLE);
    let pool = Arc::new(ConnectionPool::new(socket.to_path_buf(), max_idle));
    map.insert(socket.to_path_buf(), pool.clone());
    pool
}

/// 汇总所有池的统计，用于扩展暴露给 PHP。
pub fn all_stats() -> Vec<(PathBuf, PoolStats, usize, usize)> {
    let map = pools().lock().unwrap();
    map.iter()
        .map(|(path, pool)| {
            (
                path.clone(),
                pool.stats(),
                pool.idle_count(),
                pool.max_idle,
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::net::UnixListener;
    use std::thread;
    use std::time::Duration;

    fn make_temp_socket() -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "hi-kafka-pool-test-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&p);
        p
    }

    /// 在后台跑一个简单的 echo server 用于测试。
    fn spawn_echo_server(socket: &Path) -> thread::JoinHandle<()> {
        let socket = socket.to_path_buf();
        thread::spawn(move || {
            let listener = UnixListener::bind(&socket).expect("bind");
            for incoming in listener.incoming() {
                if let Ok(mut s) = incoming {
                    let _ = s.write_all(b"x");
                }
            }
        })
    }

    #[test]
    fn test_acquire_creates_then_reuses() {
        let socket = make_temp_socket();
        let _server = spawn_echo_server(&socket);
        thread::sleep(Duration::from_millis(50));

        let pool = Arc::new(ConnectionPool::new(socket.clone(), 4));

        // 第一次 acquire: miss
        {
            let _c1 = pool.acquire().unwrap();
        }
        // 应已归还
        assert_eq!(pool.idle_count(), 1);

        // 第二次 acquire: hit
        {
            let _c2 = pool.acquire().unwrap();
        }
        let s = pool.stats();
        assert_eq!(s.acquires_total, 2);
        assert_eq!(s.misses_total, 1);
        assert!(s.hits_total >= 1);

        let _ = std::fs::remove_file(&socket);
    }

    #[test]
    fn test_poisoned_not_returned() {
        let socket = make_temp_socket();
        let _server = spawn_echo_server(&socket);
        thread::sleep(Duration::from_millis(50));

        let pool = Arc::new(ConnectionPool::new(socket.clone(), 4));
        {
            let mut c = pool.acquire().unwrap();
            c.poison();
        }
        assert_eq!(pool.idle_count(), 0);
        assert_eq!(pool.stats().poisoned_total, 1);

        let _ = std::fs::remove_file(&socket);
    }

    #[test]
    fn test_max_idle_caps_pool() {
        let socket = make_temp_socket();
        let _server = spawn_echo_server(&socket);
        thread::sleep(Duration::from_millis(50));

        let pool = Arc::new(ConnectionPool::new(socket.clone(), 2));

        // 同时持有 3 个连接，归还时第 3 个应被丢弃
        let c1 = pool.acquire().unwrap();
        let c2 = pool.acquire().unwrap();
        let c3 = pool.acquire().unwrap();
        drop(c1);
        drop(c2);
        drop(c3);

        assert_eq!(pool.idle_count(), 2);

        let _ = std::fs::remove_file(&socket);
    }

    #[test]
    fn test_pool_for_returns_same_instance() {
        let socket = make_temp_socket();
        let a = pool_for(&socket);
        let b = pool_for(&socket);
        assert!(Arc::ptr_eq(&a, &b));
    }
}

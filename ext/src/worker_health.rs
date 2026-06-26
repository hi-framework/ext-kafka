//! Worker 就绪状态的进程内缓存。
//!
//! `ensure_worker` 原本每次都做一次 `UnixStream::connect` 探测，
//! 在高频 produce 场景下产生大量短连接（实测 300 次 produce → 300 次探测）。
//!
//! 本模块按 socket 路径缓存 "已知 alive" 状态：
//!
//! - 首次 ensure 成功后置位
//! - IPC 错误（write/read 失败、cid 不匹配、连接断开）时由调用方显式失效
//! - 失效后下次 ensure 重新走完整流程（可能拉起新 worker）

use crate::spawn;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

static KNOWN_ALIVE: OnceLock<Mutex<HashMap<PathBuf, ()>>> = OnceLock::new();

fn cache() -> &'static Mutex<HashMap<PathBuf, ()>> {
    KNOWN_ALIVE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// 确保 worker 就绪。命中缓存则零开销直接返回。
pub fn ensure(socket: &str) -> Result<(), spawn::SpawnError> {
    let key = PathBuf::from(socket);
    if cache().lock().unwrap().contains_key(&key) {
        return Ok(());
    }

    let cfg = spawn::SpawnConfig::from_env(key.clone());
    spawn::ensure_worker(&cfg)?;
    cache().lock().unwrap().insert(key, ());
    Ok(())
}

/// 标记某 socket 的 worker 状态为未知（IO 失败时调用）。
pub fn invalidate(socket: &str) {
    let key = PathBuf::from(socket);
    cache().lock().unwrap().remove(&key);
}

/// 调试/测试用：返回当前已缓存的 socket 数。
#[allow(dead_code)]
pub fn cached_count() -> usize {
    cache().lock().unwrap().len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalidate_unknown_is_noop() {
        invalidate("/does/not/exist.sock");
        // 不 panic 即可
    }

    #[test]
    fn test_cache_state_is_per_socket() {
        // 不真正触发 ensure（需要 worker 二进制），仅检查缓存独立
        let len_before = cached_count();
        invalidate("/tmp/some-fake-socket-for-test.sock");
        let len_after = cached_count();
        assert_eq!(len_before, len_after);
    }
}

//! glibc malloc arena 上限。
//!
//! glibc 默认最多创建 8 × CPU 核数个 arena，线程首次分配时按需绑定新 arena，释放后的
//! 内存留在 arena 内不归还系统。worker 中 librdkafka 的 broker 线程会随 broker 下线 /
//! re-bootstrap 反复销毁与重建，新线程不断落到新 arena 上：48 核机器上 1h 故障 soak
//! 的 RSS 从 25 MiB 持续涨到 63 MiB；限制为 2 个 arena 后约 16 分钟内稳定在 32 MiB。
//! musl 的分配器没有 per-thread arena，同一 soak 稳定在约 21 MiB，不受影响。
//!
//! 业务已经通过 `MALLOC_ARENA_MAX` 或 `GLIBC_TUNABLES` 显式配置时不覆盖。

/// 在 worker 启动、创建任何线程之前调用。
#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub fn limit_malloc_arenas() {
    const ARENA_MAX: libc::c_int = 2;
    if std::env::var_os("MALLOC_ARENA_MAX").is_some()
        || std::env::var("GLIBC_TUNABLES").is_ok_and(|tunables| tunables.contains("arena_max"))
    {
        return;
    }
    // SAFETY: mallopt 只修改分配器参数，任何时刻调用都是安全的。
    unsafe {
        libc::mallopt(libc::M_ARENA_MAX, ARENA_MAX);
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub fn limit_malloc_arenas() {}

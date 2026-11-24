//! 按进程选择 Rust 全局分配器：exec 出的 worker 进程用 mimalloc，PHP 业务进程保持
//! 系统分配器。
//!
//! musl 的分配器会积极把中等尺寸（≥1 KiB）的空闲内存还给内核。worker 每条消息都有
//! 数 KiB 到数十 KiB 的分配，exec 出的进程堆是空的，容易落进「反复缺页」的慢状态：
//! 16 KiB 单流消费的 worker CPU 从约 162 µs/条升到约 270 µs/条，吞吐下降约 1/3。
//! mimalloc 消除了这种慢状态；但若用于所有加载扩展的进程，每个 PHP 进程会多约 6 MiB
//! 独占内存，所以只在 worker 进程中启用。
//!
//! 判定在 .so 加载时由 `.init_array` 构造函数完成，早于任何 Rust 分配，因此一个进程内
//! 只会使用一种分配器，不存在跨分配器释放。fork 回退路径（exec 不可用）沿用父进程的
//! 系统分配器。librdkafka 等 C 代码不经过这里，始终使用 libc malloc。

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, Ordering};

/// 与 [`crate::worker_entry::EXEC_SOCKET_ENV`] 相同；构造函数里不能分配，所以单独
/// 保存一份 C 字符串。
const WORKER_ENV: &std::ffi::CStr = c"HI_KAFKA_INTERNAL_WORKER_SOCKET";

static WORKER_PROCESS: AtomicBool = AtomicBool::new(false);

pub struct ProcessAllocator;

#[inline]
fn worker_process() -> bool {
    WORKER_PROCESS.load(Ordering::Relaxed)
}

unsafe impl GlobalAlloc for ProcessAllocator {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if worker_process() {
            unsafe { mimalloc::MiMalloc.alloc(layout) }
        } else {
            unsafe { System.alloc(layout) }
        }
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if worker_process() {
            unsafe { mimalloc::MiMalloc.alloc_zeroed(layout) }
        } else {
            unsafe { System.alloc_zeroed(layout) }
        }
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if worker_process() {
            unsafe { mimalloc::MiMalloc.dealloc(ptr, layout) }
        } else {
            unsafe { System.dealloc(ptr, layout) }
        }
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if worker_process() {
            unsafe { mimalloc::MiMalloc.realloc(ptr, layout, new_size) }
        } else {
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }
}

/// .so 加载时运行：只读环境变量，不分配内存。
extern "C" fn detect_worker_process() {
    if !unsafe { libc::getenv(WORKER_ENV.as_ptr()) }.is_null() {
        WORKER_PROCESS.store(true, Ordering::Relaxed);
    }
}

#[used]
#[cfg_attr(target_os = "linux", unsafe(link_section = ".init_array"))]
#[cfg_attr(target_os = "macos", unsafe(link_section = "__DATA,__mod_init_func"))]
static DETECT_WORKER_PROCESS: extern "C" fn() = detect_worker_process;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_env_matches_exec_env() {
        assert_eq!(
            WORKER_ENV.to_str().unwrap(),
            crate::worker_entry::EXEC_SOCKET_ENV
        );
    }

    #[test]
    fn non_worker_process_uses_system_allocator() {
        // 测试进程没有设置 worker 环境变量。
        assert!(!worker_process());
        let mut v = vec![0u8; 64 * 1024];
        v[0] = 1;
        assert_eq!(v.iter().map(|&b| b as usize).sum::<usize>(), 1);
    }
}

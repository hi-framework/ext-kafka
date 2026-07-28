//! 进程退出（MSHUTDOWN）时的 worker 主动退出协调。
//!
//! PHP 进程退出（CLI 脚本结束 / FPM worker 回收）时，扩展主动通知它用过的每个
//! worker「我走了」（[`hi_kafka_proto::FrameType::Goodbye`]），让 worker 在最后一个
//! 使用者离开时**立即**自退，而不必干等 idle 超时（默认 5min）。worker 是节点级
//! 共享单例，所以这里只发告别、由 worker 自己判定「是否最后一个连接」——单进程退出
//! 不会误杀仍被其它进程使用的 worker。通知失败 / worker 已死 → 自动退化回 idle 自退。
//!
//! ## 健壮性约束（这条路径跑在 PHP 进程拆除期，必须极度保守）
//!
//! - **绝不 panic**：调用方（`lib.rs::module_shutdown`）用 `catch_unwind` 兜底；本模块
//!   内部全程 best-effort，不 `unwrap` 任何可能失败的 IO。
//! - **绝不 spawn**：只 `UnixStream::connect` 现有 socket，连不上即放弃（worker 已不在，
//!   本就无需清理）。**不**走 `ipc::ensure` / `spawn`——进程退出期 fork 既危险又无意义。
//! - **有界**：connect 对 UDS 近乎即时；读写各设 300ms 超时，worker 卡住也不拖慢退出。
//! - **不依赖 PHP**：纯 Rust + UDS，不调任何 PHP API。

use crate::pool;
use crate::protocol;
use hi_kafka_proto::HEADER_LEN;
use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

/// 单次 IO 的超时上限。worker 正常时这些操作是亚毫秒级；设上限只为防 worker 卡死
/// 时拖慢 PHP 进程退出。
const IO_TIMEOUT: Duration = Duration::from_millis(300);

/// MSHUTDOWN 钩子主体。遍历本进程用过的每个 socket，逐个做 best-effort 告别清理。
pub fn on_module_shutdown() {
    for socket in collect_sockets() {
        cleanup_socket(&socket);
    }
}

/// 本进程「用过」的所有 socket：连接池路径 ∪ 订阅注册表路径 ∪ 曾 ensure 过的 socket。
/// 阻塞 Client 走前两者；**Swoole/Swow 协程 driver 不进 Rust 连接池，只在第三者里**。
/// 从没碰过 Kafka 的进程三者皆空 → 整个 MSHUTDOWN 无操作（它本就不是任何 worker 的使用者）。
fn collect_sockets() -> Vec<String> {
    let mut set = BTreeSet::new();
    for path in pool::socket_paths() {
        set.insert(path.to_string_lossy().to_string());
    }
    // 曾 ensure 过 worker 但不走 Rust 连接池的路径（Swoole/Swow driver）：
    // 它们的 socket 只在 worker_health 里有记录。
    for s in crate::worker_health::ever_ensured_sockets() {
        set.insert(s);
    }
    set.into_iter().collect()
}

/// 对单个 socket 的告别清理：关空闲 RPC 连接 → 直连握手 → 发 Goodbye。
fn cleanup_socket(socket: &str) {
    // 1. 关本进程在该 socket 的空闲池连接。否则它们在进程真正退出前仍 open，
    //    会让 worker 误以为还有使用者、迟迟判不出「最后一个连接关闭」。
    pool::close_idle(Path::new(socket));

    // 2. 直连 + 握手（绝不 spawn）。ConsumerStream objects own and close
    // their leases directly; module shutdown only coordinates the RPC owner.
    let mut stream = match connect_and_handshake(socket) {
        Some(s) => s,
        None => return,
    };

    // 3. Goodbye（fire-and-forget）。worker 收到后，待本连接关闭即判定是否最后一个。
    if let Ok(frame) = protocol::build_goodbye_frame() {
        let _ = write_all(&mut stream, &frame);
    }
    // drop(stream)：连接立即关闭 → worker 端 EOF → 触发「最后一个连接关闭」判定。
}

/// 直连 socket 并完成 HELLO 握手。任一步失败返回 None（视作 worker 不可用）。
/// **不** spawn、**不** 复用池连接（池连接随后会被进程退出关掉，这里要一个独立、
/// 发完即关的连接，使 worker 能干净捕捉到它的关闭）。
fn connect_and_handshake(socket: &str) -> Option<UnixStream> {
    let mut stream = UnixStream::connect(socket).ok()?;
    stream.set_read_timeout(Some(IO_TIMEOUT)).ok()?;
    stream.set_write_timeout(Some(IO_TIMEOUT)).ok()?;

    let hello = protocol::build_hello_frame().ok()?;
    write_all(&mut stream, &hello).ok()?;

    // 读 HELLO RESP：13B header + 小 payload。
    let mut header = [0u8; HEADER_LEN];
    stream.read_exact(&mut header).ok()?;
    let payload_len = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
    // 防御：HELLO RESP payload 极小（1B major）。异常大 → 协议错乱，放弃。
    if payload_len > 64 {
        return None;
    }
    let mut payload = vec![0u8; payload_len];
    stream.read_exact(&mut payload).ok()?;

    let mut full = Vec::with_capacity(HEADER_LEN + payload_len);
    full.extend_from_slice(&header);
    full.extend_from_slice(&payload);
    protocol::parse_hello_resp(&full).ok()?;
    Some(stream)
}

fn write_all(stream: &mut UnixStream, buf: &[u8]) -> std::io::Result<()> {
    stream.write_all(buf)?;
    stream.flush()
}

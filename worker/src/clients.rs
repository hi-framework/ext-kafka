//! 连接登记与 GOODBYE 协调。
//!
//! 按对端 PID（`SO_PEERCRED` / `LOCAL_PEERPID`）记录当前连接，用来回答「告别的
//! 进程是不是 worker 最后的使用者」：
//!
//! - 是：worker 很可能随它一起结束（容器 1 号进程退出时内核会杀掉整个 PID 命名空间），
//!   先把已收下的消息发往 broker 再应答，fire-and-forget 消息才不会丢。
//! - 否：其它进程还在用 worker，它会继续运行并发送消息，立即应答，不拖慢退出
//!   （FPM 子进程回收、Swoole worker 重启都走这里）。

use crate::producer::ProducerHandle;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tracing::debug;

/// 告别进程自己的其它连接已由客户端关闭，worker 只需读完其中剩余的帧；等待它们结束
/// 的上限，防止客户端仍持有连接时把整个等待预算耗在这里。
const OWN_CONNECTIONS_SETTLE: Duration = Duration::from_secs(1);

pub struct Clients {
    /// 收到 GOODBYE 后置位、有新连接接入时复位，供主循环判定「最后的使用者已离开」。
    pub expedited: AtomicBool,
    /// GOODBYE 等待发送完成的上限（取客户端请求值与 worker drain 超时中的较小者）。
    flush_cap: Duration,
    next_id: AtomicU64,
    /// 连接 id → 对端 PID（无法获知时为 `None`）。
    peers: Mutex<HashMap<u64, Option<i32>>>,
    changed: Notify,
}

/// 一条已登记的连接；drop 时注销。
pub struct Connection {
    clients: Arc<Clients>,
    id: u64,
}

impl Clients {
    pub fn new(flush_cap: Duration) -> Arc<Self> {
        Arc::new(Self {
            expedited: AtomicBool::new(false),
            flush_cap,
            next_id: AtomicU64::new(0),
            peers: Mutex::new(HashMap::new()),
            changed: Notify::new(),
        })
    }

    pub fn register(self: &Arc<Self>, peer_pid: Option<i32>) -> Connection {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.peers().insert(id, peer_pid);
        Connection {
            clients: self.clone(),
            id,
        }
    }

    fn peers(&self) -> std::sync::MutexGuard<'_, HashMap<u64, Option<i32>>> {
        self.peers.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.clients.peers().remove(&self.id);
        self.clients.changed.notify_waiters();
    }
}

impl Connection {
    pub fn clients(&self) -> &Clients {
        &self.clients
    }

    /// 处理带 `flush_wait_ms` 的 GOODBYE：本连接所属进程是最后的使用者时，先 flush
    /// producer 再返回 `true`；还有其它进程连着 worker 时立即返回 `false`。
    pub async fn settle_goodbye(&self, wait: Duration, producer: &ProducerHandle) -> bool {
        let clients = &self.clients;
        let start = tokio::time::Instant::now();
        let deadline = start + wait.min(clients.flush_cap);
        let Some(Some(pid)) = clients.peers().get(&self.id).copied() else {
            return false; // 无法识别对端进程，也就无法判断是否最后的使用者
        };
        let settle_deadline = deadline.min(start + OWN_CONNECTIONS_SETTLE);
        loop {
            // 先创建 notified 再检查：之后的 notify_waiters 一定能唤醒它。
            let changed = clients.changed.notified();
            let (own, others) = {
                let peers = clients.peers();
                peers.iter().filter(|(id, _)| **id != self.id).fold(
                    (0, 0),
                    |(own, others), (_, peer)| {
                        if *peer == Some(pid) {
                            (own + 1, others)
                        } else {
                            (own, others + 1)
                        }
                    },
                )
            };
            if others > 0 {
                debug!(pid, others, "goodbye: other processes still connected");
                return false;
            }
            if own == 0 {
                break;
            }
            if tokio::time::timeout_at(settle_deadline, changed)
                .await
                .is_err()
            {
                break; // 客户端仍持有自己的连接：照样 flush，worker 退出前它们也会结束
            }
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if !remaining.is_zero() {
            let _ = producer.flush(remaining).await;
        }
        debug!(pid, elapsed = ?start.elapsed(), "goodbye: last client, producer flushed");
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::producer::Producer;
    use hi_kafka_proto::{ProduceFnf, ProduceResp};
    use std::sync::atomic::AtomicUsize;

    #[derive(Default)]
    struct CountingProducer {
        flushes: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl Producer for CountingProducer {
        async fn produce_fnf(&self, _msg: ProduceFnf) -> anyhow::Result<()> {
            Ok(())
        }
        async fn produce_ack(&self, _msg: ProduceFnf, _t: Duration) -> anyhow::Result<ProduceResp> {
            unreachable!()
        }
        async fn flush(&self, _timeout: Duration) -> anyhow::Result<()> {
            self.flushes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn producer() -> (Arc<CountingProducer>, ProducerHandle) {
        let p = Arc::new(CountingProducer::default());
        (p.clone(), p)
    }

    #[tokio::test]
    async fn last_client_flushes() {
        let clients = Clients::new(Duration::from_secs(5));
        let (counter, handle) = producer();
        let goodbye = clients.register(Some(10));
        assert!(
            goodbye
                .settle_goodbye(Duration::from_secs(5), &handle)
                .await
        );
        assert_eq!(counter.flushes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn other_process_skips_flush() {
        let clients = Clients::new(Duration::from_secs(5));
        let (counter, handle) = producer();
        let _other = clients.register(Some(11));
        let goodbye = clients.register(Some(10));
        let start = std::time::Instant::now();
        assert!(
            !goodbye
                .settle_goodbye(Duration::from_secs(5), &handle)
                .await
        );
        assert!(start.elapsed() < Duration::from_millis(100));
        assert_eq!(counter.flushes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn waits_for_own_connections_to_close() {
        let clients = Clients::new(Duration::from_secs(5));
        let (counter, handle) = producer();
        let own = clients.register(Some(10));
        let goodbye = clients.register(Some(10));
        let closer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(own);
        });
        let start = std::time::Instant::now();
        assert!(
            goodbye
                .settle_goodbye(Duration::from_secs(5), &handle)
                .await
        );
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(40) && elapsed < OWN_CONNECTIONS_SETTLE);
        assert_eq!(counter.flushes.load(Ordering::SeqCst), 1);
        closer.await.unwrap();
    }

    #[tokio::test]
    async fn unknown_peer_skips_flush() {
        let clients = Clients::new(Duration::from_secs(5));
        let (counter, handle) = producer();
        let goodbye = clients.register(None);
        assert!(
            !goodbye
                .settle_goodbye(Duration::from_secs(5), &handle)
                .await
        );
        assert_eq!(counter.flushes.load(Ordering::SeqCst), 0);
    }
}

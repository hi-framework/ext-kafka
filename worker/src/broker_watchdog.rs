//! librdkafka「broker 列表被清空后永久停摆」的兜底。
//!
//! librdkafka 2.10+（至少到 2.16.0）收到成功的 Metadata 响应后，会移除所有不在响应
//! 中的 broker（`rd_kafka_metadata_decommission_unavailable_brokers`），只有
//! `broker_cnt == 0 && topic_cnt == 0` 时才把响应当作无效。被 fence 的 broker（单节点
//! KRaft 暂停超过 `broker.session.timeout.ms`、集群全量重启中）对按 topic 的 Metadata
//! 请求返回「0 个 broker + leader 为 -1 的 topic」，客户端于是移除包括当前连接在内的
//! 全部 broker。此后：
//! - 没有 broker 可以发送 Metadata 请求，re-bootstrap 计时器只在发送请求时启动；
//! - 没有 broker 的状态变化，ALL_BROKERS_DOWN 也不会触发。
//!
//! 被移除的 broker 若仍被分区 leader / group coordinator 引用，会以 `INIT` 状态留在
//! broker 列表中（已从按 id 的索引删除、线程已退出，永远不会再连接）。
//!
//! 客户端从此不再连接任何 broker，集群恢复后也不会自愈，共享该 worker 的所有 PHP
//! 进程静默停摆（生产 TIMEOUT、消费无消息）。
//!
//! 兜底：通过统计回调观察 broker 列表，连续 [`EMPTY_GRACE`] 没有任何存活 broker
//! （configured / learned 且不处于 `INIT`）时，用公开 API `rd_kafka_brokers_add`
//! 重新加入 `bootstrap.servers`——与 librdkafka 自身 re-bootstrap 的做法相同；随后的
//! Metadata 响应会重建 learned broker。真实故障中 broker 处于 CONNECT / DOWN /
//! TRY_CONNECT 而非 INIT，不会触发。

use rdkafka::statistics::Statistics;
use rdkafka::types::RDKafka;
use std::ffi::CString;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// 统计回调由 `cluster::CLIENT_DEFAULTS` 中的 `statistics.interval.ms` 驱动；业务显式
/// 设为 0 即关闭兜底。
///
/// broker 列表持续为空超过该时长才重新 bootstrap，之后同样按该间隔重试。
const EMPTY_GRACE: Duration = Duration::from_secs(10);

pub struct BrokerWatchdog {
    client: AtomicPtr<RDKafka>,
    bootstrap: Option<CString>,
    empty_since: Mutex<Option<Instant>>,
}

impl BrokerWatchdog {
    pub fn new(bootstrap_servers: Option<&str>) -> Self {
        Self {
            client: AtomicPtr::new(std::ptr::null_mut()),
            bootstrap: bootstrap_servers.and_then(|servers| CString::new(servers).ok()),
            empty_since: Mutex::new(None),
        }
    }

    /// 客户端创建后登记其句柄。统计回调只在客户端存活期间触发，句柄在回调内始终有效。
    pub fn attach(&self, client: *mut RDKafka) {
        self.client.store(client, Ordering::Release);
    }

    pub fn observe(&self, statistics: &Statistics) {
        let live_brokers = statistics
            .brokers
            .values()
            .filter(|broker| {
                (broker.source == "configured" || broker.source == "learned")
                    && broker.state != "INIT"
            })
            .count();
        debug!(
            client = %statistics.name,
            live_brokers,
            brokers = ?statistics
                .brokers
                .values()
                .map(|b| format!("{}/{}/{}", b.name, b.source, b.state))
                .collect::<Vec<_>>(),
            "broker watchdog sample"
        );
        if self.should_rebootstrap(live_brokers, Instant::now()) {
            self.rebootstrap(&statistics.name);
        }
    }

    fn should_rebootstrap(&self, live_brokers: usize, now: Instant) -> bool {
        let mut empty_since = self.empty_since.lock().unwrap_or_else(|e| e.into_inner());
        if live_brokers > 0 {
            *empty_since = None;
            return false;
        }
        match *empty_since {
            None => {
                *empty_since = Some(now);
                false
            }
            Some(since) if now.duration_since(since) >= EMPTY_GRACE => {
                *empty_since = Some(now);
                true
            }
            Some(_) => false,
        }
    }

    fn rebootstrap(&self, client_name: &str) {
        let client = self.client.load(Ordering::Acquire);
        let Some(bootstrap) = self.bootstrap.as_ref() else {
            return;
        };
        if client.is_null() {
            return;
        }
        // SAFETY: client 是仍然存活的 rd_kafka_t（见 attach）；brokerlist 是有效的 C 字符串。
        let added = unsafe { rdkafka::bindings::rd_kafka_brokers_add(client, bootstrap.as_ptr()) };
        warn!(
            client = client_name,
            bootstrap = ?bootstrap,
            added,
            "librdkafka has no live brokers left (removed by a broker-less metadata response); re-adding bootstrap servers"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rebootstraps_only_after_grace_and_then_at_most_once_per_grace() {
        let watchdog = BrokerWatchdog::new(Some("127.0.0.1:9092"));
        let start = Instant::now();
        assert!(!watchdog.should_rebootstrap(0, start));
        assert!(!watchdog.should_rebootstrap(0, start + Duration::from_secs(5)));
        assert!(watchdog.should_rebootstrap(0, start + EMPTY_GRACE));
        assert!(!watchdog.should_rebootstrap(0, start + EMPTY_GRACE + Duration::from_secs(5)));
        assert!(watchdog.should_rebootstrap(0, start + EMPTY_GRACE * 2));
    }

    #[test]
    fn any_live_broker_resets_the_timer() {
        let watchdog = BrokerWatchdog::new(Some("127.0.0.1:9092"));
        let start = Instant::now();
        assert!(!watchdog.should_rebootstrap(0, start));
        assert!(!watchdog.should_rebootstrap(1, start + Duration::from_secs(9)));
        assert!(!watchdog.should_rebootstrap(0, start + EMPTY_GRACE));
        assert!(watchdog.should_rebootstrap(0, start + EMPTY_GRACE * 2));
    }
}

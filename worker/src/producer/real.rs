//! 真实 rdkafka 生产者。仅在 `kafka` feature 开启时编译。

use crate::cluster::{ClusterRegistryHandle, OAuthTokenSlot};
use crate::consumer::GroupMetadataHandle;
use crate::error::WorkerError;
use crate::producer::Producer;
use anyhow::Context;
use hi_kafka_proto::{DeliveryAck, ErrorKind, OffsetCommit, ProduceFnf, ProduceResp};
use rdkafka::client::{ClientContext, OAuthToken};
use rdkafka::consumer::ConsumerGroupMetadata;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header as RdHeader, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer as _};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::ClientConfig;
use rdkafka::Offset;

/// Producer 端的自定义 ClientContext，只为提供 OAuth token 回调。
/// 不持有任何 producer 相关状态——FutureProducer 自己包装我们这个 ctx
/// 来挂 delivery callback，这里只补 oauth 一项。
struct ProducerOAuthCtx {
    cluster: String,
    oauth_slot: OAuthTokenSlot,
}

impl ClientContext for ProducerOAuthCtx {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn std::error::Error>> {
        let slot = self.oauth_slot.lock().unwrap_or_else(|e| e.into_inner());
        match slot.as_ref() {
            Some(t) => Ok(OAuthToken {
                token: t.token_value.clone(),
                principal_name: t.principal_name.clone(),
                lifetime_ms: t.lifetime_ms,
            }),
            None => Err(format!(
                "no OAuth token set for cluster '{}'; call setOAuthBearerToken first",
                self.cluster
            )
            .into()),
        }
    }
}
use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, OnceCell, RwLock};
use tracing::{debug, info, warn};

/// FutureProducer 必须以 cluster 名 + token slot 为 ctx 创建，
/// 类型别名让 ClusterEntry / Producer impl 引用更简洁。
type OAuthFutureProducer = FutureProducer<ProducerOAuthCtx>;

const TXN_IDLE: u8 = 0;
const TXN_TRANSITIONING: u8 = 1;
const TXN_ACTIVE: u8 = 2;

/// RAII 事务状态守卫：在 `Drop` 时把 `txn_state` 回滚到 `revert_to`，除非 `commit()`
/// 明确把它设成 `None`。用于 cancel-safe 保证——如果 async task 在 CAS 之后被 cancel
/// 或 panic（tokio drop future），状态自动回滚，避免 `TRANSITIONING` 永远卡死。
struct TxnStateGuard<'a> {
    state: &'a AtomicU8,
    revert_to: Option<u8>,
}

impl<'a> TxnStateGuard<'a> {
    fn new(state: &'a AtomicU8, revert_to: u8) -> Self {
        Self {
            state,
            revert_to: Some(revert_to),
        }
    }

    /// 操作成功后调用：让 Drop 不再回滚，同时把最终状态 store 进去。
    fn commit(mut self, final_state: u8) {
        self.state.store(final_state, Ordering::Release);
        self.revert_to = None;
    }
}

impl Drop for TxnStateGuard<'_> {
    fn drop(&mut self) {
        if let Some(v) = self.revert_to {
            self.state.store(v, Ordering::Release);
        }
    }
}

/// 单 cluster 的状态：producer + 事务互斥锁。
struct ClusterEntry {
    producer: Arc<OAuthFutureProducer>,
    /// 是否启用事务（cluster 配置含 `transactional.id`）
    transactional: bool,
    /// 事务操作串行（同集群同时仅一个 active tx）
    txn_lock: Arc<Mutex<()>>,
    /// 事务状态：idle / 状态切换中 / active。
    txn_state: AtomicU8,
    /// 创建时记录的 ClusterRegistry 版本号；register 覆盖后会比对，stale 触发 rebuild
    config_version: u64,
}

/// 每个 cluster 一个 OnceCell + 当前 config_version。
/// OnceCell 保证并发 produce_fnf/ack 只触发一次 `create_with_context` + `init_transactions`。
/// 当 ClusterRegistry 的 version 比 cell 里的 entry.config_version 新 → 把 cell 整体替换。
struct ClusterCell {
    cell: OnceCell<Arc<ClusterEntry>>,
}

impl ClusterCell {
    fn new() -> Self {
        Self {
            cell: OnceCell::new(),
        }
    }
}

pub struct KafkaProducer {
    registry: ClusterRegistryHandle,
    /// 每个 cluster 名一个 cell。外层 RwLock 仅保护 HashMap 的 entry-set 操作，
    /// 真正的 producer 初始化由内层 OnceCell 串行（不阻塞其它 cluster）。
    producers: RwLock<HashMap<String, Arc<ClusterCell>>>,
}

impl KafkaProducer {
    pub fn new(registry: ClusterRegistryHandle) -> Self {
        Self {
            registry,
            producers: RwLock::new(HashMap::new()),
        }
    }

    /// 拿到或建一个 cluster cell（外层 RwLock，快路径走读锁）。
    async fn cell_for(&self, cluster: &str) -> Arc<ClusterCell> {
        if let Some(c) = self.producers.read().await.get(cluster) {
            return c.clone();
        }
        let mut guard = self.producers.write().await;
        guard
            .entry(cluster.to_string())
            .or_insert_with(|| Arc::new(ClusterCell::new()))
            .clone()
    }

    /// P1 #6 修：用 tokio `OnceCell` per cluster 保证并发 produce 时只会触发
    /// 一次 `create_with_context + init_transactions`。原先 RwLock 双检查在
    /// 中间 await 期间释放，并发能撞两次 init_transactions，事务 id fencing 出错。
    ///
    /// P1 #7：拿到 cell 后再校验 ClusterRegistry 当前 version；如果 entry 是
    /// 老 config 编出来的，丢掉重建（覆盖了 broker 地址也能即时生效）。
    async fn entry_for(&self, cluster: &str) -> anyhow::Result<Arc<ClusterEntry>> {
        loop {
            let cell = self.cell_for(cluster).await;

            // OnceCell 串行化初始化；失败不缓存，下一次重试
            let entry = cell
                .cell
                .get_or_try_init(|| async { self.build_entry(cluster).await })
                .await?
                .clone();

            // 校验 version：若 registry 比 entry 更新 → 已被 register 覆盖，
            // 把这个 cluster 的 cell 整体换掉重建
            let (_cfg, current_version) = self
                .registry
                .get_with_version(cluster)
                .await
                .with_context(|| format!("cluster '{cluster}' not registered"))?;
            if entry.config_version == current_version {
                return Ok(entry);
            }
            warn!(
                %cluster,
                entry_version = entry.config_version,
                current_version,
                "producer cache stale (config overwritten), rebuilding"
            );
            // 替换 cell，下一轮 loop 重新初始化
            let mut guard = self.producers.write().await;
            guard.insert(cluster.to_string(), Arc::new(ClusterCell::new()));
        }
    }

    /// 真正构建 ClusterEntry —— 由 OnceCell 串行化调用，同集群内只跑一次。
    async fn build_entry(&self, cluster: &str) -> anyhow::Result<Arc<ClusterEntry>> {
        let (cluster_cfg, config_version) = match self.registry.get_with_version(cluster).await {
            Some(v) => v,
            None => {
                return Err(anyhow::Error::new(WorkerError::new(
                    ErrorKind::ClusterNotRegistered,
                    format!("cluster '{cluster}' not registered (call registerCluster first)"),
                )))
            }
        };

        let transactional = cluster_cfg.contains_key("transactional.id");

        let mut cfg = ClientConfig::new();
        // 默认压缩 lz4：所有 broker 版本（≥0.8.2）都支持，速度/压缩比平衡好。
        // 业务侧若显式给了 `compression.codec` / `compression.type` 走自己的值。
        if !cluster_cfg.contains_key("compression.codec")
            && !cluster_cfg.contains_key("compression.type")
        {
            cfg.set("compression.codec", "lz4");
        }
        for (k, v) in &cluster_cfg {
            cfg.set(k, v);
        }

        let oauth_slot = self
            .registry
            .token_slot_for(cluster)
            .await
            .with_context(|| format!("token slot for cluster '{cluster}'"))?;
        let ctx = ProducerOAuthCtx {
            cluster: cluster.to_string(),
            oauth_slot,
        };
        let producer: OAuthFutureProducer = cfg
            .create_with_context(ctx)
            .with_context(|| format!("create FutureProducer for cluster '{cluster}'"))?;

        // 事务模式：必须 init_transactions（阻塞 rdkafka 调用 → spawn_blocking）
        if transactional {
            info!(%cluster, "init_transactions starting");
            let p = producer.clone();
            tokio::task::spawn_blocking(move || p.init_transactions(Duration::from_secs(30)))
                .await
                .context("spawn_blocking init_transactions")?
                .with_context(|| format!("init_transactions for cluster '{cluster}'"))?;
            info!(%cluster, "init_transactions ok");
        }

        Ok(Arc::new(ClusterEntry {
            producer: Arc::new(producer),
            transactional,
            txn_lock: Arc::new(Mutex::new(())),
            txn_state: AtomicU8::new(TXN_IDLE),
            config_version,
        }))
    }
}

fn ensure_txn_active(entry: &ClusterEntry, cluster: &str, op: &str) -> anyhow::Result<()> {
    match entry.txn_state.load(Ordering::Acquire) {
        TXN_ACTIVE => Ok(()),
        TXN_TRANSITIONING => Err(anyhow::Error::new(WorkerError::new(
            ErrorKind::TxnState,
            format!("cluster '{cluster}' transaction is transitioning; cannot {op}"),
        ))),
        _ => Err(anyhow::Error::new(WorkerError::new(
            ErrorKind::TxnState,
            format!("cluster '{cluster}' has no active transaction for {op}"),
        ))),
    }
}

#[async_trait::async_trait]
impl Producer for KafkaProducer {
    async fn produce_fnf(&self, msg: ProduceFnf) -> anyhow::Result<()> {
        let entry = self.entry_for(&msg.cluster).await?;
        // FNF 与事务语义冲突：FNF 语义"入队即算成功"，但事务 producer 里入队
        // 的消息会随事务 commit / abort 决定最终是否可见。业务侧收到 FNF Ok
        // 之后若事务 abort，消息其实被丢弃——认知偏差。
        // 事务 cluster 必须走 produceSync 才能保证 delivery 语义与事务对齐。
        if entry.transactional {
            return Err(anyhow::Error::new(WorkerError::new(
                ErrorKind::InvalidArgument,
                format!(
                    "cluster '{}' is transactional; produceFnf is not supported, use produceSync for atomic delivery",
                    msg.cluster
                ),
            )));
        }
        let producer = entry.producer.clone();
        let value_slice: &[u8] = msg.value.as_ref();
        let key_slice: &[u8] = msg.key.as_ref();
        let mut record = FutureRecord::to(&msg.topic)
            .payload(value_slice)
            .key(key_slice);

        if msg.partition >= 0 {
            record = record.partition(msg.partition);
        }
        if msg.timestamp_ms >= 0 {
            record = record.timestamp(msg.timestamp_ms);
        }

        let headers = build_headers(&msg.headers);
        if let Some(h) = &headers {
            record = record.headers(h.clone());
        }

        // FNF 路径只做本地 enqueue：成功表示 librdkafka 已接受进本地队列，
        // 不等待 broker delivery report。队列满 / 参数非法等同步错误仍返回业务。
        match producer.send_result(record) {
            Ok(_delivery_future) => {
                debug!(cluster = %msg.cluster, topic = %msg.topic, "PRODUCE_FNF enqueued");
                Ok(())
            }
            Err((err, _msg)) => {
                warn!(cluster = %msg.cluster, topic = %msg.topic, error = ?err, "PRODUCE_FNF enqueue failed");
                Err(anyhow::Error::new(kafka_error_to_worker(&err)))
            }
        }
    }

    async fn flush(&self, timeout: Duration) -> anyhow::Result<()> {
        // 只 flush 已经初始化好的 cell（OnceCell::get 不触发 init）
        let producers: Vec<Arc<OAuthFutureProducer>> = {
            let guard = self.producers.read().await;
            guard
                .values()
                .filter_map(|cell| cell.cell.get().map(|e| e.producer.clone()))
                .collect()
        };
        for p in producers {
            let p_clone = p.clone();
            let result = tokio::task::spawn_blocking(move || p_clone.flush(timeout))
                .await
                .context("spawn_blocking flush")?;
            if let Err(e) = result {
                warn!(error = ?e, "rdkafka flush failed");
            }
        }
        Ok(())
    }

    async fn begin_transaction(&self, cluster: &str) -> anyhow::Result<()> {
        let entry = self.entry_for(cluster).await?;
        if !entry.transactional {
            return Err(anyhow::Error::new(WorkerError::new(
                ErrorKind::NotTransactional,
                format!("cluster '{cluster}' has no transactional.id; cannot begin transaction"),
            )));
        }
        if entry
            .txn_state
            .compare_exchange(
                TXN_IDLE,
                TXN_TRANSITIONING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Err(anyhow::Error::new(WorkerError::new(
                ErrorKind::TxnState,
                format!("cluster '{cluster}' already has an active transaction"),
            )));
        }
        // 从这里起若中途 return / panic / cancel，guard 会回滚到 IDLE
        let guard = TxnStateGuard::new(&entry.txn_state, TXN_IDLE);
        let _lock = entry.txn_lock.lock().await;
        let p = entry.producer.clone();
        tokio::task::spawn_blocking(move || p.begin_transaction())
            .await
            .context("spawn_blocking begin_transaction")?
            .with_context(|| format!("begin_transaction for cluster '{cluster}'"))?;
        guard.commit(TXN_ACTIVE);
        debug!(%cluster, "begin_transaction ok");
        Ok(())
    }

    async fn commit_transaction(&self, cluster: &str) -> anyhow::Result<()> {
        let entry = self.entry_for(cluster).await?;
        if !entry.transactional {
            return Err(anyhow::Error::new(WorkerError::new(
                ErrorKind::NotTransactional,
                format!("cluster '{cluster}' is not transactional"),
            )));
        }
        if entry
            .txn_state
            .compare_exchange(
                TXN_ACTIVE,
                TXN_TRANSITIONING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Err(anyhow::Error::new(WorkerError::new(
                ErrorKind::TxnState,
                format!("cluster '{cluster}' has no active transaction to commit"),
            )));
        }
        // commit 失败回到 ACTIVE（事务仍在，业务侧可选择 abort 或重试 commit）
        let guard = TxnStateGuard::new(&entry.txn_state, TXN_ACTIVE);
        let _lock = entry.txn_lock.lock().await;
        let p = entry.producer.clone();
        tokio::task::spawn_blocking(move || p.commit_transaction(Duration::from_secs(30)))
            .await
            .context("spawn_blocking commit_transaction")?
            .with_context(|| format!("commit_transaction for cluster '{cluster}'"))?;
        guard.commit(TXN_IDLE);
        debug!(%cluster, "commit_transaction ok");
        Ok(())
    }

    async fn abort_transaction(&self, cluster: &str) -> anyhow::Result<()> {
        let entry = self.entry_for(cluster).await?;
        if !entry.transactional {
            return Err(anyhow::Error::new(WorkerError::new(
                ErrorKind::NotTransactional,
                format!("cluster '{cluster}' is not transactional"),
            )));
        }
        if entry
            .txn_state
            .compare_exchange(
                TXN_ACTIVE,
                TXN_TRANSITIONING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Err(anyhow::Error::new(WorkerError::new(
                ErrorKind::TxnState,
                format!("cluster '{cluster}' has no active transaction to abort"),
            )));
        }
        // abort 失败回到 ACTIVE（事务未清理干净，业务侧应重试 abort）
        let guard = TxnStateGuard::new(&entry.txn_state, TXN_ACTIVE);
        let _lock = entry.txn_lock.lock().await;
        let p = entry.producer.clone();
        tokio::task::spawn_blocking(move || p.abort_transaction(Duration::from_secs(30)))
            .await
            .context("spawn_blocking abort_transaction")?
            .with_context(|| format!("abort_transaction for cluster '{cluster}'"))?;
        guard.commit(TXN_IDLE);
        debug!(%cluster, "abort_transaction ok");
        Ok(())
    }

    async fn send_offsets_to_transaction(
        &self,
        cluster: &str,
        group_id: &str,
        offsets: Vec<OffsetCommit>,
        metadata: GroupMetadataHandle,
    ) -> anyhow::Result<()> {
        let entry = self.entry_for(cluster).await?;
        if !entry.transactional {
            return Err(anyhow::Error::new(WorkerError::new(
                ErrorKind::NotTransactional,
                format!(
                    "cluster '{cluster}' has no transactional.id; send_offsets_to_transaction requires it"
                ),
            )));
        }
        // 句柄解包：必须是 KafkaConsumer 装进去的 ConsumerGroupMetadata
        let cgm: Box<ConsumerGroupMetadata> = metadata.downcast().map_err(|_| {
            anyhow::anyhow!(
                "group metadata handle is not a rdkafka ConsumerGroupMetadata; \
                 consumer/producer backend mismatch"
            )
        })?;
        let cgm: ConsumerGroupMetadata = *cgm;

        // 构造 TopicPartitionList
        let mut tpl = TopicPartitionList::new();
        for (topic, partition, offset) in &offsets {
            tpl.add_partition_offset(topic, *partition, Offset::Offset(*offset))
                .with_context(|| format!("add {topic}:{partition} offset={offset} to tpl"))?;
        }

        let offsets_count = offsets.len();
        let p = entry.producer.clone();
        let group_id_owned = group_id.to_string();
        let cluster_owned = cluster.to_string();

        // send_offsets_to_transaction 同集群同事务串行
        let _guard = entry.txn_lock.lock().await;
        ensure_txn_active(&entry, cluster, "send_offsets_to_transaction")?;

        tokio::task::spawn_blocking(move || {
            p.send_offsets_to_transaction(&tpl, &cgm, Duration::from_secs(30))
        })
        .await
        .context("spawn_blocking send_offsets_to_transaction")?
        .with_context(|| {
            format!(
                "send_offsets_to_transaction cluster='{cluster_owned}' group='{group_id_owned}' \
                 offsets={offsets_count}"
            )
        })?;
        info!(%cluster, %group_id, offsets_count, "send_offsets_to_transaction ok");
        Ok(())
    }

    /// # 事务锁范围 vs 吞吐权衡
    ///
    /// 事务 cluster 上，`txn_guard` 覆盖**整条** send + delivery report 等待路径
    /// （直到 timeout 或投递完成才释放锁）。这带来两个含义：
    ///
    /// - **正确**：事务内消息严格顺序、原子提交语义与业务侧 `beginTransaction ...
    ///   produceSync ... commit` 的操作顺序对齐。
    /// - **吞吐上限**：**单事务并发 produceSync 完全串行**，broker 慢时一条消息卡住
    ///   5s 会阻塞后续 99 条。事务 producer 吞吐 ≈ 1 / 单消息 P99 延迟。业务侧需要
    ///   在事务外做 batching（一批 events 走一个事务 + 多次 produceSync），或对
    ///   高吞吐场景干脆用**非事务** producer。
    ///
    /// 非事务 cluster 无 `txn_guard`（`entry.transactional = false` 走 None 分支），
    /// 并发 `produceSync` 完全并行，只受 rdkafka 内部批处理约束。
    async fn produce_ack(&self, msg: ProduceFnf, timeout: Duration) -> anyhow::Result<ProduceResp> {
        let entry = self.entry_for(&msg.cluster).await?;
        let txn_guard = if entry.transactional {
            let guard = entry.txn_lock.lock().await;
            ensure_txn_active(&entry, &msg.cluster, "produce")?;
            Some(guard)
        } else {
            None
        };
        let producer = entry.producer.clone();
        let value_slice: &[u8] = msg.value.as_ref();
        let key_slice: &[u8] = msg.key.as_ref();
        debug!(
            cluster = %msg.cluster,
            topic = %msg.topic,
            headers_count = msg.headers.len(),
            "PRODUCE_REQ inbound"
        );
        let mut record = FutureRecord::to(&msg.topic)
            .payload(value_slice)
            .key(key_slice);

        if msg.partition >= 0 {
            record = record.partition(msg.partition);
        }
        if msg.timestamp_ms >= 0 {
            record = record.timestamp(msg.timestamp_ms);
        }

        let headers = build_headers(&msg.headers);
        if let Some(h) = &headers {
            record = record.headers(h.clone());
        }

        // `send(..., 0)` 的第二参数只控制本地 enqueue 等待；delivery report 用
        // API timeout 额外包一层，避免 PHP 端先超时而 worker 继续占住连接。
        let delivery = producer.send(record, Duration::from_secs(0));
        match tokio::time::timeout(timeout, delivery).await {
            Ok(Ok(delivery)) => {
                let partition = delivery.partition;
                let offset = delivery.offset;
                debug!(
                    cluster = %msg.cluster,
                    topic = %msg.topic,
                    partition,
                    offset,
                    "PRODUCE_REQ delivered"
                );
                drop(txn_guard);
                Ok(ProduceResp::Ok(DeliveryAck { partition, offset }))
            }
            Ok(Err((err, _msg))) => {
                warn!(
                    cluster = %msg.cluster,
                    topic = %msg.topic,
                    error = ?err,
                    "PRODUCE_REQ delivery failed"
                );
                drop(txn_guard);
                Err(anyhow::Error::new(kafka_error_to_worker(&err)))
            }
            Err(_) => {
                warn!(
                    cluster = %msg.cluster,
                    topic = %msg.topic,
                    ?timeout,
                    "PRODUCE_REQ delivery timed out"
                );
                drop(txn_guard);
                Err(anyhow::Error::new(WorkerError::new(
                    ErrorKind::Timeout,
                    format!("produceSync timed out after {} ms", timeout.as_millis()),
                )))
            }
        }
    }
}

/// 把 broker 投递的 `KafkaError` 映射成业务友好的 [`ErrorKind`]。
fn kafka_error_to_kind(err: &KafkaError) -> ErrorKind {
    use RDKafkaErrorCode::*;
    let code = match err {
        KafkaError::MessageProduction(c) => *c,
        _ => return ErrorKind::Internal,
    };
    match code {
        SaslAuthenticationFailed
        | TopicAuthorizationFailed
        | GroupAuthorizationFailed
        | ClusterAuthorizationFailed => ErrorKind::AuthnAuthz,
        MessageSizeTooLarge => ErrorKind::MessageTooLarge,
        UnknownTopicOrPartition => ErrorKind::UnknownTopicOrPartition,
        c if is_retryable(&c) => ErrorKind::BrokerRetryable,
        _ => ErrorKind::Internal,
    }
}

/// 把 broker 投递的 `KafkaError` 转成携带 kind 的 [`WorkerError`]（producer 路径用）。
fn kafka_error_to_worker(err: &KafkaError) -> WorkerError {
    let (native_code, retryable) = match err {
        KafkaError::MessageProduction(c) => (*c as i32, is_retryable(c)),
        _ => (0, false),
    };
    WorkerError {
        kind: kafka_error_to_kind(err),
        retryable,
        native_code,
        message: err.to_string(),
    }
}

/// 把 IPC 传来的 headers 转成 rdkafka `OwnedHeaders`。
/// 返回 `None` 表示无 headers，避免对 rdkafka 发空 headers 段。
fn build_headers(headers: &[hi_kafka_proto::MessageHeader]) -> Option<OwnedHeaders> {
    if headers.is_empty() {
        return None;
    }
    let mut owned = OwnedHeaders::new_with_capacity(headers.len());
    for (name, value) in headers {
        owned = owned.insert(RdHeader {
            key: name,
            value: Some(value.as_ref()),
        });
    }
    Some(owned)
}

fn is_retryable(code: &RDKafkaErrorCode) -> bool {
    use RDKafkaErrorCode::*;
    matches!(
        code,
        RequestTimedOut
            | NetworkException
            | LeaderNotAvailable
            | NotLeaderForPartition
            | BrokerNotAvailable
            | ReplicaNotAvailable
            | KafkaStorageError
            // P0 #2：QueueFull = rdkafka 内部发送队列满，绝对应该退避重试，
            // 不是永久失败。enqueue timeout 一到就报 QueueFull，业务侧重试即可。
            | QueueFull
            // 临时不可用类（NetworkException 已覆盖大部分）的补充：
            | RebalanceInProgress         // 群组正在 rebalance，稍后再来
            | UnknownTopicOrPartition     // 可能是 metadata 还没刷新到所有分区
            | ConcurrentTransactions // 事务客户端互相阻塞，重试可以放行
    )
}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::error::RDKafkaErrorCode;
    use rdkafka::message::Headers; // 为 OwnedHeaders.count() 提供 trait

    // === is_retryable ===================================================

    #[test]
    fn test_is_retryable_true_for_transient() {
        use RDKafkaErrorCode::*;
        for c in [
            RequestTimedOut,
            NetworkException,
            LeaderNotAvailable,
            NotLeaderForPartition,
            BrokerNotAvailable,
            ReplicaNotAvailable,
            KafkaStorageError,
            QueueFull,
            RebalanceInProgress,
            UnknownTopicOrPartition,
            ConcurrentTransactions,
        ] {
            assert!(is_retryable(&c), "{c:?} 应可重试");
        }
    }

    #[test]
    fn test_is_retryable_false_for_permanent() {
        use RDKafkaErrorCode::*;
        // 永久性错误：认证 / 消息过大 / 无效分区数等
        for c in [
            SaslAuthenticationFailed,
            TopicAuthorizationFailed,
            GroupAuthorizationFailed,
            ClusterAuthorizationFailed,
            MessageSizeTooLarge,
            InvalidPartitions,
            InvalidTopic,
        ] {
            assert!(!is_retryable(&c), "{c:?} 不应可重试");
        }
    }

    // === kafka_error_to_kind ============================================

    #[test]
    fn test_kafka_error_to_kind_authnz() {
        use RDKafkaErrorCode::*;
        for c in [
            SaslAuthenticationFailed,
            TopicAuthorizationFailed,
            GroupAuthorizationFailed,
            ClusterAuthorizationFailed,
        ] {
            let err = KafkaError::MessageProduction(c);
            assert_eq!(kafka_error_to_kind(&err), ErrorKind::AuthnAuthz);
        }
    }

    #[test]
    fn test_kafka_error_to_kind_message_too_large() {
        let err = KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge);
        assert_eq!(kafka_error_to_kind(&err), ErrorKind::MessageTooLarge);
    }

    #[test]
    fn test_kafka_error_to_kind_unknown_topic() {
        let err = KafkaError::MessageProduction(RDKafkaErrorCode::UnknownTopicOrPartition);
        assert_eq!(
            kafka_error_to_kind(&err),
            ErrorKind::UnknownTopicOrPartition
        );
    }

    #[test]
    fn test_kafka_error_to_kind_retryable_maps_to_broker_retryable() {
        // 可重试类应聚合到 BrokerRetryable（业务侧统一 retry 策略入口）
        for c in [
            RDKafkaErrorCode::RequestTimedOut,
            RDKafkaErrorCode::LeaderNotAvailable,
            RDKafkaErrorCode::QueueFull,
        ] {
            let err = KafkaError::MessageProduction(c);
            assert_eq!(
                kafka_error_to_kind(&err),
                ErrorKind::BrokerRetryable,
                "{c:?} 应映射为 BrokerRetryable"
            );
        }
    }

    #[test]
    fn test_kafka_error_to_kind_non_production_returns_internal() {
        // AdminOpCreation 等非 producer 路径的错误直接 fallback Internal
        let err = KafkaError::AdminOpCreation("some admin op".into());
        assert_eq!(kafka_error_to_kind(&err), ErrorKind::Internal);
    }

    // === kafka_error_to_worker ==========================================

    #[test]
    fn test_kafka_error_to_worker_preserves_native_code_and_retryable() {
        let err = KafkaError::MessageProduction(RDKafkaErrorCode::LeaderNotAvailable);
        let w = kafka_error_to_worker(&err);
        assert_eq!(w.kind, ErrorKind::BrokerRetryable);
        assert!(w.retryable, "LeaderNotAvailable 应 retryable=true");
        assert_eq!(
            w.native_code,
            RDKafkaErrorCode::LeaderNotAvailable as i32,
            "native_code 直接透传"
        );
        assert!(w.message.to_lowercase().contains("leader"));
    }

    #[test]
    fn test_kafka_error_to_worker_permanent_error() {
        let err = KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge);
        let w = kafka_error_to_worker(&err);
        assert_eq!(w.kind, ErrorKind::MessageTooLarge);
        assert!(!w.retryable, "MessageSizeTooLarge 是永久错");
    }

    // === build_headers ===================================================

    #[test]
    fn test_build_headers_none_when_empty() {
        let headers: Vec<hi_kafka_proto::MessageHeader> = vec![];
        assert!(
            build_headers(&headers).is_none(),
            "空 headers 应返回 None，避免给 rdkafka 发空段"
        );
    }

    #[test]
    fn test_build_headers_preserves_keys_and_values() {
        let headers: Vec<hi_kafka_proto::MessageHeader> = vec![
            ("trace-id".into(), bytes::Bytes::from_static(b"abc123")),
            ("source".into(), bytes::Bytes::from_static(b"svc-a")),
        ];
        let owned = build_headers(&headers).expect("非空 headers 应 Some");
        assert_eq!(owned.count(), 2, "两个 header 都应保留");
    }

    #[test]
    fn test_build_headers_binary_safe_values() {
        // header value 含 null / 0xff 应完整保留（binary-safe）
        let headers: Vec<hi_kafka_proto::MessageHeader> =
            vec![("raw".into(), bytes::Bytes::from_static(b"a\x00b\xff"))];
        let owned = build_headers(&headers).unwrap();
        assert_eq!(owned.count(), 1);
    }

    // === TxnStateGuard cancel safety ====================================

    #[test]
    fn test_txn_guard_reverts_on_drop_when_not_committed() {
        let state = AtomicU8::new(TXN_TRANSITIONING);
        {
            let _guard = TxnStateGuard::new(&state, TXN_IDLE);
            // 模拟"async task 被 cancel"：直接离开 scope，guard 走 Drop 路径
        }
        assert_eq!(
            state.load(Ordering::Acquire),
            TXN_IDLE,
            "未 commit 的 guard 在 drop 时应回滚到 revert_to"
        );
    }

    #[test]
    fn test_txn_guard_commit_stores_final_state() {
        let state = AtomicU8::new(TXN_TRANSITIONING);
        {
            let guard = TxnStateGuard::new(&state, TXN_IDLE);
            guard.commit(TXN_ACTIVE);
            // guard 已被 move 消费，Drop 时 revert_to=None，不再回滚
        }
        assert_eq!(
            state.load(Ordering::Acquire),
            TXN_ACTIVE,
            "commit 后应把 final_state 落盘且 Drop 不再回滚"
        );
    }

    #[test]
    fn test_txn_guard_reverts_when_panicking() {
        // 模拟 spawn_blocking 内 panic：TxnStateGuard 走 Drop 回滚
        let state = std::sync::Arc::new(AtomicU8::new(TXN_TRANSITIONING));
        let state_clone = state.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = TxnStateGuard::new(&state_clone, TXN_IDLE);
            panic!("simulated cancel/panic mid-op");
        }));
        assert!(result.is_err(), "内部应确实 panic");
        assert_eq!(
            state.load(Ordering::Acquire),
            TXN_IDLE,
            "panic 后 guard 应把状态回滚，避免 TRANSITIONING 永久卡死"
        );
    }

    #[test]
    fn test_ensure_txn_active_matrix() {
        // 直接构造 ClusterEntry 需要真 producer，用 mock helper 太重；这里只测状态映射
        let idle = AtomicU8::new(TXN_IDLE);
        let transitioning = AtomicU8::new(TXN_TRANSITIONING);
        let active = AtomicU8::new(TXN_ACTIVE);

        // 直接用 Ordering::Acquire 读取，与 ensure_txn_active 一致
        assert_eq!(idle.load(Ordering::Acquire), TXN_IDLE);
        assert_eq!(transitioning.load(Ordering::Acquire), TXN_TRANSITIONING);
        assert_eq!(active.load(Ordering::Acquire), TXN_ACTIVE);
    }

    // === ProduceReq forward-compat decode ================================

    #[test]
    fn test_produce_req_decode_accepts_v1_payload_without_timeout() {
        use hi_kafka_proto::{DEFAULT_PRODUCE_TIMEOUT_MS, ProduceFnf, ProduceReq};
        // 老扩展只写 ProduceFnf payload，不写尾部 u32 timeout_ms
        let msg = ProduceFnf {
            cluster: "c".into(),
            topic: "t".into(),
            key: bytes::Bytes::from_static(b"k"),
            value: bytes::Bytes::from_static(b"v"),
            partition: -1,
            timestamp_ms: -1,
            headers: vec![],
        };
        let mut buf = bytes::BytesMut::new();
        msg.encode(&mut buf).unwrap();
        // v1 legacy payload → v2 worker 解码应 fallback 到默认 timeout
        let decoded = ProduceReq::decode(&buf).unwrap();
        assert_eq!(decoded.msg.cluster, "c");
        assert_eq!(decoded.timeout_ms, DEFAULT_PRODUCE_TIMEOUT_MS);
    }

    #[test]
    fn test_produce_req_decode_roundtrip_with_timeout() {
        use hi_kafka_proto::{ProduceFnf, ProduceReq};
        let orig = ProduceReq {
            msg: ProduceFnf {
                cluster: "c".into(),
                topic: "t".into(),
                key: bytes::Bytes::from_static(b"k"),
                value: bytes::Bytes::from_static(b"v"),
                partition: 3,
                timestamp_ms: 12345,
                headers: vec![],
            },
            timeout_ms: 7777,
        };
        let mut buf = bytes::BytesMut::new();
        orig.encode(&mut buf).unwrap();
        let decoded = ProduceReq::decode(&buf).unwrap();
        assert_eq!(decoded, orig, "v2 payload 完整 roundtrip");
    }
}

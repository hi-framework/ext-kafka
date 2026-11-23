use super::{
    ConsumerError, ConsumerSource, ConsumerSourceFactory, GlobalBudget, SourceNotice, SourceRead,
    SourceRecord,
};
use crate::broker_watchdog::BrokerWatchdog;
use crate::cluster::{ClusterRegistryHandle, OAuthTokenSlot};
use crate::error::WorkerError;
use crate::watermark::PartitionKey;
use async_trait::async_trait;
use bytes::Bytes;
use hi_kafka_proto::{
    ConsumeOpen, ErrorKind, MessageHeader, OperationOutcome, MAX_MESSAGE_HEADERS,
};
use rdkafka::client::{ClientContext, OAuthToken};
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer,
};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Headers, Message};
use rdkafka::statistics::Statistics;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::{ClientConfig, Offset};
use std::sync::{Arc, Mutex};
use tracing::warn;

/// librdkafka 本地队列低于该条数时继续 fetch。预取内存的上界由强制设置的
/// `queued.max.messages.kbytes` 保证，这里只影响流水线深度。2026-11 在单节点 colima
/// broker 上对比 1 / 100 / 1000 / 100000：前三者差异落在噪声内，librdkafka 默认
/// 100000 在 1 KiB 消息下稳定更慢，因此保留 1。高延迟 broker 可在 consumer 配置中覆盖。
const DEFAULT_QUEUED_MIN_MESSAGES: &str = "1";

pub struct KafkaSourceFactory {
    registry: ClusterRegistryHandle,
}

impl KafkaSourceFactory {
    pub fn new(registry: ClusterRegistryHandle) -> Self {
        Self { registry }
    }
}

struct KafkaContext {
    notices: Arc<Mutex<Vec<SourceNotice>>>,
    /// rebalance 回调在 `recv()` 内部执行；没有新消息时 `recv()` 不会返回。
    /// 入队 notice 后唤醒 actor，保证 ownership 变化立即生效，而不是等到下一条消息。
    notice_signal: Arc<tokio::sync::Notify>,
    oauth: OAuthTokenSlot,
    cluster: String,
    watchdog: Arc<BrokerWatchdog>,
}

impl ClientContext for KafkaContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn stats(&self, statistics: Statistics) {
        self.watchdog.observe(&statistics);
    }
    fn generate_oauth_token(
        &self,
        _config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn std::error::Error>> {
        let slot = self.oauth.lock().unwrap_or_else(|e| e.into_inner());
        let token = slot
            .as_ref()
            .ok_or_else(|| format!("no OAuth token for cluster '{}'", self.cluster))?;
        Ok(OAuthToken {
            token: token.token_value.clone(),
            principal_name: token.principal_name.clone(),
            lifetime_ms: token.lifetime_ms,
        })
    }
}

impl ConsumerContext for KafkaContext {
    fn pre_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Revoke(tpl) = rebalance {
            self.push(SourceNotice::Revoke(partitions(tpl)));
        }
    }
    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Assign(tpl) = rebalance {
            self.push(SourceNotice::Assign(partitions(tpl)));
        }
    }
}

impl KafkaContext {
    fn push(&self, notice: SourceNotice) {
        let mut notices = self.notices.lock().unwrap_or_else(|e| e.into_inner());
        if notices.len() >= 16 {
            notices.clear();
            notices.push(SourceNotice::Fatal(
                "rebalance notice queue overflow; subscription state is no longer trustworthy"
                    .into(),
            ));
        } else if !matches!(notices.first(), Some(SourceNotice::Fatal(_))) {
            notices.push(notice);
        }
        drop(notices);
        self.notice_signal.notify_one();
    }
}

fn partitions(tpl: &TopicPartitionList) -> Vec<(String, i32)> {
    tpl.elements()
        .into_iter()
        .map(|e| (e.topic().to_owned(), e.partition()))
        .collect()
}

/// 单次 commit 的等待上限，低于客户端默认的 5s 控制超时：客户端拿到的是 worker 的
/// 结构化 TIMEOUT，而不是本地读超时后失配的连接。
const COMMIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(4);

type CommitTask = tokio::task::JoinHandle<Result<(), KafkaError>>;

struct KafkaSource {
    consumer: Arc<StreamConsumer<KafkaContext>>,
    notices: Arc<Mutex<Vec<SourceNotice>>>,
    notice_signal: Arc<tokio::sync::Notify>,
    /// 超时后仍在 librdkafka 中阻塞的同步 commit。coordinator 不可用时同步 commit 会
    /// 阻塞到 socket 超时；同一时刻只保留一个，避免阻塞线程随重试堆积。
    pending_commit: Option<CommitTask>,
}

impl KafkaSource {
    fn pop_notice(&self) -> Option<SourceNotice> {
        let mut notices = self.notices.lock().unwrap_or_else(|e| e.into_inner());
        if notices.is_empty() {
            None
        } else {
            Some(notices.remove(0))
        }
    }

    fn drain_notices(&self) -> Vec<SourceNotice> {
        let mut notices = self.notices.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut *notices)
    }
}

#[async_trait]
impl ConsumerSourceFactory for KafkaSourceFactory {
    async fn create(
        &self,
        request: &ConsumeOpen,
    ) -> Result<Box<dyn ConsumerSource>, ConsumerError> {
        let (cluster_config, _generation) = self
            .registry
            .get_with_version(&request.cluster)
            .await
            .ok_or_else(|| ConsumerError::ClusterNotRegistered(request.cluster.clone()))?;
        let oauth = self
            .registry
            .token_slot_for(&request.cluster)
            .await
            .ok_or_else(|| ConsumerError::ClusterNotRegistered(request.cluster.clone()))?;
        let notices = Arc::new(Mutex::new(Vec::new()));
        let notice_signal = Arc::new(tokio::sync::Notify::new());
        let mut config = ClientConfig::new();
        // 可调的默认值：本地预取队列的内存上界由下面强制的 queued.max.messages.kbytes
        // 保证，这里只决定流水线深度。
        config
            .set("queued.min.messages", DEFAULT_QUEUED_MIN_MESSAGES)
            .set("fetch.queue.backoff.ms", "10");
        for (key, value) in crate::cluster::CLIENT_DEFAULTS {
            config.set(*key, *value);
        }
        for (key, value) in cluster_config {
            config.set(key, value);
        }
        for (key, value) in &request.config {
            config.set(key, value);
        }
        // 不变量：只有应用 ACK 能推进 offset；预取字节受 credit 窗口约束。
        config
            .set("group.id", &request.group_id)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set(
                "queued.max.messages.kbytes",
                request
                    .max_in_flight_bytes
                    .div_ceil(1024)
                    .max(1)
                    .to_string(),
            );
        if !request.client_id.is_empty() {
            config.set("client.id", &request.client_id);
        }
        let watchdog = Arc::new(BrokerWatchdog::new(config.get("bootstrap.servers")));
        let context = KafkaContext {
            notices: notices.clone(),
            notice_signal: notice_signal.clone(),
            oauth,
            cluster: request.cluster.clone(),
            watchdog: watchdog.clone(),
        };
        let consumer: StreamConsumer<KafkaContext> = config
            .create_with_context(context)
            .map_err(|e| ConsumerError::Backend(anyhow::anyhow!(e)))?;
        watchdog.attach(consumer.client().native_ptr());
        consumer
            .subscribe(
                &request
                    .topics
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| ConsumerError::Backend(anyhow::anyhow!(e)))?;
        Ok(Box::new(KafkaSource {
            consumer: Arc::new(consumer),
            notices,
            notice_signal,
            pending_commit: None,
        }))
    }
}

#[async_trait]
impl ConsumerSource for KafkaSource {
    async fn next_record(&mut self, budget: &GlobalBudget) -> Result<SourceRead, ConsumerError> {
        // `StreamConsumer::recv` is cancellation-safe. After it yields a
        // borrowed record there are deliberately no more awaits: budget
        // acquisition is try-only, so actor command cancellation cannot drop
        // a record after advancing the librdkafka fetch position.
        let message = loop {
            let received = tokio::select! {
                biased;
                // `recv()` 可安全取消：rebalance 回调已在其内部执行完毕并入队 notice，
                // 取消只是放弃等待下一条消息。
                _ = self.notice_signal.notified() => {
                    let notices = self.drain_notices();
                    if notices.is_empty() {
                        continue;
                    }
                    return Ok(SourceRead::RebalancePending {
                        notices,
                        record: None,
                        blocked: None,
                    });
                }
                received = self.consumer.recv() => received,
            };
            match received {
                Ok(message) => break message,
                Err(error) if is_recoverable_consumer_error(&error) => {
                    warn!(
                        error = %error,
                        native_code = error.rdkafka_error_code().map_or(0, |code| code as i32),
                        "transient Kafka consumer error; keeping subscription alive"
                    );

                    // recv() can run rebalance callbacks before yielding an error. Return
                    // those notices to the actor before polling Kafka again so ownership
                    // changes are never hidden behind a broker reconnect loop.
                    let notices = self.drain_notices();
                    if !notices.is_empty() {
                        return Ok(SourceRead::RebalancePending {
                            notices,
                            record: None,
                            blocked: None,
                        });
                    }
                    tokio::task::yield_now().await;
                }
                Err(error) => {
                    return Err(ConsumerError::Backend(anyhow::Error::new(
                        kafka_consumer_error_to_worker(&error),
                    )));
                }
            }
        };

        // Rebalance callbacks run inside recv(). A callback and a fetched
        // record can therefore become visible together. Detach at most this
        // one globally-budgeted record and let the actor apply every queued
        // ownership notice before publishing it. Seeking here would force a
        // second librdkafka fetch and double the decompressed queue high-water.
        let notices = self.drain_notices();
        if !notices.is_empty() {
            let still_assigned = self
                .consumer
                .assignment()
                .map_err(|e| ConsumerError::Backend(anyhow::anyhow!(e)))?
                .elements()
                .iter()
                .any(|entry| {
                    entry.topic() == message.topic() && entry.partition() == message.partition()
                });
            if !still_assigned {
                drop(message);
                return Ok(SourceRead::RebalancePending {
                    notices,
                    record: None,
                    blocked: None,
                });
            }
        }
        let header_count = message.headers().map_or(0, Headers::count);
        if header_count > MAX_MESSAGE_HEADERS {
            return Err(ConsumerError::RecordTooLarge(format!(
                "Kafka record contains {header_count} headers; protocol limit is {MAX_MESSAGE_HEADERS}"
            )));
        }
        let header_bytes = message
            .headers()
            .map(|headers| {
                (0..headers.count())
                    .map(|i| headers.get(i))
                    .map(|header| {
                        header.key.len() as u64 + header.value.map_or(0, |v| v.len()) as u64 + 6
                    })
                    .sum::<u64>()
            })
            .unwrap_or(0);
        let wire_size = 79u64
            .saturating_add(message.topic().len() as u64)
            .saturating_add(message.key().map_or(0, |v| v.len()) as u64)
            .saturating_add(message.payload().map_or(0, |v| v.len()) as u64)
            .saturating_add(header_bytes);
        let message_permit = match budget.acquire_message() {
            Ok(permit) => permit,
            Err(ConsumerError::BudgetUnavailable) => {
                let blocked = (
                    PartitionKey::new(message.topic(), message.partition()),
                    message.offset(),
                    wire_size,
                );
                return if notices.is_empty() {
                    Ok(SourceRead::BudgetBlocked(blocked.0, blocked.1, blocked.2))
                } else {
                    Ok(SourceRead::RebalancePending {
                        notices,
                        record: None,
                        blocked: Some(blocked),
                    })
                };
            }
            Err(error) => return Err(error),
        };
        let permit = match budget.acquire_bytes(message_permit, wire_size) {
            Ok(permit) => permit,
            Err(ConsumerError::BudgetUnavailable) => {
                let partition = PartitionKey::new(message.topic(), message.partition());
                let offset = message.offset();
                drop(message);
                return if notices.is_empty() {
                    Ok(SourceRead::BudgetBlocked(partition, offset, wire_size))
                } else {
                    Ok(SourceRead::RebalancePending {
                        notices,
                        record: None,
                        blocked: Some((partition, offset, wire_size)),
                    })
                };
            }
            Err(error) => return Err(error),
        };
        let headers = message
            .headers()
            .map(|headers| {
                (0..headers.count())
                    .map(|i| headers.get(i))
                    .map(|h| {
                        (
                            h.key.to_owned(),
                            Bytes::copy_from_slice(h.value.unwrap_or_default()),
                        )
                    })
                    .collect::<Vec<MessageHeader>>()
            })
            .unwrap_or_default();
        let record = SourceRecord {
            topic: message.topic().to_owned(),
            partition: message.partition(),
            offset: message.offset(),
            timestamp_ms: message.timestamp().to_millis().unwrap_or(-1),
            key: message.key().map(Bytes::copy_from_slice),
            value: message.payload().map(Bytes::copy_from_slice),
            headers,
        };
        if notices.is_empty() {
            Ok(SourceRead::Record(record, permit))
        } else {
            Ok(SourceRead::RebalancePending {
                notices,
                record: Some((record, permit)),
                blocked: None,
            })
        }
    }

    fn take_notice(&mut self) -> Option<SourceNotice> {
        self.pop_notice()
    }

    async fn commit(&mut self, offsets: &[(PartitionKey, i64)]) -> Result<(), ConsumerError> {
        if offsets.is_empty() {
            return Ok(());
        }
        let mut tpl = TopicPartitionList::new();
        for (partition, offset) in offsets {
            tpl.add_partition_offset(
                &partition.topic,
                partition.partition,
                Offset::Offset(*offset),
            )
            .map_err(|e| ConsumerError::Backend(anyhow::anyhow!(e)))?;
        }
        let mut task = match self.pending_commit.take() {
            // 上一次超时的 commit 还没返回：不再叠加新的阻塞调用，先等它。
            Some(previous) if !previous.is_finished() => previous,
            _ => {
                let consumer = self.consumer.clone();
                tokio::task::spawn_blocking(move || consumer.commit(&tpl, CommitMode::Sync))
            }
        };
        match tokio::time::timeout(COMMIT_TIMEOUT, &mut task).await {
            Ok(joined) => joined
                .map_err(|join| ConsumerError::Backend(anyhow::anyhow!(join)))?
                .map_err(|error| {
                    ConsumerError::Backend(anyhow::Error::new(commit_error_to_worker(&error)))
                }),
            Err(_) => {
                self.pending_commit = Some(task);
                Err(ConsumerError::Backend(anyhow::Error::new(WorkerError {
                    kind: ErrorKind::Timeout,
                    retryable: true,
                    outcome: OperationOutcome::Unknown,
                    native_code: 0,
                    message: format!(
                        "commit did not complete within {}s (coordinator unavailable?)",
                        COMMIT_TIMEOUT.as_secs()
                    ),
                })))
            }
        }
    }

    async fn seek(&mut self, offsets: &[(PartitionKey, i64)]) -> Result<(), ConsumerError> {
        let consumer = self.consumer.clone();
        let offsets = offsets.to_vec();
        tokio::task::spawn_blocking(move || {
            for (partition, offset) in offsets {
                consumer.seek(
                    &partition.topic,
                    partition.partition,
                    Offset::Offset(offset),
                    std::time::Duration::from_secs(5),
                )?;
            }
            Ok::<_, rdkafka::error::KafkaError>(())
        })
        .await
        .map_err(|join| ConsumerError::Backend(anyhow::anyhow!(join)))?
        .map_err(|error| ConsumerError::Backend(anyhow::anyhow!(error)))
    }

    async fn pause(&mut self, partitions: &[(String, i32)]) -> Result<(), ConsumerError> {
        let tpl = control_tpl(&self.consumer, partitions)?;
        self.consumer
            .pause(&tpl)
            .map_err(|e| ConsumerError::Backend(anyhow::anyhow!(e)))
    }

    async fn resume(&mut self, partitions: &[(String, i32)]) -> Result<(), ConsumerError> {
        let tpl = control_tpl(&self.consumer, partitions)?;
        self.consumer
            .resume(&tpl)
            .map_err(|e| ConsumerError::Backend(anyhow::anyhow!(e)))
    }
}

fn is_recoverable_consumer_error(error: &KafkaError) -> bool {
    matches!(
        error,
        KafkaError::MessageConsumption(code) if is_recoverable_consumer_code(code)
    )
}

fn is_recoverable_consumer_code(code: &RDKafkaErrorCode) -> bool {
    use RDKafkaErrorCode::*;
    matches!(
        code,
        BrokerDestroy
            | BrokerTransportFailure
            | Resolve
            | AllBrokersDown
            | OperationTimedOut
            | RequestTimedOut
            | NetworkException
            | LeaderNotAvailable
            | NotLeaderForPartition
            | BrokerNotAvailable
            | CoordinatorLoadInProgress
            | CoordinatorNotAvailable
            | NotCoordinator
            | WaitingForCoordinator
            | RebalanceInProgress
    )
}

fn kafka_consumer_error_to_worker(error: &KafkaError) -> WorkerError {
    use RDKafkaErrorCode::*;

    let code = error.rdkafka_error_code();
    let retryable = is_recoverable_consumer_error(error);
    let kind = match code {
        Some(SaslAuthenticationFailed) => ErrorKind::Authentication,
        Some(TopicAuthorizationFailed | GroupAuthorizationFailed | ClusterAuthorizationFailed) => {
            ErrorKind::Authorization
        }
        Some(OffsetOutOfRange) => ErrorKind::OffsetInvalid,
        Some(UnknownTopicOrPartition) => ErrorKind::UnknownTopicOrPartition,
        Some(_) if retryable => ErrorKind::BrokerRetryable,
        _ => ErrorKind::Internal,
    };

    WorkerError {
        kind,
        retryable,
        outcome: OperationOutcome::NotApplied,
        native_code: code.map_or(0, |value| value as i32),
        message: error.to_string(),
    }
}

/// commit 失败的结构化分类。rebalance 进行中（generation 变化、成员重新加入）与
/// coordinator 瞬时不可用都可以在稍后重试：未提交的 watermark 仍保留在 actor 中，
/// 下一次 commit 会重新提交。超时的结果未知——broker 可能已经持久化。
fn commit_error_to_worker(error: &KafkaError) -> WorkerError {
    use RDKafkaErrorCode::*;

    let code = error.rdkafka_error_code();
    let (kind, retryable, outcome) = match code {
        Some(RebalanceInProgress | IllegalGeneration | UnknownMemberId | FencedInstanceId) => {
            (ErrorKind::Rebalance, true, OperationOutcome::NotApplied)
        }
        Some(
            CoordinatorNotAvailable
            | NotCoordinator
            | CoordinatorLoadInProgress
            | WaitingForCoordinator
            | BrokerTransportFailure
            | AllBrokersDown
            | NetworkException,
        ) => (
            ErrorKind::BrokerRetryable,
            true,
            OperationOutcome::NotApplied,
        ),
        Some(RequestTimedOut | OperationTimedOut) => {
            (ErrorKind::Timeout, true, OperationOutcome::Unknown)
        }
        Some(GroupAuthorizationFailed | TopicAuthorizationFailed) => (
            ErrorKind::Authorization,
            false,
            OperationOutcome::NotApplied,
        ),
        _ => (ErrorKind::Commit, false, OperationOutcome::NotApplied),
    };
    WorkerError {
        kind,
        retryable,
        outcome,
        native_code: code.map_or(0, |value| value as i32),
        message: error.to_string(),
    }
}

fn control_tpl(
    consumer: &StreamConsumer<KafkaContext>,
    partitions: &[(String, i32)],
) -> Result<TopicPartitionList, ConsumerError> {
    if partitions.is_empty() {
        return consumer
            .assignment()
            .map_err(|e| ConsumerError::Backend(anyhow::anyhow!(e)));
    }
    let mut tpl = TopicPartitionList::new();
    for (topic, partition) in partitions {
        tpl.add_partition(topic, *partition);
    }
    Ok(tpl)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_failures_are_recoverable() {
        for code in [
            RDKafkaErrorCode::BrokerTransportFailure,
            RDKafkaErrorCode::AllBrokersDown,
            RDKafkaErrorCode::OperationTimedOut,
            RDKafkaErrorCode::NetworkException,
            RDKafkaErrorCode::RebalanceInProgress,
        ] {
            let error = KafkaError::MessageConsumption(code);
            assert!(
                is_recoverable_consumer_error(&error),
                "{code:?} should keep the consumer alive"
            );
        }
    }

    #[test]
    fn fatal_consumption_error_is_not_recoverable() {
        let error = KafkaError::MessageConsumptionFatal(RDKafkaErrorCode::BrokerTransportFailure);
        assert!(!is_recoverable_consumer_error(&error));
    }

    #[test]
    fn commit_during_rebalance_is_retryable() {
        let error = KafkaError::ConsumerCommit(RDKafkaErrorCode::IllegalGeneration);
        let response = commit_error_to_worker(&error).to_error_resp();
        assert_eq!(response.kind, ErrorKind::Rebalance);
        assert!(response.retryable);
        assert_eq!(response.outcome, OperationOutcome::NotApplied);

        let timeout = KafkaError::ConsumerCommit(RDKafkaErrorCode::RequestTimedOut);
        let response = commit_error_to_worker(&timeout).to_error_resp();
        assert_eq!(response.outcome, OperationOutcome::Unknown);

        let waiting = KafkaError::ConsumerCommit(RDKafkaErrorCode::WaitingForCoordinator);
        assert!(commit_error_to_worker(&waiting).to_error_resp().retryable);

        let fatal = KafkaError::ConsumerCommit(RDKafkaErrorCode::OffsetMetadataTooLarge);
        assert!(!commit_error_to_worker(&fatal).to_error_resp().retryable);
    }

    #[test]
    fn consumer_error_preserves_structured_metadata() {
        let transport = KafkaError::MessageConsumption(RDKafkaErrorCode::BrokerTransportFailure);
        let response = kafka_consumer_error_to_worker(&transport).to_error_resp();
        assert_eq!(response.kind, ErrorKind::BrokerRetryable);
        assert!(response.retryable);
        assert_eq!(response.native_code, -195);
        assert_eq!(response.message, transport.to_string());

        let authentication =
            KafkaError::MessageConsumptionFatal(RDKafkaErrorCode::SaslAuthenticationFailed);
        let response = kafka_consumer_error_to_worker(&authentication).to_error_resp();
        assert_eq!(response.kind, ErrorKind::Authentication);
        assert!(!response.retryable);
        assert_eq!(
            response.native_code,
            RDKafkaErrorCode::SaslAuthenticationFailed as i32
        );
    }
}

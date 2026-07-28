use super::{
    ConsumerError, ConsumerSource, ConsumerSourceFactory, GlobalBudget, SourceNotice, SourceRead,
    SourceRecord,
};
use crate::cluster::{ClusterRegistryHandle, OAuthTokenSlot};
use crate::watermark::PartitionKey;
use async_trait::async_trait;
use bytes::Bytes;
use hi_kafka_proto::{ConsumeOpen, MessageHeader, MAX_MESSAGE_HEADERS};
use rdkafka::client::{ClientContext, OAuthToken};
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer,
};
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::{ClientConfig, Offset};
use std::sync::{Arc, Mutex};

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
    oauth: OAuthTokenSlot,
    cluster: String,
}

impl ClientContext for KafkaContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;
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
    }
}

fn partitions(tpl: &TopicPartitionList) -> Vec<(String, i32)> {
    tpl.elements()
        .into_iter()
        .map(|e| (e.topic().to_owned(), e.partition()))
        .collect()
}

struct KafkaSource {
    consumer: Arc<StreamConsumer<KafkaContext>>,
    notices: Arc<Mutex<Vec<SourceNotice>>>,
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
        let context = KafkaContext {
            notices: notices.clone(),
            oauth,
            cluster: request.cluster.clone(),
        };
        let mut config = ClientConfig::new();
        for (key, value) in cluster_config {
            config.set(key, value);
        }
        for (key, value) in &request.config {
            config.set(key, value);
        }
        config
            .set("group.id", &request.group_id)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("queued.min.messages", "1")
            .set("fetch.queue.backoff.ms", "10")
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
        let consumer: StreamConsumer<KafkaContext> = config
            .create_with_context(context)
            .map_err(|e| ConsumerError::Backend(anyhow::anyhow!(e)))?;
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
        let message = self
            .consumer
            .recv()
            .await
            .map_err(|e| ConsumerError::Backend(anyhow::anyhow!(e)))?;

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
        let consumer = self.consumer.clone();
        tokio::task::spawn_blocking(move || consumer.commit(&tpl, CommitMode::Sync))
            .await
            .map_err(|join| ConsumerError::Backend(anyhow::anyhow!(join)))?
            .map_err(|error| ConsumerError::Backend(anyhow::anyhow!(error)))
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

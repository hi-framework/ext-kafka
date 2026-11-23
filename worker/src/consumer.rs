//! Protocol-v2 consumer subscription actors.
//!
//! A subscription actor is the only owner of its Kafka consumer, delivery
//! epoch, credits, in-flight permits and ACK watermarks.  There is no poll
//! buffer and no batch materialisation anywhere in this module.

#[cfg(feature = "kafka")]
use crate::cluster::ClusterRegistryHandle;
use crate::shutdown::ShutdownHandle;
use crate::stream_core::{StreamStateError, SubscriptionCore};
use crate::watermark::{PartitionKey, WatermarkError};
use crate::Metrics;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use hi_kafka_proto::{
    ConsumeAck, ConsumeClose, ConsumeCommit, ConsumeCommitted, ConsumeMessage, ConsumeNack,
    ConsumeOpen, ConsumeReady, ConsumerPartitions, ErrorKind, ErrorResp, FlowControl, FrameType,
    MessageHeader, RebalanceNotice, ResumeToken, HEADER_LEN, MAX_PAYLOAD_LEN,
};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Mutex, OwnedSemaphorePermit, Semaphore};
use tracing::{debug, warn};

#[cfg(feature = "kafka")]
mod real;

const DEFAULT_MAX_SUBSCRIPTIONS: usize = 1024;
const DEFAULT_GLOBAL_MESSAGES: u32 = 65_536;
const DEFAULT_GLOBAL_BYTES: u32 = 512 * 1024 * 1024;
const DEFAULT_SUB_MESSAGES: u32 = 256;
const MAX_CONSUMER_WIRE_BYTES: u64 = (MAX_PAYLOAD_LEN + HEADER_LEN) as u64;
const DEFAULT_SUB_BYTES: u64 = MAX_CONSUMER_WIRE_BYTES;
const DEFAULT_RESUME_GRACE: Duration = Duration::from_secs(10);
const BACKPRESSURE_PAUSE_DEBOUNCE: Duration = Duration::from_millis(5);

#[derive(Debug, Clone)]
pub struct SourceRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<MessageHeader>,
}

#[cfg_attr(not(feature = "kafka"), allow(dead_code))]
pub(crate) enum SourceRead {
    Record(SourceRecord, GlobalPermit),
    BudgetBlocked(PartitionKey, i64, u64),
    RebalancePending {
        notices: Vec<SourceNotice>,
        record: Option<(SourceRecord, GlobalPermit)>,
        blocked: Option<(PartitionKey, i64, u64)>,
    },
}

impl SourceRecord {
    pub fn wire_size(&self) -> u64 {
        let headers = self
            .headers
            .iter()
            .map(|h| h.0.len() as u64 + h.1.len() as u64 + 6)
            .sum::<u64>();
        79u64
            .saturating_add(self.topic.len() as u64)
            .saturating_add(self.key.as_ref().map_or(0, Bytes::len) as u64)
            .saturating_add(self.value.as_ref().map_or(0, Bytes::len) as u64)
            .saturating_add(headers)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceNotice {
    Assign(Vec<(String, i32)>),
    Revoke(Vec<(String, i32)>),
    Fatal(String),
}

#[async_trait]
pub(crate) trait ConsumerSource: Send {
    /// Acquire the exact global delivery budget while the Kafka record is
    /// still borrowed, then detach/copy it into process-owned bytes.
    async fn next_record(&mut self, budget: &GlobalBudget) -> Result<SourceRead, ConsumerError>;
    fn take_notice(&mut self) -> Option<SourceNotice>;
    async fn commit(&mut self, offsets: &[(PartitionKey, i64)]) -> Result<(), ConsumerError>;
    async fn seek(&mut self, offsets: &[(PartitionKey, i64)]) -> Result<(), ConsumerError>;
    /// 空列表表示 librdkafka 当前的完整 assignment。
    async fn pause(&mut self, partitions: &[(String, i32)]) -> Result<(), ConsumerError>;
    /// 空列表表示 librdkafka 当前的完整 assignment。
    async fn resume(&mut self, partitions: &[(String, i32)]) -> Result<(), ConsumerError>;
}

#[async_trait]
pub(crate) trait ConsumerSourceFactory: Send + Sync {
    async fn create(&self, request: &ConsumeOpen)
        -> Result<Box<dyn ConsumerSource>, ConsumerError>;
}

#[derive(Debug, thiserror::Error)]
pub enum ConsumerError {
    #[error("cluster '{0}' is not registered")]
    ClusterNotRegistered(String),
    #[error("subscription {0} was not found")]
    SubscriptionNotFound(u64),
    #[error("subscription limit reached")]
    SubscriptionLimit,
    #[error("invalid subscription request: {0}")]
    InvalidArgument(String),
    #[error("stale subscription epoch: got {got}, current {current}")]
    StaleEpoch { got: u64, current: u64 },
    #[error("resume token is invalid")]
    InvalidResumeToken,
    #[error("resume grace expired")]
    ResumeExpired,
    #[error("subscription actor stopped")]
    ActorStopped,
    #[error("global delivery byte budget is temporarily exhausted")]
    BudgetUnavailable,
    #[error("record cannot be represented inside the negotiated limits: {0}")]
    RecordTooLarge(String),
    #[error("backend: {0}")]
    Backend(#[from] anyhow::Error),
    #[error("rebalance state lost: {0}")]
    RebalanceState(String),
    #[error(transparent)]
    State(#[from] StreamStateError),
}

impl ConsumerError {
    pub fn response(&self) -> ErrorResp {
        let response = |kind| ErrorResp::new(kind, self.to_string());
        let retryable = |kind| {
            let mut value = response(kind);
            value.retryable = true;
            value
        };
        match self {
            Self::ClusterNotRegistered(_) => response(ErrorKind::ClusterNotRegistered),
            Self::SubscriptionNotFound(_) => response(ErrorKind::SubscriptionNotFound),
            Self::SubscriptionLimit | Self::BudgetUnavailable => {
                retryable(ErrorKind::CreditExhausted)
            }
            Self::InvalidArgument(_) => response(ErrorKind::InvalidArgument),
            Self::StaleEpoch { .. }
            | Self::InvalidResumeToken
            | Self::ResumeExpired
            | Self::State(StreamStateError::Watermark(WatermarkError::StaleEpoch { .. })) => {
                response(ErrorKind::StaleEpoch)
            }
            Self::ActorStopped => retryable(ErrorKind::WorkerUnavailable),
            Self::RecordTooLarge(_)
            | Self::State(StreamStateError::Watermark(WatermarkError::RecordTooLarge { .. })) => {
                response(ErrorKind::RecordTooLarge)
            }
            Self::RebalanceState(_) => response(ErrorKind::Rebalance),
            Self::State(StreamStateError::Watermark(WatermarkError::StaleDelivery(_))) => {
                response(ErrorKind::StaleDelivery)
            }
            Self::State(StreamStateError::Credit(_))
            | Self::State(StreamStateError::Watermark(
                WatermarkError::MessageLimit | WatermarkError::ByteLimit,
            )) => response(ErrorKind::CreditExhausted),
            Self::State(_) => response(ErrorKind::Internal),
            Self::Backend(error) => crate::error::WorkerError::from_anyhow(error),
        }
    }
}

#[derive(Debug)]
pub struct OutboundFrame {
    pub kind: FrameType,
    pub cid: u64,
    pub payload: Bytes,
}

impl OutboundFrame {
    fn encoded<T>(
        kind: FrameType,
        cid: u64,
        encode: impl FnOnce(&mut BytesMut) -> Result<T, hi_kafka_proto::PayloadError>,
    ) -> Result<Self, ConsumerError> {
        let mut payload = BytesMut::new();
        encode(&mut payload).map_err(|e| ConsumerError::InvalidArgument(e.to_string()))?;
        Ok(Self {
            kind,
            cid,
            payload: payload.freeze(),
        })
    }
}

pub(crate) struct GlobalPermit {
    _message: OwnedSemaphorePermit,
    bytes: OwnedSemaphorePermit,
}

impl GlobalPermit {
    fn finish_copy(&mut self, wire_bytes: u64) {
        let transient = self
            .bytes
            .split(wire_bytes as usize)
            .expect("copy reservation must contain one transient wire image");
        drop(transient);
    }
}

#[derive(Clone)]
#[cfg_attr(not(feature = "kafka"), allow(dead_code))]
pub(crate) struct GlobalBudget {
    messages: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    byte_limit: u32,
}

impl GlobalBudget {
    #[cfg_attr(not(feature = "kafka"), allow(dead_code))]
    pub(crate) fn acquire_message(&self) -> Result<OwnedSemaphorePermit, ConsumerError> {
        self.messages
            .clone()
            .try_acquire_owned()
            .map_err(|_| ConsumerError::BudgetUnavailable)
    }

    #[cfg_attr(not(feature = "kafka"), allow(dead_code))]
    pub(crate) fn acquire_bytes(
        &self,
        message: OwnedSemaphorePermit,
        bytes: u64,
    ) -> Result<GlobalPermit, ConsumerError> {
        let bytes = u32::try_from(bytes)
            .map_err(|_| ConsumerError::RecordTooLarge("wire size exceeds u32".into()))?;
        let reserved_bytes = bytes
            .checked_mul(2)
            .ok_or_else(|| ConsumerError::RecordTooLarge("copy budget overflow".into()))?;
        if reserved_bytes > self.byte_limit {
            return Err(ConsumerError::RecordTooLarge(format!(
                "record requires {reserved_bytes} bytes during detach/encode, global byte budget is {}",
                self.byte_limit
            )));
        }
        let byte_permit = self
            .bytes
            .clone()
            .try_acquire_many_owned(reserved_bytes)
            .map_err(|_| ConsumerError::BudgetUnavailable)?;
        Ok(GlobalPermit {
            _message: message,
            bytes: byte_permit,
        })
    }

    async fn wait_until_available(&self, bytes: u64) -> Result<(), ConsumerError> {
        let message = self
            .messages
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ConsumerError::ActorStopped)?;
        let bytes = u32::try_from(bytes)
            .map_err(|_| ConsumerError::RecordTooLarge("wire size exceeds u32".into()))?;
        let reserved = bytes
            .checked_mul(2)
            .ok_or_else(|| ConsumerError::RecordTooLarge("copy budget overflow".into()))?;
        if reserved > self.byte_limit {
            return Err(ConsumerError::RecordTooLarge(format!(
                "record requires {reserved} bytes during detach/encode, global byte budget is {}",
                self.byte_limit
            )));
        }
        let byte_permit = self
            .bytes
            .clone()
            .acquire_many_owned(reserved)
            .await
            .map_err(|_| ConsumerError::ActorStopped)?;
        drop(byte_permit);
        drop(message);
        Ok(())
    }
}

enum ActorCommand {
    Ack(ConsumeAck, oneshot::Sender<Result<(), ConsumerError>>),
    Nack(ConsumeNack, oneshot::Sender<Result<(), ConsumerError>>),
    Flow(FlowControl, oneshot::Sender<Result<(), ConsumerError>>),
    Commit(
        ConsumeCommit,
        u64,
        oneshot::Sender<Result<(), ConsumerError>>,
    ),
    Pause(
        u64,
        Vec<(String, i32)>,
        oneshot::Sender<Result<(), ConsumerError>>,
    ),
    Resume(
        u64,
        Vec<(String, i32)>,
        oneshot::Sender<Result<(), ConsumerError>>,
    ),
    Attach {
        previous_epoch: u64,
        message_credit: u32,
        byte_credit: u64,
        outbound: mpsc::Sender<OutboundFrame>,
        reply: oneshot::Sender<Result<ConsumerAttachState, ConsumerError>>,
    },
    Disconnect(u64),
    Close(ConsumeClose, oneshot::Sender<Result<(), ConsumerError>>),
}

#[derive(Clone)]
struct SubscriptionHandle {
    token: ResumeToken,
    session: ConsumerSession,
}

pub struct ConsumerManager {
    factory: Arc<dyn ConsumerSourceFactory>,
    subscriptions: Arc<Mutex<HashMap<u64, SubscriptionHandle>>>,
    next_id: AtomicU64,
    subscription_slots: Arc<Semaphore>,
    max_messages: u32,
    max_bytes: u64,
    resume_grace: Duration,
    budget: GlobalBudget,
    shutdown: ShutdownHandle,
    metrics: Arc<Metrics>,
}

pub type ConsumerHandle = Arc<ConsumerManager>;

pub struct ConsumerAttachment {
    pub ready: ConsumeReady,
    pub lease: u64,
    pub session: ConsumerSession,
}

struct ConsumerAttachState {
    ready: ConsumeReady,
    lease: u64,
}

#[derive(Clone)]
pub struct ConsumerSession {
    subscription_id: u64,
    commands: mpsc::Sender<ActorCommand>,
}

impl ConsumerSession {
    async fn command(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<(), ConsumerError>>) -> ActorCommand,
    ) -> Result<(), ConsumerError> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(command(reply))
            .await
            .map_err(|_| ConsumerError::ActorStopped)?;
        response.await.map_err(|_| ConsumerError::ActorStopped)?
    }

    fn check_subscription(&self, subscription_id: u64) -> Result<(), ConsumerError> {
        if subscription_id != self.subscription_id {
            return Err(ConsumerError::SubscriptionNotFound(subscription_id));
        }
        Ok(())
    }

    pub async fn ack(&self, value: ConsumeAck) -> Result<(), ConsumerError> {
        self.check_subscription(value.subscription_id)?;
        self.command(|reply| ActorCommand::Ack(value, reply)).await
    }

    pub async fn nack(&self, value: ConsumeNack) -> Result<(), ConsumerError> {
        self.check_subscription(value.subscription_id)?;
        self.command(|reply| ActorCommand::Nack(value, reply)).await
    }

    pub async fn flow(&self, value: FlowControl) -> Result<(), ConsumerError> {
        self.check_subscription(value.subscription_id)?;
        self.command(|reply| ActorCommand::Flow(value, reply)).await
    }

    pub async fn commit(&self, value: ConsumeCommit, cid: u64) -> Result<(), ConsumerError> {
        self.check_subscription(value.subscription_id)?;
        self.command(|reply| ActorCommand::Commit(value, cid, reply))
            .await
    }

    pub async fn close(&self, value: ConsumeClose) -> Result<(), ConsumerError> {
        self.check_subscription(value.subscription_id)?;
        self.command(|reply| ActorCommand::Close(value, reply))
            .await
    }

    pub async fn pause(&self, value: ConsumerPartitions) -> Result<(), ConsumerError> {
        self.check_subscription(value.subscription_id)?;
        self.command(|reply| ActorCommand::Pause(value.subscription_epoch, value.partitions, reply))
            .await
    }

    pub async fn resume_partitions(&self, value: ConsumerPartitions) -> Result<(), ConsumerError> {
        self.check_subscription(value.subscription_id)?;
        self.command(|reply| {
            ActorCommand::Resume(value.subscription_epoch, value.partitions, reply)
        })
        .await
    }

    pub async fn disconnect(&self, attachment_lease: u64) {
        let _ = self
            .commands
            .send(ActorCommand::Disconnect(attachment_lease))
            .await;
    }
}

impl ConsumerManager {
    pub(crate) fn new(
        factory: Arc<dyn ConsumerSourceFactory>,
        shutdown: ShutdownHandle,
        metrics: Arc<Metrics>,
    ) -> ConsumerHandle {
        let global_bytes = env_u32("HI_KAFKA_GLOBAL_IN_FLIGHT_BYTES", DEFAULT_GLOBAL_BYTES).max(1);
        Arc::new(Self {
            factory,
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            next_id: AtomicU64::new(1),
            subscription_slots: Arc::new(Semaphore::new(
                env_usize("HI_KAFKA_MAX_SUBSCRIPTIONS", DEFAULT_MAX_SUBSCRIPTIONS).max(1),
            )),
            max_messages: env_u32("HI_KAFKA_CONSUMER_MAX_MESSAGES", DEFAULT_SUB_MESSAGES).max(1),
            max_bytes: env_u64("HI_KAFKA_CONSUMER_MAX_BYTES", DEFAULT_SUB_BYTES).max(1),
            resume_grace: Duration::from_millis(env_u64(
                "HI_KAFKA_CONSUMER_RESUME_GRACE_MS",
                DEFAULT_RESUME_GRACE.as_millis() as u64,
            )),
            budget: GlobalBudget {
                messages: Arc::new(Semaphore::new(
                    env_u32(
                        "HI_KAFKA_GLOBAL_IN_FLIGHT_MESSAGES",
                        DEFAULT_GLOBAL_MESSAGES,
                    )
                    .max(1) as usize,
                )),
                bytes: Arc::new(Semaphore::new(global_bytes as usize)),
                byte_limit: global_bytes,
            },
            shutdown,
            metrics,
        })
    }

    pub async fn open(
        &self,
        request: ConsumeOpen,
        outbound: mpsc::Sender<OutboundFrame>,
    ) -> Result<ConsumerAttachment, ConsumerError> {
        validate_open(&request)?;
        let slot = self
            .subscription_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| ConsumerError::SubscriptionLimit)?;
        let max_messages = request.max_in_flight_messages.min(self.max_messages);
        let max_bytes = request
            .max_in_flight_bytes
            .min(self.max_bytes)
            .min(MAX_CONSUMER_WIRE_BYTES);
        let mut source_request = request.clone();
        source_request.initial_message_credit =
            source_request.initial_message_credit.min(max_messages);
        source_request.initial_byte_credit = source_request.initial_byte_credit.min(max_bytes);
        source_request.max_in_flight_messages = max_messages;
        source_request.max_in_flight_bytes = max_bytes;
        let source = self.factory.create(&source_request).await?;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let token = random_token()?;
        let core = SubscriptionCore::new(
            id,
            1,
            request.initial_message_credit.min(max_messages),
            request.initial_byte_credit.min(max_bytes),
            max_messages,
            max_bytes,
        )?;
        let (commands, receiver) =
            mpsc::channel((max_messages as usize).saturating_mul(2).clamp(16, 4096));
        let session = ConsumerSession {
            subscription_id: id,
            commands,
        };
        let mut subscriptions = self.subscriptions.lock().await;
        subscriptions.retain(|_, handle| !handle.session.commands.is_closed());
        subscriptions.insert(
            id,
            SubscriptionHandle {
                token,
                session: session.clone(),
            },
        );
        drop(subscriptions);

        let actor = SubscriptionActor {
            id,
            token,
            core,
            source,
            commands: receiver,
            outbound: Some(outbound),
            permits: HashMap::new(),
            blocked_delivery_bytes: None,
            blocked_global_bytes: None,
            assignment: BTreeSet::new(),
            user_paused: BTreeSet::new(),
            backpressure_paused: false,
            backpressure_started: None,
            attachment_lease: 1,
            budget: self.budget.clone(),
            grace: self.resume_grace,
            disconnected_at: None,
            shutdown: self.shutdown.clone(),
            metrics: self.metrics.clone(),
            _slot: slot,
        };
        Metrics::inc(&self.metrics.consumer_open_total);
        Metrics::inc(&self.metrics.consumer_active);
        tokio::spawn(actor.run());
        Ok(ConsumerAttachment {
            ready: ConsumeReady {
                subscription_id: id,
                subscription_epoch: 1,
                resume_token: token,
                max_in_flight_messages: max_messages,
                max_in_flight_bytes: max_bytes,
            },
            lease: 1,
            session,
        })
    }

    pub async fn resume(
        &self,
        request: hi_kafka_proto::ConsumeResume,
        outbound: mpsc::Sender<OutboundFrame>,
    ) -> Result<ConsumerAttachment, ConsumerError> {
        let handle = {
            let mut subscriptions = self.subscriptions.lock().await;
            let handle = subscriptions.get(&request.subscription_id).cloned();
            if handle
                .as_ref()
                .is_some_and(|value| value.session.commands.is_closed())
            {
                subscriptions.remove(&request.subscription_id);
                None
            } else {
                handle
            }
        }
        .ok_or(ConsumerError::SubscriptionNotFound(request.subscription_id))?;
        if !constant_time_eq(&handle.token.0, &request.resume_token.0) {
            return Err(ConsumerError::InvalidResumeToken);
        }
        let (reply, response) = oneshot::channel();
        handle
            .session
            .commands
            .send(ActorCommand::Attach {
                previous_epoch: request.previous_epoch,
                message_credit: request.initial_message_credit,
                byte_credit: request.initial_byte_credit,
                outbound,
                reply,
            })
            .await
            .map_err(|_| ConsumerError::SubscriptionNotFound(request.subscription_id))?;
        let state = response
            .await
            .map_err(|_| ConsumerError::SubscriptionNotFound(request.subscription_id))??;
        Metrics::inc(&self.metrics.consumer_resume_stream_total);
        Ok(ConsumerAttachment {
            ready: state.ready,
            lease: state.lease,
            session: handle.session,
        })
    }

    pub async fn active_subscriptions(&self) -> usize {
        let mut values = self.subscriptions.lock().await;
        values.retain(|_, handle| !handle.session.commands.is_closed());
        values.len()
    }

    /// One slot per possible in-flight delivery plus bounded control-frame
    /// headroom. This guarantees the actor never blocks before the negotiated
    /// delivery window is exhausted, while keeping the queue strictly bounded.
    pub fn outbound_capacity(&self) -> usize {
        (self.max_messages as usize).saturating_add(32).max(33)
    }
}

struct SubscriptionActor {
    id: u64,
    token: ResumeToken,
    core: SubscriptionCore,
    source: Box<dyn ConsumerSource>,
    commands: mpsc::Receiver<ActorCommand>,
    outbound: Option<mpsc::Sender<OutboundFrame>>,
    permits: HashMap<u64, GlobalPermit>,
    blocked_delivery_bytes: Option<u64>,
    blocked_global_bytes: Option<u64>,
    assignment: BTreeSet<PartitionKey>,
    user_paused: BTreeSet<PartitionKey>,
    backpressure_paused: bool,
    backpressure_started: Option<tokio::time::Instant>,
    attachment_lease: u64,
    budget: GlobalBudget,
    grace: Duration,
    disconnected_at: Option<tokio::time::Instant>,
    shutdown: ShutdownHandle,
    metrics: Arc<Metrics>,
    _slot: OwnedSemaphorePermit,
}

impl Drop for SubscriptionActor {
    fn drop(&mut self) {
        Metrics::sub(&self.metrics.consumer_active, 1);
        let in_flight = self.core.in_flight();
        Metrics::sub(
            &self.metrics.consumer_in_flight_messages,
            in_flight.messages as u64,
        );
        Metrics::sub(&self.metrics.consumer_in_flight_bytes, in_flight.bytes);
    }
}

impl SubscriptionActor {
    async fn run(mut self) {
        loop {
            if self.shutdown.is_draining() {
                let _ = self.push_close().await;
                return;
            }
            if let Some(at) = self.disconnected_at {
                let deadline = at + self.grace;
                tokio::select! {
                    command = self.commands.recv() => if !self.handle_command(command).await { return; },
                    _ = tokio::time::sleep_until(deadline) => return,
                    _ = self.shutdown.wait_draining() => return,
                }
                continue;
            }
            if let Some(notice) = self.source.take_notice() {
                if let Err(error) = self.handle_notice(notice).await {
                    Metrics::inc(&self.metrics.consumer_stream_errors_total);
                    warn!(
                        subscription_id = self.id,
                        ?error,
                        "consumer rebalance state lost"
                    );
                    let _ = self
                        .push_error(ErrorKind::Rebalance, error.to_string())
                        .await;
                    return;
                }
                continue;
            }
            if let Err(error) = self.sync_backpressure_pause().await {
                Metrics::inc(&self.metrics.consumer_stream_errors_total);
                let _ = self.push_response(error.response()).await;
                return;
            }
            if let Some(bytes) = self.blocked_global_bytes {
                tokio::select! {
                    command = self.commands.recv() => if !self.handle_command(command).await { return; },
                    available = self.budget.wait_until_available(bytes) => {
                        if let Err(error) = available {
                            Metrics::inc(&self.metrics.consumer_stream_errors_total);
                            let _ = self.push_response(error.response()).await;
                            return;
                        }
                        self.blocked_global_bytes = None;
                    }
                    _ = self.shutdown.wait_draining() => { let _ = self.push_close().await; return; }
                }
                continue;
            }
            if let Some(bytes) = self.blocked_delivery_bytes {
                if self.core.has_delivery_capacity(bytes) {
                    self.blocked_delivery_bytes = None;
                } else {
                    if !self.wait_for_command_or_pause_deadline().await {
                        return;
                    }
                    continue;
                }
            }
            let credit = self.core.available_credit();
            if self.outbound.is_none() || credit.0 == 0 || credit.1 == 0 {
                if !self.wait_for_command_or_pause_deadline().await {
                    return;
                }
                continue;
            }
            tokio::select! {
                command = self.commands.recv() => if !self.handle_command(command).await { return; },
                record = self.source.next_record(&self.budget) => {
                    match record {
                        Ok(SourceRead::Record(record, permit)) => match self.deliver(record, permit).await {
                            Ok(Some(bytes)) => self.blocked_delivery_bytes = Some(bytes),
                            Ok(None) => {},
                            Err(error) => {
                                Metrics::inc(&self.metrics.consumer_stream_errors_total);
                                warn!(subscription_id = self.id, ?error, "consumer delivery stopped");
                                let _ = self.push_response(error.response()).await;
                                return;
                            }
                        },
                        Ok(SourceRead::BudgetBlocked(partition, offset, wire_size)) => {
                            if let Err(error) = self.source.seek(&[(partition, offset)]).await {
                                Metrics::inc(&self.metrics.consumer_stream_errors_total);
                                let _ = self.push_response(error.response()).await;
                                return;
                            }
                            self.blocked_global_bytes = Some(wire_size);
                        }
                        Ok(SourceRead::RebalancePending { notices, record, blocked }) => {
                            for notice in notices {
                                if let Err(error) = self.handle_notice(notice).await {
                                    Metrics::inc(&self.metrics.consumer_stream_errors_total);
                                    let _ = self.push_response(error.response()).await;
                                    return;
                                }
                            }
                            if let Some((partition, offset, wire_size)) = blocked {
                                if let Err(error) = self.source.seek(&[(partition, offset)]).await {
                                    Metrics::inc(&self.metrics.consumer_stream_errors_total);
                                    let _ = self.push_response(error.response()).await;
                                    return;
                                }
                                self.blocked_global_bytes = Some(wire_size);
                            }
                            if let Some((record, permit)) = record {
                                match self.deliver(record, permit).await {
                                    Ok(Some(bytes)) => self.blocked_delivery_bytes = Some(bytes),
                                    Ok(None) => {},
                                    Err(error) => {
                                        Metrics::inc(&self.metrics.consumer_stream_errors_total);
                                        warn!(subscription_id = self.id, ?error, "consumer delivery stopped after rebalance");
                                        let _ = self.push_response(error.response()).await;
                                        return;
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            Metrics::inc(&self.metrics.consumer_stream_errors_total);
                            warn!(subscription_id = self.id, ?error, "consumer source stopped");
                            let _ = self.push_response(error.response()).await;
                            return;
                        }
                    }
                }
                _ = self.shutdown.wait_draining() => { let _ = self.push_close().await; return; }
            }
        }
    }

    async fn deliver(
        &mut self,
        record: SourceRecord,
        mut permit: GlobalPermit,
    ) -> Result<Option<u64>, ConsumerError> {
        let wire_size = record.wire_size();
        if wire_size > self.core.credit_limits().1 {
            self.source
                .seek(&[(
                    PartitionKey::new(record.topic, record.partition),
                    record.offset,
                )])
                .await?;
            return Err(ConsumerError::RecordTooLarge(format!(
                "record wire size {wire_size} exceeds subscription limit {}",
                self.core.credit_limits().1
            )));
        }
        if !self.core.has_delivery_capacity(wire_size) {
            self.source
                .seek(&[(
                    PartitionKey::new(record.topic, record.partition),
                    record.offset,
                )])
                .await?;
            return Ok(Some(wire_size));
        }
        let delivery_id = match self.core.reserve_delivery(
            PartitionKey::new(record.topic.clone(), record.partition),
            record.offset,
            wire_size,
        ) {
            Ok(id) => id,
            Err(error) => {
                self.source
                    .seek(&[(
                        PartitionKey::new(record.topic, record.partition),
                        record.offset,
                    )])
                    .await?;
                return Err(error.into());
            }
        };
        Metrics::inc(&self.metrics.consumer_in_flight_messages);
        Metrics::add(&self.metrics.consumer_in_flight_bytes, wire_size);
        let message = ConsumeMessage {
            subscription_id: self.id,
            subscription_epoch: self.core.epoch(),
            delivery_id,
            topic: record.topic,
            partition: record.partition,
            offset: record.offset,
            timestamp_ms: record.timestamp_ms,
            key: record.key,
            value: record.value,
            headers: record.headers,
            wire_size,
        };
        let frame = OutboundFrame::encoded(FrameType::ConsumeMessage, 0, |b| message.encode(b))?;
        drop(message);
        permit.finish_copy(wire_size);
        self.outbound
            .as_ref()
            .ok_or(ConsumerError::ActorStopped)?
            .send(frame)
            .await
            .map_err(|_| ConsumerError::ActorStopped)?;
        self.permits.insert(delivery_id, permit);
        Metrics::inc(&self.metrics.consumer_messages_total);
        Ok(None)
    }

    async fn handle_command(&mut self, command: Option<ActorCommand>) -> bool {
        let Some(command) = command else {
            return false;
        };
        match command {
            ActorCommand::Ack(value, reply) => {
                let result = self.core.ack(value).inspect(|ack| {
                    self.permits.remove(&value.delivery_id);
                    Metrics::inc(&self.metrics.consumer_acked_total);
                    Metrics::sub(
                        &self.metrics.consumer_in_flight_messages,
                        ack.released.messages as u64,
                    );
                    Metrics::sub(&self.metrics.consumer_in_flight_bytes, ack.released.bytes);
                });
                let result = result.map(|_| ()).map_err(Into::into);
                let _ = reply.send(result);
            }
            ActorCommand::Nack(value, reply) => {
                let result = self
                    .check_identity(value.subscription_id, value.subscription_epoch)
                    .and_then(|_| {
                        if self.permits.contains_key(&value.delivery_id) {
                            Ok(())
                        } else {
                            Err(ConsumerError::State(StreamStateError::Watermark(
                                WatermarkError::StaleDelivery(value.delivery_id),
                            )))
                        }
                    });
                if result.is_ok() {
                    Metrics::inc(&self.metrics.consumer_nacked_total);
                }
                let terminate = result.is_ok();
                let _ = reply.send(result);
                if terminate {
                    // NACK is terminal in every public driver. Dropping the
                    // actor leaves the group immediately and guarantees the
                    // next subscription starts from the broker-committed
                    // offset; keeping a resumable paused owner would strand
                    // its partitions for the entire grace window.
                    return false;
                }
            }
            ActorCommand::Flow(value, reply) => {
                let _ = reply.send(self.core.grant(value).map_err(Into::into));
            }
            ActorCommand::Commit(value, cid, reply) => {
                let result = self.check_identity(value.subscription_id, value.subscription_epoch);
                let result = if result.is_ok() {
                    self.commit(cid).await
                } else {
                    result
                };
                let _ = reply.send(result);
            }
            ActorCommand::Pause(epoch, partitions, reply) => {
                let result = self.check_identity(self.id, epoch);
                let result = if result.is_ok() {
                    self.user_pause(partitions).await
                } else {
                    result
                };
                if result.is_ok() {
                    Metrics::inc(&self.metrics.consumer_pause_total);
                }
                let _ = reply.send(result);
            }
            ActorCommand::Resume(epoch, partitions, reply) => {
                let result = self.check_identity(self.id, epoch);
                let result = if result.is_ok() {
                    self.user_resume(partitions).await
                } else {
                    result
                };
                if result.is_ok() {
                    Metrics::inc(&self.metrics.consumer_resume_total);
                }
                let _ = reply.send(result);
            }
            ActorCommand::Attach {
                previous_epoch,
                message_credit,
                byte_credit,
                outbound,
                reply,
            } => {
                let result = if self.disconnected_at.is_none()
                    || previous_epoch.saturating_add(1) != self.core.epoch()
                {
                    Err(ConsumerError::StaleEpoch {
                        got: previous_epoch,
                        current: self.core.epoch(),
                    })
                } else if self
                    .disconnected_at
                    .is_some_and(|at| at.elapsed() > self.grace)
                {
                    Err(ConsumerError::ResumeExpired)
                } else {
                    let limits = self.core.credit_limits();
                    let message_credit = message_credit.min(limits.0);
                    let byte_credit = byte_credit.min(limits.1);
                    let next_lease = self
                        .attachment_lease
                        .checked_add(1)
                        .ok_or(ConsumerError::ActorStopped);
                    next_lease.and_then(|next_lease| {
                        self.reset_epoch(self.core.epoch(), message_credit, byte_credit)?;
                        self.attachment_lease = next_lease;
                        self.outbound = Some(outbound);
                        self.disconnected_at = None;
                        Ok(ConsumerAttachState {
                            ready: ConsumeReady {
                                subscription_id: self.id,
                                subscription_epoch: self.core.epoch(),
                                resume_token: self.token,
                                max_in_flight_messages: limits.0,
                                max_in_flight_bytes: limits.1,
                            },
                            lease: next_lease,
                        })
                    })
                };
                let _ = reply.send(result);
            }
            ActorCommand::Disconnect(attachment_lease) => {
                if attachment_lease == self.attachment_lease && self.disconnected_at.is_none() {
                    let earliest = self.core.earliest_unacked();
                    if let Err(error) = self.pause_and_seek(&earliest).await {
                        Metrics::inc(&self.metrics.consumer_stream_errors_total);
                        let _ = self
                            .push_error(ErrorKind::OffsetInvalid, error.to_string())
                            .await;
                        return false;
                    }
                    if let Err(error) = self.reset_epoch(self.core.epoch().saturating_add(1), 0, 0)
                    {
                        let _ = self
                            .push_error(ErrorKind::Internal, error.to_string())
                            .await;
                        return false;
                    }
                    self.permits.clear();
                    self.outbound = None;
                    self.disconnected_at = Some(tokio::time::Instant::now());
                }
            }
            ActorCommand::Close(value, reply) => {
                let result = self.check_identity(value.subscription_id, value.subscription_epoch);
                let _ = reply.send(result);
                return false;
            }
        }
        true
    }

    async fn commit(&mut self, cid: u64) -> Result<(), ConsumerError> {
        let offsets = self.core.committable_offsets();
        let source_offsets = offsets
            .iter()
            .map(|o| {
                (
                    PartitionKey::new(o.topic.clone(), o.partition),
                    o.next_offset,
                )
            })
            .collect::<Vec<_>>();
        self.source.commit(&source_offsets).await?;
        Metrics::inc(&self.metrics.consumer_commit_total);
        if let Some(outbound) = &self.outbound {
            let value = ConsumeCommitted {
                subscription_id: self.id,
                subscription_epoch: self.core.epoch(),
                offsets,
            };
            outbound
                .send(OutboundFrame::encoded(
                    FrameType::ConsumeCommitted,
                    cid,
                    |b| value.encode(b),
                )?)
                .await
                .map_err(|_| ConsumerError::ActorStopped)?;
        }
        Ok(())
    }

    async fn handle_notice(&mut self, notice: SourceNotice) -> Result<(), ConsumerError> {
        debug!(subscription_id = self.id, epoch = self.core.epoch(), ?notice, assignment = ?self.assignment, "rebalance notice");
        let (kind, partitions) = match notice {
            SourceNotice::Assign(p) => {
                Metrics::inc(&self.metrics.consumer_rebalance_assign_total);
                // cooperative 协议下 Assign 只携带增量分区，必须合并而不是覆盖；
                // eager 协议的 Revoke 总是先清空整个 assignment，合并结果与覆盖相同。
                self.assignment.extend(
                    p.iter()
                        .map(|(topic, partition)| PartitionKey::new(topic.clone(), *partition)),
                );
                let paused = if self.backpressure_paused {
                    p.clone()
                } else {
                    p.iter()
                        .filter(|(topic, partition)| {
                            self.user_paused
                                .contains(&PartitionKey::new(topic.clone(), *partition))
                        })
                        .cloned()
                        .collect()
                };
                if !paused.is_empty() {
                    self.source.pause(&paused).await?;
                }
                (FrameType::RebalanceAssign, p)
            }
            SourceNotice::Revoke(p) => {
                Metrics::inc(&self.metrics.consumer_rebalance_revoke_total);
                let revoked = p
                    .iter()
                    .map(|(topic, partition)| PartitionKey::new(topic.clone(), *partition))
                    .collect::<Vec<_>>();
                for (topic, partition) in &p {
                    let key = PartitionKey::new(topic.clone(), *partition);
                    self.assignment.remove(&key);
                    self.user_paused.remove(&key);
                }
                // Cooperative rebalances may revoke only a subset. librdkafka's
                // fetch position already moved past delivered records, so seek
                // every retained partition back to its first unacked offset
                // before invalidating the epoch. Revoked partitions restart
                // from the broker-committed offset under their next owner.
                let retained = self
                    .core
                    .earliest_unacked()
                    .into_iter()
                    .filter(|(key, _)| {
                        !p.iter().any(|(topic, partition)| {
                            topic == &key.topic && *partition == key.partition
                        })
                    })
                    .collect::<Vec<_>>();
                self.source.seek(&retained).await?;
                self.reset_epoch(self.core.epoch().saturating_add(1), 0, 0)?;
                // Ownership has moved to another group member. Local ACKs
                // which were not broker-committed cannot follow the revoked
                // partition and must not reject its eventual redelivery.
                self.core.forget_partitions(&revoked);
                self.permits.clear();
                (FrameType::RebalanceRevoke, p)
            }
            SourceNotice::Fatal(message) => return Err(ConsumerError::RebalanceState(message)),
        };
        if let Some(outbound) = &self.outbound {
            let value = RebalanceNotice {
                subscription_id: self.id,
                subscription_epoch: self.core.epoch(),
                partitions,
            };
            outbound
                .send(OutboundFrame::encoded(kind, 0, |b| value.encode(b))?)
                .await
                .map_err(|_| ConsumerError::ActorStopped)?;
        }
        Ok(())
    }

    async fn push_close(&mut self) -> Result<(), ConsumerError> {
        if let Some(outbound) = &self.outbound {
            let close = ConsumeClose {
                subscription_id: self.id,
                subscription_epoch: self.core.epoch(),
            };
            outbound
                .send(OutboundFrame::encoded(FrameType::ConsumeClose, 0, |b| {
                    close.encode(b)
                })?)
                .await
                .map_err(|_| ConsumerError::ActorStopped)?;
        }
        Ok(())
    }

    fn backpressure_required(&self) -> bool {
        let credit = self.core.available_credit();
        self.outbound.is_none()
            || credit.0 == 0
            || credit.1 == 0
            || self.blocked_global_bytes.is_some()
            || self
                .blocked_delivery_bytes
                .is_some_and(|bytes| !self.core.has_delivery_capacity(bytes))
    }

    async fn sync_backpressure_pause(&mut self) -> Result<(), ConsumerError> {
        let required = self.backpressure_required();
        if !required {
            self.backpressure_started = None;
            if !self.backpressure_paused {
                return Ok(());
            }
            // backpressure pause 作用于 librdkafka 的真实 assignment（空列表）；没有用户
            // 暂停时也按真实 assignment 恢复，不依赖 actor 视图与 librdkafka 完全同步。
            if self.user_paused.is_empty() {
                debug!(
                    subscription_id = self.id,
                    "backpressure resume (full assignment)"
                );
                self.source.resume(&[]).await?;
            } else {
                let resumable = self
                    .assignment
                    .iter()
                    .filter(|partition| !self.user_paused.contains(*partition))
                    .map(|partition| (partition.topic.clone(), partition.partition))
                    .collect::<Vec<_>>();
                debug!(subscription_id = self.id, ?resumable, "backpressure resume");
                if !resumable.is_empty() {
                    self.source.resume(&resumable).await?;
                }
            }
            self.backpressure_paused = false;
            return Ok(());
        }
        if self.backpressure_paused {
            return Ok(());
        }
        let started = self
            .backpressure_started
            .get_or_insert_with(tokio::time::Instant::now);
        let immediate = self.outbound.is_none() || self.blocked_global_bytes.is_some();
        if !immediate && started.elapsed() < BACKPRESSURE_PAUSE_DEBOUNCE {
            return Ok(());
        }
        debug!(subscription_id = self.id, "backpressure pause");
        self.source.pause(&[]).await?;
        self.backpressure_paused = true;
        Ok(())
    }

    async fn pause_and_seek(
        &mut self,
        offsets: &[(PartitionKey, i64)],
    ) -> Result<(), ConsumerError> {
        self.source.pause(&[]).await?;
        self.backpressure_paused = true;
        self.backpressure_started = Some(tokio::time::Instant::now());
        self.source.seek(offsets).await
    }

    async fn wait_for_command_or_pause_deadline(&mut self) -> bool {
        if !self.backpressure_paused {
            if let Some(started) = self.backpressure_started {
                let deadline = started + BACKPRESSURE_PAUSE_DEBOUNCE;
                tokio::select! {
                    command = self.commands.recv() => return self.handle_command(command).await,
                    _ = tokio::time::sleep_until(deadline) => return true,
                    _ = self.shutdown.wait_draining() => {
                        let _ = self.push_close().await;
                        return false;
                    }
                }
            }
        }
        let command = self.commands.recv().await;
        self.handle_command(command).await
    }

    async fn user_pause(&mut self, partitions: Vec<(String, i32)>) -> Result<(), ConsumerError> {
        let targets = self.resolve_partitions(partitions);
        if !targets.is_empty() {
            self.source.pause(&targets).await?;
            self.user_paused.extend(
                targets
                    .iter()
                    .map(|(topic, partition)| PartitionKey::new(topic.clone(), *partition)),
            );
        }
        Ok(())
    }

    async fn user_resume(&mut self, partitions: Vec<(String, i32)>) -> Result<(), ConsumerError> {
        let targets = self.resolve_partitions(partitions);
        if !self.backpressure_paused && !targets.is_empty() {
            self.source.resume(&targets).await?;
        }
        for (topic, partition) in &targets {
            self.user_paused
                .remove(&PartitionKey::new(topic.clone(), *partition));
        }
        Ok(())
    }

    fn resolve_partitions(&self, partitions: Vec<(String, i32)>) -> Vec<(String, i32)> {
        if partitions.is_empty() {
            self.assignment
                .iter()
                .map(|partition| (partition.topic.clone(), partition.partition))
                .collect()
        } else {
            partitions
        }
    }

    async fn push_error(&mut self, kind: ErrorKind, message: String) -> Result<(), ConsumerError> {
        self.push_response(ErrorResp::new(kind, message)).await
    }

    async fn push_response(&mut self, error: ErrorResp) -> Result<(), ConsumerError> {
        if let Some(outbound) = &self.outbound {
            outbound
                .send(OutboundFrame::encoded(FrameType::Error, 0, |b| {
                    error.encode(b)
                })?)
                .await
                .map_err(|_| ConsumerError::ActorStopped)?;
        }
        Ok(())
    }

    fn reset_epoch(
        &mut self,
        epoch: u64,
        message_credit: u32,
        byte_credit: u64,
    ) -> Result<(), ConsumerError> {
        let released = self.core.reconnect(epoch, message_credit, byte_credit)?;
        Metrics::sub(
            &self.metrics.consumer_in_flight_messages,
            released.messages as u64,
        );
        Metrics::sub(&self.metrics.consumer_in_flight_bytes, released.bytes);
        Ok(())
    }

    fn check_identity(&self, id: u64, epoch: u64) -> Result<(), ConsumerError> {
        if id != self.id {
            return Err(ConsumerError::SubscriptionNotFound(id));
        }
        if epoch != self.core.epoch() {
            return Err(ConsumerError::StaleEpoch {
                got: epoch,
                current: self.core.epoch(),
            });
        }
        Ok(())
    }
}

struct LoggingFactory;
struct LoggingSource;

#[async_trait]
impl ConsumerSourceFactory for LoggingFactory {
    async fn create(
        &self,
        _request: &ConsumeOpen,
    ) -> Result<Box<dyn ConsumerSource>, ConsumerError> {
        Ok(Box::new(LoggingSource))
    }
}

#[async_trait]
impl ConsumerSource for LoggingSource {
    async fn next_record(&mut self, _budget: &GlobalBudget) -> Result<SourceRead, ConsumerError> {
        std::future::pending().await
    }
    fn take_notice(&mut self) -> Option<SourceNotice> {
        None
    }
    async fn commit(&mut self, _offsets: &[(PartitionKey, i64)]) -> Result<(), ConsumerError> {
        Ok(())
    }
    async fn seek(&mut self, _offsets: &[(PartitionKey, i64)]) -> Result<(), ConsumerError> {
        Ok(())
    }
    async fn pause(&mut self, _partitions: &[(String, i32)]) -> Result<(), ConsumerError> {
        Ok(())
    }
    async fn resume(&mut self, _partitions: &[(String, i32)]) -> Result<(), ConsumerError> {
        Ok(())
    }
}

pub fn logging(shutdown: ShutdownHandle, metrics: Arc<Metrics>) -> ConsumerHandle {
    ConsumerManager::new(Arc::new(LoggingFactory), shutdown, metrics)
}

#[cfg(feature = "kafka")]
pub fn kafka(
    registry: ClusterRegistryHandle,
    shutdown: ShutdownHandle,
    metrics: Arc<Metrics>,
) -> ConsumerHandle {
    ConsumerManager::new(
        Arc::new(real::KafkaSourceFactory::new(registry)),
        shutdown,
        metrics,
    )
}

fn validate_open(request: &ConsumeOpen) -> Result<(), ConsumerError> {
    if request.cluster.is_empty()
        || request.group_id.is_empty()
        || request.topics.is_empty()
        || request.topics.iter().any(String::is_empty)
    {
        return Err(ConsumerError::InvalidArgument(
            "cluster, group_id and topics must be non-empty".into(),
        ));
    }
    if request.max_in_flight_messages == 0 || request.max_in_flight_bytes == 0 {
        return Err(ConsumerError::InvalidArgument(
            "in-flight limits must be non-zero".into(),
        ));
    }
    Ok(())
}

fn random_token() -> Result<ResumeToken, ConsumerError> {
    let mut token = [0u8; hi_kafka_proto::RESUME_TOKEN_LEN];
    getrandom::fill(&mut token).map_err(|e| ConsumerError::Backend(anyhow::anyhow!(e)))?;
    Ok(ResumeToken(token))
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod actor_tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    enum Script {
        Record(SourceRecord),
        Notices(Vec<SourceNotice>),
    }

    #[derive(Default)]
    struct Calls {
        paused: Vec<Vec<(String, i32)>>,
        resumed: Vec<Vec<(String, i32)>>,
    }

    /// 由测试通过 channel 逐条喂入 record / rebalance notice 的 source，记录所有
    /// librdkafka 控制调用。`pause(&[])` 记为空列表，语义同真实实现的「全部 assignment」。
    struct ScriptedSource {
        script: mpsc::UnboundedReceiver<Script>,
        calls: Arc<StdMutex<Calls>>,
    }

    #[async_trait]
    impl ConsumerSource for ScriptedSource {
        async fn next_record(
            &mut self,
            budget: &GlobalBudget,
        ) -> Result<SourceRead, ConsumerError> {
            match self.script.recv().await {
                Some(Script::Record(record)) => {
                    let message = budget.acquire_message()?;
                    let permit = budget.acquire_bytes(message, record.wire_size())?;
                    Ok(SourceRead::Record(record, permit))
                }
                Some(Script::Notices(notices)) => Ok(SourceRead::RebalancePending {
                    notices,
                    record: None,
                    blocked: None,
                }),
                None => std::future::pending().await,
            }
        }
        fn take_notice(&mut self) -> Option<SourceNotice> {
            None
        }
        async fn commit(&mut self, _offsets: &[(PartitionKey, i64)]) -> Result<(), ConsumerError> {
            Ok(())
        }
        async fn seek(&mut self, _offsets: &[(PartitionKey, i64)]) -> Result<(), ConsumerError> {
            Ok(())
        }
        async fn pause(&mut self, partitions: &[(String, i32)]) -> Result<(), ConsumerError> {
            self.calls.lock().unwrap().paused.push(partitions.to_vec());
            Ok(())
        }
        async fn resume(&mut self, partitions: &[(String, i32)]) -> Result<(), ConsumerError> {
            self.calls.lock().unwrap().resumed.push(partitions.to_vec());
            Ok(())
        }
    }

    struct ScriptedFactory(StdMutex<Option<ScriptedSource>>);

    #[async_trait]
    impl ConsumerSourceFactory for ScriptedFactory {
        async fn create(
            &self,
            _request: &ConsumeOpen,
        ) -> Result<Box<dyn ConsumerSource>, ConsumerError> {
            Ok(Box::new(
                self.0.lock().unwrap().take().expect("single subscription"),
            ))
        }
    }

    struct Harness {
        script: mpsc::UnboundedSender<Script>,
        calls: Arc<StdMutex<Calls>>,
        outbound: mpsc::Receiver<OutboundFrame>,
        session: ConsumerSession,
        ready: ConsumeReady,
        _manager: ConsumerHandle,
    }

    async fn open() -> Harness {
        let (script, receiver) = mpsc::unbounded_channel();
        let calls = Arc::new(StdMutex::new(Calls::default()));
        let factory = ScriptedFactory(StdMutex::new(Some(ScriptedSource {
            script: receiver,
            calls: calls.clone(),
        })));
        let manager = ConsumerManager::new(
            Arc::new(factory),
            crate::shutdown::ShutdownState::new(),
            Metrics::new(),
        );
        let (tx, outbound) = mpsc::channel(manager.outbound_capacity());
        let attachment = manager
            .open(
                ConsumeOpen {
                    cluster: "c".into(),
                    group_id: "g".into(),
                    topics: vec!["t".into()],
                    config: vec![],
                    initial_message_credit: 1,
                    initial_byte_credit: 1 << 20,
                    max_in_flight_messages: 1,
                    max_in_flight_bytes: 1 << 20,
                    client_id: String::new(),
                },
                tx,
            )
            .await
            .unwrap();
        Harness {
            script,
            calls,
            outbound,
            session: attachment.session,
            ready: attachment.ready,
            _manager: manager,
        }
    }

    fn record(partition: i32, offset: i64) -> SourceRecord {
        SourceRecord {
            topic: "t".into(),
            partition,
            offset,
            timestamp_ms: 0,
            key: None,
            value: Some(Bytes::from_static(b"v")),
            headers: vec![],
        }
    }

    fn tp(partitions: &[i32]) -> Vec<(String, i32)> {
        partitions.iter().map(|p| ("t".to_string(), *p)).collect()
    }

    async fn next_frame(harness: &mut Harness, kind: FrameType) -> OutboundFrame {
        let frame = tokio::time::timeout(Duration::from_secs(2), harness.outbound.recv())
            .await
            .expect("frame deadline")
            .expect("outbound open");
        assert_eq!(frame.kind, kind);
        frame
    }

    async fn wait_until(calls: &Arc<StdMutex<Calls>>, check: impl Fn(&Calls) -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !check(&calls.lock().unwrap()) {
            assert!(tokio::time::Instant::now() < deadline, "condition deadline");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    async fn user_pause(h: &Harness, epoch: u64, partitions: &[i32]) {
        h.session
            .pause(ConsumerPartitions {
                subscription_id: h.ready.subscription_id,
                subscription_epoch: epoch,
                partitions: tp(partitions),
            })
            .await
            .unwrap();
    }

    async fn deliver_and_ack(h: &mut Harness, partition: i32) {
        h.script.send(Script::Record(record(partition, 7))).unwrap();
        let frame = next_frame(h, FrameType::ConsumeMessage).await;
        let message = ConsumeMessage::decode(&frame.payload).unwrap();
        // 唯一的 credit 已用完：actor 去抖后对整个 assignment 做 backpressure pause。
        wait_until(&h.calls, |c| c.paused.contains(&vec![])).await;
        h.session
            .ack(ConsumeAck {
                subscription_id: h.ready.subscription_id,
                subscription_epoch: message.subscription_epoch,
                delivery_id: message.delivery_id,
                message_credit: 1,
                byte_credit: message.wire_size,
            })
            .await
            .unwrap();
        wait_until(&h.calls, |c| !c.resumed.is_empty()).await;
    }

    /// cooperative-sticky 下 Assign notice 只携带增量分区。actor 视图必须合并，否则
    /// 存在用户暂停时按视图恢复会漏掉此前已拥有的分区，使其永久停在暂停状态。
    #[tokio::test]
    async fn incremental_assignment_is_merged_into_the_actor_view() {
        let mut h = open().await;
        h.script
            .send(Script::Notices(vec![SourceNotice::Assign(tp(&[0, 1]))]))
            .unwrap();
        next_frame(&mut h, FrameType::RebalanceAssign).await;
        h.script
            .send(Script::Notices(vec![SourceNotice::Assign(tp(&[2]))]))
            .unwrap();
        next_frame(&mut h, FrameType::RebalanceAssign).await;
        user_pause(&h, 1, &[1]).await;

        deliver_and_ack(&mut h, 0).await;

        assert_eq!(h.calls.lock().unwrap().resumed.last(), Some(&tp(&[0, 2])));
    }

    /// 部分 revoke 只移除被收回的分区，剩余分区在 backpressure 结束后照常恢复。
    #[tokio::test]
    async fn partial_revoke_keeps_retained_partitions_resumable() {
        let mut h = open().await;
        h.script
            .send(Script::Notices(vec![
                SourceNotice::Assign(tp(&[0, 1, 2])),
                SourceNotice::Revoke(tp(&[1])),
            ]))
            .unwrap();
        next_frame(&mut h, FrameType::RebalanceAssign).await;
        let revoke = next_frame(&mut h, FrameType::RebalanceRevoke).await;
        let epoch = RebalanceNotice::decode(&revoke.payload)
            .unwrap()
            .subscription_epoch;
        user_pause(&h, epoch, &[2]).await;

        // revoke 提升 epoch 后 credit 归零；补充 credit 触发 backpressure resume。
        wait_until(&h.calls, |c| c.paused.contains(&vec![])).await;
        h.session
            .flow(FlowControl {
                subscription_id: h.ready.subscription_id,
                subscription_epoch: epoch,
                message_credit: 1,
                byte_credit: 1 << 20,
            })
            .await
            .unwrap();
        wait_until(&h.calls, |c| !c.resumed.is_empty()).await;

        assert_eq!(h.calls.lock().unwrap().resumed.last(), Some(&tp(&[0])));
    }

    /// 没有用户暂停时按 librdkafka 的真实 assignment（空列表）恢复，与 pause 对称，
    /// 不依赖 actor 视图与 librdkafka 完全同步。
    #[tokio::test]
    async fn backpressure_resume_without_user_pause_targets_full_assignment() {
        let mut h = open().await;
        h.script
            .send(Script::Notices(vec![SourceNotice::Assign(tp(&[0, 1]))]))
            .unwrap();
        next_frame(&mut h, FrameType::RebalanceAssign).await;

        deliver_and_ack(&mut h, 0).await;

        assert_eq!(h.calls.lock().unwrap().resumed.last(), Some(&vec![]));
    }
}

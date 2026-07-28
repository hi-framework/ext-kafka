//! Consumer delivery ownership and contiguous ACK watermarks.
//!
//! This module is deliberately independent from librdkafka and IPC. The
//! subscription actor is the sole caller, so offset correctness can be tested
//! exhaustively without a broker or PHP runtime.

use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionKey {
    pub topic: String,
    pub partition: i32,
}

impl PartitionKey {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Delivery {
    partition: PartitionKey,
    offset: i64,
    bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PartitionWatermark {
    /// Delivered offsets in broker order and whether each was ACKed. Kafka
    /// offsets are not necessarily adjacent (compaction/control records), so
    /// contiguity means delivery order, never integer `offset + 1` adjacency.
    pending: BTreeMap<i64, bool>,
    last_delivered: Option<i64>,
    /// First offset that must be fetched again after epoch invalidation.
    resume_next: Option<i64>,
    /// Exact Kafka next offset eligible for store/commit.
    committable_next: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InFlightLimits {
    pub messages: usize,
    pub bytes: u64,
}

impl InFlightLimits {
    pub fn validate(self) -> Result<Self, WatermarkError> {
        if self.messages == 0 || self.bytes == 0 {
            return Err(WatermarkError::InvalidLimits);
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Released {
    pub messages: usize,
    pub bytes: u64,
}

/// Sending permission negotiated with the PHP stream owner. Credits and
/// in-flight permits are separate: ACK releases memory ownership, while only
/// an explicit grant allows another delivery to be sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreditWindow {
    available_messages: u32,
    available_bytes: u64,
    max_messages: u32,
    max_bytes: u64,
}

impl CreditWindow {
    pub fn new(
        initial_messages: u32,
        initial_bytes: u64,
        max_messages: u32,
        max_bytes: u64,
    ) -> Result<Self, CreditError> {
        if max_messages == 0 || max_bytes == 0 {
            return Err(CreditError::InvalidLimits);
        }
        if initial_messages > max_messages || initial_bytes > max_bytes {
            return Err(CreditError::ExceedsNegotiatedWindow);
        }
        Ok(Self {
            available_messages: initial_messages,
            available_bytes: initial_bytes,
            max_messages,
            max_bytes,
        })
    }

    pub fn available(&self) -> (u32, u64) {
        (self.available_messages, self.available_bytes)
    }

    pub fn limits(&self) -> (u32, u64) {
        (self.max_messages, self.max_bytes)
    }

    pub fn reset(&mut self, messages: u32, bytes: u64) -> Result<(), CreditError> {
        if messages > self.max_messages || bytes > self.max_bytes {
            return Err(CreditError::ExceedsNegotiatedWindow);
        }
        self.available_messages = messages;
        self.available_bytes = bytes;
        Ok(())
    }

    pub fn consume(&mut self, bytes: u64) -> Result<(), CreditError> {
        if self.available_messages == 0 {
            return Err(CreditError::MessagesExhausted);
        }
        if bytes > self.available_bytes {
            return Err(CreditError::BytesExhausted);
        }
        self.available_messages -= 1;
        self.available_bytes -= bytes;
        Ok(())
    }

    pub fn grant(&mut self, messages: u32, bytes: u64) -> Result<(), CreditError> {
        self.validate_grant(messages, bytes)?;
        self.available_messages += messages;
        self.available_bytes += bytes;
        Ok(())
    }

    pub fn validate_grant(&self, messages: u32, bytes: u64) -> Result<(), CreditError> {
        let next_messages = self
            .available_messages
            .checked_add(messages)
            .ok_or(CreditError::Overflow)?;
        let next_bytes = self
            .available_bytes
            .checked_add(bytes)
            .ok_or(CreditError::Overflow)?;
        if next_messages > self.max_messages || next_bytes > self.max_bytes {
            return Err(CreditError::ExceedsNegotiatedWindow);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CreditError {
    #[error("credit message and byte limits must be non-zero")]
    InvalidLimits,
    #[error("message credit exhausted")]
    MessagesExhausted,
    #[error("byte credit exhausted")]
    BytesExhausted,
    #[error("credit arithmetic overflow")]
    Overflow,
    #[error("credit exceeds the negotiated window")]
    ExceedsNegotiatedWindow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckResult {
    pub released: Released,
    pub advanced_to: Option<(PartitionKey, i64)>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WatermarkError {
    #[error("in-flight message and byte limits must be non-zero")]
    InvalidLimits,
    #[error("stale subscription epoch: got {got}, current {current}")]
    StaleEpoch { got: u64, current: u64 },
    #[error("delivery {0} is unknown or no longer valid")]
    StaleDelivery(u64),
    #[error("delivery id {0} already exists")]
    DuplicateDelivery(u64),
    #[error(
        "record offset moved backwards for {partition:?}: got {got}, expected at least {expected}"
    )]
    OffsetMovedBackwards {
        partition: PartitionKey,
        got: i64,
        expected: i64,
    },
    #[error("record is larger than the subscription byte limit: {record_bytes} > {limit}")]
    RecordTooLarge { record_bytes: u64, limit: u64 },
    #[error("subscription in-flight message credit exhausted")]
    MessageLimit,
    #[error("subscription in-flight byte credit exhausted")]
    ByteLimit,
    #[error("in-flight byte accounting overflow")]
    ByteOverflow,
}

#[derive(Debug)]
pub struct AckWatermarks {
    epoch: u64,
    limits: InFlightLimits,
    in_flight_bytes: u64,
    deliveries: HashMap<u64, Delivery>,
    partitions: HashMap<PartitionKey, PartitionWatermark>,
}

impl AckWatermarks {
    pub fn new(epoch: u64, limits: InFlightLimits) -> Result<Self, WatermarkError> {
        Ok(Self {
            epoch,
            limits: limits.validate()?,
            in_flight_bytes: 0,
            deliveries: HashMap::new(),
            partitions: HashMap::new(),
        })
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn in_flight(&self) -> Released {
        Released {
            messages: self.deliveries.len(),
            bytes: self.in_flight_bytes,
        }
    }

    /// Reserves ownership before a record is detached from librdkafka.
    pub fn deliver(
        &mut self,
        epoch: u64,
        delivery_id: u64,
        partition: PartitionKey,
        offset: i64,
        bytes: u64,
    ) -> Result<(), WatermarkError> {
        self.check_epoch(epoch)?;
        if bytes > self.limits.bytes {
            return Err(WatermarkError::RecordTooLarge {
                record_bytes: bytes,
                limit: self.limits.bytes,
            });
        }
        if self.deliveries.len() >= self.limits.messages {
            return Err(WatermarkError::MessageLimit);
        }
        let next_bytes = self
            .in_flight_bytes
            .checked_add(bytes)
            .ok_or(WatermarkError::ByteOverflow)?;
        if next_bytes > self.limits.bytes {
            return Err(WatermarkError::ByteLimit);
        }
        if self.deliveries.contains_key(&delivery_id) {
            return Err(WatermarkError::DuplicateDelivery(delivery_id));
        }

        let watermark = self.partitions.entry(partition.clone()).or_default();
        match watermark.last_delivered {
            Some(previous) if offset <= previous => {
                return Err(WatermarkError::OffsetMovedBackwards {
                    partition,
                    got: offset,
                    expected: previous.saturating_add(1),
                });
            }
            Some(_) => {}
            None if watermark.committable_next.is_some_and(|next| offset < next) => {
                return Err(WatermarkError::OffsetMovedBackwards {
                    partition,
                    got: offset,
                    expected: watermark.committable_next.unwrap(),
                });
            }
            None => {}
        }
        watermark.last_delivered = Some(offset);
        watermark.resume_next = None;
        watermark.pending.insert(offset, false);

        self.in_flight_bytes = next_bytes;
        self.deliveries.insert(
            delivery_id,
            Delivery {
                partition,
                offset,
                bytes,
            },
        );
        Ok(())
    }

    pub fn ack(&mut self, epoch: u64, delivery_id: u64) -> Result<AckResult, WatermarkError> {
        self.check_epoch(epoch)?;
        let delivery = self
            .deliveries
            .remove(&delivery_id)
            .ok_or(WatermarkError::StaleDelivery(delivery_id))?;
        self.in_flight_bytes -= delivery.bytes;

        let watermark = self
            .partitions
            .get_mut(&delivery.partition)
            .expect("delivery partition must have watermark state");
        *watermark
            .pending
            .get_mut(&delivery.offset)
            .expect("delivery offset must have partition state") = true;

        let mut advanced = false;
        while let Some((&offset, &acked)) = watermark.pending.first_key_value() {
            if !acked {
                break;
            }
            watermark.pending.pop_first();
            watermark.committable_next = Some(offset.saturating_add(1));
            advanced = true;
        }

        Ok(AckResult {
            released: Released {
                messages: 1,
                bytes: delivery.bytes,
            },
            advanced_to: advanced.then(|| {
                (
                    delivery.partition,
                    watermark
                        .committable_next
                        .expect("advanced watermark has next offset"),
                )
            }),
        })
    }

    /// Invalidates every outstanding delivery while retaining only contiguous
    /// processed watermarks. ACKs above a gap are intentionally forgotten and
    /// must be redelivered in the new epoch.
    pub fn invalidate_epoch(&mut self, new_epoch: u64) -> Released {
        let released = self.in_flight();
        self.epoch = new_epoch;
        self.in_flight_bytes = 0;
        self.deliveries.clear();
        for state in self.partitions.values_mut() {
            state.resume_next = state
                .pending
                .first_key_value()
                .map(|(&offset, _)| offset)
                .or(state.committable_next);
            state.pending.clear();
            state.last_delivered = state.committable_next.map(|next| next.saturating_sub(1));
        }
        released
    }

    pub fn committable_offsets(&self) -> Vec<(PartitionKey, i64)> {
        let mut offsets: Vec<_> = self
            .partitions
            .iter()
            .filter_map(|(partition, state)| {
                state
                    .committable_next
                    .map(|offset| (partition.clone(), offset))
            })
            .collect();
        offsets.sort_by(|a, b| a.0.cmp(&b.0));
        offsets
    }

    pub fn earliest_unacked(&self) -> Vec<(PartitionKey, i64)> {
        let mut offsets: Vec<_> = self
            .partitions
            .iter()
            .filter_map(|(partition, state)| {
                state
                    .pending
                    .first_key_value()
                    .map(|(&offset, _)| offset)
                    .or(state.resume_next)
                    .or(state.committable_next)
                    .map(|offset| (partition.clone(), offset))
            })
            .collect();
        offsets.sort_by(|a, b| a.0.cmp(&b.0));
        offsets
    }

    /// Drops every local offset claim for partitions whose Kafka ownership was
    /// revoked. Their ACKed-but-uncommitted progress is deliberately lost:
    /// when a partition is assigned again, the broker-committed offset is the
    /// only authoritative restart point.
    pub fn forget_partitions(&mut self, partitions: &[PartitionKey]) {
        for partition in partitions {
            self.partitions.remove(partition);
        }
    }

    fn check_epoch(&self, epoch: u64) -> Result<(), WatermarkError> {
        if epoch != self.epoch {
            return Err(WatermarkError::StaleEpoch {
                got: epoch,
                current: self.epoch,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tracker() -> AckWatermarks {
        AckWatermarks::new(
            1,
            InFlightLimits {
                messages: 4,
                bytes: 100,
            },
        )
        .unwrap()
    }

    #[test]
    fn out_of_order_ack_never_crosses_gap() {
        let mut tracker = tracker();
        let p = PartitionKey::new("events", 0);
        tracker.deliver(1, 10, p.clone(), 50, 10).unwrap();
        tracker.deliver(1, 11, p.clone(), 51, 10).unwrap();

        let second = tracker.ack(1, 11).unwrap();
        assert_eq!(second.advanced_to, None);
        assert!(tracker.committable_offsets().is_empty());

        let first = tracker.ack(1, 10).unwrap();
        assert_eq!(first.advanced_to, Some((p.clone(), 52)));
        assert_eq!(tracker.committable_offsets(), vec![(p, 52)]);
    }

    #[test]
    fn kafka_offset_holes_do_not_create_phantom_ack_gaps() {
        let mut tracker = tracker();
        let p = PartitionKey::new("compacted", 0);
        tracker.deliver(1, 1, p.clone(), 5, 10).unwrap();
        tracker.deliver(1, 2, p.clone(), 7, 10).unwrap();

        tracker.ack(1, 2).unwrap();
        assert!(tracker.committable_offsets().is_empty());
        tracker.ack(1, 1).unwrap();

        assert_eq!(tracker.committable_offsets(), vec![(p, 8)]);
    }

    #[test]
    fn epoch_invalidation_forgets_ack_above_gap() {
        let mut tracker = tracker();
        let p = PartitionKey::new("events", 0);
        tracker.deliver(1, 1, p.clone(), 7, 20).unwrap();
        tracker.deliver(1, 2, p.clone(), 8, 20).unwrap();
        tracker.ack(1, 2).unwrap();

        assert_eq!(
            tracker.invalidate_epoch(2),
            Released {
                messages: 1,
                bytes: 20
            }
        );
        assert_eq!(tracker.earliest_unacked(), vec![(p.clone(), 7)]);
        assert!(tracker.committable_offsets().is_empty());
        assert!(matches!(
            tracker.ack(1, 1),
            Err(WatermarkError::StaleEpoch { .. })
        ));

        tracker.deliver(2, 3, p.clone(), 7, 20).unwrap();
        tracker.deliver(2, 4, p.clone(), 8, 20).unwrap();
        tracker.ack(2, 3).unwrap();
        tracker.ack(2, 4).unwrap();
        assert_eq!(tracker.committable_offsets(), vec![(p, 9)]);
    }

    #[test]
    fn revoked_partition_forgets_uncommitted_local_watermark() {
        let mut tracker = tracker();
        let p = PartitionKey::new("events", 0);
        tracker.deliver(1, 1, p.clone(), 1, 10).unwrap();
        tracker.ack(1, 1).unwrap();
        tracker.deliver(1, 2, p.clone(), 2, 10).unwrap();
        tracker.ack(1, 2).unwrap();
        assert_eq!(tracker.committable_offsets(), vec![(p.clone(), 3)]);

        tracker.invalidate_epoch(2);
        tracker.forget_partitions(std::slice::from_ref(&p));

        assert!(tracker.committable_offsets().is_empty());
        tracker.deliver(2, 3, p.clone(), 1, 10).unwrap();
        tracker.ack(2, 3).unwrap();
        assert_eq!(tracker.committable_offsets(), vec![(p, 2)]);
    }

    #[test]
    fn enforces_count_and_byte_limits_without_eviction() {
        let mut tracker = AckWatermarks::new(
            1,
            InFlightLimits {
                messages: 2,
                bytes: 25,
            },
        )
        .unwrap();
        tracker
            .deliver(1, 1, PartitionKey::new("a", 0), 0, 10)
            .unwrap();
        tracker
            .deliver(1, 2, PartitionKey::new("b", 0), 0, 10)
            .unwrap();
        assert_eq!(
            tracker.deliver(1, 3, PartitionKey::new("c", 0), 0, 1),
            Err(WatermarkError::MessageLimit)
        );
        tracker.ack(1, 1).unwrap();
        assert_eq!(
            tracker.deliver(1, 3, PartitionKey::new("c", 0), 0, 20),
            Err(WatermarkError::ByteLimit)
        );
        assert_eq!(tracker.in_flight().messages, 1);
    }

    #[test]
    fn rejects_oversized_and_duplicate_delivery() {
        let mut tracker = tracker();
        let p = PartitionKey::new("events", 0);
        assert!(matches!(
            tracker.deliver(1, 1, p.clone(), 0, 101),
            Err(WatermarkError::RecordTooLarge { .. })
        ));
        tracker.deliver(1, 1, p.clone(), 0, 10).unwrap();
        assert_eq!(
            tracker.deliver(1, 1, p, 1, 10),
            Err(WatermarkError::DuplicateDelivery(1))
        );
    }

    #[test]
    fn credit_requires_explicit_bounded_replenishment() {
        let mut credit = CreditWindow::new(2, 20, 2, 20).unwrap();
        credit.consume(12).unwrap();
        assert_eq!(credit.available(), (1, 8));
        assert_eq!(credit.consume(9), Err(CreditError::BytesExhausted));
        credit.consume(8).unwrap();
        assert_eq!(credit.consume(0), Err(CreditError::MessagesExhausted));

        credit.grant(1, 12).unwrap();
        assert_eq!(credit.available(), (1, 12));
        assert_eq!(
            credit.grant(2, 0),
            Err(CreditError::ExceedsNegotiatedWindow)
        );
    }
}

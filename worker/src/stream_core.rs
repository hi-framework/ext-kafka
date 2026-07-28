//! Single-owner state used by every v2 subscription actor.

use hi_kafka_proto::{CommittedOffset, ConsumeAck, FlowControl};

use crate::watermark::{
    AckResult, AckWatermarks, CreditError, CreditWindow, InFlightLimits, PartitionKey, Released,
    WatermarkError,
};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StreamStateError {
    #[error("request targets subscription {got}, owner is {current}")]
    WrongSubscription { got: u64, current: u64 },
    #[error(transparent)]
    Credit(#[from] CreditError),
    #[error(transparent)]
    Watermark(#[from] WatermarkError),
    #[error("delivery id exhausted")]
    DeliveryIdExhausted,
}

#[derive(Debug)]
pub struct SubscriptionCore {
    subscription_id: u64,
    next_delivery_id: u64,
    credits: CreditWindow,
    watermarks: AckWatermarks,
}

impl SubscriptionCore {
    pub fn new(
        subscription_id: u64,
        epoch: u64,
        initial_message_credit: u32,
        initial_byte_credit: u64,
        max_in_flight_messages: u32,
        max_in_flight_bytes: u64,
    ) -> Result<Self, StreamStateError> {
        Ok(Self {
            subscription_id,
            next_delivery_id: 1,
            credits: CreditWindow::new(
                initial_message_credit,
                initial_byte_credit,
                max_in_flight_messages,
                max_in_flight_bytes,
            )?,
            watermarks: AckWatermarks::new(
                epoch,
                InFlightLimits {
                    messages: max_in_flight_messages as usize,
                    bytes: max_in_flight_bytes,
                },
            )?,
        })
    }

    pub fn subscription_id(&self) -> u64 {
        self.subscription_id
    }

    pub fn epoch(&self) -> u64 {
        self.watermarks.epoch()
    }

    pub fn available_credit(&self) -> (u32, u64) {
        self.credits.available()
    }

    pub fn credit_limits(&self) -> (u32, u64) {
        self.credits.limits()
    }

    pub fn in_flight(&self) -> Released {
        self.watermarks.in_flight()
    }

    pub fn has_delivery_capacity(&self, wire_bytes: u64) -> bool {
        let (credit_messages, credit_bytes) = self.credits.available();
        let (max_messages, max_bytes) = self.credits.limits();
        let in_flight = self.watermarks.in_flight();
        credit_messages > 0
            && credit_bytes >= wire_bytes
            && in_flight.messages < max_messages as usize
            && in_flight
                .bytes
                .checked_add(wire_bytes)
                .is_some_and(|bytes| bytes <= max_bytes)
    }

    /// Atomically consumes stream credit and reserves in-flight ownership.
    /// Call this after its exact wire size is known and before publishing it.
    pub fn reserve_delivery(
        &mut self,
        partition: PartitionKey,
        offset: i64,
        wire_bytes: u64,
    ) -> Result<u64, StreamStateError> {
        let delivery_id = self.next_delivery_id;
        let next_delivery_id = delivery_id
            .checked_add(1)
            .ok_or(StreamStateError::DeliveryIdExhausted)?;

        self.credits.consume(wire_bytes)?;
        if let Err(error) =
            self.watermarks
                .deliver(self.epoch(), delivery_id, partition, offset, wire_bytes)
        {
            // This grant exactly reverses the consume and therefore cannot
            // exceed the negotiated window.
            self.credits
                .grant(1, wire_bytes)
                .expect("credit rollback must fit");
            return Err(error.into());
        }
        self.next_delivery_id = next_delivery_id;
        Ok(delivery_id)
    }

    /// ACK and its optional credit delta are one atomic protocol operation.
    /// Validate the credit delta first so a malformed grant cannot advance an
    /// offset and then report an error.
    pub fn ack(&mut self, ack: ConsumeAck) -> Result<AckResult, StreamStateError> {
        self.check_subscription(ack.subscription_id)?;
        self.credits
            .validate_grant(ack.message_credit, ack.byte_credit)?;
        let result = self
            .watermarks
            .ack(ack.subscription_epoch, ack.delivery_id)?;
        self.credits
            .grant(ack.message_credit, ack.byte_credit)
            .expect("validated credit grant must succeed");
        Ok(result)
    }

    pub fn grant(&mut self, flow: FlowControl) -> Result<(), StreamStateError> {
        self.check_subscription(flow.subscription_id)?;
        if flow.subscription_epoch != self.epoch() {
            // Rebalance notices and client control frames cross on a
            // full-duplex stream. A grant for an invalidated epoch owns no
            // business result and must not affect (or terminate) the current
            // epoch. The client replenishes again after its next notice.
            return Ok(());
        }
        self.credits.grant(flow.message_credit, flow.byte_credit)?;
        Ok(())
    }

    pub fn committable_offsets(&self) -> Vec<CommittedOffset> {
        self.watermarks
            .committable_offsets()
            .into_iter()
            .map(|(partition, next_offset)| CommittedOffset {
                topic: partition.topic,
                partition: partition.partition,
                next_offset,
            })
            .collect()
    }

    /// Starts a new delivery epoch. All old delivery IDs and ACKs become
    /// invalid; only the contiguous processed watermark survives.
    pub fn reconnect(
        &mut self,
        new_epoch: u64,
        initial_message_credit: u32,
        initial_byte_credit: u64,
    ) -> Result<Released, StreamStateError> {
        self.credits
            .reset(initial_message_credit, initial_byte_credit)?;
        let released = self.watermarks.invalidate_epoch(new_epoch);
        Ok(released)
    }

    pub fn earliest_unacked(&self) -> Vec<(PartitionKey, i64)> {
        self.watermarks.earliest_unacked()
    }

    pub fn forget_partitions(&mut self, partitions: &[PartitionKey]) {
        self.watermarks.forget_partitions(partitions);
    }

    fn check_subscription(&self, got: u64) -> Result<(), StreamStateError> {
        if got != self.subscription_id {
            return Err(StreamStateError::WrongSubscription {
                got,
                current: self.subscription_id,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn core() -> SubscriptionCore {
        SubscriptionCore::new(10, 1, 2, 100, 2, 100).unwrap()
    }

    #[test]
    fn ack_atomically_releases_and_replenishes() {
        let mut core = core();
        let delivery = core
            .reserve_delivery(PartitionKey::new("events", 0), 40, 25)
            .unwrap();
        assert_eq!(core.available_credit(), (1, 75));
        assert_eq!(core.in_flight().bytes, 25);

        let result = core
            .ack(ConsumeAck {
                subscription_id: 10,
                subscription_epoch: 1,
                delivery_id: delivery,
                message_credit: 1,
                byte_credit: 25,
            })
            .unwrap();
        assert_eq!(result.advanced_to.unwrap().1, 41);
        assert_eq!(core.available_credit(), (2, 100));
        assert_eq!(core.in_flight().bytes, 0);
    }

    #[test]
    fn invalid_credit_does_not_ack() {
        let mut core = core();
        let delivery = core
            .reserve_delivery(PartitionKey::new("events", 0), 40, 25)
            .unwrap();
        let error = core
            .ack(ConsumeAck {
                subscription_id: 10,
                subscription_epoch: 1,
                delivery_id: delivery,
                message_credit: 2,
                byte_credit: 25,
            })
            .unwrap_err();
        assert!(matches!(
            error,
            StreamStateError::Credit(CreditError::ExceedsNegotiatedWindow)
        ));
        assert!(core.committable_offsets().is_empty());
        assert_eq!(core.in_flight().messages, 1);
    }

    #[test]
    fn delivery_capacity_tracks_credit_and_in_flight_ownership() {
        let mut core = core();
        assert!(core.has_delivery_capacity(60));
        let first = core
            .reserve_delivery(PartitionKey::new("events", 0), 40, 60)
            .unwrap();
        assert!(!core.has_delivery_capacity(50));
        assert!(core.has_delivery_capacity(40));

        core.ack(ConsumeAck {
            subscription_id: 10,
            subscription_epoch: 1,
            delivery_id: first,
            message_credit: 1,
            byte_credit: 60,
        })
        .unwrap();
        assert!(core.has_delivery_capacity(100));
    }

    #[test]
    fn reconnect_rejects_old_ack_and_preserves_only_contiguous_offset() {
        let mut core = core();
        let first = core
            .reserve_delivery(PartitionKey::new("events", 0), 40, 10)
            .unwrap();
        let second = core
            .reserve_delivery(PartitionKey::new("events", 0), 41, 10)
            .unwrap();
        core.ack(ConsumeAck {
            subscription_id: 10,
            subscription_epoch: 1,
            delivery_id: second,
            message_credit: 0,
            byte_credit: 0,
        })
        .unwrap();

        core.reconnect(2, 2, 100).unwrap();
        assert!(core.committable_offsets().is_empty());
        assert!(matches!(
            core.ack(ConsumeAck {
                subscription_id: 10,
                subscription_epoch: 1,
                delivery_id: first,
                message_credit: 0,
                byte_credit: 0,
            }),
            Err(StreamStateError::Watermark(
                WatermarkError::StaleEpoch { .. }
            ))
        ));
    }

    #[test]
    fn stale_flow_is_discarded_without_mutating_current_epoch() {
        let mut core = core();
        core.reconnect(2, 0, 0).unwrap();

        core.grant(FlowControl {
            subscription_id: 10,
            subscription_epoch: 1,
            message_credit: 2,
            byte_credit: 100,
        })
        .unwrap();

        assert_eq!(core.epoch(), 2);
        assert_eq!(core.available_credit(), (0, 0));
    }
}

//! Event delivery decisions. SQLite adapters supply durable facts and apply
//! each returned plan in a single transaction; clocks and transport stay out
//! of this module.

pub const RESERVED_CLASS: &str = "__EventStream";
/// Base64 envelopes may use two bytes per character in the retained codec.
/// Leave room below its 64 MiB ceiling for framing and grant metadata.
pub const MAX_PROTOCOL_BATCH_BYTES: usize = 16 << 20;
pub const MAX_PROTOCOL_BATCH_RECORDS: usize = 10_000;

pub fn retry_delay_ms(attempt: u32, random: u64) -> i64 {
    let cap = 100u64.saturating_mul(1u64 << attempt.min(10)).min(60_000);
    (random % (cap + 1)) as i64
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    InvalidSequence,
    SequenceGap { expected: i64 },
    InvalidCursor,
    CursorExpired { earliest: i64 },
    SubscriptionReset,
    StaleReceipt,
    LeaseExpired,
    InvalidLease,
    DeadlineOverflow,
    InvalidLimits,
    SubscriptionPaused,
    LastRequiredSubscription,
    PendingDelivery,
}

impl std::fmt::Display for Failure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SequenceGap { expected } => {
                write!(formatter, "event sequence gap; expected {expected}")
            }
            Self::CursorExpired { earliest } => write!(
                formatter,
                "event cursor expired; earliest offset is {earliest}"
            ),
            Self::InvalidSequence => formatter.write_str("invalid event sequence"),
            Self::InvalidCursor => formatter.write_str("invalid event cursor"),
            Self::SubscriptionReset => formatter.write_str("subscription incarnation changed"),
            Self::StaleReceipt => formatter.write_str("stale delivery receipt"),
            Self::LeaseExpired => formatter.write_str("delivery lease expired"),
            Self::InvalidLease => formatter.write_str("invalid delivery lease"),
            Self::DeadlineOverflow => formatter.write_str("delivery deadline overflow"),
            Self::InvalidLimits => formatter.write_str("invalid event delivery limits"),
            Self::SubscriptionPaused => formatter.write_str("subscription is paused"),
            Self::LastRequiredSubscription => {
                formatter.write_str("cannot delete the last required subscription")
            }
            Self::PendingDelivery => {
                formatter.write_str("subscription still has unprocessed events")
            }
        }
    }
}

impl std::error::Error for Failure {}

/// Host API limits; resource retention and durable capacity remain separate
/// stream configuration. Counts leave room for signed SQLite identity fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub max_event_bytes: usize,
    pub max_key_bytes: usize,
    pub max_family_bytes: usize,
    pub max_outbox_rows: usize,
    pub max_outbox_bytes: usize,
    pub max_source_lanes: usize,
    pub max_producer_lanes: usize,
    pub max_subscriptions: usize,
    pub max_subscription_tombstones: usize,
    pub max_operation_receipts: usize,
    pub max_batch_records: usize,
    pub max_batch_bytes: usize,
    pub max_waiters: usize,
    pub max_host_waiters: usize,
    pub max_dispatch_concurrency: usize,
    pub max_inbox_receipts: usize,
    pub max_poll_ms: i64,
    pub lease_ms: i64,
    pub max_lease_lifetime_ms: i64,
    pub max_retry_delay_ms: i64,
    pub sweep_rows: usize,
    pub sweep_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_event_bytes: 256 << 10,
            max_key_bytes: 1024,
            max_family_bytes: 128,
            max_outbox_rows: 10_000,
            max_outbox_bytes: 128 << 20,
            max_source_lanes: 128,
            max_producer_lanes: 100_000,
            max_subscriptions: 128,
            max_subscription_tombstones: 100_000,
            max_operation_receipts: 100_000,
            max_batch_records: 100,
            max_batch_bytes: 1 << 20,
            max_waiters: 1024,
            max_host_waiters: 4096,
            max_dispatch_concurrency: 32,
            max_inbox_receipts: 100_000,
            max_poll_ms: 30_000,
            lease_ms: 30_000,
            max_lease_lifetime_ms: 300_000,
            max_retry_delay_ms: 300_000,
            sweep_rows: 1000,
            sweep_bytes: 4 << 20,
        }
    }
}

impl Limits {
    pub fn event_bytes(self) -> usize {
        self.max_event_bytes.min(MAX_PROTOCOL_BATCH_BYTES)
    }

    pub fn batch_bytes(self) -> usize {
        self.max_batch_bytes.min(MAX_PROTOCOL_BATCH_BYTES)
    }

    pub fn batch_records(self) -> usize {
        self.max_batch_records.min(MAX_PROTOCOL_BATCH_RECORDS)
    }

    pub fn unrestricted() -> Self {
        let capacity = i64::MAX as usize;
        Self {
            max_event_bytes: capacity,
            max_key_bytes: capacity,
            max_family_bytes: capacity,
            max_outbox_rows: capacity,
            max_outbox_bytes: capacity,
            max_source_lanes: capacity,
            max_producer_lanes: capacity,
            max_subscriptions: capacity,
            max_subscription_tombstones: capacity,
            max_operation_receipts: capacity,
            max_batch_records: capacity,
            max_batch_bytes: capacity,
            max_waiters: capacity,
            max_host_waiters: capacity,
            max_dispatch_concurrency: capacity,
            max_inbox_receipts: capacity,
            max_poll_ms: i64::MAX,
            lease_ms: 30_000,
            max_lease_lifetime_ms: i64::MAX,
            max_retry_delay_ms: i64::MAX,
            sweep_rows: capacity,
            sweep_bytes: capacity,
        }
    }

    pub fn validate(self) -> Result<(), Failure> {
        let capacities = [
            self.max_event_bytes,
            self.max_key_bytes,
            self.max_family_bytes,
            self.max_outbox_rows,
            self.max_outbox_bytes,
            self.max_source_lanes,
            self.max_producer_lanes,
            self.max_subscriptions,
            self.max_subscription_tombstones,
            self.max_operation_receipts,
            self.max_batch_records,
            self.max_batch_bytes,
            self.max_waiters,
            self.max_host_waiters,
            self.max_dispatch_concurrency,
            self.max_inbox_receipts,
            self.sweep_rows,
            self.sweep_bytes,
        ];
        if capacities
            .into_iter()
            .any(|value| value == 0 || value > i64::MAX as usize)
            || self.max_event_bytes > self.max_batch_bytes
            || self.max_event_bytes > self.max_outbox_bytes
            || self.max_poll_ms <= 0
            || self.lease_ms <= 0
            || self.max_lease_lifetime_ms < self.lease_ms
            || self.max_retry_delay_ms < 0
        {
            return Err(Failure::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Append {
    pub skip: usize,
    pub accepted_through: i64,
}

/// An ordered lane can retry a prefix without retaining an event-ID window.
/// Returning a gap never authorizes partial acceptance of the request.
pub fn append(accepted_through: i64, sequences: &[i64]) -> Result<Append, Failure> {
    if accepted_through < 0 || sequences.is_empty() {
        return Err(Failure::InvalidSequence);
    }
    let mut previous: Option<i64> = None;
    let mut skip = 0;
    let mut high = accepted_through;
    for &sequence in sequences {
        if sequence <= 0 || previous.is_some_and(|prior| prior.checked_add(1) != Some(sequence)) {
            return Err(Failure::InvalidSequence);
        }
        previous = Some(sequence);
        if sequence <= accepted_through {
            skip += 1;
        } else {
            let expected = high.checked_add(1).ok_or(Failure::InvalidSequence)?;
            if sequence != expected {
                return Err(Failure::SequenceGap { expected });
            }
            high = sequence;
        }
    }
    Ok(Append {
        skip,
        accepted_through: high,
    })
}

pub fn source_seal(next_sequence: i64, settled_through: i64, pending: i64) -> Result<i64, Failure> {
    if pending < 0 || next_sequence <= 0 || settled_through < 0 {
        return Err(Failure::InvalidSequence);
    }
    if pending != 0 {
        return Err(Failure::PendingDelivery);
    }
    let terminal = next_sequence - 1;
    if terminal != settled_through {
        return Err(Failure::InvalidSequence);
    }
    Ok(terminal)
}

pub fn destination_seal(accepted_through: i64, terminal: i64) -> Result<(), Failure> {
    if terminal < 0 || accepted_through != terminal {
        return Err(Failure::InvalidSequence);
    }
    Ok(())
}

/// Local retirement conditions complement the operator's external quiescence
/// procedure. No stream-local facts can discover uncontacted source outboxes.
pub fn retire(
    force: bool,
    unsealed_producers: i64,
    lagging_subscriptions: i64,
    live_leases: i64,
) -> Result<(), Failure> {
    if unsealed_producers < 0 || lagging_subscriptions < 0 || live_leases < 0 {
        return Err(Failure::InvalidLimits);
    }
    if !force && (unsealed_producers != 0 || lagging_subscriptions != 0 || live_leases != 0) {
        return Err(Failure::PendingDelivery);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubscriptionLag {
    pub records: i64,
    pub retained_records: i64,
    pub first_retained: i64,
    pub history_expired: bool,
}

/// Unacknowledged offset distance includes lost history; retained lag does not.
/// Streams have contiguous offsets and delete only a prefix.
pub fn subscription_lag(
    earliest: i64,
    end: i64,
    checkpoint: i64,
) -> Result<SubscriptionLag, Failure> {
    if earliest <= 0 || end < earliest || checkpoint < 0 || checkpoint >= end {
        return Err(Failure::InvalidCursor);
    }
    let next = checkpoint + 1;
    let first_retained = next.max(earliest);
    Ok(SubscriptionLag {
        records: end - next,
        retained_records: end - first_retained,
        first_retained,
        history_expired: next < earliest,
    })
}

pub fn cursor(earliest: i64, next: i64, requested: i64) -> Result<(), Failure> {
    if earliest <= 0 || next < earliest || requested > next || requested <= 0 {
        return Err(Failure::InvalidCursor);
    }
    if requested < earliest {
        return Err(Failure::CursorExpired { earliest });
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    pub subscription: [u8; 16],
    pub incarnation: [u8; 16],
    pub generation: i64,
    pub token: [u8; 32],
    pub first: i64,
    pub last: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    pub receipt: Receipt,
    pub issued_at_ms: i64,
    pub expires_at_ms: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Acknowledgment {
    Advance { through: i64 },
    AlreadyAcknowledged,
    Obsolete,
}

/// Repeated acknowledgment survives a lost response, but an old or forged
/// receipt cannot advance current progress. Authentication precedes this
/// decision in the adapter.
pub fn acknowledge(
    subscription: [u8; 16],
    incarnation: [u8; 16],
    checkpoint: i64,
    lease: Option<&Lease>,
    last_ack: Option<&Receipt>,
    receipt: &Receipt,
    now_ms: i64,
) -> Result<Acknowledgment, Failure> {
    if receipt.subscription != subscription || receipt.incarnation != incarnation {
        return Err(Failure::SubscriptionReset);
    }
    if checkpoint < 0
        || receipt.first <= 0
        || receipt.last < receipt.first
        || receipt.generation <= 0
    {
        return Err(Failure::StaleReceipt);
    }
    if last_ack == Some(receipt) {
        return Ok(Acknowledgment::AlreadyAcknowledged);
    }
    if receipt.last <= checkpoint {
        return Ok(Acknowledgment::Obsolete);
    }
    let lease = lease.ok_or(Failure::StaleReceipt)?;
    if &lease.receipt != receipt || checkpoint.checked_add(1) != Some(receipt.first) {
        return Err(Failure::StaleReceipt);
    }
    if now_ms >= lease.expires_at_ms {
        return Err(Failure::LeaseExpired);
    }
    Ok(Acknowledgment::Advance {
        through: receipt.last,
    })
}

/// Extension requests contain an absolute target so transport retries cannot
/// repeatedly add time. A stale lease cannot be resurrected by extension.
pub fn extend(
    lease: &Lease,
    receipt: &Receipt,
    now_ms: i64,
    requested_expiry_ms: i64,
    max_lifetime_ms: i64,
) -> Result<i64, Failure> {
    if &lease.receipt != receipt {
        return Err(Failure::StaleReceipt);
    }
    if now_ms >= lease.expires_at_ms {
        return Err(Failure::LeaseExpired);
    }
    if lease.issued_at_ms < 0 || max_lifetime_ms <= 0 || requested_expiry_ms < 0 {
        return Err(Failure::InvalidLease);
    }
    let bound = lease
        .issued_at_ms
        .checked_add(max_lifetime_ms)
        .ok_or(Failure::DeadlineOverflow)?;
    if requested_expiry_ms > bound {
        return Err(Failure::InvalidLease);
    }
    Ok(lease.expires_at_ms.max(requested_expiry_ms))
}

/// The age/byte sweep proposes a prefix. Required checkpoints and live leases
/// restrict that prefix; they never invent acknowledgment of missing history.
pub fn deletion_frontier(
    candidate: i64,
    required_checkpoints: impl IntoIterator<Item = i64>,
    live_batch_starts: impl IntoIterator<Item = i64>,
) -> Result<i64, Failure> {
    if candidate < 0 {
        return Err(Failure::InvalidCursor);
    }
    let mut through = candidate;
    for checkpoint in required_checkpoints {
        if checkpoint < 0 {
            return Err(Failure::InvalidCursor);
        }
        through = through.min(checkpoint);
    }
    for first in live_batch_starts {
        if first <= 0 {
            return Err(Failure::InvalidLease);
        }
        through = through.min(first - 1);
    }
    Ok(through)
}

/// Byte bookkeeping uses a ring of size 2^63. A retained window is bounded
/// by i64::MAX bytes, so its distance is unique across any number of turns.
/// These positions are not sequence/offset identities and are never exposed
/// as delivery authority. Wrapping avoids a lifetime byte-admission ceiling.
pub fn advance_byte_position(position: i64, bytes: i64) -> Result<i64, Failure> {
    if position < 0 || bytes < 0 {
        return Err(Failure::InvalidLimits);
    }
    Ok(((position as u64 + bytes as u64) & i64::MAX as u64) as i64)
}

pub fn byte_distance(start: i64, end: i64) -> Result<i64, Failure> {
    if start < 0 || end < 0 {
        return Err(Failure::InvalidLimits);
    }
    Ok(((end as u64).wrapping_sub(start as u64) & i64::MAX as u64) as i64)
}

/// Source revision captured by an operator before requesting a lane retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProducerRevision {
    pub generation: i64,
    pub next_sequence: i64,
    pub settled_through: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryRefusal {
    Stale,
    Exhausted,
}

/// Invalidate a prior dispatch response when re-arming a pending obligation.
pub fn retry_producer(
    expected: ProducerRevision,
    current: ProducerRevision,
    pending: bool,
) -> Result<Option<i64>, RetryRefusal> {
    if expected != current {
        return Err(RetryRefusal::Stale);
    }
    if !pending {
        return Ok(None);
    }
    next_dispatch_generation(current.generation).map(Some)
}

pub fn next_dispatch_generation(current: i64) -> Result<i64, RetryRefusal> {
    current.checked_add(1).ok_or(RetryRefusal::Exhausted)
}

/// Durable facts supplied by the source administration adapter.
pub enum OperationReceipt {
    New {
        retained: u64,
        capacity: u64,
    },
    Existing {
        operator_matches: bool,
        arguments_match: bool,
    },
}

#[derive(Debug)]
pub enum OperationRefusal {
    Forbidden,
    Conflict,
    Capacity,
}

/// Returns true for a new operation, false for immutable receipt replay.
pub fn admit_operation(receipt: OperationReceipt) -> Result<bool, OperationRefusal> {
    match receipt {
        OperationReceipt::Existing {
            operator_matches: false,
            ..
        } => Err(OperationRefusal::Forbidden),
        OperationReceipt::Existing {
            arguments_match: false,
            ..
        } => Err(OperationRefusal::Conflict),
        OperationReceipt::Existing { .. } => Ok(false),
        OperationReceipt::New { retained, capacity } if retained >= capacity => {
            Err(OperationRefusal::Capacity)
        }
        OperationReceipt::New { .. } => Ok(true),
    }
}

#[cfg(test)]
mod lag_tests {
    use super::*;

    #[test]
    fn byte_positions_measure_retained_windows_across_wrap_without_identity_rollover() {
        let start = i64::MAX - 3;
        let end = advance_byte_position(start, 10).unwrap();
        assert_eq!(end, 6);
        assert_eq!(byte_distance(start, end).unwrap(), 10);
        assert_eq!(advance_byte_position(0, i64::MAX).unwrap(), i64::MAX);
        let end = advance_byte_position(i64::MAX, i64::MAX).unwrap();
        assert_eq!(byte_distance(i64::MAX, end).unwrap(), i64::MAX);
        assert_eq!(byte_distance(end, end).unwrap(), 0);
        assert!(advance_byte_position(-1, 1).is_err());
        assert!(advance_byte_position(0, -1).is_err());
        assert!(byte_distance(-1, 0).is_err());
    }

    #[test]
    fn lag_distinguishes_lost_history_and_handles_exhausted_offsets() {
        let lag = subscription_lag(5, 8, 2).unwrap();
        assert_eq!(lag.records, 5);
        assert_eq!(lag.retained_records, 3);
        assert_eq!(lag.first_retained, 5);
        assert!(lag.history_expired);
        let empty = subscription_lag(i64::MAX, i64::MAX, i64::MAX - 1).unwrap();
        assert_eq!(empty.records, 0);
        assert_eq!(empty.retained_records, 0);
        assert!(!empty.history_expired);
        let full = subscription_lag(1, i64::MAX, 0).unwrap();
        assert_eq!(full.records, i64::MAX - 1);
        assert_eq!(full.retained_records, i64::MAX - 1);
        for (earliest, end, checkpoint) in
            [(0, 1, 0), (2, 1, 0), (1, 1, -1), (1, i64::MAX, i64::MAX)]
        {
            assert!(subscription_lag(earliest, end, checkpoint).is_err());
        }
    }
}

/// Framework source control and recovery remain available after business
/// admission closes. A recreated source never revalidates an old context or
/// retained storage handle, even within the same writer activation.
pub fn source_admitted(captured: u64, current: u64, closed: bool, framework: bool) -> bool {
    framework || (!closed && captured == current)
}

#[cfg(test)]
mod source_admission_tests {
    #[test]
    fn closure_and_recreation_never_revalidate_business_handles() {
        assert!(super::source_admitted(0, 0, false, false));
        assert!(!super::source_admitted(0, 1, true, false));
        assert!(!super::source_admitted(0, 2, false, false));
        assert!(super::source_admitted(2, 2, false, false));
        assert!(super::source_admitted(0, 1, true, true));
        assert!(!super::source_admitted(1, 1, true, false));
    }
}

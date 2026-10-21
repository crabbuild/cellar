//! Local readiness observations; these never grant query or command authority.

/// Role in the sampled committed configuration. During a joint change an
/// outgoing voter remains a voter until the new configuration is committed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RaftReplicaRole {
    Leader,
    /// A nonleader voter, including a follower, candidate or pre-candidate.
    Voter,
    Learner,
    Removed,
    /// Configuration/term changed while the status observation was in flight.
    Unknown,
}

/// Readiness of the version in `CellStatus::version` at local capture time.
/// Capacity and state can change before invocation. Minimum-version reads must
/// still validate their token; every linearizable query obtains a fresh ReadIndex
/// barrier. None of these cached observations establishes a current quorum.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RaftQueryReadiness {
    pub registered_queries: bool,
    /// The published image matches both durable apply and the sampled runtime.
    pub coherent_image: bool,
    /// Local execution is unblocked for an explicitly historical stale read.
    pub historical: bool,
    /// Historical readiness plus the exact RecentSource freshness policy.
    pub recent_source: bool,
    /// Local monotonic age; None means no valid current-source observation.
    pub source_age_ms: Option<u64>,
    pub source_timeout_ms: u64,
}

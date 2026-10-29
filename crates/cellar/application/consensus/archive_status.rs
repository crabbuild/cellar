// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Local archive observations are independent of quorum write admission.

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArchiveReadiness {
    /// This process has not confirmed any recoverable cut. Another publisher
    /// may have one; zero local coverage does not prove an empty bucket.
    Unconfirmed,
    /// A confirmed cut exists, but newer committed entries are not covered.
    Lagging,
    /// Local confirmed coverage reaches the observed committed position.
    Current,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArchiveFailure {
    Storage,
    Timeout,
    Integrity,
    Capacity,
    Unconfirmed,
    Unavailable,
}

/// Advisory, process-local observations. These do not grant disaster recovery
/// authority, prove current bucket availability, or strengthen a quorum receipt.
/// Inspect/pin the exact archive reference and fence the old deployment before
/// disaster recovery. Observations reset when this process restarts.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ArchiveStatus {
    pub readiness: ArchiveReadiness,
    /// Committed minus locally confirmed archive position, in Raft entries.
    pub lag_entries: u64,
    /// Time since this process first observed the current uninterrupted backlog.
    /// This is NOT the age of the oldest unarchived write or a wall-clock RPO.
    /// None means no backlog has been observed by this process.
    pub observed_lag_ms: Option<u64>,
    /// Conservative retained image/log bytes; not incremental upload bytes.
    pub retained_bytes: u64,
    pub publishing: bool,
    pub last_success_age_ms: Option<u64>,
    pub consecutive_failures: u64,
    /// Cleared only by a confirmed publication accepted by the local engine.
    pub last_failure: Option<ArchiveFailure>,
}

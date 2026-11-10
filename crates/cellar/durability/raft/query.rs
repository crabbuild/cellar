// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

/// The one RecentSource policy used by query admission and readiness reporting.
/// Missing/future observations, overflow and the exact expiry boundary refuse.
pub(crate) fn recent_source_deadline(observed: Option<u64>, now: u64, timeout: u64) -> Option<u64> {
    observed
        .filter(|at| *at <= now)
        .and_then(|at| at.checked_add(timeout))
        .filter(|deadline| now < *deadline)
}

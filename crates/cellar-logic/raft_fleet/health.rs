// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Receiver-local failure classification. Neither unavailability nor elapsed
//! time establishes isolation, loss of voting rights, or permission to repair.

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum NodeHealth {
    Healthy,
    Suspect,
    Unavailable,
    #[default]
    Unknown,
}

/// The adapter authenticates responsive samples and preserves direct-connect
/// failures. Missing discovery, local pressure and unusable replies are Unknown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HealthObservation {
    Responsive,
    DirectFailure,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HealthPolicy {
    grace_ms: u64,
    max_age_ms: u64,
}

impl HealthPolicy {
    pub fn new(grace_ms: u64, max_age_ms: u64) -> Option<Self> {
        (grace_ms > 0 && max_age_ms > 0).then_some(Self {
            grace_ms,
            max_age_ms,
        })
    }
}

/// Ephemeral state for one exact boot. Callers discard it on any scope change;
/// it must never be restored from persisted or remote timestamps.
#[derive(Clone, Debug, Default)]
pub struct HealthTracker {
    sampled_at: Option<u64>,
    completed_at: Option<u64>,
    first_failure: Option<u64>,
    failures: u32,
    state: NodeHealth,
}

impl HealthTracker {
    pub fn observe(
        &mut self,
        policy: HealthPolicy,
        now_ms: u64,
        sample_age_ms: u64,
        observation: HealthObservation,
    ) {
        let Some(sampled_at) = now_ms.checked_sub(sample_age_ms) else {
            *self = Self::default();
            return;
        };
        if sample_age_ms > policy.max_age_ms
            || self.completed_at.is_some_and(|last| sampled_at < last)
        {
            *self = Self::default();
            return;
        }
        let continuous = self.fresh(policy, now_ms);
        self.sampled_at = Some(sampled_at);
        self.completed_at = Some(now_ms);
        match observation {
            HealthObservation::Responsive => {
                self.first_failure = None;
                self.failures = 0;
                self.state = NodeHealth::Healthy;
            }
            HealthObservation::Unknown => {
                self.first_failure = None;
                self.failures = 0;
                self.state = NodeHealth::Unknown;
            }
            HealthObservation::DirectFailure => {
                if !continuous || self.first_failure.is_none() {
                    self.first_failure = Some(now_ms);
                    self.failures = 0;
                }
                self.failures = self.failures.saturating_add(1);
                self.state = if self.failures >= 2
                    && self.first_failure.is_some_and(|first| {
                        now_ms
                            .checked_sub(first)
                            .is_some_and(|age| age >= policy.grace_ms)
                    }) {
                    NodeHealth::Unavailable
                } else {
                    NodeHealth::Suspect
                };
            }
        }
    }

    fn fresh(&self, policy: HealthPolicy, now_ms: u64) -> bool {
        self.age_ms(now_ms)
            .is_some_and(|age| age <= policy.max_age_ms)
    }

    pub fn state(&self, policy: HealthPolicy, now_ms: u64) -> NodeHealth {
        if self.fresh(policy, now_ms) {
            self.state
        } else {
            NodeHealth::Unknown
        }
    }

    pub fn consecutive_failures(&self, policy: HealthPolicy, now_ms: u64) -> u32 {
        if self.fresh(policy, now_ms) {
            self.failures
        } else {
            0
        }
    }

    pub fn age_ms(&self, now_ms: u64) -> Option<u64> {
        now_ms.checked_sub(self.sampled_at?)
    }
}

#[cfg(test)]
#[path = "health_tests.rs"]
mod tests;

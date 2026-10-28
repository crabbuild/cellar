// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

//! Effect dispatch keeps ordinary work off the core's lease timer thread.

use super::{CompletedEffect, EffectFuture, Ownership, TimerArm, TimerSlots};
use crate::ownership_store::now_ms;
use cellar_logic::{CasGuard, Event, Failure, LeaseCasOutcome, NodeLeaseRecord, OpId, Timer};
use futures_util::stream::FuturesUnordered;
use std::future::Future;
use tokio_util::time::{delay_queue, DelayQueue};

enum EffectJob {
    Spawn(EffectFuture),
    // Lease authority must progress even when the host workers are occupied.
    Lease(EffectFuture),
}

#[cfg(cellar_internal_tests)]
impl EffectJob {
    fn into_future(self) -> EffectFuture {
        match self {
            Self::Spawn(future) | Self::Lease(future) => future,
        }
    }
}

pub(crate) enum LeaseRequest {
    ReadSelf {
        op: OpId,
        node: String,
    },
    Cas {
        op: OpId,
        guard: CasGuard,
        record: NodeLeaseRecord,
        authority_expires_ms: Option<u64>,
    },
}

/// Futures and timer arms emitted by one Actor transition.
#[derive(Default)]
pub struct StepOutput {
    effects: Vec<EffectJob>,
    pub timers: Vec<TimerArm>,
}

impl StepOutput {
    /// Queue ordinary effect work. In production the drain spawns it on the host
    /// runtime immediately after the Actor step, so it runs to completion and
    /// the core polls only its join handle.
    ///
    /// The difference from `spawn_detached` is only visible when a deterministic
    /// driver takes the queue: dropping an unpolled future discards this work
    /// before it starts.
    pub(crate) fn spawn_scoped(
        &mut self,
        future: impl Future<Output = CompletedEffect> + Send + 'static,
    ) {
        self.effects.push(EffectJob::Spawn(Box::pin(future)));
    }

    /// Production behavior is identical to `spawn_scoped`: the work runs to
    /// completion on the host runtime and the core polls only a join handle.
    ///
    /// This method exists because deterministic worlds drop queued futures and
    /// name these tasks by their `actor.rs` spawn site. Starting the work at the
    /// call keeps seeded stop, restore, and durability regressions on their
    /// existing schedules.
    ///
    /// `#[track_caller]` is load-bearing: the simulated spawn records the caller
    /// location as the task's spawn site.
    #[track_caller]
    pub(crate) fn spawn_detached(
        &mut self,
        future: impl Future<Output = CompletedEffect> + Send + 'static,
    ) {
        let task = crate::asyncrt::spawn(future);
        self.spawn_scoped(async move { task.await.expect("actor effect task panicked") });
    }

    pub(crate) fn lease(&mut self, request: LeaseRequest, ownership: &Ownership) {
        let ownership = ownership.clone();
        let future: EffectFuture = match request {
            LeaseRequest::ReadSelf { op, node } => Box::pin(async move {
                let result = ownership.read_self_node_lease(&node).await;
                CompletedEffect::plain(Event::SelfNodeLeaseRead {
                    op,
                    now_ms: now_ms(),
                    now_mono_ms: crate::asyncrt::mono_ms(),
                    result,
                })
            }),
            LeaseRequest::Cas {
                op,
                guard,
                record,
                authority_expires_ms,
            } => Box::pin(async move {
                let attempt_started_mono_ms = crate::asyncrt::mono_ms();
                let node = record.node.clone();
                let candidate_expires_ms = record.expires_ms;
                // Logged before the CAS: with only the completion line, a
                // renewal hung on a storage tail is indistinguishable from
                // a timer that never fired (the n6 fence, 2026-08-11).
                tracing::info!(
                    event = "node_lease_attempt_started",
                    %node,
                    attempt = if authority_expires_ms.is_some() {
                        "renew"
                    } else {
                        "acquire"
                    },
                    prior_authority_headroom_ms = authority_expires_ms
                        .map(|expires_ms| expires_ms.saturating_sub(now_ms()))
                        .unwrap_or(0),
                    "node lease attempt started"
                );
                // A renewal must return while proven authority remains,
                // because only a returned attempt lets the ambiguity
                // read-back run before the watchdog. The 10:15Z Blob
                // brownout fenced 9 nodes whose sole hung attempt was
                // still inside the transport's 15 s timeout when the
                // 10 s TTL expired. Bound the attempt to half the
                // remaining authority (capped) and map timeout
                // to Ambiguous — the same conservative outcome a lost
                // response already produces, so safety is unchanged.
                // The stamp survives a timed-out attempt: the backend
                // writes it through the out-parameter synchronously at
                // serialization, before the transport await, so even a
                // dropped future has reported what the possibly-landed
                // body carried.
                let mut stamped_log_state = None;
                let mut attempt_bound_ms = None;
                let mut wait_elapsed_ms = None;
                let result = match authority_expires_ms {
                    Some(expires_ms) => {
                        let remaining = expires_ms.saturating_sub(now_ms());
                        // A fixed minimum can exceed the remaining authority
                        // after a failed attempt and its retry delay. Reserve
                        // half even near expiry so a committed-but-lost reply
                        // can still be proven by the ordinary readback.
                        let bound = std::time::Duration::from_millis((remaining / 2).min(2_500));
                        attempt_bound_ms = Some(bound.as_millis() as u64);
                        let wait_started = crate::asyncrt::mono_ms();
                        let waited = crate::asyncrt::timeout(
                            bound,
                            ownership.cas_node_lease(guard, record, &mut stamped_log_state),
                        )
                        .await;
                        wait_elapsed_ms =
                            Some(crate::asyncrt::mono_ms().saturating_sub(wait_started));
                        match waited {
                            Ok(result) => result,
                            Err(_) => Err(Failure::Ambiguous),
                        }
                    }
                    None => {
                        ownership
                            .cas_node_lease(guard, record, &mut stamped_log_state)
                            .await
                    }
                };
                let completed_ms = now_ms();
                let elapsed_ms = crate::asyncrt::mono_ms().saturating_sub(attempt_started_mono_ms);
                // This is observed wait delivery, not a claim that storage
                // consumed the time. Local-store phase evidence separates
                // native completion from a caller which was not polled.
                let wait_overrun_ms = wait_elapsed_ms
                    .zip(attempt_bound_ms)
                    .map(|(elapsed, bound)| elapsed.saturating_sub(bound));
                let prior_authority_headroom_ms = authority_expires_ms
                    .map(|expires_ms| expires_ms.saturating_sub(completed_ms))
                    .unwrap_or(0);
                let candidate_headroom_ms = candidate_expires_ms.saturating_sub(completed_ms);
                let attempt = if authority_expires_ms.is_some() {
                    "renew"
                } else {
                    "acquire"
                };
                let outcome = match &result {
                    Ok(LeaseCasOutcome::Applied { .. }) => "applied",
                    Ok(LeaseCasOutcome::Rejected) => "rejected",
                    Err(Failure::Ambiguous) => "ambiguous",
                    Err(Failure::Definite) => "definite_failure",
                };
                if matches!(&result, Ok(LeaseCasOutcome::Applied { .. })) {
                    tracing::info!(
                        event = "node_lease_attempt",
                        %node,
                        attempt,
                        outcome,
                        elapsed_ms,
                        attempt_bound_ms,
                        wait_elapsed_ms,
                        wait_overrun_ms,
                        prior_authority_headroom_ms,
                        candidate_headroom_ms,
                        "node lease attempt completed"
                    );
                } else {
                    tracing::warn!(
                        event = "node_lease_attempt",
                        %node,
                        attempt,
                        outcome,
                        elapsed_ms,
                        attempt_bound_ms,
                        wait_elapsed_ms,
                        wait_overrun_ms,
                        prior_authority_headroom_ms,
                        candidate_headroom_ms,
                        "node lease attempt did not apply"
                    );
                }
                CompletedEffect::plain(Event::NodeLeaseCasCompleted {
                    op,
                    now_mono_ms: crate::asyncrt::mono_ms(),
                    result,
                    stamped_log_state,
                })
            }),
        };
        self.effects.push(EffectJob::Lease(future));
    }

    // Binary test harnesses compile the library without cfg(test).
    // cellar_internal_tests is never set in a shipped build.
    #[cfg(cellar_internal_tests)]
    pub fn take_effects(&mut self) -> Vec<EffectFuture> {
        self.effects.drain(..).map(EffectJob::into_future).collect()
    }

    #[cfg(cellar_internal_tests)]
    pub fn effects_is_empty(&self) -> bool {
        self.effects.is_empty()
    }

    // A driver that runs only the newest effect must retain every older job.
    #[cfg(cellar_internal_tests)]
    pub fn pop_effect(&mut self) -> Option<EffectFuture> {
        self.effects.pop().map(EffectJob::into_future)
    }

    #[cfg(cellar_internal_tests)]
    pub fn effects_len(&self) -> usize {
        self.effects.len()
    }
}

pub(crate) fn drain_step_output(
    out: &mut StepOutput,
    effects: &mut FuturesUnordered<EffectFuture>,
    delays: &mut DelayQueue<TimerArm>,
    timers: &mut TimerSlots<delay_queue::Key>,
) -> Result<(), crate::asyncrt::DomainClosed> {
    if !crate::asyncrt::services().accepts_work() {
        return Err(crate::asyncrt::DomainClosed);
    }
    for job in out.effects.drain(..) {
        match job {
            EffectJob::Spawn(future) => {
                // Spawn only at the production drain so deterministic drivers
                // can take the futures and retain their existing schedule.
                let task = crate::asyncrt::try_spawn(future)?;
                effects.push(Box::pin(async move {
                    match task.await {
                        Ok(completed) => completed,
                        // Cancellation supplies no completion event or proof.
                        // The owning actor is also being cancelled; park until
                        // its abort drops this future in the owning domain.
                        Err(_) if !crate::asyncrt::services().accepts_work() => {
                            std::future::pending().await
                        }
                        Err(error) => panic!("actor effect task panicked: {error}"),
                    }
                }));
            }
            EffectJob::Lease(future) => effects.push(future),
        }
    }
    for arm in out.timers.drain(..) {
        let delay = timer_delay(&arm, crate::asyncrt::mono_ms());
        let key = delays.insert(arm.clone(), delay);
        if let Some(displaced) = timers.install(&arm, key) {
            delays.remove(&displaced);
        }
    }
    Ok(())
}

// DelayQueue has a finite timer wheel. Durable alarm timestamps can be much
// farther away, so revisit them without reporting an early TimerFired event.
const TIMER_CHECKPOINT_MS: u64 = 24 * 60 * 60 * 1000;

fn timer_delay(arm: &TimerArm, now_mono_ms: u64) -> std::time::Duration {
    std::time::Duration::from_millis(
        arm.at_mono_ms
            .saturating_sub(now_mono_ms)
            .min(TIMER_CHECKPOINT_MS),
    )
}

pub(super) fn ready_timer(
    arm: TimerArm,
    delays: &mut DelayQueue<TimerArm>,
    timers: &mut TimerSlots<delay_queue::Key>,
    now_mono_ms: u64,
) -> Option<Timer> {
    timers.fire(&arm.slot, arm.ordinal)?;
    if arm.at_mono_ms <= now_mono_ms {
        return Some(arm.timer);
    }
    let key = delays.insert(arm.clone(), timer_delay(&arm, now_mono_ms));
    let displaced = timers.install(&arm, key);
    debug_assert!(displaced.is_none());
    None
}

#[cfg(test)]
#[path = "effects_tests.rs"]
mod tests;

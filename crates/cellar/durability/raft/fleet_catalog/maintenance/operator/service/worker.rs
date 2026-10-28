// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Node job-set lifetime, bounded collection and independent shutdown signals.
use super::*;
use crate::raft::budget::Permit;
use crate::raft::fleet_catalog::deadline::Deadline;
use std::sync::atomic::Ordering;

pub(crate) struct RestartServiceWork {
    driver: FleetRestartOperatorDriver,
    options: FleetRestartServiceOptions,
    stop: watch::Receiver<bool>,
    done: watch::Sender<bool>,
    status: watch::Sender<FleetRestartServiceStatus>,
    node_stop: Option<watch::Receiver<bool>>,
    memory: Option<Permit>,
    admitted: bool,
}

impl RestartServiceWork {
    pub(super) fn new(
        driver: FleetRestartOperatorDriver,
        options: FleetRestartServiceOptions,
        stop: watch::Receiver<bool>,
        done: watch::Sender<bool>,
        status: watch::Sender<FleetRestartServiceStatus>,
    ) -> Self {
        Self {
            driver,
            options,
            stop,
            done,
            status,
            node_stop: None,
            memory: None,
            admitted: false,
        }
    }

    pub(crate) fn admit(&mut self, host: &FleetHostAuthorization) -> Result<()> {
        if host.catalog.birth_owner()? != self.driver.catalog.birth_owner()? {
            return Err(FleetCatalogError::Invalid("restart service fleet"));
        }
        let handle = &self.driver.observer.handle;
        if handle.closing.load(Ordering::Acquire)
            || handle.admission.is_draining()
            || *self.stop.borrow()
        {
            return Err(FleetCatalogError::Closed);
        }
        let mut slot = handle
            .fleet_restart_service
            .lock()
            .map_err(|_| FleetCatalogError::Closed)?;
        if slot.is_some() {
            return Err(FleetCatalogError::Overloaded);
        }
        let memory = handle
            .control_budget
            .acquire(2 * ROOT_LIMIT)
            .ok_or(FleetCatalogError::Overloaded)?;
        // Select the installed backend and Node domain. A caller cannot replace
        // either by constructing another client with the same descriptive ID.
        let source = &host.catalog.inner;
        self.driver.catalog = FleetCatalog {
            inner: Arc::new(Inner {
                bucket: source.bucket.clone(),
                domain: handle.domain.clone(),
                prefix: source.prefix.clone(),
                cluster: source.cluster,
                incarnation: source.incarnation,
                turn: Mutex::new(()),
            }),
        };
        let (stop, wait) = watch::channel(false);
        *slot = Some(stop);
        self.node_stop = Some(wait);
        self.memory = Some(memory);
        self.admitted = true;
        self.status.send_modify(|s| s.running = true);
        Ok(())
    }

    pub(crate) async fn run(mut self) {
        let mut stop = self.stop.clone();
        let Some(mut node_stop) = self.node_stop.take() else {
            return;
        };
        if *stop.borrow_and_update() || *node_stop.borrow_and_update() {
            return;
        }
        crate::asyncrt::select_biased! { "restart service cancellation precedes the next effect";
            _ = stop.changed() => (),
            _ = node_stop.changed() => (),
            _ = self.poll() => (),
        }
    }

    async fn poll(&mut self) {
        loop {
            let handle = &self.driver.observer.handle;
            if handle.closing.load(Ordering::Acquire) || handle.admission.is_draining() {
                return;
            }
            self.status
                .send_modify(|s| s.rounds = s.rounds.saturating_add(1));
            let result = crate::asyncrt::timeout(self.options.turn_timeout, self.round())
                .await
                .unwrap_or(Err(FleetCatalogError::Unconfirmed));
            let delay = match result {
                Ok(active) => {
                    self.status.send_modify(|s| {
                        s.consecutive_failures = 0;
                        s.last_failure = None;
                    });
                    if active {
                        Duration::from_millis(50)
                    } else {
                        self.options.interval
                    }
                }
                Err(FleetCatalogError::Closed) => return,
                Err(error) => {
                    let failure = match error {
                        FleetCatalogError::Conflict => FleetMovementServiceFailure::Conflict,
                        FleetCatalogError::Overloaded => FleetMovementServiceFailure::Overloaded,
                        FleetCatalogError::Unconfirmed => FleetMovementServiceFailure::Unconfirmed,
                        FleetCatalogError::Storage(_) => FleetMovementServiceFailure::Storage,
                        FleetCatalogError::Observation(_) => {
                            FleetMovementServiceFailure::Observation
                        }
                        _ => FleetMovementServiceFailure::Rejected,
                    };
                    self.status.send_modify(|s| {
                        s.consecutive_failures = s.consecutive_failures.saturating_add(1);
                        s.last_failure = Some(failure);
                    });
                    let failures = self.status.borrow().consecutive_failures;
                    Duration::from_millis(
                        (self.options.interval.as_millis() as u64)
                            .saturating_mul(1u64 << failures.min(16))
                            .min((self.options.interval.as_millis() as u64).max(10_000)),
                    )
                }
            };
            let domain = &self.driver.catalog.inner.domain;
            let Ok(now) = domain.mono_ms() else {
                return;
            };
            let Ok(timer) = domain.sleep_until(now.saturating_add(delay.as_millis() as u64)) else {
                return;
            };
            timer.await;
        }
    }

    async fn round(&mut self) -> Result<bool> {
        let deadline = Deadline::new(&self.driver.catalog, self.options.turn_timeout)?;
        let domain = &self.driver.catalog.inner.domain;
        let start = domain.mono_ms().map_err(|_| FleetCatalogError::Closed)?;
        for _ in 0..self.options.max_steps_per_round {
            let progress = self.driver.reconcile(deadline.remaining()?).await?;
            let Some(progress) = progress else {
                self.status.send_modify(|s| {
                    s.operation = None;
                    s.phase = None;
                    s.checked_histories = 0;
                });
                return Ok(false);
            };
            self.status.send_modify(|s| {
                s.steps = s.steps.saturating_add(1);
                s.operation = Some(progress.admission.identity().operation);
                s.phase = Some(progress.phase);
                s.checked_histories = progress.checked_histories;
            });
            if progress.blocked.is_some()
                || progress.ready.is_some()
                || progress.cancelled.is_some()
                || progress.completed.is_some()
                || progress.waiting
            {
                return Ok(false);
            }
            let now = domain.mono_ms().map_err(|_| FleetCatalogError::Closed)?;
            if now.saturating_sub(start) >= self.options.active_quantum.as_millis() as u64 {
                break;
            }
        }
        Ok(true)
    }
}

impl Drop for RestartServiceWork {
    fn drop(&mut self) {
        if self.admitted {
            if let Ok(mut slot) = self.driver.observer.handle.fleet_restart_service.lock() {
                slot.take();
            }
        }
        self.memory.take();
        self.status.send_modify(|s| s.running = false);
        self.done.send_replace(true);
    }
}

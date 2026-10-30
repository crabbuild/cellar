// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Retained preparations are retryable without reopening an engine-pinned store.
use super::*;
use crate::raft::transport::PhysicalRegistration;
use cellar_logic::cell_group::GroupHistory;
use futures_util::{future::BoxFuture, stream::FuturesUnordered, StreamExt};

struct Entry {
    directory: RangeDirectory,
    route: GroupRoute,
    config: Arc<Config>,
    reservation: registration::Reservation,
    permit: Option<Permit>,
    prepared: Option<prepare::Prepared>,
    waiters: Vec<Waiter>,
    running: bool,
    active: bool,
}

pub(in super::super) enum Completed {
    Prepared(
        GroupHistory,
        Result<Box<CellGroupRegistration>, CellGroupControllerError>,
    ),
    Attached(
        GroupHistory,
        Result<PhysicalRegistration, CellGroupControllerError>,
    ),
}

#[derive(Default)]
pub(in super::super) struct Manager {
    entries: BTreeMap<GroupHistory, Entry>,
    jobs: FuturesUnordered<BoxFuture<'static, Completed>>,
}

impl Manager {
    pub fn is_running(&self) -> bool {
        !self.jobs.is_empty()
    }

    pub async fn next(&mut self) -> Option<Completed> {
        self.jobs.next().await
    }

    pub fn request(&mut self, request: Request, shared: &Shared) {
        let Request {
            directory,
            route,
            config,
            waiter,
        } = request;
        let group = route.history;
        let ready = match shared.groups.contains(&group) {
            Ok(ready) => ready,
            Err(error) => {
                waiter.finish(Err(error.into()));
                return;
            }
        };
        if let Some(entry) = self.entries.get_mut(&group) {
            if !entry.directory.same_owner(&directory) || entry.route != route {
                waiter.finish(Err(CellGroupControllerError::Invalid(
                    "conflicting provision binding",
                )));
                return;
            }
            if entry.active {
                // This owner already attached the exact assignment. No network
                // read or fresh quorum claim is implied by this idempotent reply.
                waiter.finish(if ready {
                    Ok(())
                } else {
                    Err(RuntimeError::NotFound.into())
                });
                return;
            }
            entry.waiters.push(waiter);
            if !entry.running {
                self.launch(group, shared.runtime.clone());
            }
            return;
        }
        let admitted = (|| {
            // Manual registrations cannot be adopted by a different lifecycle.
            if ready {
                return Err(RuntimeError::AlreadyAttached);
            }
            let permit = config
                .preparations
                .acquire(config.bytes)
                .ok_or(RuntimeError::Overloaded)?;
            let reservation = shared.groups.reserve(group)?;
            Ok((permit, reservation))
        })();
        let (permit, reservation) = match admitted {
            Ok(value) => value,
            Err(error) => {
                waiter.finish(Err(error.into()));
                return;
            }
        };
        self.entries.insert(
            group,
            Entry {
                directory,
                route,
                config,
                reservation,
                permit: Some(permit),
                prepared: None,
                waiters: vec![waiter],
                running: false,
                active: false,
            },
        );
        self.launch(group, shared.runtime.clone());
    }

    fn launch(&mut self, group: GroupHistory, runtime: CellGroupRuntime) {
        let entry = self.entries.get_mut(&group).expect("provision entry");
        entry.running = true;
        let directory = entry.directory.clone();
        let route = entry.route.clone();
        let config = entry.config.clone();
        let prepared = entry.prepared.clone();
        self.jobs.push(Box::pin(async move {
            Completed::Prepared(
                group,
                prepare::recover(directory, route, config, runtime, prepared)
                    .await
                    .map(Box::new),
            )
        }));
    }

    pub fn complete(
        &mut self,
        done: Completed,
        shared: &Shared,
        transport: &RaftTransport,
        roster: &mut BTreeMap<GroupHistory, CellGroupApplier>,
        routes: &mut BTreeMap<GroupHistory, PhysicalRegistration>,
    ) -> Result<(), CellGroupControllerError> {
        match done {
            Completed::Prepared(group, Ok(input)) => {
                let entry = self.entries.get_mut(&group).expect("prepared entry");
                // Save the owners before polling attachment: early Raft events
                // must see the applier, and a catalog refusal must retain pins.
                entry.prepared = Some(prepare::Prepared {
                    applier: input.applier.clone(),
                    current: input.current.clone(),
                });
                roster.insert(group, input.applier.clone());
                let runtime = shared.runtime.clone();
                let transport = transport.clone();
                self.jobs.push(Box::pin(async move {
                    Completed::Attached(
                        group,
                        registration::attach(*input, runtime, transport).await,
                    )
                }));
                Ok(())
            }
            Completed::Prepared(group, Err(error)) => self.failed(group, error),
            Completed::Attached(group, result) => match result {
                Ok(guard) => {
                    let entry = self.entries.get_mut(&group).expect("attached entry");
                    if let Err(error) = entry.reservation.activate() {
                        // Quarantine may have raced an early runtime event. Keep
                        // the route and all owners until the joined shutdown.
                        routes.insert(group, guard);
                        return self.failed(group, error.into());
                    }
                    routes.insert(group, guard);
                    entry.active = true;
                    entry.running = false;
                    entry.prepared = None;
                    entry.permit = None;
                    for waiter in std::mem::take(&mut entry.waiters) {
                        waiter.finish(Ok(()));
                    }
                    Ok(())
                }
                Err(error) => self.failed(group, error),
            },
        }
    }

    fn failed(
        &mut self,
        group: GroupHistory,
        error: CellGroupControllerError,
    ) -> Result<(), CellGroupControllerError> {
        let entry = self.entries.get_mut(&group).expect("failed entry");
        entry.running = false;
        let waiters = std::mem::take(&mut entry.waiters);
        // Before recovery installs a physical owner, a failed attempt can give
        // back its preparation/roster capacity. Immutable birth claims persist.
        if entry.prepared.is_none() {
            self.entries.remove(&group);
        }
        for waiter in waiters {
            waiter.finish(Err(error.clone()));
        }
        // Registration refusal happens before any transport submission. An
        // identity/capacity refusal is retryable without stopping healthy groups;
        // an actual closed service or uncertain engine/runtime still is fatal.
        let uncertain = match &error {
            CellGroupControllerError::Transport(TransportError::Runtime(error)) => {
                failure::node_wide(&CellGroupControllerError::Runtime(error.clone()))
                    || matches!(error, RuntimeError::AlreadyAttached)
            }
            CellGroupControllerError::Transport(error) => matches!(error, TransportError::Closed),
            // Recovery checks the engine marker again after installing its pins.
            // A competing selection cannot safely reopen the same locked files.
            CellGroupControllerError::Apply(error)
                if matches!(
                    error.as_ref(),
                    ExecutionError::Invalid("recovery selection changed")
                ) =>
            {
                true
            }
            _ => {
                failure::node_wide(&error)
                    || matches!(
                        error,
                        CellGroupControllerError::Runtime(RuntimeError::AlreadyAttached)
                    )
            }
        };
        if uncertain {
            return Err(error);
        }
        Ok(())
    }

    pub async fn join(&mut self) {
        // Do not launch attachment after close, but do join every entered
        // blocking recovery and attachment before releasing dependencies.
        while let Some(done) = self.jobs.next().await {
            if let Completed::Prepared(group, Ok(input)) = done {
                self.entries
                    .get_mut(&group)
                    .expect("closing entry")
                    .prepared = Some(prepare::Prepared {
                    applier: input.applier,
                    current: input.current,
                });
            }
        }
        for entry in self.entries.values_mut() {
            for waiter in std::mem::take(&mut entry.waiters) {
                waiter.finish(Err(CellGroupControllerError::Closed));
            }
        }
    }
}

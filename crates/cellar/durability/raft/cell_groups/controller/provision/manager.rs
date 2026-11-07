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
    authority: Authority,
    reservation: registration::Reservation,
    permit: Option<Permit>,
    prepared: Option<prepare::Prepared>,
    archive: Option<CellGroupArchiveRegistration>,
    waiters: Vec<Waiter>,
    running: bool,
    active: bool,
}

pub(in super::super) struct Recovered {
    input: prepare::Recovery,
    archive: Option<CellGroupArchiveRegistration>,
}

pub(in super::super) enum Completed {
    Prepared(
        GroupHistory,
        Result<Box<Recovered>, CellGroupControllerError>,
    ),
    Attached(
        GroupHistory,
        Result<PhysicalRegistration, CellGroupControllerError>,
    ),
    Checked(
        GroupHistory,
        Result<(), CellGroupControllerError>,
        Waiter,
        Permit,
    ),
}

impl Completed {
    pub(in super::super) fn group(&self) -> GroupHistory {
        match self {
            Self::Prepared(group, _) | Self::Attached(group, _) | Self::Checked(group, _, _, _) => {
                *group
            }
        }
    }
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

    pub fn request(&mut self, request: Request, shared: &Shared, transport: &RaftTransport) {
        let Request {
            directory,
            route,
            config,
            authority,
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
            if !entry.directory.same_owner(&directory)
                || entry.route != route
                || !entry.authority.same_owner(&authority)
            {
                waiter.finish(Err(CellGroupControllerError::Invalid(
                    "conflicting provision binding",
                )));
                return;
            }
            if entry.active {
                // This owner already attached the exact assignment. No network
                // read or fresh quorum claim is implied by this idempotent reply.
                if !ready {
                    waiter.finish(Err(RuntimeError::NotFound.into()));
                    return;
                }
                if matches!(authority, Authority::Initial) {
                    waiter.finish(Ok(()));
                } else {
                    self.validate_attached(directory, route, config, authority, waiter, shared);
                }
                return;
            }
            entry.waiters.push(waiter);
            if !entry.running {
                // A completed attempt may be retried with a fresh invocation
                // deadline. A still-entered attempt never extends its deadline.
                entry.authority = authority;
                self.launch(group, shared.runtime.clone(), transport.clone());
            }
            return;
        }
        if ready && matches!(authority, Authority::Managed(_)) {
            // Observe an existing controller-owned attachment without adopting
            // its applier, roster reservation or physical route lifecycle.
            self.validate_attached(directory, route, config, authority, waiter, shared);
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
                authority,
                reservation,
                permit: Some(permit),
                prepared: None,
                archive: None,
                waiters: vec![waiter],
                running: false,
                active: false,
            },
        );
        self.launch(group, shared.runtime.clone(), transport.clone());
    }

    fn validate_attached(
        &mut self,
        directory: RangeDirectory,
        route: GroupRoute,
        config: Arc<Config>,
        authority: Authority,
        waiter: Waiter,
        shared: &Shared,
    ) {
        let permit = config.profile(&directory).and_then(|_| {
            config
                .preparations
                .acquire(config.bytes)
                .ok_or(RuntimeError::Overloaded.into())
        });
        let permit = match permit {
            Ok(permit) => permit,
            Err(error) => {
                waiter.finish(Err(error));
                return;
            }
        };
        let runtime = shared.runtime.clone();
        self.jobs.push(Box::pin(async move {
            let result = prepare::validate(directory, &route, config, runtime, authority).await;
            Completed::Checked(route.history, result, waiter, permit)
        }));
    }

    fn launch(&mut self, group: GroupHistory, runtime: CellGroupRuntime, transport: RaftTransport) {
        let entry = self.entries.get_mut(&group).expect("provision entry");
        entry.running = true;
        let directory = entry.directory.clone();
        let route = entry.route.clone();
        let config = entry.config.clone();
        let prepared = entry.prepared.clone();
        let authority = entry.authority.clone();
        let archive = entry.archive.clone();
        self.jobs.push(Box::pin(async move {
            let result = async {
                let archive = if let Some(archives) = &config.archives {
                    let _parallel = config
                        .parallel
                        .acquire()
                        .await
                        .map_err(|_| CellGroupControllerError::Closed)?;
                    let birth = prepare::resolve(&directory, &route, &runtime, &authority).await?;
                    let archive = archives
                        .prepare(&directory, &birth, &config.settings, &runtime, archive)
                        .await?;
                    authority.check_live(&runtime)?;
                    Some(archive)
                } else {
                    None
                };
                let input = prepare::recover(
                    directory,
                    route,
                    config,
                    runtime,
                    transport,
                    authority,
                    prepare::Resources {
                        existing: prepared,
                        archive: archive.clone(),
                    },
                )
                .await?;
                Ok(Box::new(Recovered { input, archive }))
            }
            .await;
            Completed::Prepared(group, result)
        }));
    }

    pub fn complete(
        &mut self,
        done: Completed,
        shared: &Shared,
        transport: &RaftTransport,
        roster: &mut BTreeMap<GroupHistory, CellGroupApplier>,
        routes: &mut BTreeMap<GroupHistory, PhysicalRegistration>,
        archives: &mut archive::Manager,
    ) -> Result<(), CellGroupControllerError> {
        match done {
            Completed::Prepared(group, Ok(input)) => {
                let Recovered { input, archive } = *input;
                self.entries
                    .get_mut(&group)
                    .expect("prepared archive entry")
                    .archive = archive;
                let input = match input {
                    prepare::Recovery::Seed(staged) => {
                        let entry = self.entries.get_mut(&group).expect("staged entry");
                        // Save the physical owner before entering transfer. Even
                        // a lost/expired install reply must retain these pins.
                        entry.prepared = Some(staged.prepared.clone());
                        if let Err(error) = entry.authority.check_live(&shared.runtime) {
                            return self.failed(group, error);
                        }
                        let archive = entry.archive.clone();
                        self.jobs.push(Box::pin(async move {
                            Completed::Prepared(
                                group,
                                staged
                                    .install()
                                    .await
                                    .map(|registration| Recovered {
                                        input: prepare::Recovery::Ready(registration),
                                        archive,
                                    })
                                    .map(Box::new),
                            )
                        }));
                        return Ok(());
                    }
                    prepare::Recovery::Ready(input) => input,
                };
                let entry = self.entries.get_mut(&group).expect("prepared entry");
                // Save the owners before polling attachment: early Raft events
                // must see the applier, and a catalog refusal must retain pins.
                entry.prepared = Some(prepare::Prepared {
                    applier: input.applier.clone(),
                    current: input.current.clone(),
                });
                if let Err(error) = entry.authority.check_live(&shared.runtime) {
                    return self.failed(group, error);
                }
                roster.insert(group, input.applier.clone());
                let runtime = shared.runtime.clone();
                let transport = transport.clone();
                self.jobs.push(Box::pin(async move {
                    Completed::Attached(
                        group,
                        registration::attach(input, runtime, transport).await,
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
                    if let Some(archive) = entry.archive.take() {
                        // Transfer the prepared owner directly into the actor's
                        // bounded archive lane. Never call and await this same
                        // controller's public registration queue from its actor.
                        archives.provision(archive, shared)?;
                    }
                    entry.active = true;
                    entry.running = false;
                    entry.prepared = None;
                    entry.permit = None;
                    // Attachment can have finished after the caller expired.
                    // Keep the real active slot/route, but suppress late success.
                    let result = entry.authority.check_live(&shared.runtime);
                    for waiter in std::mem::take(&mut entry.waiters) {
                        waiter.finish(result.clone());
                    }
                    Ok(())
                }
                Err(error) => self.failed(group, error),
            },
            Completed::Checked(group, result, waiter, _permit) => {
                let result = result.and_then(|()| {
                    if shared.groups.contains(&group)? {
                        Ok(())
                    } else {
                        Err(RuntimeError::NotFound.into())
                    }
                });
                let fatal = result
                    .as_ref()
                    .err()
                    .filter(|error| failure::node_wide(error))
                    .cloned();
                waiter.finish(result);
                match fatal {
                    Some(error) => Err(error),
                    None => Ok(()),
                }
            }
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

    pub fn retire(&mut self, group: GroupHistory) -> bool {
        if self
            .entries
            .get(&group)
            .is_some_and(|entry| entry.running || !entry.active)
        {
            return false;
        }
        self.entries.remove(&group);
        true
    }

    pub async fn join(&mut self) {
        // Do not launch attachment after close, but do join every entered
        // blocking recovery and attachment before releasing dependencies.
        while let Some(done) = self.jobs.next().await {
            if let Completed::Prepared(group, Ok(input)) = done {
                let Recovered { input, archive } = *input;
                let entry = self.entries.get_mut(&group).expect("closing entry");
                entry.archive = archive;
                let prepared = match input {
                    prepare::Recovery::Ready(input) => prepare::Prepared {
                        applier: input.applier,
                        current: input.current,
                    },
                    prepare::Recovery::Seed(staged) => staged.prepared,
                };
                entry.prepared = Some(prepared);
            }
        }
        for entry in self.entries.values_mut() {
            for waiter in std::mem::take(&mut entry.waiters) {
                waiter.finish(Err(CellGroupControllerError::Closed));
            }
        }
    }
}

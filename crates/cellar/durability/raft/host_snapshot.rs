// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native checkpoint, transfer and publication ownership. The SQL owner seals
//! images; only matching runtime install events permit their publication.

use super::*;
use crate::raft::snapshot::SnapshotCheckpoint;
use crate::raft::transport::snapshots::{ReceivedSnapshot, SourceReservation};

pub(super) struct SourceRequest {
    message: Box<::raft::prelude::Message>,
    reservation: SourceReservation,
}

#[derive(PartialEq, Eq)]
enum Phase {
    Fetching,
    Downloaded,
    Preparing,
    Prepared,
    Authorizing,
    Installing,
    Publish,
    Publishing,
    NeedAbort,
    Aborting,
}

pub(super) struct Incoming {
    ticket: ApplyTicket,
    phase: Phase,
    received: Option<ReceivedSnapshot>,
    completion: Option<SnapshotCompletion>,
    retry_at: u64,
}

impl Incoming {
    pub(super) fn allows_apply(&self) -> bool {
        matches!(self.phase, Phase::NeedAbort | Phase::Aborting)
    }
}

impl Driver {
    pub(super) fn encode_snapshot(
        &mut self,
        scope: GroupScope,
        purpose: SqlPurpose,
        capture: Box<crate::raft::snapshot::SnapshotCapture>,
        reservation: SourceReservation,
        work: Option<Arc<client::Work>>,
        bytes: Option<Permit>,
    ) {
        // Source admission bounds every encoder, including checkpoints. The
        // blocking owner retains the pin, scratch charges and application work
        // even if its network/request waiter is cancelled. Shutdown joins jobs.
        let checkpoint = matches!(&purpose, SqlPurpose::Checkpoint);
        let performance = self.local.performance.clone();
        let job = self.log.domain().blocking(move || {
            let _work = work;
            let timer = performance.timer(super::super::performance::Operation::SnapshotEncode);
            let result = capture
                .encode()
                .map_err(super::super::execution::storage)
                .map(|package| {
                    let value = if checkpoint {
                        let material = Box::new(package.checkpoint_material());
                        drop(package);
                        drop(reservation);
                        Value::Checkpoint(material)
                    } else {
                        Value::SnapshotSource {
                            package: Box::new(package),
                            reservation,
                        }
                    };
                    sql::Output {
                        value,
                        _bytes: bytes,
                    }
                });
            timer.finish(result.is_ok());
            result
        });
        self.jobs.push(Box::pin(async move {
            let result = match job {
                Ok(job) => match job.await {
                    Ok(result) => result,
                    Err(error) => Err(ExecutionError::Storage(anyhow::Error::new(error))),
                },
                Err(_) => Err(ExecutionError::Refused(CommandFailure::Unavailable)),
            };
            Job::SnapshotEncoded {
                scope,
                purpose,
                result: Box::new(result),
            }
        }));
    }

    fn snapshot_send_failure(&mut self, scope: &GroupScope, member: u64) {
        let runtime = self.runtime.clone();
        let scope = scope.clone();
        self.jobs.push(Box::pin(async move {
            let _ = runtime.admit_unreachable(&scope, member).await;
            let _ = runtime.admit_snapshot_status(&scope, member, false).await;
            Job::Hint
        }));
    }

    pub(super) fn send_snapshot(&mut self, scope: &GroupScope, message: ::raft::prelude::Message) {
        let member = message.to;
        if !self.options.enable_snapshots
            || self
                .cells
                .get(scope)
                .is_none_or(|cell| cell.failed || cell.source_request.is_some())
        {
            self.snapshot_send_failure(scope, member);
            return;
        }
        let now = crate::asyncrt::mono_ms();
        if self
            .snapshot_source
            .as_ref()
            .is_some_and(|source| source.finished(now))
        {
            self.snapshot_source.take();
        }
        if self
            .snapshot_source
            .as_ref()
            .is_some_and(|source| source.matches(scope, member, message.get_snapshot()))
        {
            if self.transport.submit(scope, message).is_err() {
                self.snapshot_send_failure(scope, member);
            }
            return;
        }
        match self.transport.reserve_snapshot_source() {
            Ok(reservation) => {
                if let Some(cell) = self.cells.get_mut(scope) {
                    cell.source_request = Some(SourceRequest {
                        message: Box::new(message),
                        reservation,
                    });
                }
            }
            Err(_) => self.snapshot_send_failure(scope, member),
        }
    }

    pub(super) fn receive_snapshot(
        &mut self,
        ticket: ApplyTicket,
        snapshot: ::raft::prelude::Snapshot,
        source: u64,
    ) {
        let scope = ticket.replica().scope.clone();
        let Some(cell) = self.cells.get_mut(&scope) else {
            return;
        };
        if cell.incoming.is_some() || cell.group.identity() != ticket.replica() {
            self.fail(&scope);
            return;
        }
        let abort = !self.options.enable_snapshots || cell.apply_inflight || source == 0;
        cell.incoming = Some(Incoming {
            ticket: ticket.clone(),
            phase: if abort {
                Phase::NeedAbort
            } else {
                Phase::Fetching
            },
            received: None,
            completion: None,
            retry_at: 0,
        });
        self.refuse(&scope, CommandFailure::Unavailable);
        if !abort {
            let transport = self.transport.clone();
            let limits = self.snapshot_limits;
            self.jobs.push(Box::pin(async move {
                let result = transport
                    .fetch_snapshot(&ticket, source, snapshot, limits)
                    .await;
                Job::SnapshotFetched {
                    ticket,
                    result: Box::new(result),
                }
            }));
        }
    }

    pub(super) fn snapshot_installed(&mut self, ticket: &ApplyTicket) {
        let scope = &ticket.replica().scope;
        let valid = self
            .cells
            .get_mut(scope)
            .and_then(|cell| cell.incoming.as_mut())
            .is_some_and(|incoming| {
                if incoming.ticket != *ticket
                    || !matches!(incoming.phase, Phase::Authorizing | Phase::Installing)
                {
                    return false;
                }
                incoming.completion = None;
                incoming.phase = Phase::Publish;
                true
            });
        if !valid {
            self.fail(scope);
        }
    }

    pub(super) fn snapshot_aborted(&mut self, ticket: &ApplyTicket) {
        let scope = &ticket.replica().scope;
        if let Some(cell) = self.cells.get_mut(scope) {
            if cell.incoming.as_ref().is_some_and(|incoming| {
                incoming.ticket == *ticket
                    && matches!(incoming.phase, Phase::NeedAbort | Phase::Aborting)
            }) {
                cell.incoming.take();
            } else {
                self.fail(scope);
            }
        }
    }

    pub(super) fn pump_snapshots(&mut self) {
        let now = crate::asyncrt::mono_ms();
        // Many short histories can exhaust the shared log cache before any
        // reaches its entry-count threshold. Begin reclaiming at half capacity
        // and prefer groups above that capacity's average share. The hard
        // writer bound and the physical checkpoint proof remain unchanged.
        let cache_pressure = self.log.cache_under_pressure();
        let cache_share = self.log.limits().max_cache_bytes / 2 / self.cells.len().max(1);
        if self
            .snapshot_source
            .as_ref()
            .is_some_and(|source| source.finished(now))
        {
            self.snapshot_source.take();
        }
        for scope in self.cells.keys().cloned().collect::<Vec<_>>() {
            let Some(cell) = self.cells.get_mut(&scope) else {
                continue;
            };
            if cell.failed {
                continue;
            }
            if let Some(incoming) = cell.incoming.as_mut() {
                if now < incoming.retry_at {
                    continue;
                }
                let ticket = incoming.ticket.clone();
                match incoming.phase {
                    Phase::NeedAbort => {
                        incoming.phase = Phase::Aborting;
                        let runtime = self.runtime.clone();
                        self.jobs.push(Box::pin(async move {
                            let result = runtime.abort_snapshot(ticket.clone()).await;
                            Job::SnapshotAbort { ticket, result }
                        }));
                    }
                    Phase::Prepared => {
                        let Some(completion) = incoming.completion.clone() else {
                            self.fail(&scope);
                            continue;
                        };
                        incoming.phase = Phase::Authorizing;
                        let runtime = self.runtime.clone();
                        self.jobs.push(Box::pin(async move {
                            let result = runtime.complete_snapshot(completion).await;
                            Job::SnapshotAuthorized { ticket, result }
                        }));
                    }
                    Phase::Downloaded if !cell.sql_busy && !cell.apply_inflight => {
                        let Some(received) = incoming.received.take() else {
                            self.fail(&scope);
                            continue;
                        };
                        incoming.phase = Phase::Preparing;
                        self.sql(
                            &scope,
                            SqlPurpose::SnapshotPrepare(ticket.clone()),
                            Action::PrepareSnapshot {
                                ticket,
                                received: Box::new(received),
                            },
                        );
                    }
                    Phase::Publish if !cell.sql_busy => {
                        incoming.phase = Phase::Publishing;
                        self.sql(
                            &scope,
                            SqlPurpose::SnapshotPublish(ticket.clone()),
                            Action::PublishSnapshot { ticket },
                        );
                    }
                    _ => (),
                }
                continue;
            }
            if cell.sql_busy {
                continue;
            }
            if let Some(source) = cell.source_request.take() {
                let snapshot = Box::new(source.message.get_snapshot().clone());
                self.sql(
                    &scope,
                    SqlPurpose::SnapshotSource(source.message),
                    Action::CaptureSnapshot {
                        snapshot,
                        limits: self.snapshot_limits,
                        reservation: source.reservation,
                    },
                );
                continue;
            }
            if !self.options.enable_snapshots
                || self.options.checkpoint_entries == 0
                || cell.checkpoint_busy
                || cell.apply_inflight
                || cell.pending.is_some()
                || now < cell.checkpoint_retry_at
            {
                continue;
            }
            let due = cell.group.read().is_ok_and(|cache| {
                let applied_entries = cache
                    .applied
                    .position
                    .index
                    .get()
                    .saturating_sub(cache.snapshot.get_metadata().index);
                applied_entries >= self.options.checkpoint_entries
                    || (cache_pressure && applied_entries > 0 && cache.bytes() >= cache_share)
            });
            if !due {
                continue;
            }
            if let Ok(reservation) = self.transport.reserve_snapshot_source() {
                cell.checkpoint_busy = true;
                self.sql(
                    &scope,
                    SqlPurpose::Checkpoint,
                    Action::CaptureCheckpoint {
                        limits: self.snapshot_limits,
                        reservation,
                    },
                );
            }
        }
    }

    pub(super) fn on_snapshot_sql(
        &mut self,
        scope: GroupScope,
        purpose: SqlPurpose,
        result: Result<sql::Output, ExecutionError>,
    ) {
        if let Some(cell) = self.cells.get_mut(&scope) {
            cell.sql_busy = false;
        }
        self.on_snapshot_encoded(scope, purpose, result);
    }

    pub(super) fn on_snapshot_encoded(
        &mut self,
        scope: GroupScope,
        purpose: SqlPurpose,
        result: Result<sql::Output, ExecutionError>,
    ) {
        let Some(cell) = self.cells.get_mut(&scope) else {
            return;
        };
        match purpose {
            SqlPurpose::Checkpoint => match result {
                Ok(sql::Output {
                    value: Value::Checkpoint(checkpoint),
                    ..
                }) => self.submit_checkpoint(scope, checkpoint),
                _ => {
                    cell.checkpoint_busy = false;
                    cell.checkpoint_retry_at =
                        crate::asyncrt::mono_ms().saturating_add(self.options.poll_ms);
                }
            },
            SqlPurpose::SnapshotSource(message) => {
                let member = message.to;
                match result {
                    Ok(sql::Output {
                        value:
                            Value::SnapshotSource {
                                package,
                                reservation,
                            },
                        ..
                    }) if package.snapshot() == *message.get_snapshot() => {
                        if package.encoded_length() == 0 {
                            drop(reservation);
                            if self.transport.submit(&scope, *message).is_err() {
                                self.snapshot_send_failure(&scope, member);
                            }
                            return;
                        }
                        match self
                            .transport
                            .serve_snapshot(&scope, member, *package, reservation)
                        {
                            Ok(source) => {
                                let lease = source.id();
                                self.snapshot_source = Some(source);
                                match self.transport.submit(&scope, *message) {
                                    Ok(delivery) => self.jobs.push(Box::pin(async move {
                                        Job::SnapshotSent {
                                            lease,
                                            result: delivery.accepted().await,
                                        }
                                    })),
                                    Err(_) => {
                                        self.snapshot_source.take();
                                        self.snapshot_send_failure(&scope, member);
                                    }
                                }
                            }
                            Err(_) => self.snapshot_send_failure(&scope, member),
                        }
                    }
                    _ => self.snapshot_send_failure(&scope, member),
                }
            }
            SqlPurpose::SnapshotPrepare(ticket) => {
                let Some(incoming) = cell.incoming.as_mut().filter(|incoming| {
                    incoming.ticket == ticket && incoming.phase == Phase::Preparing
                }) else {
                    self.fail(&scope);
                    return;
                };
                match result {
                    Ok(sql::Output {
                        value: Value::SnapshotPrepared(completion),
                        ..
                    }) => {
                        incoming.completion = Some(*completion);
                        incoming.phase = Phase::Prepared;
                    }
                    Err(
                        ExecutionError::Invalid(_)
                        | ExecutionError::Refused(CommandFailure::Overloaded),
                    ) => incoming.phase = Phase::NeedAbort,
                    _ => self.fail(&scope),
                }
            }
            SqlPurpose::SnapshotPublish(ticket) => {
                let valid = cell.incoming.as_ref().is_some_and(|incoming| {
                    incoming.ticket == ticket && incoming.phase == Phase::Publishing
                });
                match result {
                    Ok(sql::Output {
                        value: Value::SnapshotPublished(view),
                        ..
                    }) if valid => {
                        cell.image = view;
                        cell.incoming.take();
                        if let Ok(Some(progress)) =
                            progress(&cell.group, cell.image.as_ref(), cell.archived)
                        {
                            let _ = cell.core.observe_progress(progress);
                        }
                    }
                    _ => self.fail(&scope),
                }
            }
            _ => self.fail(&scope),
        }
    }

    fn submit_checkpoint(&mut self, scope: GroupScope, checkpoint: Box<SnapshotCheckpoint>) {
        let runtime = self.runtime.clone();
        self.jobs.push(Box::pin(async move {
            Job::Checkpoint {
                scope,
                result: runtime.checkpoint(*checkpoint).await,
            }
        }));
    }

    pub(super) fn on_snapshot_job(&mut self, job: Job) {
        match job {
            Job::SnapshotSent { lease, result } => {
                if result.is_err()
                    && self
                        .snapshot_source
                        .as_ref()
                        .is_some_and(|source| source.id() == lease)
                {
                    self.snapshot_source.take();
                }
            }
            Job::Checkpoint { scope, result } => {
                if result.is_err() {
                    if let Some(cell) = self.cells.get_mut(&scope) {
                        cell.checkpoint_busy = false;
                        cell.checkpoint_retry_at =
                            crate::asyncrt::mono_ms().saturating_add(self.options.poll_ms);
                    }
                }
            }
            Job::SnapshotFetched { ticket, result } => {
                let Some(incoming) = self
                    .cells
                    .get_mut(&ticket.replica().scope)
                    .and_then(|cell| cell.incoming.as_mut())
                    .filter(|incoming| {
                        incoming.ticket == ticket && incoming.phase == Phase::Fetching
                    })
                else {
                    return;
                };
                match *result {
                    Ok(received) => {
                        incoming.received = Some(received);
                        incoming.phase = Phase::Downloaded;
                    }
                    Err(_) => incoming.phase = Phase::NeedAbort,
                }
            }
            Job::SnapshotAuthorized { ticket, result } => {
                let scope = &ticket.replica().scope;
                let Some(incoming) = self
                    .cells
                    .get_mut(scope)
                    .and_then(|cell| cell.incoming.as_mut())
                    .filter(|incoming| {
                        incoming.ticket == ticket && incoming.phase == Phase::Authorizing
                    })
                else {
                    return;
                };
                match result {
                    Ok(()) => incoming.phase = Phase::Installing,
                    Err(RuntimeError::Overloaded | RuntimeError::NotReady) => {
                        incoming.phase = Phase::Prepared;
                        incoming.retry_at =
                            crate::asyncrt::mono_ms().saturating_add(self.options.poll_ms);
                    }
                    Err(_) => self.fail(scope),
                }
            }
            Job::SnapshotAbort { ticket, result } => {
                let scope = &ticket.replica().scope;
                let Some(incoming) = self
                    .cells
                    .get_mut(scope)
                    .and_then(|cell| cell.incoming.as_mut())
                    .filter(|incoming| {
                        incoming.ticket == ticket && incoming.phase == Phase::Aborting
                    })
                else {
                    return;
                };
                match result {
                    Ok(()) => (),
                    Err(RuntimeError::Overloaded) => {
                        incoming.phase = Phase::NeedAbort;
                        incoming.retry_at =
                            crate::asyncrt::mono_ms().saturating_add(self.options.poll_ms);
                    }
                    Err(_) => self.fail(scope),
                }
            }
            _ => (),
        }
    }
}

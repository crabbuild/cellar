//! One owned timer/client allocator for committed alarms and outbox attempts.
use super::super::effects::RaftEffectOptions;
use super::*;
use std::ops::Bound::{Excluded, Included, Unbounded};

pub(super) struct Scheduler {
    options: RaftEffectOptions,
    cursor: Option<GroupScope>,
    next_at: u64,
    sequence: u64,
    busy: Option<u64>,
    enabled: bool,
    memory: Arc<Budget>,
}

impl Scheduler {
    pub(super) fn new(options: RaftEffectOptions) -> Self {
        Self {
            options,
            cursor: None,
            next_at: 0,
            sequence: 0,
            busy: None,
            enabled: true,
            memory: Budget::new(options.metadata_bytes, 1),
        }
    }
}

enum CompletedKind {
    Status(Result<Box<GroupStatus>, RuntimeError>),
    Acted(Result<(), CommandFailure>),
}

pub(super) struct Completed {
    sequence: u64,
    scope: GroupScope,
    kind: CompletedKind,
    reservation: Option<Permit>,
}

impl Driver {
    pub(super) fn pump_effects(&mut self) {
        let Ok(now) = self.log.domain().mono_ms() else {
            return;
        };
        let Some(owner) = self.effect_scheduler.as_mut() else {
            return;
        };
        if !owner.enabled
            || owner.busy.is_some()
            || now < owner.next_at
            || self.closing.load(Ordering::Acquire)
            || self.local.draining.load(Ordering::Acquire)
        {
            return;
        }
        owner.next_at = now.saturating_add(owner.options.interval_ms);
        let cursor = owner.cursor.clone();
        let after = cursor.clone().map_or(Unbounded, Excluded);
        let through = cursor.map_or(Unbounded, Included);
        let mut selected = None;
        // Bound discovery even when most registered histories have no alarms.
        for (scope, cell) in self
            .cells
            .range((after, Unbounded))
            .chain(self.cells.range((Unbounded, through)))
            .take(4)
        {
            owner.cursor = Some(scope.clone());
            if cell.failed
                || cell.retired
                || cell.pending.is_some()
                || cell.sql_busy
                || cell.incoming.is_some()
                || cell.configuration.is_some()
                || cell.apply_inflight
                || cell.publication.is_some()
            {
                continue;
            }
            let binding = self
                .local
                .bindings
                .get(&(scope.namespace, scope.cell.clone()));
            if binding.is_some_and(|b| {
                b.scope == *scope
                    && (b.class.alarm_limits().is_some() || b.class.outbox_definition().is_some())
            }) {
                selected = Some(scope.clone());
                break;
            }
        }
        let Some(scope) = selected else { return };
        let Some(reservation) = owner.memory.acquire(owner.options.metadata_bytes) else {
            return;
        };
        let Some(sequence) = owner.sequence.checked_add(1) else {
            owner.enabled = false;
            return;
        };
        owner.sequence = sequence;
        owner.busy = Some(sequence);
        let runtime = self.runtime.clone();
        let domain = self.log.domain();
        let timeout = owner.options.operation_timeout_ms;
        self.jobs.push(Box::pin(async move {
            let result=match domain.sleep_until(now.saturating_add(timeout)) {
                Ok(timer)=>{
                    let call=Box::pin(runtime.status(&scope));
                    crate::asyncrt::select_biased! { "the alarm observation deadline precedes a late status";
                        _=timer=>Err(RuntimeError::NotReady),
                        result=call=>result.map(Box::new),
                    }
                }
                Err(_)=>Err(RuntimeError::Closed),
            };
            Job::Effect(Box::new(Completed {sequence,scope,kind:CompletedKind::Status(result),reservation:Some(reservation)}))
        }));
    }

    fn finish_effect_turn(&mut self, sequence: u64) {
        if let Some(owner) = self.effect_scheduler.as_mut() {
            if owner.busy == Some(sequence) {
                owner.busy = None;
                if let Ok(now) = self.log.domain().mono_ms() {
                    owner.next_at = now.saturating_add(owner.options.interval_ms);
                }
            }
        }
    }

    pub(super) fn finish_effect(&mut self, completed: Completed) {
        let Completed {
            sequence,
            scope,
            kind,
            reservation,
        } = completed;
        if !self
            .effect_scheduler
            .as_ref()
            .is_some_and(|o| o.busy == Some(sequence))
        {
            return;
        }
        if self.closing.load(Ordering::Acquire) || self.local.draining.load(Ordering::Acquire) {
            self.finish_effect_turn(sequence);
            return;
        }
        let status = match kind {
            CompletedKind::Status(Ok(status)) => status,
            CompletedKind::Acted(result) => {
                if let Err(error) = result {
                    tracing::debug!(%error,"alarm attempt requires committed-state reconciliation");
                }
                self.finish_effect_turn(sequence);
                return;
            }
            _ => {
                self.finish_effect_turn(sequence);
                return;
            }
        };
        let Some(cell) = self.cells.get_mut(&scope) else {
            self.finish_effect_turn(sequence);
            return;
        };
        if cell.failed
            || cell.retired
            || cell.pending.is_some()
            || cell.sql_busy
            || cell.incoming.is_some()
            || cell.configuration.is_some()
            || CommandAuthority::from_status(cell.group.clone(), &status).is_err()
            || cell.publication.is_some()
        {
            self.finish_effect_turn(sequence);
            return;
        }
        // Alternate per history, rather than by node-wide turn parity: an even
        // number of registered groups must not pin one adapter's priority.
        cell.prefer_delivery = !cell.prefer_delivery;
        let prefer_delivery = cell.prefer_delivery;
        let Some(reservation) = reservation else {
            self.finish_effect_turn(sequence);
            return;
        };
        let Ok(now) = u64::try_from(crate::asyncrt::wall_ms()) else {
            self.finish_effect_turn(sequence);
            return;
        };
        self.sql(
            &scope,
            SqlPurpose::EffectScan(sequence),
            Action::ScanEffects {
                now,
                prefer_delivery,
                reservation,
            },
        );
    }

    pub(super) fn finish_effect_scan(
        &mut self,
        scope: GroupScope,
        sequence: u64,
        result: Result<sql::Output, ExecutionError>,
    ) {
        if !self
            .effect_scheduler
            .as_ref()
            .is_some_and(|o| o.busy == Some(sequence))
        {
            return;
        }
        if let Some(cell) = self.cells.get_mut(&scope) {
            cell.sql_busy = false;
        }
        if self.closing.load(Ordering::Acquire) || self.local.draining.load(Ordering::Acquire) {
            self.finish_effect_turn(sequence);
            return;
        }
        let scan = match result {
            Ok(sql::Output {
                value: Value::EffectScan(Some(scan)),
                ..
            }) => *scan,
            _ => {
                self.finish_effect_turn(sequence);
                return;
            }
        };
        let Some(cell) = self.cells.get(&scope) else {
            self.finish_effect_turn(sequence);
            return;
        };
        if cell.failed
            || cell.retired
            || cell.pending.is_some()
            || cell.incoming.is_some()
            || cell.publication.is_some()
        {
            self.finish_effect_turn(sequence);
            return;
        }
        let Some(due) = scan.due else {
            self.finish_effect_turn(sequence);
            return;
        };
        if due.minimum() == Durability::S3 && cell.archived < scan.position.index {
            self.finish_effect_turn(sequence);
            return;
        }
        let Some(binding) = self
            .local
            .bindings
            .get(&(scope.namespace, scope.cell.clone()))
            .cloned()
            .filter(|b| b.scope == scope)
        else {
            self.finish_effect_turn(sequence);
            return;
        };
        let Ok((operation, input)) = due.command() else {
            self.finish_effect_turn(sequence);
            return;
        };
        let Some(contract) = binding.class.system_contract(&operation) else {
            self.finish_effect_turn(sequence);
            return;
        };
        let Some(owner) = self.effect_scheduler.as_ref() else {
            return;
        };
        let Ok(now) = u64::try_from(crate::asyncrt::wall_ms()) else {
            self.finish_effect_turn(sequence);
            return;
        };
        let Some(retry_until) = now.checked_add(owner.options.retry_window_ms) else {
            self.finish_effect_turn(sequence);
            return;
        };
        let mut options = client::CommandOptions::new(scan.next, retry_until);
        options.durability = due.minimum();
        options.timeout = std::time::Duration::from_millis(owner.options.operation_timeout_ms);
        let local = crate::application::consensus::RaftCellHandle {
            handle: self.local.clone(),
            binding,
        };
        // This exact next client sequence comes from committed state, after the
        // host has no previous producer for this group and the runtime reached
        // current-term readiness. It cannot rebind an unresolved old producer.
        let call = local.system_command(contract, input, options.clone());
        let runtime = self.runtime.clone();
        let group = cell.group.clone();
        let domain = self.log.domain();
        let draining = self.local.draining.clone();
        let application_work = self.local.work.clone();
        self.jobs.push(Box::pin(async move {
            let _reservation = scan._reservation;
            let result = async {
                let receipt = call.await?;
                match due {
                    super::super::effects::scan::DueEffect::Alarm(_) => {
                        if receipt.result == [0] || receipt.result == [1] {
                            Ok(())
                        } else {
                            Err(CommandFailure::CorruptState)
                        }
                    }
                    super::super::effects::scan::DueEffect::Delivery {
                        selected,
                        definition,
                        ..
                    } => {
                        let Some(super::super::effects::outbox::ClaimedDelivery {
                            request,
                            generation,
                            sink,
                        }) = super::super::effects::outbox::claimed(
                            &definition,
                            &scope,
                            local.binding.class.schema_digest(),
                            &selected,
                            &receipt.result,
                        )
                        .map_err(effect_failure)?
                        else {
                            return Ok(());
                        };
                        // Recheck actual current-term authority immediately before
                        // send, then cancel stale waiters during the bounded send.
                        let _work = application_work.enter();
                        let proof = deliver_owned(
                            DeliveryOwner {
                                domain: &domain,
                                runtime: &runtime,
                                group: &group,
                                term: receipt.version.position.term,
                                draining: &draining,
                            },
                            request.clone(),
                            sink,
                            options.timeout,
                        )
                        .await?;
                        request.verify_receipt(&proof).map_err(effect_failure)?;
                        let input = super::super::effects::outbox::completion_input(
                            selected, generation, &proof,
                        )
                        .map_err(effect_failure)?;
                        let contract = local
                            .binding
                            .class
                            .system_contract(super::super::effects::outbox::COMPLETE)
                            .ok_or(CommandFailure::CorruptState)?;
                        let next = scan
                            .next
                            .sequence()
                            .checked_add(1)
                            .ok_or(CommandFailure::Overloaded)?;
                        let now = u64::try_from(crate::asyncrt::wall_ms())
                            .map_err(|_| CommandFailure::Unavailable)?;
                        options.id = CommandId::new(scan.next.client, next)
                            .map_err(|_| CommandFailure::Overloaded)?;
                        if now >= retry_until {
                            return Err(CommandFailure::Unavailable);
                        }
                        let result = local.system_command(contract, input, options).await?;
                        if result.result == [0] || result.result == [1] {
                            Ok(())
                        } else {
                            Err(CommandFailure::CorruptState)
                        }
                    }
                }
            }
            .await;
            Job::Effect(Box::new(Completed {
                sequence,
                scope,
                kind: CompletedKind::Acted(result),
                reservation: None,
            }))
        }));
    }
}

fn effect_failure(error: anyhow::Error) -> CommandFailure {
    error
        .downcast_ref::<CommandFailure>()
        .cloned()
        .unwrap_or(CommandFailure::CorruptState)
}

struct DeliveryOwner<'a> {
    domain: &'a crate::asyncrt::DomainToken,
    runtime: &'a RaftRuntime,
    group: &'a GroupLog,
    term: Term,
    draining: &'a AtomicBool,
}

async fn deliver_owned(
    owner: DeliveryOwner<'_>,
    request: super::super::effects::RaftDeliveryRequest,
    sink: Arc<dyn super::super::effects::RaftEffectSink>,
    timeout: std::time::Duration,
) -> Result<Vec<u8>, CommandFailure> {
    let DeliveryOwner {
        domain,
        runtime,
        group,
        term,
        draining,
    } = owner;
    let start = domain.mono_ms().map_err(|_| CommandFailure::Unavailable)?;
    let deadline = start
        .checked_add(timeout.as_millis() as u64)
        .ok_or(CommandFailure::Unavailable)?;
    let mut send = None;
    loop {
        let now = domain.mono_ms().map_err(|_| CommandFailure::Unavailable)?;
        if now >= deadline {
            return Err(CommandFailure::Unavailable);
        }
        let timer = domain
            .sleep_until(deadline)
            .map_err(|_| CommandFailure::Unavailable)?;
        let status = Box::pin(runtime.status(&group.identity().scope));
        let status = crate::asyncrt::select_biased! { "the effect deadline rejects a late authority observation";
            _=timer=>return Err(CommandFailure::Unavailable),
            status=status=>status.map_err(|_|CommandFailure::Unavailable)?,
        };
        if status.term != term || CommandAuthority::from_status(group.clone(), &status).is_err() {
            return Err(CommandFailure::Unavailable);
        }
        if send.is_none() {
            if draining.load(Ordering::Acquire) {
                return Err(CommandFailure::Unavailable);
            }
            send = Some(sink.deliver(request.clone()));
        }
        let now = domain.mono_ms().map_err(|_| CommandFailure::Unavailable)?;
        let timer = domain
            .sleep_until(now.saturating_add(100).min(deadline))
            .map_err(|_| CommandFailure::Unavailable)?;
        let pending = send.as_mut().ok_or(CommandFailure::CorruptState)?;
        let result = crate::asyncrt::select_biased! { "recheck source authority before accepting a late effect reply";
            _=timer=>None,
            result=pending=>Some(result),
        };
        if let Some(result) = result {
            return result;
        }
    }
}

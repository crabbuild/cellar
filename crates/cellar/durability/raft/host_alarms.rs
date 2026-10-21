//! One bounded node-owned timer; followed groups never invoke alarm callbacks.
use super::super::effects::RaftAlarmOptions;
use super::*;
use std::ops::Bound::{Excluded, Included, Unbounded};

pub(super) struct Scheduler {
    options: RaftAlarmOptions,
    cursor: Option<GroupScope>,
    next_at: u64,
    sequence: u64,
    busy: Option<u64>,
    enabled: bool,
    memory: Arc<Budget>,
}

impl Scheduler {
    pub(super) fn new(options: RaftAlarmOptions) -> Self {
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
    pub(super) fn pump_alarms(&mut self) {
        let Ok(now) = self.log.domain().mono_ms() else {
            return;
        };
        let Some(owner) = self.alarms.as_mut() else {
            return;
        };
        if !owner.enabled
            || owner.busy.is_some()
            || now < owner.next_at
            || self.closing.load(Ordering::Acquire)
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
            if binding.is_some_and(|b| b.scope == *scope && b.class.alarm_limits().is_some()) {
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
            Job::Alarm(Box::new(Completed {sequence,scope,kind:CompletedKind::Status(result),reservation:Some(reservation)}))
        }));
    }

    fn finish_alarm_turn(&mut self, sequence: u64) {
        if let Some(owner) = self.alarms.as_mut() {
            if owner.busy == Some(sequence) {
                owner.busy = None;
                if let Ok(now) = self.log.domain().mono_ms() {
                    owner.next_at = now.saturating_add(owner.options.interval_ms);
                }
            }
        }
    }

    pub(super) fn finish_alarm(&mut self, completed: Completed) {
        let Completed {
            sequence,
            scope,
            kind,
            reservation,
        } = completed;
        if !self
            .alarms
            .as_ref()
            .is_some_and(|o| o.busy == Some(sequence))
        {
            return;
        }
        if self.closing.load(Ordering::Acquire) {
            self.finish_alarm_turn(sequence);
            return;
        }
        let status = match kind {
            CompletedKind::Status(Ok(status)) => status,
            CompletedKind::Acted(result) => {
                if let Err(error) = result {
                    tracing::debug!(%error,"alarm attempt requires committed-state reconciliation");
                }
                self.finish_alarm_turn(sequence);
                return;
            }
            _ => {
                self.finish_alarm_turn(sequence);
                return;
            }
        };
        let Some(cell) = self.cells.get(&scope) else {
            self.finish_alarm_turn(sequence);
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
            self.finish_alarm_turn(sequence);
            return;
        }
        let Some(reservation) = reservation else {
            self.finish_alarm_turn(sequence);
            return;
        };
        let Ok(now) = u64::try_from(crate::asyncrt::wall_ms()) else {
            self.finish_alarm_turn(sequence);
            return;
        };
        self.sql(
            &scope,
            SqlPurpose::AlarmScan(sequence),
            Action::ScanAlarms { now, reservation },
        );
    }

    pub(super) fn finish_alarm_scan(
        &mut self,
        scope: GroupScope,
        sequence: u64,
        result: Result<sql::Output, ExecutionError>,
    ) {
        if !self
            .alarms
            .as_ref()
            .is_some_and(|o| o.busy == Some(sequence))
        {
            return;
        }
        if let Some(cell) = self.cells.get_mut(&scope) {
            cell.sql_busy = false;
        }
        if self.closing.load(Ordering::Acquire) {
            self.finish_alarm_turn(sequence);
            return;
        }
        let scan = match result {
            Ok(sql::Output {
                value: Value::AlarmScan(Some(scan)),
                ..
            }) => *scan,
            _ => {
                self.finish_alarm_turn(sequence);
                return;
            }
        };
        let Some(cell) = self.cells.get(&scope) else {
            self.finish_alarm_turn(sequence);
            return;
        };
        if cell.failed
            || cell.retired
            || cell.pending.is_some()
            || cell.incoming.is_some()
            || cell.publication.is_some()
        {
            self.finish_alarm_turn(sequence);
            return;
        }
        let Some(due) = scan.due else {
            self.finish_alarm_turn(sequence);
            return;
        };
        if due.minimum == Durability::S3 && cell.archived < scan.position.index {
            self.finish_alarm_turn(sequence);
            return;
        }
        let Some(binding) = self
            .local
            .bindings
            .get(&(scope.namespace, scope.cell.clone()))
            .cloned()
            .filter(|b| b.scope == scope)
        else {
            self.finish_alarm_turn(sequence);
            return;
        };
        let Some(contract) = binding.class.system_contract(&due.operation) else {
            self.finish_alarm_turn(sequence);
            return;
        };
        let Some(owner) = self.alarms.as_ref() else {
            return;
        };
        let Ok(now) = u64::try_from(crate::asyncrt::wall_ms()) else {
            self.finish_alarm_turn(sequence);
            return;
        };
        let Some(retry_until) = now.checked_add(owner.options.retry_window_ms) else {
            self.finish_alarm_turn(sequence);
            return;
        };
        let mut options = client::CommandOptions::new(scan.next, retry_until);
        options.durability = due.minimum;
        options.timeout = std::time::Duration::from_millis(owner.options.operation_timeout_ms);
        let local = crate::application::consensus::RaftCellHandle {
            handle: self.local.clone(),
            binding,
        };
        // This exact next client sequence comes from committed state, after the
        // host has no previous producer for this group and the runtime reached
        // current-term readiness. It cannot rebind an unresolved old producer.
        let call = local.system_command(contract, due.input, options);
        self.jobs.push(Box::pin(async move {
            let _reservation = scan._reservation;
            let result = call.await.and_then(|receipt| {
                if receipt.result == [0] || receipt.result == [1] {
                    Ok(())
                } else {
                    Err(CommandFailure::CorruptState)
                }
            });
            Job::Alarm(Box::new(Completed {
                sequence,
                scope,
                kind: CompletedKind::Acted(result),
                reservation: None,
            }))
        }));
    }
}

//! Host/engine-lifetime observations with bounded storage and short lock scopes.
use crate::application::consensus::{
    RaftDurationBucket, RaftOperationPerformance, RaftPerformanceCounter, RaftPerformanceSnapshot,
};
use crate::asyncrt::DomainToken;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};

pub(super) const BOUNDS_US: [u64; 16] = [
    50,
    100,
    250,
    500,
    1000,
    2500,
    5000,
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    1_000_000,
    5_000_000,
    30_000_000,
    u64::MAX,
];

#[derive(Clone, Copy)]
pub(crate) enum Operation {
    EngineQueue,
    EngineFileSync,
    EngineDirectorySync,
    EnginePersist,
    SqlQueue,
    SqlStage,
    SqlPrepare,
    SqlPublish,
    SqlQuery,
    SqlArchiveCapture,
    SqlOther,
    ObjectGet,
    ObjectPutCreate,
    ObjectPutUpdate,
    ObjectPutOverwrite,
    ControllerProbe,
    ControllerAction,
    ControlQueue,
    DataQueue,
    ControlExchange,
    DataExchange,
    ControlAdmit,
    DataAdmit,
    ArchiveAdmissionCheck,
    ArchiveAdmissionWait,
}

const OPERATIONS: [Operation; 25] = [
    Operation::EngineQueue,
    Operation::EngineFileSync,
    Operation::EngineDirectorySync,
    Operation::EnginePersist,
    Operation::SqlQueue,
    Operation::SqlStage,
    Operation::SqlPrepare,
    Operation::SqlPublish,
    Operation::SqlQuery,
    Operation::SqlArchiveCapture,
    Operation::SqlOther,
    Operation::ObjectGet,
    Operation::ObjectPutCreate,
    Operation::ObjectPutUpdate,
    Operation::ObjectPutOverwrite,
    Operation::ControllerProbe,
    Operation::ControllerAction,
    Operation::ControlQueue,
    Operation::DataQueue,
    Operation::ControlExchange,
    Operation::DataExchange,
    Operation::ControlAdmit,
    Operation::DataAdmit,
    Operation::ArchiveAdmissionCheck,
    Operation::ArchiveAdmissionWait,
];

impl Operation {
    fn name(self) -> &'static str {
        match self {
            Self::EngineQueue => "engine_queue",
            Self::EngineFileSync => "engine_file_sync",
            Self::EngineDirectorySync => "engine_directory_sync",
            Self::EnginePersist => "engine_persist",
            Self::SqlQueue => "sql_queue",
            Self::SqlStage => "sql_stage",
            Self::SqlPrepare => "sql_prepare",
            Self::SqlPublish => "sql_publish",
            Self::SqlQuery => "sql_query",
            Self::SqlArchiveCapture => "sql_archive_capture",
            Self::SqlOther => "sql_other",
            Self::ObjectGet => "object_get",
            Self::ObjectPutCreate => "object_put_create",
            Self::ObjectPutUpdate => "object_put_update",
            Self::ObjectPutOverwrite => "object_put_overwrite",
            Self::ControllerProbe => "controller_probe",
            Self::ControllerAction => "controller_action",
            Self::ControlQueue => "transport_control_queue",
            Self::DataQueue => "transport_data_queue",
            Self::ControlExchange => "transport_control_exchange",
            Self::DataExchange => "transport_data_exchange",
            Self::ControlAdmit => "transport_control_admit",
            Self::DataAdmit => "transport_data_admit",
            Self::ArchiveAdmissionCheck => "archive_admission_check",
            Self::ArchiveAdmissionWait => "archive_admission_wait",
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Counter {
    EngineReadBytes,
    EngineWriteRequestedBytes,
    EngineWriteCompletedBytes,
    PersistGroups,
    PersistEntries,
    PersistEntryBytes,
    SqlStageInputBytes,
    SqlPreparedProposalBytes,
    SqlPreparedLtxBytes,
    SqlQueryInputBytes,
    SqlQueryOutputBytes,
    ObjectReadBytes,
    ObjectPutRequestedBytes,
    ObjectPutConfirmedBytes,
    ControlReadBytes,
    ControlWriteBytes,
    DataReadBytes,
    DataWriteBytes,
    QueryReadBytes,
    QueryWriteBytes,
    ArchiveAdmissionGranted,
    ArchiveAdmissionObservationRefused,
    ArchiveAdmissionRetainedBytesRefused,
    ArchiveAdmissionRetainedEntriesRefused,
    ArchiveAdmissionReservedBytesDeferred,
    ArchiveAdmissionReservedEntriesDeferred,
}

const COUNTERS: [Counter; 26] = [
    Counter::EngineReadBytes,
    Counter::EngineWriteRequestedBytes,
    Counter::EngineWriteCompletedBytes,
    Counter::PersistGroups,
    Counter::PersistEntries,
    Counter::PersistEntryBytes,
    Counter::SqlStageInputBytes,
    Counter::SqlPreparedProposalBytes,
    Counter::SqlPreparedLtxBytes,
    Counter::SqlQueryInputBytes,
    Counter::SqlQueryOutputBytes,
    Counter::ObjectReadBytes,
    Counter::ObjectPutRequestedBytes,
    Counter::ObjectPutConfirmedBytes,
    Counter::ControlReadBytes,
    Counter::ControlWriteBytes,
    Counter::DataReadBytes,
    Counter::DataWriteBytes,
    Counter::QueryReadBytes,
    Counter::QueryWriteBytes,
    Counter::ArchiveAdmissionGranted,
    Counter::ArchiveAdmissionObservationRefused,
    Counter::ArchiveAdmissionRetainedBytesRefused,
    Counter::ArchiveAdmissionRetainedEntriesRefused,
    Counter::ArchiveAdmissionReservedBytesDeferred,
    Counter::ArchiveAdmissionReservedEntriesDeferred,
];

impl Counter {
    fn name(self) -> &'static str {
        match self {
            Self::EngineReadBytes => "engine_file_read_bytes",
            Self::EngineWriteRequestedBytes => "engine_file_write_requested_bytes",
            Self::EngineWriteCompletedBytes => "engine_file_write_completed_bytes",
            Self::PersistGroups => "engine_persist_groups",
            Self::PersistEntries => "engine_persist_entries",
            Self::PersistEntryBytes => "engine_persist_entry_bytes",
            Self::SqlStageInputBytes => "sql_stage_input_bytes",
            Self::SqlPreparedProposalBytes => "sql_prepared_proposal_bytes",
            Self::SqlPreparedLtxBytes => "sql_prepared_ltx_bytes",
            Self::SqlQueryInputBytes => "sql_query_input_bytes",
            Self::SqlQueryOutputBytes => "sql_query_output_bytes",
            Self::ObjectReadBytes => "object_get_body_bytes",
            Self::ObjectPutRequestedBytes => "object_put_requested_bytes",
            Self::ObjectPutConfirmedBytes => "object_put_confirmed_bytes",
            Self::ControlReadBytes => "transport_control_read_bytes",
            Self::ControlWriteBytes => "transport_control_write_bytes",
            Self::DataReadBytes => "transport_data_read_bytes",
            Self::DataWriteBytes => "transport_data_write_bytes",
            Self::QueryReadBytes => "transport_query_read_bytes",
            Self::QueryWriteBytes => "transport_query_write_bytes",
            Self::ArchiveAdmissionGranted => "archive_admission_granted",
            Self::ArchiveAdmissionObservationRefused => "archive_admission_observation_refused",
            Self::ArchiveAdmissionRetainedBytesRefused => {
                "archive_admission_retained_bytes_refused"
            }
            Self::ArchiveAdmissionRetainedEntriesRefused => {
                "archive_admission_retained_entries_refused"
            }
            Self::ArchiveAdmissionReservedBytesDeferred => {
                "archive_admission_reserved_bytes_deferred"
            }
            Self::ArchiveAdmissionReservedEntriesDeferred => {
                "archive_admission_reserved_entries_deferred"
            }
        }
    }
}

#[derive(Default)]
struct Samples {
    started: u64,
    succeeded: u64,
    failed: u64,
    abandoned: u64,
    unavailable: u64,
    sum: u64,
    buckets: [u64; 16],
}

pub(crate) struct Registry {
    clock: DomainToken,
    source: String,
    available: AtomicBool,
    saturated: AtomicBool,
    counters: [AtomicU64; COUNTERS.len()],
    operations: [Mutex<Samples>; OPERATIONS.len()],
}

impl Registry {
    pub(crate) fn new(clock: DomainToken) -> Arc<Self> {
        use rand::RngCore as _;
        let mut source = [0; 16];
        crate::asyncrt::rng("raft-performance-source").fill_bytes(&mut source);
        Arc::new(Self {
            clock,
            source: hex::encode(source),
            available: AtomicBool::new(true),
            saturated: AtomicBool::new(false),
            counters: std::array::from_fn(|_| AtomicU64::new(0)),
            operations: std::array::from_fn(|_| Mutex::new(Samples::default())),
        })
    }

    pub(crate) fn mark_unavailable(&self) {
        self.available.store(false, Ordering::Relaxed);
    }

    fn add(&self, value: &mut u64, amount: u64) {
        match value.checked_add(amount) {
            Some(next) => *value = next,
            None => {
                *value = u64::MAX;
                self.saturated.store(true, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn increment(&self, counter: Counter, amount: u64) {
        let _ = self.counters[counter as usize].fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |value| {
                Some(value.checked_add(amount).unwrap_or_else(|| {
                    self.saturated.store(true, Ordering::Relaxed);
                    u64::MAX
                }))
            },
        );
    }

    fn update(&self, operation: Operation, f: impl FnOnce(&mut Samples)) {
        match self.operations[operation as usize].lock() {
            Ok(mut sample) => f(&mut sample),
            Err(_) => self.available.store(false, Ordering::Relaxed),
        }
    }

    pub(crate) fn timer(self: &Arc<Self>, operation: Operation) -> Timer {
        let start = self.clock.mono_us().ok();
        self.update(operation, |sample| self.add(&mut sample.started, 1));
        Timer {
            registry: self.clone(),
            operation,
            start,
            outcome: None,
        }
    }

    fn record(&self, operation: Operation, outcome: Option<bool>, elapsed: Option<u64>) {
        self.update(operation, |sample| {
            match outcome {
                Some(true) => self.add(&mut sample.succeeded, 1),
                Some(false) => self.add(&mut sample.failed, 1),
                None => self.add(&mut sample.abandoned, 1),
            };
            if let Some(us) = elapsed {
                let bucket = BOUNDS_US.partition_point(|bound| *bound < us);
                self.add(&mut sample.buckets[bucket], 1);
                self.add(&mut sample.sum, us);
            } else {
                self.add(&mut sample.unavailable, 1);
            }
        });
    }

    pub(crate) fn snapshot(&self) -> RaftPerformanceSnapshot {
        let sampled_at_us = self.clock.mono_us().ok();
        let operations = OPERATIONS
            .iter()
            .map(|operation| {
                let sample = match self.operations[*operation as usize].lock() {
                    Ok(sample) => sample,
                    Err(poison) => {
                        self.available.store(false, Ordering::Relaxed);
                        poison.into_inner()
                    }
                };
                let mut count = 0u64;
                let buckets = BOUNDS_US
                    .iter()
                    .zip(sample.buckets)
                    .map(|(bound, n)| {
                        self.add(&mut count, n);
                        RaftDurationBucket {
                            upper_us: (*bound != u64::MAX).then_some(*bound),
                            count,
                        }
                    })
                    .collect();
                RaftOperationPerformance {
                    name: operation.name(),
                    started: sample.started,
                    succeeded: sample.succeeded,
                    failed: sample.failed,
                    abandoned: sample.abandoned,
                    timing_unavailable: sample.unavailable,
                    count,
                    sum_us: sample.sum,
                    buckets,
                }
            })
            .collect();
        RaftPerformanceSnapshot {
            source: self.source.clone(),
            available: self.available.load(Ordering::Relaxed),
            saturated: self.saturated.load(Ordering::Relaxed),
            sampled_at_us,
            counters: COUNTERS
                .iter()
                .map(|counter| RaftPerformanceCounter {
                    name: counter.name(),
                    value: self.counters[*counter as usize].load(Ordering::Relaxed),
                })
                .collect(),
            operations,
        }
    }
}

pub(crate) struct Timer {
    registry: Arc<Registry>,
    operation: Operation,
    start: Option<u64>,
    outcome: Option<bool>,
}

impl Timer {
    pub(crate) fn finish(mut self, succeeded: bool) {
        self.outcome = Some(succeeded);
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let elapsed = self
            .start
            .zip(self.registry.clock.mono_us().ok())
            .and_then(|(start, end)| end.checked_sub(start));
        self.registry.record(self.operation, self.outcome, elapsed);
    }
}

#[cfg(test)]
#[path = "performance_tests.rs"]
mod tests;

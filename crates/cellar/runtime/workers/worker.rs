// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Adapted for Cellar from Celld: crates/celld/js.rs.

//! Host worker dispatch for compiled Rust applications.

use crate::host_turn::{CellJob, CellStorage, InFlight, Op};
use crate::worker_config::WorkerConfig;
use anyhow::Result;
use std::any::Any;
use std::sync::Arc;

/// Memory retained by a worker backend, when its allocator can measure it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HeapBytes {
    pub physical: u64,
    pub external: u64,
}

/// A serially entered worker. Its backend owns executor-specific state;
/// host dispatch carries only shared jobs, turns and storage authority.
pub struct Worker {
    backend: Box<dyn WorkerBackend>,
}

/// A worker executor's boundary with placement and lifecycle orchestration.
/// Dropping the backend must release its original cells and all private state.
pub(crate) trait WorkerBackend: Any + Send {
    fn heap_bytes(&self) -> Option<HeapBytes>;

    fn turn_begin(
        &mut self,
        job: crate::WorkerJob,
        trace: Option<crate::telemetry::TraceContext>,
    ) -> (Option<InFlight>, Vec<Op>);

    fn turn_cancel(&mut self, entry: &mut InFlight) -> Vec<Op>;

    fn turn_cancel_for_shutdown(&mut self, entry: &mut InFlight) -> Vec<Op>;

    fn turn_retire_input_gates(&mut self, entry: &InFlight);

    fn turn_abandon_input_gates(&mut self, entry: &InFlight);

    fn turn_deliver(
        &mut self,
        entry: &mut InFlight,
        op: u64,
        res: Result<crate::asyncrt::OpOut, String>,
    ) -> Vec<Op>;

    fn own_cell(
        &mut self,
        cell: &str,
        storage: Option<CellStorage<'_>>,
    ) -> Result<cellar_logic::wake::AlarmSnapshot>;

    fn own_embedded_cell(
        &mut self,
        cell: &str,
        parent: &crate::storage::StorageIdentity,
        name: &str,
        id_sc: Vec<u8>,
        props_sc: Vec<u8>,
        file: (std::path::PathBuf, bool),
    ) -> Result<u64>;

    fn close_facet_tree(
        &mut self,
        cell: &str,
        root: &str,
        facet_path: &[String],
    ) -> (Vec<String>, Result<()>);

    fn settle_heap(&mut self);

    fn take_alarm_moves(&mut self) -> Vec<(String, cellar_logic::wake::AlarmSnapshot)>;

    fn set_id_name(&mut self, scope: &str, name: &str) -> Result<()>;

    fn turn_begin_cell(
        &mut self,
        job: CellJob,
        trace: Option<crate::telemetry::TraceContext>,
    ) -> (Option<InFlight>, Vec<Op>);

    fn turn_poll(&mut self, entry: &mut InFlight) -> Vec<Op>;

    fn turn_cancel_pending_events(&mut self, entry: &mut InFlight) -> Vec<Op>;

    fn turn_finish_alarm(&mut self, entry: &mut InFlight) -> Result<Option<u64>>;
}

impl Worker {
    /// Load a registered compiled Rust application.
    pub fn load_config(config: Arc<WorkerConfig>) -> Result<Self> {
        anyhow::ensure!(config.native_application.is_some(),
            "Cellar requires a registered compiled Rust application; JavaScript source execution has been removed");
        Ok(Self {
            backend: Box::new(crate::native_worker::NativeWorker::new(config)?),
        })
    }

    #[cfg(cellar_internal_tests)]
    pub fn load(options: crate::engine_api::WorkerConfigOptions) -> Result<Self> {
        Self::load_config(Arc::new(WorkerConfig::new(options)))
    }

    #[cfg(any(test, cellar_internal_tests))]
    pub(crate) fn backend_mut<T: Any>(&mut self) -> Option<&mut T> {
        (&mut *self.backend as &mut dyn Any).downcast_mut()
    }

    #[cfg(cellar_internal_tests)]
    pub(crate) fn backend<T: Any>(&self) -> Option<&T> {
        (&*self.backend as &dyn Any).downcast_ref()
    }

    pub fn heap_bytes(&self) -> Option<HeapBytes> {
        self.backend.heap_bytes()
    }

    pub fn turn_begin(
        &mut self,
        job: crate::WorkerJob,
        trace: Option<crate::telemetry::TraceContext>,
    ) -> (Option<InFlight>, Vec<Op>) {
        self.backend.turn_begin(job, trace)
    }

    pub fn turn_cancel(&mut self, entry: &mut InFlight) -> Vec<Op> {
        self.backend.turn_cancel(entry)
    }

    pub fn turn_cancel_for_shutdown(&mut self, entry: &mut InFlight) -> Vec<Op> {
        self.backend.turn_cancel_for_shutdown(entry)
    }

    pub(crate) fn turn_retire_input_gates(&mut self, entry: &InFlight) {
        self.backend.turn_retire_input_gates(entry)
    }

    pub(crate) fn turn_abandon_input_gates(&mut self, entry: &InFlight) {
        self.backend.turn_abandon_input_gates(entry)
    }

    pub fn turn_deliver(
        &mut self,
        entry: &mut InFlight,
        op: u64,
        res: Result<crate::asyncrt::OpOut, String>,
    ) -> Vec<Op> {
        self.backend.turn_deliver(entry, op, res)
    }

    pub fn own_cell(
        &mut self,
        cell: &str,
        storage: Option<CellStorage<'_>>,
    ) -> Result<cellar_logic::wake::AlarmSnapshot> {
        self.backend.own_cell(cell, storage)
    }

    pub(crate) fn settle_heap(&mut self) {
        self.backend.settle_heap()
    }

    pub(crate) fn own_embedded_cell(
        &mut self,
        cell: &str,
        parent: &crate::storage::StorageIdentity,
        name: &str,
        id_sc: Vec<u8>,
        props_sc: Vec<u8>,
        file: (std::path::PathBuf, bool),
    ) -> Result<u64> {
        self.backend
            .own_embedded_cell(cell, parent, name, id_sc, props_sc, file)
    }

    pub(crate) fn close_facet_tree(
        &mut self,
        cell: &str,
        root: &str,
        facet_path: &[String],
    ) -> (Vec<String>, Result<()>) {
        self.backend.close_facet_tree(cell, root, facet_path)
    }

    pub fn take_alarm_moves(&mut self) -> Vec<(String, cellar_logic::wake::AlarmSnapshot)> {
        self.backend.take_alarm_moves()
    }

    pub fn set_id_name(&mut self, scope: &str, name: &str) -> Result<()> {
        self.backend.set_id_name(scope, name)
    }

    pub fn turn_begin_cell(
        &mut self,
        job: CellJob,
        trace: Option<crate::telemetry::TraceContext>,
    ) -> (Option<InFlight>, Vec<Op>) {
        self.backend.turn_begin_cell(job, trace)
    }

    pub fn turn_poll(&mut self, entry: &mut InFlight) -> Vec<Op> {
        self.backend.turn_poll(entry)
    }

    pub(crate) fn turn_cancel_pending_events(&mut self, entry: &mut InFlight) -> Vec<Op> {
        self.backend.turn_cancel_pending_events(entry)
    }

    pub fn turn_finish_alarm(&mut self, entry: &mut InFlight) -> Result<Option<u64>> {
        self.backend.turn_finish_alarm(entry)
    }
}

#[cfg(all(test, not(cellar_internal_tests)))]
mod tests {
    use super::*;
    use crate::engine_api::{Compat, WorkerConfigOptions};

    fn config() -> WorkerConfig {
        WorkerConfig::new(WorkerConfigOptions {
            src: "not a JavaScript module!".into(),
            script_name: "cold-native".into(),
            do_classes: Vec::new(),
            bindings: Vec::new(),
            blob_bindings: Vec::new(),
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: Vec::new(),
            node: "test".into(),
            modules: Vec::new(),
            compat: Compat::default(),
        })
    }

    #[test]
    fn native_bootstrap_never_initializes_or_looks_up_the_source_backend() {
        let application = crate::application::Registry::new(|_, _| {
            Box::pin(async { Ok(crate::application::Output::Done) })
        });
        let mut native = Worker::load_config(Arc::new(
            config().with_native_application(Arc::new(application)),
        ))
        .unwrap();
        assert!(native
            .backend_mut::<crate::native_worker::NativeWorker>()
            .is_some());
        drop(native);
        let error = Worker::load_config(Arc::new(config())).err().unwrap();
        assert!(error
            .to_string()
            .contains("requires a registered compiled Rust application"));
    }
}

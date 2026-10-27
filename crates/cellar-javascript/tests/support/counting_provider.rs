// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Distinguish explicit process-handle retirement from cleanup at event drop.
use cellar::sdk::{containers::MaintenanceBackend, Context};
use cellar_container::{
    Environment, Fencing, Limits, Operation, Provider, ProviderFuture, ProviderSession,
};
use serde_json::Value;
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

#[derive(Default)]
pub struct Counts {
    pub exec_calls: AtomicUsize,
    pub created: AtomicUsize,
    pub released: AtomicUsize,
    pub active: AtomicUsize,
    pub unreleased: AtomicUsize,
    pub stdout_reads: AtomicUsize,
}

pub struct CountingProvider {
    pub inner: Arc<dyn Provider>,
    pub counts: Arc<Counts>,
}

impl Provider for CountingProvider {
    fn maintenance_backend(&self) -> Option<Arc<dyn MaintenanceBackend>> {
        self.inner.maintenance_backend()
    }

    fn binding_name(&self) -> Option<&str> {
        self.inner.binding_name()
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn environment(&self) -> Environment {
        self.inner.environment()
    }

    fn fencing(&self) -> Fencing {
        self.inner.fencing()
    }

    fn max_resources(&self) -> Option<usize> {
        self.inner.max_resources()
    }

    fn synchronous_running(&self) -> bool {
        self.inner.synchronous_running()
    }

    fn queued_lifecycle(&self) -> bool {
        self.inner.queued_lifecycle()
    }

    fn operations(&self) -> &[Operation] {
        self.inner.operations()
    }

    fn open(&self) -> Box<dyn ProviderSession> {
        Box::new(Session {
            inner: self.inner.open(),
            counts: self.counts.clone(),
            handles: Mutex::new(HashSet::new()),
        })
    }
}

struct Session {
    inner: Box<dyn ProviderSession>,
    counts: Arc<Counts>,
    handles: Mutex<HashSet<String>>,
}

impl ProviderSession for Session {
    fn preflight(
        &self,
        context: &Context,
        operation: Operation,
        input: &Value,
    ) -> anyhow::Result<()> {
        self.inner.preflight(context, operation, input)
    }

    fn running_snapshot(&self, context: &Context) -> anyhow::Result<bool> {
        self.inner.running_snapshot(context)
    }

    fn execute<'a>(
        &'a self,
        context: &'a Context,
        operation: Operation,
        input: Value,
        limits: &'a Limits,
    ) -> ProviderFuture<'a> {
        Box::pin(async move {
            let id = input.get("id").and_then(Value::as_str).map(str::to_owned);
            if matches!(operation, Operation::Exec | Operation::ExecOutput) {
                self.counts.exec_calls.fetch_add(1, Ordering::SeqCst);
            }
            if operation == Operation::ReadStdout {
                self.counts.stdout_reads.fetch_add(1, Ordering::SeqCst);
            }
            let result = self.inner.execute(context, operation, input, limits).await;
            if let Ok(value) = &result {
                if operation == Operation::Exec {
                    if let Some(id) = value.as_str() {
                        self.handles.lock().unwrap().insert(id.to_owned());
                        self.counts.created.fetch_add(1, Ordering::SeqCst);
                        self.counts.active.fetch_add(1, Ordering::SeqCst);
                    }
                } else if operation == Operation::Release
                    && id.is_some_and(|id| self.handles.lock().unwrap().remove(&id))
                {
                    self.counts.released.fetch_add(1, Ordering::SeqCst);
                    self.counts.active.fetch_sub(1, Ordering::SeqCst);
                }
            }
            result
        })
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let remaining = self.handles.get_mut().unwrap().len();
        self.counts
            .unreleased
            .fetch_add(remaining, Ordering::SeqCst);
        self.counts.active.fetch_sub(remaining, Ordering::SeqCst);
    }
}

//! Hold real native activation destruction while a read reaches its reset queue.
use super::*;
use std::sync::{atomic::AtomicBool, Condvar, Mutex};
use tokio::sync::Semaphore;

pub(super) struct Reset {
    pub(super) armed: AtomicBool,
    pub(super) entered: Semaphore,
    pub(super) timed_out: AtomicBool,
    released: Mutex<bool>,
    changed: Condvar,
}

impl Default for Reset {
    fn default() -> Self {
        Self {
            armed: AtomicBool::new(false),
            entered: Semaphore::new(0),
            timed_out: AtomicBool::new(false),
            released: Mutex::new(false),
            changed: Condvar::new(),
        }
    }
}

impl Reset {
    pub(super) fn open(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

pub(super) struct Release(pub(super) Arc<Reset>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.open();
    }
}

pub(super) struct Observed {
    pub(super) application: Arc<PythonApplication>,
    pub(super) reset: Arc<Reset>,
}

pub(super) struct Reading(pub(super) tokio::task::JoinHandle<reqwest::Result<reqwest::Response>>);

impl Drop for Reading {
    fn drop(&mut self) {
        // The normal path joins this handle. A failed surrounding assertion
        // cancels its read before the caller-owned test runtime joins workers.
        self.0.abort();
    }
}

impl Application for Observed {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        self.application.clone().invoke(context, event)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.application.supports_cell(class)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        Ok(Arc::new(Instance {
            cell: self.application.create_cell(context, identity)?,
            reset: self.reset.clone(),
        }))
    }

    fn validate_configuration(
        &self,
        options: &cellar::engine_api::WorkerConfigOptions,
    ) -> Result<()> {
        self.application.validate_configuration(options)
    }

    fn validate_deployment(&self, deployment: &cellar::fleet::LoadedDeployment) -> Result<()> {
        self.application.validate_deployment(deployment)
    }
}

struct Instance {
    cell: Arc<dyn Cell>,
    reset: Arc<Reset>,
}

impl Cell for Instance {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        self.cell.clone().invoke(context, event)
    }
}

impl Drop for Instance {
    fn drop(&mut self) {
        if self.reset.armed.swap(false, Ordering::AcqRel) {
            self.reset.entered.add_permits(1);
            // This deliberately blocks one caller-owned test executor worker,
            // never the interpreter thread. Other workers drive the real read
            // and actor queue. A failed probe cannot leave the worker parked.
            let (_released, timeout) = self
                .reset
                .changed
                .wait_timeout_while(
                    self.reset.released.lock().unwrap(),
                    Duration::from_secs(3),
                    |released| !*released,
                )
                .unwrap();
            if timeout.timed_out() {
                self.reset.timed_out.store(true, Ordering::Release);
            }
        }
    }
}

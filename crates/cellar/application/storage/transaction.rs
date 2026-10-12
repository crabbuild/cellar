// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;
use crate::native_worker::Ready;
use futures_util::FutureExt;
use std::task::Poll;

const ACTIVE: u8 = 0;
const COMMITTED: u8 = 1;
const ROLLED_BACK: u8 = 2;

/// Reserve slots at the call boundary, rather than when a waiting gate wakes.
/// The completion lives inside the scheduled work and releases on every exit.
#[derive(Default)]
pub(super) struct Queue(Arc<Mutex<std::collections::VecDeque<Queued>>>);

struct Queued {
    id: Arc<()>,
    finished: bool,
    start: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
    done: tokio::sync::oneshot::Sender<Result<(), String>>,
}

struct Slot {
    queue: Arc<Mutex<std::collections::VecDeque<Queued>>>,
    id: Arc<()>,
    admission: Ready,
}

impl Queue {
    fn reserve(&self) -> (Slot, Ready) {
        let (done, receive) = tokio::sync::oneshot::channel();
        let ready: Ready = async move {
            // Abandoning a slot still advances the queue. The input gate owns
            // retirement/failure, so a slot never transfers that failure to
            // a later transaction by itself.
            let _ = receive.await;
            Ok(())
        }
        .boxed()
        .shared();
        let (start, receive_start) = tokio::sync::oneshot::channel();
        let admission: Ready = async move {
            receive_start
                .await
                .map_err(|_| "native transaction slot was abandoned".to_string())?
        }
        .boxed()
        .shared();
        let id = Arc::new(());
        let mut queue = self.0.lock().unwrap();
        let start = if queue.is_empty() {
            let _ = start.send(Ok(()));
            None
        } else {
            Some(start)
        };
        queue.push_back(Queued {
            id: id.clone(),
            finished: false,
            start,
            done,
        });
        (
            Slot {
                queue: self.0.clone(),
                id,
                admission,
            },
            ready,
        )
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        let mut signals = Vec::new();
        let mut queue = self.queue.lock().unwrap();
        if let Some(slot) = queue
            .iter_mut()
            .find(|slot| Arc::ptr_eq(&slot.id, &self.id))
        {
            slot.finished = true;
        }
        // A cancelled queued slot cannot let a later one skip its active
        // predecessor. Advance completed slots iteratively, avoiding a deep
        // chain of futures after many cancelled reservations.
        while queue.front().is_some_and(|slot| slot.finished) {
            signals.push(queue.pop_front().unwrap().done);
        }
        if let Some(slot) = queue.front_mut() {
            if let Some(start) = slot.start.take() {
                signals.push(start);
            }
        }
        drop(queue);
        for signal in signals {
            let _ = signal.send(Ok(()));
        }
    }
}

pub(crate) struct Transaction {
    status: AtomicU8,
    closed: AtomicBool,
    parent: Option<Arc<Transaction>>,
    savepoint: String,
    nested: bool,
    asynchronous: bool,
    queue: Queue,
    pending: Mutex<Vec<Ready>>,
}

impl Transaction {
    pub(crate) fn check_active(&self) -> Result<()> {
        let mut current = Some(self);
        let mut committed = false;
        while let Some(transaction) = current {
            match transaction.status.load(Ordering::Acquire) {
                ROLLED_BACK => anyhow::bail!("Cannot use a rolled back transaction"),
                COMMITTED => committed = true,
                _ => {}
            }
            current = transaction.parent.as_deref();
        }
        anyhow::ensure!(!committed, "Cannot use a transaction that has already committed; did you move it outside the closure?");
        Ok(())
    }

    pub(crate) fn retain_block(&self, ready: Ready) -> Result<()> {
        self.check_active()?;
        anyhow::ensure!(
            self.asynchronous,
            "asynchronous blocks cannot start inside transaction_sync"
        );
        self.pending.lock().unwrap().push(ready);
        Ok(())
    }

    fn ancestor_closed(&self) -> bool {
        let mut parent = self.parent.as_deref();
        while let Some(transaction) = parent {
            if transaction.closed.load(Ordering::Acquire) {
                return true;
            }
            parent = transaction.parent.as_deref();
        }
        false
    }

    fn control(&self, storage: &Storage, action: &str) -> Result<()> {
        // Cleanup needs its original connection after a failed critical
        // section. It ignores the failure flag, never activation identity.
        crate::native_worker::transaction_control(
            storage.installed_activation_scope()?,
            action,
            self.nested,
            &self.savepoint,
        )
    }

    async fn drain(&self) {
        loop {
            let pending = std::mem::take(&mut *self.pending.lock().unwrap());
            if pending.is_empty() {
                break;
            }
            for child in pending {
                let _ = child.await;
            }
        }
    }
}

/// The active helper view is installed only while its callback is polling.
/// Another future belonging to the same event cannot borrow it across awaits.
pub(crate) struct Current {
    activation: Arc<Activation>,
    previous: Option<Arc<Transaction>>,
}

impl Current {
    pub(crate) fn enter(view: &Storage) -> Self {
        let previous = std::mem::replace(
            &mut *view.0.resources.transaction.lock().unwrap(),
            view.1.clone(),
        );
        Self {
            activation: view.0.clone(),
            previous,
        }
    }
}

impl Drop for Current {
    fn drop(&mut self) {
        *self.activation.resources.transaction.lock().unwrap() = self.previous.take();
    }
}

struct Guard {
    view: Storage,
    previous_open: Option<Arc<Transaction>>,
}

impl Guard {
    fn start(
        storage: &Storage,
        parent: Option<Arc<Transaction>>,
        asynchronous: bool,
    ) -> Result<Self> {
        storage.activation_scope()?;
        if let Some(parent) = &parent {
            parent.check_active()?;
        }
        let previous_open = storage.0.resources.open_transaction.lock().unwrap().clone();
        anyhow::ensure!(
            match (&previous_open, &parent) {
                (None, None) => true,
                (Some(open), Some(parent)) => Arc::ptr_eq(open, parent),
                _ => false,
            },
            "native transaction does not own the innermost savepoint"
        );
        let serial = storage
            .0
            .resources
            .transaction_serial
            .fetch_add(1, Ordering::Relaxed);
        let transaction = Arc::new(Transaction {
            status: AtomicU8::new(ACTIVE),
            closed: AtomicBool::new(false),
            nested: parent.is_some(),
            parent,
            savepoint: format!("cells_tx_{serial}"),
            asynchronous,
            queue: Queue::default(),
            pending: Mutex::new(Vec::new()),
        });
        transaction.control(storage, "start")?;
        *storage.0.resources.open_transaction.lock().unwrap() = Some(transaction.clone());
        Ok(Self {
            view: Storage(storage.0.clone(), Some(transaction)),
            previous_open,
        })
    }

    fn finish(self) -> Result<()> {
        let transaction = self.view.1.as_ref().expect("transaction view");
        if transaction.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        let action = if transaction.status.load(Ordering::Acquire) == ROLLED_BACK {
            "rollback"
        } else {
            "commit"
        };
        if action == "commit" {
            self.view.activation_scope()?;
        }
        transaction.control(&self.view, action)?;
        transaction.closed.store(true, Ordering::Release);
        if action == "commit" {
            transaction.status.store(COMMITTED, Ordering::Release);
        }
        Ok(())
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let transaction = self.view.1.as_ref().expect("transaction view");
        if !transaction.closed.load(Ordering::Acquire) {
            // An outer rollback destroys its children's savepoints. A child
            // cancelled afterwards must not roll back the next transaction.
            if !transaction.ancestor_closed()
                && !self.view.0.resources.retired.load(Ordering::Acquire)
            {
                if let Err(error) = transaction.control(&self.view, "rollback") {
                    let reason = format!("a transaction rollback failed: {error}");
                    if self.view.installed_activation_scope().is_ok() {
                        crate::storage::poison_transaction(&self.view.0.scope, reason.clone());
                    }
                    self.view.0.fail(reason);
                }
            }
            transaction.status.store(ROLLED_BACK, Ordering::Release);
            transaction.closed.store(true, Ordering::Release);
        }
        let mut open = self.view.0.resources.open_transaction.lock().unwrap();
        let mut current = open.as_ref();
        let mut owns = false;
        while let Some(layer) = current {
            if Arc::ptr_eq(layer, transaction) {
                owns = true;
                break;
            }
            current = layer.parent.as_ref();
        }
        if owns {
            *open = self
                .previous_open
                .take()
                .filter(|previous| !previous.closed.load(Ordering::Acquire));
        }
    }
}

async fn callback_result<T, F, Fut>(view: Storage, callback: F) -> Result<T>
where
    F: FnOnce(Storage) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let future = {
        let _current = Current::enter(&view);
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback(view.clone())))
            .map_err(crate::native_worker::panic_error)?
    };
    let mut future = std::pin::pin!(future);
    futures_util::future::poll_fn(|cx| {
        let _current = Current::enter(&view);
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.as_mut().poll(cx)))
            .unwrap_or_else(|panic| Poll::Ready(Err(crate::native_worker::panic_error(panic))))
    })
    .await
}

impl Storage {
    /// Hold the host input gate through an asynchronous transaction. Ordinary
    /// callback errors and panics roll back without resetting the cell. Root
    /// transactions reserve slots in call order; nested children finish before
    /// this boundary commits or rolls back, even if their futures are dropped.
    pub fn transaction<T, F, Fut>(&self, context: &Context, callback: F) -> HandlerFuture<T>
    where
        T: Send + 'static,
        F: FnOnce(Storage) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let prepared = (|| -> Result<_> {
            context.check_turn()?;
            self.activation_scope()?;
            anyhow::ensure!(
                context
                    .io
                    .native_activation
                    .as_ref()
                    .is_some_and(|activation| Arc::ptr_eq(activation, &self.0)),
                "native transaction context belongs to another cell"
            );
            if let Some(view) = &self.1 {
                view.check_active()?;
            }
            let parent = self
                .0
                .resources
                .transaction
                .lock()
                .unwrap()
                .clone()
                .or_else(|| self.1.clone());
            if let Some(parent) = &parent {
                parent.check_active()?;
                anyhow::ensure!(
                    parent.asynchronous,
                    "asynchronous transactions cannot start inside transaction_sync"
                );
            }
            let (slot, done) = parent.as_ref().map_or_else(
                || self.0.resources.transaction_queue.reserve(),
                |parent| parent.queue.reserve(),
            );
            if let Some(parent) = &parent {
                parent.pending.lock().unwrap().push(done);
            }
            Ok((parent, slot))
        })();
        let (parent, slot) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let storage = self.clone();
        // Catch a transaction's failure inside the critical section, retaining
        // the original distinction between rollback and actor reset.
        let block = context.block_concurrency_while(move |_| async move {
            let result = async {
                slot.admission.clone().await.map_err(anyhow::Error::msg)?;
                let guard = Guard::start(&storage, parent, true)?;
                let result = callback_result(guard.view.clone(), callback).await;
                guard.view.1.as_ref().unwrap().drain().await;
                match result {
                    Ok(value) => {
                        guard.finish()?;
                        Ok(value)
                    }
                    Err(error) => {
                        drop(guard);
                        Err(error)
                    }
                }
            }
            .await;
            drop(slot); // release before the critical section drains children
            Ok(result)
        });
        Box::pin(async move { block.await? })
    }

    /// The callback executes within this poll and commits atomically on
    /// success. Errors and panics roll back; root helpers take the innermost
    /// active layer. An escaped view refuses use after its boundary.
    pub fn transaction_sync<T>(&self, callback: impl FnOnce(&Storage) -> Result<T>) -> Result<T> {
        self.scope()?;
        let parent = self
            .0
            .resources
            .transaction
            .lock()
            .unwrap()
            .clone()
            .or_else(|| self.1.clone());
        let guard = Guard::start(self, parent, false)?;
        let result = {
            let _current = Current::enter(&guard.view);
            callback(&guard.view)?
        };
        guard.finish()?;
        Ok(result)
    }

    /// Mark this view terminal immediately. An asynchronous child can still
    /// own a savepoint, so defer the physical rollback until children drain.
    pub fn rollback(&self) -> Result<()> {
        let transaction = self
            .1
            .as_ref()
            .ok_or_else(|| anyhow!("rollback() must be called on a transaction"))?;
        if transaction.status.load(Ordering::Acquire) == ROLLED_BACK {
            return Ok(());
        }
        transaction.check_active()?;
        self.activation_scope()?;
        let mut current = self.0.resources.transaction.lock().unwrap().clone();
        let mut owned = false;
        while let Some(layer) = current {
            if Arc::ptr_eq(&layer, transaction) {
                owned = true;
                break;
            }
            current = layer.parent.clone();
        }
        anyhow::ensure!(
            owned,
            "rollback() is outside the active transaction callback"
        );
        if transaction.pending.lock().unwrap().is_empty() {
            self.check_transaction_authority()?;
            transaction.control(self, "rollback_explicit")?;
            transaction.closed.store(true, Ordering::Release);
        }
        transaction.status.store(ROLLED_BACK, Ordering::Release);
        Ok(())
    }
}

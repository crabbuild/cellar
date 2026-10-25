// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::native_worker::{InputGate, Ready};
use futures_util::future::FutureExt;
use std::pin::Pin;
use std::sync::Weak;
use std::task::{Context as TaskContext, Poll};

pub(crate) struct Blocks {
    gate: Mutex<Weak<InputGate>>,
    current: Mutex<Option<Arc<Block>>>,
    pub(crate) initialization: Mutex<Vec<Ready>>,
    pub(crate) initializing: std::sync::atomic::AtomicBool,
}

impl Default for Blocks {
    fn default() -> Self {
        Self {
            gate: Mutex::new(Weak::new()),
            current: Mutex::new(None),
            initialization: Mutex::new(Vec::new()),
            initializing: std::sync::atomic::AtomicBool::new(true),
        }
    }
}

struct Block {
    gate: Arc<InputGate>,
    depth: usize,
    children: Mutex<Vec<Ready>>,
    active: std::sync::atomic::AtomicBool,
}

struct Lifetime {
    block: Arc<Block>,
    settled: bool,
}

impl Drop for Lifetime {
    fn drop(&mut self) {
        self.block
            .active
            .store(false, std::sync::atomic::Ordering::Release);
        if !self.settled {
            self.block
                .gate
                .fail("the cell's critical section ended without releasing".into());
        }
    }
}

/// Opaque native callback authority for optional language/FFI adapters.
/// Operations are polled in the original invocation and inherit only this
/// callback's block/transaction view. A retired boundary cannot be revived.
#[derive(Clone)]
pub struct CallbackScope {
    context: Context,
    authority: Arc<InvocationAuthority>,
    // Retaining an expired adapter token must never retain the host input gate.
    block: Weak<Block>,
    transaction: Option<Storage>,
}

impl CallbackScope {
    /// Execute an inline FFI callback with this boundary's native guards.
    pub fn run_sync<T>(
        &self,
        context: &Context,
        callback: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        self.check(context)?;
        let block = self
            .block
            .upgrade()
            .ok_or_else(|| anyhow!("native callback boundary is closed"))?;
        let _block = Current::enter(&self.context.state.blocks, block);
        let _transaction = self
            .transaction
            .as_ref()
            .map(storage::TransactionCurrent::enter);
        callback()
    }

    /// Roll back this callback's transaction view; subsequent operations refuse.
    pub fn rollback(&self, context: &Context) -> Result<()> {
        self.check(context)?;
        self.transaction
            .as_ref()
            .ok_or_else(|| anyhow!("callback has no transaction"))?
            .rollback()
    }

    pub fn check(&self, context: &Context) -> Result<()> {
        self.authority.check(context)?;
        let block = self
            .block
            .upgrade()
            .ok_or_else(|| anyhow!("native callback boundary is closed"))?;
        anyhow::ensure!(
            block.active.load(std::sync::atomic::Ordering::Acquire),
            "native callback boundary is closed"
        );
        if let Some(view) = &self.transaction {
            view.1.as_ref().unwrap().check_active()?;
        }
        Ok(())
    }

    /// Install native callback guards for each poll, restoring them before any
    /// unrelated continuation runs. Construct SDK effects inside this future.
    pub fn run<T: Send + 'static>(
        &self,
        context: &Context,
        future: impl Future<Output = Result<T>> + Send + 'static,
    ) -> HandlerFuture<T> {
        let scope = self.clone();
        let context = context.clone();
        Box::pin(async move {
            let mut future = std::pin::pin!(future);
            futures_util::future::poll_fn(move |cx| {
                if let Err(error) = scope.check(&context) {
                    return Poll::Ready(Err(error));
                }
                let Some(block) = scope.block.upgrade() else {
                    return Poll::Ready(Err(anyhow!("native callback boundary is closed")));
                };
                let _block = Current::enter(&scope.context.state.blocks, block);
                let _transaction = scope
                    .transaction
                    .as_ref()
                    .map(storage::TransactionCurrent::enter);
                future.as_mut().poll(cx)
            })
            .await
        })
    }
}

struct Current<'a> {
    blocks: &'a Blocks,
    previous: Option<Arc<Block>>,
}

impl<'a> Current<'a> {
    fn enter(blocks: &'a Blocks, block: Arc<Block>) -> Self {
        let previous = blocks.current.lock().unwrap().replace(block);
        Self { blocks, previous }
    }
}

impl Drop for Current<'_> {
    fn drop(&mut self) {
        *self.blocks.current.lock().unwrap() = self.previous.take();
    }
}

impl Context {
    /// Capture the currently polled native critical-section/transaction callback.
    /// The returned token carries no authority in another event or facet.
    pub fn callback_scope(&self) -> Result<CallbackScope> {
        let authority = Arc::new(self.invocation_authority()?);
        let block = self
            .state
            .blocks
            .current
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow!("no native callback is being polled"))?;
        let transaction = self.io.native_activation.as_ref().and_then(|activation| {
            activation
                .resources
                .transaction
                .lock()
                .unwrap()
                .clone()
                .map(|transaction| {
                    Storage(
                        activation.clone(),
                        Some(transaction),
                        self.io.source_generation,
                    )
                })
        });
        let scope = CallbackScope {
            context: self.clone(),
            authority,
            block: Arc::downgrade(&block),
            transaction,
        };
        scope.check(self)?;
        Ok(scope)
    }

    /// Hold the cell's host input gate while the callback and every nested
    /// block finish. The work belongs to the cell and survives an ordinary
    /// client disconnect. Failure or the 30-second limit resets the instance.
    /// The gate is requested immediately, even if the returned future is dropped.
    pub fn block_concurrency_while<T, F, Fut>(&self, callback: F) -> HandlerFuture<T>
    where
        T: Send + 'static,
        F: FnOnce(Context) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let prepared = (|| -> Result<_> {
            self.check_turn()?;
            let identity = self
                .identity
                .as_ref()
                .ok_or_else(|| anyhow!("stateless handlers have no cell input gate"))?;
            let blocks = &self.state.blocks;
            let transaction_view = self.io.native_activation.as_ref().and_then(|activation| {
                activation
                    .resources
                    .transaction
                    .lock()
                    .unwrap()
                    .clone()
                    .map(|transaction| {
                        Storage(
                            activation.clone(),
                            Some(transaction),
                            self.io.source_generation,
                        )
                    })
            });
            let parent = blocks.current.lock().unwrap().clone();
            let depth = parent.as_ref().map_or(1, |parent| parent.depth + 1);
            anyhow::ensure!(
                depth <= 64,
                "blockConcurrencyWhile() calls are nested too deeply."
            );
            let mut gate_slot = blocks.gate.lock().unwrap();
            let gate = match gate_slot.upgrade() {
                Some(gate) => gate,
                None => {
                    let gate = InputGate::acquire(self.io.clone(), identity.scope.clone())?;
                    *gate_slot = Arc::downgrade(&gate);
                    gate
                }
            };
            let block = Arc::new(Block {
                gate,
                depth,
                children: Mutex::new(Vec::new()),
                active: std::sync::atomic::AtomicBool::new(true),
            });
            let (done, receive_done) = tokio::sync::oneshot::channel();
            let ready: Ready = async move {
                receive_done
                    .await
                    .unwrap_or_else(|_| Err("native critical section was abandoned".into()))
            }
            .boxed()
            .shared();
            if let Some(view) = &transaction_view {
                view.1.as_ref().unwrap().retain_block(ready.clone())?;
            }
            if let Some(parent) = parent {
                parent.children.lock().unwrap().push(ready);
            } else if blocks
                .initializing
                .load(std::sync::atomic::Ordering::Acquire)
            {
                blocks.initialization.lock().unwrap().push(ready);
            }
            Ok((block, done, transaction_view))
        })();
        let (block, done, transaction_view) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let context = self.clone();
        let (answer, receive) = tokio::sync::oneshot::channel();
        let lifetime = Lifetime {
            block,
            settled: false,
        };
        self.state.background.lock().unwrap().push(Box::pin(async move {
            let mut lifetime = lifetime;
            let result: Result<T> = async {
                lifetime.block.gate.ready.clone().await.map_err(anyhow::Error::msg)?;
                context.check_turn()?;
                if let Some(view) = &transaction_view { view.1.as_ref().unwrap().check_active()?; }
                let future = {
                    let _current = Current::enter(&context.state.blocks, lifetime.block.clone());
                    let _transaction = transaction_view.as_ref().map(storage::TransactionCurrent::enter);
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback(context.clone())))
                        .map_err(crate::native_worker::panic_error)?
                };
                // The callback is pinned without requiring callers' futures to be Unpin.
                let mut future = std::pin::pin!(future);
                let polled = futures_util::future::poll_fn(|cx| {
                    let _current = Current::enter(&context.state.blocks, lifetime.block.clone());
                    let _transaction = transaction_view.as_ref().map(storage::TransactionCurrent::enter);
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.as_mut().poll(cx)))
                        .unwrap_or_else(|panic| Poll::Ready(Err(crate::native_worker::panic_error(panic))))
                });
                let mut retirement = lifetime.block.gate.retirement();
                let completed = crate::asyncrt::select_biased! {
                    "retirement cancels a native block before its old user future can resume";
                    retired = retirement.wait_for(|retired| *retired) => {
                        let _ = retired;
                        anyhow::bail!("native cell activation has retired");
                    },
                    completed = crate::asyncrt::timeout(std::time::Duration::from_secs(30), polled) => completed,
                };
                let value = completed
                    .map_err(|_| anyhow!("A call to blockConcurrencyWhile() in a Durable Object waited for too long. The call was canceled and the Durable Object was reset."))??;
                // Children run as native turn work, including ones the callback
                // started without awaiting. Drain repeated batches: a child can
                // itself create more work while its parent waits.
                loop {
                    let children = std::mem::take(&mut *lifetime.block.children.lock().unwrap());
                    if children.is_empty() { break; }
                    for child in children { child.await.map_err(anyhow::Error::msg)?; }
                }
                Ok(value)
            }.await;
            let outcome = result.as_ref().map(|_| ()).map_err(|error| error.to_string());
            if let Err(reason) = &outcome { lifetime.block.gate.fail(reason.clone()); }
            lifetime.settled = true;
            let _ = done.send(outcome);
            let _ = answer.send(result);
            Ok(())
        }));
        let activation = self.io.native_activation.clone();
        Box::pin(async move {
            receive
                .await
                .unwrap_or_else(|_| Err(anyhow!("native critical section was abandoned")))
                .map_err(|error| {
                    // Failure cancels sibling/parent tasks. A dropped result
                    // channel must retain the reset's original reason.
                    activation
                        .as_ref()
                        .and_then(|activation| activation.resources.failure.lock().unwrap().clone())
                        .map_or(error, anyhow::Error::msg)
                })
        })
    }
}

pub(crate) fn poll_initialization(
    ready: &mut Vec<Ready>,
    cx: &mut TaskContext<'_>,
) -> Poll<Result<()>> {
    let mut pending = false;
    let mut error = None;
    ready.retain_mut(|ready| match Pin::new(ready).poll(cx) {
        Poll::Pending => {
            pending = true;
            true
        }
        Poll::Ready(Ok(())) => false,
        Poll::Ready(Err(reason)) => {
            error.get_or_insert(reason);
            false
        }
    });
    if let Some(error) = error {
        Poll::Ready(Err(anyhow!(error)))
    } else if pending {
        Poll::Pending
    } else {
        Poll::Ready(Ok(()))
    }
}

pub(crate) struct Cancellation {
    pub(crate) lifetime: Arc<crate::runtime::RequestCancellationLifetime>,
    pub(crate) armed: bool,
}

impl Cancellation {
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

pub(crate) struct CallTask<T> {
    pub(crate) task: crate::asyncrt::TaskHandle<T>,
    pub(crate) lifetime: Arc<crate::runtime::RequestCancellationLifetime>,
    pub(crate) finished: bool,
}

impl<T> Future for CallTask<T> {
    type Output = std::result::Result<T, crate::asyncrt::TaskPanic>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.task).poll(cx);
        if result.is_ready() {
            this.finished = true;
        }
        result
    }
}

impl<T> Drop for CallTask<T> {
    fn drop(&mut self) {
        if !self.finished {
            self.lifetime.publish_abort();
            self.task.abort();
        }
    }
}

impl Drop for Cancellation {
    fn drop(&mut self) {
        if self.armed {
            self.lifetime.publish_abort();
        }
    }
}

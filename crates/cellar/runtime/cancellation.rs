//! Monotonic invocation authority, separate from consumable driver wakeups.
use anyhow::Result;
use cellar_native::web::AbortSignal;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, Weak,
    },
};

/// Opaque host capability. Application metadata cannot manufacture or clear it.
#[doc(hidden)]
pub struct InvocationCancellation {
    revoked: AtomicBool,
    linked: AtomicBool,
    parent: Option<Weak<Self>>,
    // One short registration/revocation critical section per call tree. It
    // serializes concurrent subtree revocations until every live descendant
    // is marked; SDK checks themselves only read the atomic flag.
    tree: Arc<Mutex<()>>,
    children: Mutex<Vec<Weak<Self>>>,
    wake: tokio::sync::Notify,
}

impl InvocationCancellation {
    pub(crate) fn new(parent: Option<&Arc<Self>>) -> Arc<Self> {
        let tree = parent.map_or_else(|| Arc::new(Mutex::new(())), |parent| parent.tree.clone());
        let _registration = tree.lock().unwrap();
        let revoked = parent.is_some_and(|parent| parent.is_revoked());
        let scope = Arc::new(Self {
            revoked: AtomicBool::new(revoked),
            linked: AtomicBool::new(parent.is_some() && !revoked),
            parent: parent.map(Arc::downgrade),
            tree: tree.clone(),
            children: Mutex::new(Vec::new()),
            wake: tokio::sync::Notify::new(),
        });
        if let Some(parent) = parent.filter(|_| !revoked) {
            let mut children = parent.children.lock().unwrap();
            // Keep only live authority; long-lived callers cannot accumulate
            // a weak registration for every already-finished subrequest.
            children.retain(|child| child.upgrade().is_some_and(|child| !child.is_revoked()));
            children.push(Arc::downgrade(&scope));
        }
        scope
    }

    pub(crate) fn is_revoked(&self) -> bool {
        self.revoked.load(Ordering::Acquire)
    }

    // Once a callee fixes its reply, a late caller abort must not cancel its
    // valid waitUntil/native I/O. Detaching never clears an existing revocation.
    pub(crate) fn detach(self: &Arc<Self>) {
        let _registration = self.tree.lock().unwrap();
        self.linked.store(false, Ordering::Release);
        if let Some(parent) = self.parent.as_ref().and_then(Weak::upgrade) {
            let own = Arc::downgrade(self);
            parent
                .children
                .lock()
                .unwrap()
                .retain(|child| !Weak::ptr_eq(child, &own));
        }
    }

    pub(crate) fn revoke(self: &Arc<Self>) {
        let _revocation = self.tree.lock().unwrap();
        // Iterative traversal avoids a call-stack walk through nested RPCs.
        // Children are weak, so this graph cannot retain completed invocations.
        let mut pending = vec![self.clone()];
        while let Some(scope) = pending.pop() {
            if scope.revoked.swap(true, Ordering::AcqRel) {
                continue;
            }
            let children = std::mem::take(&mut *scope.children.lock().unwrap());
            scope.wake.notify_waiters();
            pending.extend(
                children
                    .into_iter()
                    .filter_map(|child| child.upgrade())
                    .filter(|child| child.linked.load(Ordering::Acquire)),
            );
        }
    }

    pub(crate) async fn cancelled(&self) {
        let notified = self.wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.is_revoked() {
            notified.await;
        }
    }
}

fn failure() -> anyhow::Error {
    crate::application::rpc::Failure::new(crate::application::rpc::Code::Cancelled).into()
}

/// Source-side cancellation does not depend on either application's poll loop.
/// Dropping the operation runs its existing RPC/peer abort guards.
pub(crate) async fn receive<T>(
    parent: Option<&Arc<InvocationCancellation>>,
    signal: Option<&AbortSignal>,
    operation: impl Future<Output = Result<T>>,
) -> Result<T> {
    if parent.is_none() && signal.is_none() {
        return operation.await;
    }
    let inherited = async {
        match parent {
            Some(parent) => parent.cancelled().await,
            None => std::future::pending().await,
        }
    };
    let explicit = async {
        match signal {
            Some(signal) => {
                signal.cancelled().await;
            }
            None => std::future::pending().await,
        }
    };
    crate::asyncrt::select_biased! { "cancellation wins an undelivered RPC result";
        _ = inherited => Err(failure()),
        _ = explicit => Err(failure()),
        result = operation => {
            if parent.is_some_and(|parent| parent.is_revoked())
                || signal.is_some_and(|signal| signal.throw_if_aborted().is_err()) {
                Err(failure())
            } else { result }
        },
    }
}

#[cfg(test)]
#[path = "cancellation_tests.rs"]
mod tests;

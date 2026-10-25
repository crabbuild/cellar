// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! An owned RPC result's transport lifetime, independent of its value bytes.
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::watch;

/// Ownership of a live RPC result. Aliases share one transport; dropping the
/// last alias or explicitly closing it revokes the exported result's lifetime.
/// A lease grants no SDK authority: callbacks still use their original event.
#[derive(Clone)]
pub struct Lease(Arc<Inner>);

struct Inner {
    closed: watch::Sender<bool>,
    resource: Mutex<Option<Box<dyn Send>>>,
}

/// Revokes a lease without keeping its transport or exported event alive.
#[derive(Clone)]
pub struct LeaseControl(Weak<Inner>);

/// Observes closure without retaining an ownership alias.
pub struct LeaseClosed(watch::Receiver<bool>);

/// Closes a lease when an event or transport owner drops, without retaining
/// the result. Keeping this guard does not keep an ownership alias alive.
pub struct LeaseRevocation(LeaseControl);

impl Drop for LeaseRevocation {
    fn drop(&mut self) {
        self.0.close();
    }
}

impl Default for Lease {
    fn default() -> Self {
        Self::new()
    }
}

impl Lease {
    /// Create a transport lifetime. Producers returning an SDK result should
    /// use [`Self::for_context`] to bind it to their native event as well.
    pub fn new() -> Self {
        let (closed, _) = watch::channel(false);
        Self(Arc::new(Inner {
            closed,
            resource: Mutex::new(None),
        }))
    }

    /// Retain the original native event until this result closes. Retirement,
    /// cancellation or the event deadline drops its guard and closes the lease;
    /// aliases cannot extend that native authority.
    ///
    /// ```no_run
    /// use cellar::sdk::{Context, Output, rpc::Lease};
    /// use cellar::engine_api::RpcData;
    /// fn result(context: &Context) -> anyhow::Result<Output> {
    ///     let lease = Lease::for_context(context)?;
    ///     Ok(Output::Rpc(RpcData::Json("null".into()).with_lease(lease)?))
    /// }
    /// ```
    pub fn for_context(context: &crate::application::Context) -> anyhow::Result<Self> {
        let lease = Self::new();
        let closed = lease.closed();
        let revocation = lease.control().revoke_on_drop();
        context.wait_until(async move {
            let _revocation = revocation;
            closed.wait().await;
            Ok(())
        })?;
        Ok(lease)
    }

    /// Obtain a revocation handle that does not retain this result.
    pub fn control(&self) -> LeaseControl {
        LeaseControl(Arc::downgrade(&self.0))
    }

    /// Observe explicit closure or the last ownership alias being dropped.
    pub fn closed(&self) -> LeaseClosed {
        LeaseClosed(self.0.closed.subscribe())
    }

    /// Whether the result has been explicitly or externally revoked.
    pub fn is_closed(&self) -> bool {
        *self.0.closed.borrow()
    }

    /// Revoke every alias and release the owned transport, once.
    pub fn close(&self) {
        self.control().close();
    }

    /// The peer decoder installs one owned body-reader task before publishing
    /// the lease. Dropping/closing the lease releases that transport resource.
    pub(crate) fn hold(&self, resource: impl Send + 'static) -> anyhow::Result<()> {
        let mut slot = self.0.resource.lock().unwrap();
        if !self.is_closed() {
            anyhow::ensure!(slot.is_none(), "RPC lease transport installed twice");
            *slot = Some(Box::new(resource));
        }
        Ok(())
    }
}

impl LeaseControl {
    /// Revoke the result when its event or transport owner is dropped.
    pub fn revoke_on_drop(self) -> LeaseRevocation {
        LeaseRevocation(self)
    }

    /// Revoke a live result; closing an already released result is harmless.
    pub fn close(&self) {
        if let Some(inner) = self.0.upgrade() {
            inner.closed.send_replace(true);
            // No SDK work or user callback runs while this lock is held.
            let resource = inner.resource.lock().unwrap().take();
            drop(resource);
        }
    }
}

impl LeaseClosed {
    /// Wait without keeping the result's ownership or transport alive.
    pub async fn wait(mut self) {
        loop {
            let closed = *self.0.borrow_and_update();
            if closed {
                return;
            }
            if self.0.changed().await.is_err() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Resource(Arc<AtomicBool>);

    impl Drop for Resource {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn observer_does_not_retain_last_alias_or_transport() {
        let lease = Lease::new();
        let closed = lease.closed();
        let alias = lease.clone();
        let dropped = Arc::new(AtomicBool::new(false));
        lease.hold(Resource(dropped.clone())).unwrap();
        drop(lease);
        assert!(!alias.is_closed() && !dropped.load(Ordering::SeqCst));
        drop(alias);
        closed.wait().await;
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn owner_revocation_closes_aliases_and_releases_transport_once() {
        let lease = Lease::new();
        let alias = lease.clone();
        let closed = lease.closed();
        let control = lease.control();
        let dropped = Arc::new(AtomicBool::new(false));
        lease.hold(Resource(dropped.clone())).unwrap();
        control.close();
        closed.wait().await;
        assert!(alias.is_closed() && dropped.load(Ordering::SeqCst));
        control.close();
        drop(lease);
        drop(alias);
        control.close();
    }
}

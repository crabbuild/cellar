//! Generation-local admission. Catalog/partition state remains authoritative;
//! this latch only prevents staged bindings from admitting application work.

use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) struct Readiness {
    ready: AtomicBool,
    result: tokio::sync::watch::Sender<Option<Result<(), String>>>,
}

impl Readiness {
    pub(crate) fn new(ready: bool) -> Self {
        let (result, _) = tokio::sync::watch::channel(ready.then_some(Ok(())));
        Self {
            ready: AtomicBool::new(ready),
            result,
        }
    }

    pub(crate) fn begin(&self) {
        self.ready.store(false, Ordering::Release);
        self.result.send_replace(None);
    }

    pub(crate) fn finish(&self, result: Result<(), String>) {
        self.ready.store(result.is_ok(), Ordering::Release);
        self.result.send_replace(Some(result));
    }

    pub(crate) fn check(&self) -> crate::application::Result<()> {
        if !self.ready.load(Ordering::Acquire) {
            return Err(super::error::error(
                super::ErrorCode::Unavailable,
                "event bindings have not completed resource activation",
            ));
        }
        Ok(())
    }

    pub(crate) async fn wait(&self) -> anyhow::Result<()> {
        let mut result = self.result.subscribe();
        loop {
            if let Some(result) = result.borrow_and_update().clone() {
                return result.map_err(anyhow::Error::msg);
            }
            result
                .changed()
                .await
                .map_err(|_| anyhow::anyhow!("event activation observer closed"))?;
        }
    }
}

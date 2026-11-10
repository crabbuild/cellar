// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A changing archive descriptor must not bypass the actual payload fault.
use super::*;

struct PayloadDenial<'a>(&'a std::sync::atomic::AtomicBool);

impl Drop for PayloadDenial<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

pub(super) async fn refused<T, F, Fut>(store: &fault::Store, mut work: F) -> Result<()>
where
    F: FnMut(Duration) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, FleetCatalogError>>,
{
    let before = store.denied_group_payload_reads.load(Ordering::Acquire);
    ensure!(!store.deny_group_payload_reads.swap(true, Ordering::AcqRel));
    let _denial = PayloadDenial(&store.deny_group_payload_reads);
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    let result = tokio::time::timeout_at(deadline, async {
        loop {
            let result =
                work(deadline.saturating_duration_since(tokio::time::Instant::now())).await;
            if matches!(&result, Err(FleetCatalogError::Conflict))
                && store.denied_group_payload_reads.load(Ordering::Acquire) == before
            {
                // Repeat the same operation and selected holds. Descriptor CAS
                // contention before payload I/O does not exercise this oracle.
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            return result;
        }
    })
    .await
    .context("archive fault never reached payload I/O within its original deadline")?;
    ensure!(
        result.is_err() && store.denied_group_payload_reads.load(Ordering::Acquire) > before,
        "archive payload refusal was not exercised: succeeded={}, error={:?}",
        result.is_ok(),
        result.as_ref().err()
    );
    Ok(())
}

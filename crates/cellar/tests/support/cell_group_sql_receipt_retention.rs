// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Concrete synthetic-store custody for the owning receipt-reuse target.

#[allow(unused_imports)]
use super::runtime_fixture::catalog_fault as fault;
use super::*;
use futures_util::FutureExt as _;
#[path = "cell_group_memory_retention.rs"]
mod memory;

pub(super) async fn reconcile(rooted: bool) -> Result<()> {
    let store = Arc::new(object_store::memory::InMemory::new());
    let outcome = std::panic::AssertUnwindSafe(runtime_fixture::reconcile_captured_receipt(
        rooted,
        store.clone(),
    ))
    .catch_unwind()
    .await;
    let retained = memory::retain(&store, "receipt-reuse").await;
    if let Err(error) = &retained {
        eprintln!("receipt-reuse memory export error: {error:#}");
    }
    match outcome {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    retained?;
    Ok(())
}

pub(super) async fn retirement(reconcile: bool) -> Result<()> {
    let store = Arc::new(object_store::memory::InMemory::new());
    let outcome = std::panic::AssertUnwindSafe(runtime_fixture::preview_retirement_progress(
        reconcile,
        store.clone(),
    ))
    .catch_unwind()
    .await;
    let retained = memory::retain(&store, "preview-retirement").await;
    if let Err(error) = &retained {
        eprintln!("preview-retirement memory export error: {error:#}");
    }
    match outcome {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    retained?;
    Ok(())
}

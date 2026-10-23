//! A bundled member releases only its own verified proof and capture permit.
#![allow(clippy::disallowed_methods)]
use super::*;
use anyhow::{ensure, Context as _, Result};
use futures_util::TryStreamExt;
use object_store::ObjectStore;
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;

use crate::raft::host::catalog_fault as faults;
#[allow(dead_code)]
#[path = "../../tests/support/raft_archive_fixture.rs"]
mod fixture;
#[allow(dead_code)]
#[path = "../../tests/support/raft_ltx_fixture.rs"]
mod images;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bundled_cut_completes_and_releases_its_permit_before_held_partner() -> Result<()> {
    owned_completion_before_partner(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn completed_cut_survives_partner_cancellation_and_releases_all_permits() -> Result<()> {
    owned_completion_before_partner(true).await
}

async fn owned_completion_before_partner(cancel_partner: bool) -> Result<()> {
    // Library tests can install unrelated process fallback domains. Bind every
    // archive timer to this test's live owner instead of borrowing that fallback.
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain.run(completion_before_partner(cancel_partner)).await;
    domain.shutdown().await;
    result?
}

async fn completion_before_partner(cancel_partner: bool) -> Result<()> {
    let store = Arc::new(faults::Store::default());
    let mut a = fixture::Fixture::new_group(ArchiveLimits::default(), store.clone(), 7).await?;
    let mut b = fixture::Fixture::new_group(ArchiveLimits::default(), store.clone(), 8).await?;
    let cuts = [a.commit(17, vec![]).await?, b.commit(29, vec![]).await?];
    let archives = [a.archive.clone(), b.archive.clone()];
    let budget = super::super::budget::Budget::new(26, 2);
    let scopes = [a.birth.scope().clone(), b.birth.scope().clone()];
    let (cancellation, _) = tokio::sync::watch::channel(false);
    let captured = archives
        .iter()
        .cloned()
        .zip(cuts)
        .zip(scopes)
        .map(|((archive, cut), scope)| Captured {
            scope,
            archive: archive.with_cancellation(cancellation.subscribe()),
            cut,
            reservation: Some(budget.acquire(13).expect("two bounded capture slots")),
        })
        .collect();
    store.one_head_hang.store(true, Ordering::Release);
    store.hold_next_archive_head.store(true, Ordering::Release);
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let publication = publish_each(captured, a.verifier.clone(), move |completed| {
        let _ = sender.send(completed);
    });
    tokio::pin!(publication);
    let observed = tokio::time::timeout(Duration::from_secs(5), async {
        let completed = crate::asyncrt::select! {
            _ = &mut publication => anyhow::bail!("batch ended while one head was held"),
            completed = receiver.recv() => completed.context("completion channel closed")?,
        };
        ensure!(completed.result.is_ok());
        ensure!(store.one_head_waiting.load(Ordering::Acquire));
        let held = store
            .held_archive_head
            .lock()
            .unwrap()
            .clone()
            .context("no held head")?;
        ensure!(!held
            .as_ref()
            .contains(&hex::encode(completed.scope.group.bytes())));
        ensure!(
            budget.usage() == (13, 1),
            "completed cut still retained its capture permit"
        );
        Ok::<_, anyhow::Error>(completed.scope)
    })
    .await
    .context("verified cut waited for unrelated held head")
    .and_then(|r| r);
    // Always release and join physical owners before reporting the regression.
    if cancel_partner {
        let _ = cancellation.send(true);
    } else {
        store.one_head_hang.store(false, Ordering::Release);
        store.one_head_resume.notify_waiters();
    }
    let joined = tokio::time::timeout(Duration::from_secs(5), &mut publication).await;
    store.one_head_hang.store(false, Ordering::Release);
    store.one_head_resume.notify_waiters();
    let joined = match joined {
        Ok(success) => Ok(success),
        Err(error) => {
            // Releasing the test gate permits cleanup without erasing the
            // failed cancellation/join deadline from the assertion below.
            let _ = (&mut publication).await;
            Err(error)
        }
    };
    let result = async {
        let first = observed?;
        ensure!(joined? != cancel_partner);
        let second = receiver
            .recv()
            .await
            .context("missing partner completion")?;
        ensure!(second.scope != first);
        if cancel_partner {
            ensure!(matches!(second.result, Err(ArchiveError::Stopped)));
        } else {
            ensure!(second.result.is_ok());
        }
        ensure!(receiver.recv().await.is_none(), "duplicate completion");
        ensure!(budget.usage() == (0, 0));
        let objects = store.inner.list(None).try_collect::<Vec<_>>().await?;
        ensure!(
            objects
                .iter()
                .filter(|o| o.location.as_ref().contains("/archives/bundles/"))
                .count()
                == 1,
            "independent completion must retain the shared bundle"
        );
        let mut published = 0;
        for archive in archives {
            if let Some(set) = archive.recovery_set().await? {
                if cancel_partner {
                    ensure!(set.scope() == &first);
                }
                published += 1;
            }
        }
        ensure!(published == if cancel_partner { 1 } else { 2 });
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let close_a = a.close().await;
    let close_b = b.close().await;
    result?;
    close_a?;
    close_b?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn covered_cut_completes_while_partner_upload_is_pending() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain.run(completion_during_flush()).await;
    domain.shutdown().await;
    result?
}

async fn completion_during_flush() -> Result<()> {
    let store = Arc::new(faults::Store::default());
    let mut a = fixture::Fixture::new_group(ArchiveLimits::default(), store.clone(), 7).await?;
    let mut b = fixture::Fixture::new_group(ArchiveLimits::default(), store.clone(), 8).await?;
    let first = a.commit(17, vec![]).await?;
    a.publish(first.clone()).await?;
    let second = b.commit(29, vec![]).await?;
    let scope = a.birth.scope().clone();
    let budget = super::super::budget::Budget::new(26, 2);
    let captured = [(&a, first), (&b, second)]
        .into_iter()
        .map(|(fixture, cut)| Captured {
            scope: fixture.birth.scope().clone(),
            archive: fixture.archive.clone(),
            cut,
            reservation: Some(budget.acquire(13).expect("two bounded capture slots")),
        })
        .collect();
    // The covered member must read and replay its existing proof. Hold that
    // read until the other member's only new upload owns the coordinator flush.
    store.payload_hang.store(true, Ordering::Release);
    store.hold_next_payload.store(true, Ordering::Release);
    store.hold_payload_puts.store(true, Ordering::Release);
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let publication = publish_each(captured, a.verifier.clone(), move |completed| {
        let _ = sender.send(completed);
    });
    tokio::pin!(publication);
    let observed = tokio::time::timeout(Duration::from_secs(5), async {
        while !store.payload_waiting.load(Ordering::Acquire)
            || !store.payload_put_waiting.load(Ordering::Acquire)
        {
            crate::asyncrt::select! {
                _ = &mut publication => anyhow::bail!("publication ended before both physical gates"),
                _ = tokio::time::sleep(Duration::from_millis(1)) => (),
            }
        }
        store.release_payload();
        let completed = crate::asyncrt::select! {
            _ = &mut publication => anyhow::bail!("publication ended while upload was held"),
            completed = receiver.recv() => completed.context("missing covered-cut completion")?,
        };
        ensure!(completed.scope == scope && completed.result.is_ok());
        ensure!(store.payload_put_waiting.load(Ordering::Acquire));
        ensure!(budget.usage() == (13, 1));
        Ok::<_, anyhow::Error>(())
    }).await.context("covered cut stopped being polled during partner upload").and_then(|r| r);
    store.release_payload();
    store.hold_payload_puts.store(false, Ordering::Release);
    store.payload_put_resume.notify_waiters();
    let joined = (&mut publication).await;
    let close_a = a.close().await;
    let close_b = b.close().await;
    observed?;
    ensure!(joined);
    close_a?;
    close_b?;
    let completed = receiver.recv().await.context("missing upload completion")?;
    ensure!(completed.scope != scope && completed.result.is_ok());
    ensure!(receiver.recv().await.is_none());
    ensure!(budget.usage() == (0, 0));
    Ok(())
}

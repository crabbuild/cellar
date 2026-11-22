// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fresh selection and immutable birth records must share a bounded read window.
use super::*;
use anyhow::ensure;
use std::{future::Future, pin::Pin};

struct Reads {
    head: Arc<fault::RegistryGate>,
    reservation: Arc<fault::RegistryGate>,
}

impl Reads {
    fn new() -> Self {
        let reads = Self {
            head: Arc::new(fault::RegistryGate::default()),
            reservation: Arc::new(fault::RegistryGate::default()),
        };
        reads.head.armed.store(true, Ordering::Release);
        reads.reservation.armed.store(true, Ordering::Release);
        reads
    }

    async fn check(&self, f: &Fixture, birth: &CellGroupBirth) -> Result<(), CatalogError> {
        fault::gate_store_read(
            &f.store,
            format!("/groups/{}", hex::encode(birth.route().history.group.bytes())),
            self.reservation.clone(),
            fault::gate_read(
                f.key("head").to_string(),
                self.head.clone(),
                f.run(f.directory.require_current_group_birth(birth)),
            ),
        )
        .await
    }

    async fn entered<F>(&self, pending: &mut Pin<Box<F>>) -> Result<()>
    where
        F: Future<Output = Result<(), CatalogError>>,
    {
        cellar::asyncrt::select! {
            result = pending => anyhow::bail!("birth validation finished before both reads entered: {result:?}"),
            result = tokio::time::timeout(Duration::from_secs(2), async {
                futures_util::join!(self.head.entered.notified(), self.reservation.entered.notified());
            }) => result.context("birth validation serialized independent reads")?,
        }
        Ok(())
    }
}

pub(crate) async fn current_birth_reads_overlap(store: Arc<fault::Store>) -> Result<()> {
    let f = Fixture::with_store(DirectoryLimits::default(), route().schema_digest, store).await?;
    let work = async {
        let birth = selected_birth(&f).await?;
        let key = f.key(&format!(
            "births/{}/{}",
            hex::encode(birth.route().history.group.bytes()),
            hex::encode(birth.route().history.incarnation.bytes())
        ));

        // Either read may finish first; neither alone is sufficient authority.
        for head_first in [true, false] {
            let reads = Reads::new();
            let mut pending = Box::pin(reads.check(&f, &birth));
            reads.entered(&mut pending).await?;
            let (first, last) = if head_first {
                (&reads.head, &reads.reservation)
            } else {
                (&reads.reservation, &reads.head)
            };
            first.resume.notify_one();
            ensure!(tokio::time::timeout(Duration::from_millis(40), &mut pending).await.is_err());
            last.resume.notify_one();
            pending.await?;
        }

        // Immutable bytes are still read on every check, and corruption cannot
        // drop the already-entered fresh-selection read on the error path.
        let original = f.store.inner.get(&key).await?.bytes().await?;
        f.store.inner.put(&key, b"changed birth".to_vec().into()).await?;
        let reads = Reads::new();
        let mut pending = Box::pin(reads.check(&f, &birth));
        reads.entered(&mut pending).await?;
        reads.reservation.resume.notify_one();
        ensure!(tokio::time::timeout(Duration::from_millis(100), &mut pending).await.is_err());
        reads.head.resume.notify_one();
        ensure!(matches!(pending.await, Err(CatalogError::Corrupt("selected group birth changed"))));
        f.store.inner.put(&key, original.into()).await?;

        // Cancelling the owning future cancels both reads; no detached worker
        // can keep this check alive or prevent a fresh validation afterward.
        let reads = Reads::new();
        let mut pending = Box::pin(reads.check(&f, &birth));
        reads.entered(&mut pending).await?;
        drop(pending);
        f.run(f.directory.require_current_group_birth(&birth)).await?;

        let other = f.reopen().await?;
        let (foreign, counts) = fault::count_reads(f.run(other.require_current_group_birth(&birth))).await;
        ensure!(matches!(foreign, Err(CatalogError::Conflict("foreign group birth observation"))) && counts.is_empty());

        // A fresh topology change must fence the old birth even if its records
        // also fail. Preserve selection-error precedence and join the peer.
        let view = f.run(f.directory.observe()).await?;
        let mut children = split(birth.route(), 91000);
        f.reserve(&mut children).await?;
        let replacement = f.directory.prepare_replacement(
            &view,
            operation(91002),
            std::slice::from_ref(birth.route()),
            &children,
        )?;
        f.run(f.directory.publish(&replacement)).await?;
        f.store.inner.put(&key, b"changed birth".to_vec().into()).await?;
        let reads = Reads::new();
        let mut pending = Box::pin(reads.check(&f, &birth));
        reads.entered(&mut pending).await?;
        reads.head.resume.notify_one();
        ensure!(tokio::time::timeout(Duration::from_millis(100), &mut pending).await.is_err());
        reads.reservation.resume.notify_one();
        ensure!(matches!(pending.await, Err(CatalogError::Fenced)));
        Ok(())
    }.await;
    f.domain.shutdown().await;
    work
}

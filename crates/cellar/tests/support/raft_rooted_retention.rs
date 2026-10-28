// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Persistent ownership and CAS fault tests only. Descriptors here deliberately
//! contain synthetic digests: registry ownership is not data verification.

use super::*;
use anyhow::{ensure, Context as _};
use futures_util::TryStreamExt as _;
use object_store::{path::Path as ObjectPath, ObjectStore as _};
use std::sync::atomic::Ordering;

async fn fixture() -> Result<(Arc<archive_faults::Store>, CellArchive, RootRegistry)> {
    let store = Arc::new(archive_faults::Store::default());
    let (_, _, archive) = archive::history(store.clone()).await?;
    let registry = archive.create_root_registry().await?;
    Ok((store, archive, registry))
}

fn position(index: u64, term: u64) -> Result<LogPosition> {
    Ok(LogPosition {
        index: LogIndex::new(index),
        term: Term::new(term),
    })
}

fn reservation(result: RootAllocation) -> Result<RootReservation> {
    match result {
        RootAllocation::Reservation(ticket) => Ok(ticket),
        _ => bail!("expected reservation"),
    }
}

fn reference(result: RootAllocation) -> Result<RootReference> {
    match result {
        RootAllocation::Reference(reference) => Ok(reference),
        _ => bail!("expected reference"),
    }
}

async fn reserve(
    registry: &RootRegistry,
    image: u8,
    parent: Option<RootReference>,
) -> Result<RootReservation> {
    let intent = registry.prepare_reserve([image; 32], parent).await?;
    reservation(registry.commit(&intent).await?)
}

async fn bind(
    registry: &RootRegistry,
    ticket: RootReservation,
    select: Option<LogPosition>,
) -> Result<RootReference> {
    let descriptor = RootDescriptor::new(ticket, [255; 32], 1234)?;
    let intent = registry.prepare_bind(ticket, descriptor, select).await?;
    reference(registry.commit(&intent).await?)
}

async fn fresh(
    registry: &RootRegistry,
    image: u8,
    select: Option<LogPosition>,
) -> Result<RootReference> {
    bind(registry, reserve(registry, image, None).await?, select).await
}

async fn key(store: &archive_faults::Store, filename: &str) -> Result<ObjectPath> {
    store
        .inner
        .list(None)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .find(|entry| entry.location.filename() == Some(filename))
        .map(|entry| entry.location)
        .context("registry object")
}

#[tokio::test]
async fn restart_reconciles_persisted_intent_and_retains_exact_reference() -> Result<()> {
    let (store, archive, registry) = fixture().await?;
    let intent = registry.prepare_reserve([7; 32], None).await?;
    let saved_intent = serde_json::to_vec(&intent)?;
    let saved_id = serde_json::to_vec(&registry.id())?;
    ensure!(
        registry.protection().await?.revision == 1,
        "prepare wrote state"
    );
    let ticket = reservation(registry.commit(&intent).await?)?;
    ensure!(
        reservation(
            registry
                .commit(&serde_json::from_slice(&saved_intent)?)
                .await?
        )? == ticket
    );
    let held = bind(&registry, ticket, Some(position(7, 2)?)).await?;
    let saved_hold = serde_json::to_vec(&held)?;
    drop(registry);
    drop(archive);

    let (_, _, reopened) = archive::history(store.clone()).await?;
    let registry = reopened
        .open_root_registry(serde_json::from_slice(&saved_id)?)
        .await?;
    let restored = serde_json::from_slice(&saved_hold)?;
    ensure!(registry.referenced(restored).await? == held.root());
    ensure!(registry.protection().await?.roots == vec![held.root()]);
    ensure!(registry.protection().await?.pending_generations.is_empty());
    ensure!(registry.release(restored).await?);
    // The selected root has its own durable hold after the caller releases.
    let intent = registry.prepare_acquire_selected(held.root()).await?;
    let acquired = reference(registry.commit(&intent).await?)?;
    ensure!(acquired != held);
    ensure!(registry.referenced(acquired).await? == held.root());
    ensure!(store
        .inner
        .list(None)
        .try_collect::<Vec<_>>()
        .await?
        .iter()
        .all(|entry| entry.location.filename() != Some("head")));
    Ok(())
}

#[tokio::test]
async fn lost_write_responses_reconcile_without_allocating_twice() -> Result<()> {
    let (store, _, registry) = fixture().await?;
    let intent = registry.prepare_reserve([8; 32], None).await?;
    store.fail_registry_before.store(true, Ordering::Release);
    ensure!(matches!(
        registry.commit(&intent).await,
        Err(ArchiveError::Unconfirmed)
    ));
    ensure!(registry.protection().await?.revision == 1);
    store.fail_registry_after.store(true, Ordering::Release);
    let ticket = reservation(registry.commit(&intent).await?)?;
    let writes = store.registry_writes.load(Ordering::Acquire);
    ensure!(reservation(registry.commit(&intent).await?)? == ticket);
    ensure!(store.registry_writes.load(Ordering::Acquire) == writes);

    let intent = registry
        .prepare_bind(ticket, RootDescriptor::new(ticket, [8; 32], 1024)?, None)
        .await?;
    store.fail_registry_after.store(true, Ordering::Release);
    let held = reference(registry.commit(&intent).await?)?;
    store.fail_registry_after.store(true, Ordering::Release);
    ensure!(registry.release(held).await?);
    ensure!(!registry.release(held).await?);
    ensure!(matches!(
        registry.commit(&intent).await,
        Err(ArchiveError::ReferenceExpired)
    ));
    ensure!(registry.protection().await?.roots.is_empty());
    Ok(())
}

#[tokio::test]
async fn identical_concurrent_requests_never_share_an_allocation() -> Result<()> {
    let (store, _, registry) = fixture().await?;
    let first = registry.prepare_reserve([9; 32], None).await?;
    let second = registry.prepare_reserve([9; 32], None).await?;
    ensure!(first != second);
    store.registry_before.armed.store(true, Ordering::Release);
    let mut blocked = Box::pin(registry.commit(&first));
    cellar::asyncrt::select! {
        _ = store.registry_before.entered.notified() => {},
        result = &mut blocked => bail!("CAS did not pause: {result:?}"),
    }
    let winner = reservation(registry.commit(&second).await?)?;
    store.registry_before.resume.notify_one();
    ensure!(matches!(blocked.await, Err(ArchiveError::ReferenceExpired)));
    ensure!(matches!(
        registry.commit(&first).await,
        Err(ArchiveError::ReferenceExpired)
    ));
    let next = reserve(&registry, 9, None).await?;
    ensure!(next.generation() > winner.generation());
    ensure!(
        registry.protection().await?.pending_generations
            == vec![winner.generation(), next.generation()]
    );
    Ok(())
}

#[tokio::test]
async fn release_winning_clone_race_cannot_resurrect_a_root() -> Result<()> {
    let (store, _, registry) = fixture().await?;
    let held = fresh(&registry, 1, None).await?;
    let intent = registry.prepare_clone(held).await?;
    store.registry_before.armed.store(true, Ordering::Release);
    let mut blocked = Box::pin(registry.commit(&intent));
    cellar::asyncrt::select! {
        _ = store.registry_before.entered.notified() => {},
        result = &mut blocked => bail!("CAS did not pause: {result:?}"),
    }
    ensure!(registry.release(held).await?);
    store.registry_before.resume.notify_one();
    ensure!(matches!(blocked.await, Err(ArchiveError::ReferenceExpired)));
    ensure!(matches!(
        registry.prepare_clone(held).await,
        Err(ArchiveError::ReferenceExpired)
    ));
    ensure!(registry.protection().await?.roots.is_empty());
    Ok(())
}

#[tokio::test]
async fn clone_winning_release_race_keeps_its_independent_hold() -> Result<()> {
    let (store, _, registry) = fixture().await?;
    let held = fresh(&registry, 2, None).await?;
    store.registry_before.armed.store(true, Ordering::Release);
    let mut blocked = Box::pin(registry.release(held));
    cellar::asyncrt::select! {
        _ = store.registry_before.entered.notified() => {},
        result = &mut blocked => bail!("CAS did not pause: {result:?}"),
    }
    let intent = registry.prepare_clone(held).await?;
    let cloned = reference(registry.commit(&intent).await?)?;
    store.registry_before.resume.notify_one();
    ensure!(blocked.await?);
    ensure!(registry.referenced(cloned).await? == held.root());
    ensure!(registry.protection().await?.roots == vec![held.root()]);
    ensure!(registry.release(cloned).await?);
    ensure!(registry.protection().await?.roots.is_empty());
    Ok(())
}

#[tokio::test]
async fn pending_child_protects_parent_after_release_and_selection_rotation() -> Result<()> {
    let (_, _, registry) = fixture().await?;
    let parent = fresh(&registry, 3, Some(position(3, 1)?)).await?;
    let child = reserve(&registry, 4, Some(parent)).await?;
    ensure!(registry.release(parent).await?);
    let newer = fresh(&registry, 5, Some(position(4, 1)?)).await?;
    ensure!(registry.protection().await?.roots == vec![parent.root(), newer.root()]);
    ensure!(registry.protection().await?.pending_generations == vec![child.generation()]);
    ensure!(registry.abort(child).await?);
    ensure!(registry.protection().await?.roots == vec![newer.root()]);
    ensure!(matches!(
        registry.prepare_reserve([6; 32], Some(parent)).await,
        Err(ArchiveError::ReferenceExpired)
    ));
    Ok(())
}

#[tokio::test]
async fn abort_fences_a_delayed_bind_and_never_reuses_the_generation() -> Result<()> {
    let (store, _, registry) = fixture().await?;
    let ticket = reserve(&registry, 6, None).await?;
    let intent = registry
        .prepare_bind(ticket, RootDescriptor::new(ticket, [6; 32], 100)?, None)
        .await?;
    store.registry_before.armed.store(true, Ordering::Release);
    let mut blocked = Box::pin(registry.commit(&intent));
    cellar::asyncrt::select! {
        _ = store.registry_before.entered.notified() => {},
        result = &mut blocked => bail!("CAS did not pause: {result:?}"),
    }
    store.fail_registry_after.store(true, Ordering::Release);
    ensure!(registry.abort(ticket).await?);
    store.registry_before.resume.notify_one();
    ensure!(matches!(blocked.await, Err(ArchiveError::ReferenceExpired)));
    ensure!(!registry.abort(ticket).await?);
    ensure!(registry.protection().await?.roots.is_empty());
    let next = reserve(&registry, 6, None).await?;
    ensure!(next.generation() > ticket.generation());
    Ok(())
}

#[tokio::test]
async fn changed_readback_after_lost_response_is_explicitly_reconcilable() -> Result<()> {
    let (store, archive, registry) = fixture().await?;
    let held = fresh(&registry, 7, None).await?;
    let intent = registry.prepare_clone(held).await?;
    let persisted = serde_json::to_vec(&intent)?;
    store.registry_after.armed.store(true, Ordering::Release);
    let mut blocked = Box::pin(registry.commit(&intent));
    cellar::asyncrt::select! {
        _ = store.registry_after.entered.notified() => {},
        result = &mut blocked => bail!("CAS did not pause: {result:?}"),
    }
    // The first CAS committed, but its caller has not observed success. An
    // unrelated reservation advances the record before the read-back.
    let other = reserve(&registry, 8, None).await?;
    store.fail_registry_after.store(true, Ordering::Release);
    store.registry_after.resume.notify_one();
    ensure!(matches!(blocked.await, Err(ArchiveError::Unconfirmed)));
    let writes = store.registry_writes.load(Ordering::Acquire);
    let reopened = archive.open_root_registry(registry.id()).await?;
    let cloned = reference(
        reopened
            .commit(&serde_json::from_slice(&persisted)?)
            .await?,
    )?;
    ensure!(
        store.registry_writes.load(Ordering::Acquire) == writes,
        "reconciliation allocated again"
    );
    ensure!(reopened.release(held).await?);
    ensure!(reopened.referenced(cloned).await? == held.root());
    ensure!(reopened.abort(other).await?);
    ensure!(reopened.release(cloned).await?);
    ensure!(matches!(
        reopened.commit(&intent).await,
        Err(ArchiveError::ReferenceExpired)
    ));
    Ok(())
}

#[tokio::test]
async fn cancelled_waiter_can_reconcile_a_committed_allocation() -> Result<()> {
    let (store, archive, registry) = fixture().await?;
    let intent = registry.prepare_reserve([9; 32], None).await?;
    store.registry_after.armed.store(true, Ordering::Release);
    let mut pending = Box::pin(registry.commit(&intent));
    cellar::asyncrt::select! {
        _ = store.registry_after.entered.notified() => {},
        result = &mut pending => bail!("CAS did not pause: {result:?}"),
    }
    drop(pending);
    let reopened = archive.open_root_registry(registry.id()).await?;
    let ticket = reservation(reopened.commit(&intent).await?)?;
    ensure!(reopened.protection().await?.pending_generations == vec![ticket.generation()]);
    ensure!(reopened.abort(ticket).await?);
    ensure!(matches!(
        reopened.commit(&intent).await,
        Err(ArchiveError::ReferenceExpired)
    ));
    Ok(())
}

#[tokio::test]
async fn concurrent_limits_do_not_limit_lifetime_churn() -> Result<()> {
    let (store, _, registry) = fixture().await?;
    let held = fresh(&registry, 255, None).await?;
    let mut clones = Vec::new();
    for _ in 0..63 {
        let intent = registry.prepare_clone(held).await?;
        clones.push(reference(registry.commit(&intent).await?)?);
    }
    ensure!(matches!(
        registry.prepare_clone(held).await,
        Err(ArchiveError::Limit)
    ));
    let mut tickets = Vec::new();
    for _ in 0..8 {
        tickets.push(reserve(&registry, 255, Some(held)).await?);
    }
    ensure!(matches!(
        registry.prepare_reserve([255; 32], None).await,
        Err(ArchiveError::Limit)
    ));
    ensure!(
        store
            .inner
            .head(&key(&store, "registry").await?)
            .await?
            .size
            <= 64 * 1024
    );
    for ticket in tickets {
        ensure!(registry.abort(ticket).await?);
    }
    for cloned in clones {
        ensure!(registry.release(cloned).await?);
    }
    let mut previous = registry.protection().await?.revision;
    for _ in 0..128 {
        let intent = registry.prepare_clone(held).await?;
        let cloned = reference(registry.commit(&intent).await?)?;
        ensure!(registry.release(cloned).await?);
        ensure!(matches!(
            registry.commit(&intent).await,
            Err(ArchiveError::ReferenceExpired)
        ));
        let revision = registry.protection().await?.revision;
        ensure!(revision > previous);
        previous = revision;
    }
    ensure!(registry.referenced(held).await? == held.root());
    Ok(())
}

#[tokio::test]
async fn stale_selection_cannot_replace_a_newer_cut() -> Result<()> {
    let (_, _, registry) = fixture().await?;
    let old = fresh(&registry, 10, Some(position(5, 2)?)).await?;
    let newer = fresh(&registry, 11, Some(position(6, 3)?)).await?;
    let before = registry.protection().await?.revision;
    for cut in [
        position(5, 3)?,
        position(6, 2)?,
        position(6, 3)?,
        position(7, 2)?,
    ] {
        ensure!(matches!(
            registry.select(old, cut).await,
            Err(ArchiveError::Conflict)
        ));
    }
    ensure!(registry.protection().await?.revision == before);
    ensure!(matches!(
        registry.prepare_acquire_selected(old.root()).await,
        Err(ArchiveError::ReferenceExpired)
    ));
    ensure!(registry.release(newer).await?);
    let intent = registry.prepare_acquire_selected(newer.root()).await?;
    ensure!(reference(registry.commit(&intent).await?)?.root() == newer.root());
    Ok(())
}

#[tokio::test]
async fn missing_or_corrupt_registry_never_resets_its_sequence() -> Result<()> {
    let (store, archive, registry) = fixture().await?;
    let held = fresh(&registry, 12, None).await?;
    let path = key(&store, "registry").await?;
    let valid = store.inner.get(&path).await?.bytes().await?;
    let mut corrupt = valid.to_vec();
    *corrupt.last_mut().context("registry checksum")? ^= 1;
    store.inner.put(&path, corrupt.into()).await?;
    ensure!(matches!(
        archive.open_root_registry(registry.id()).await,
        Err(ArchiveError::Corrupt(_))
    ));
    ensure!(registry.referenced(held).await.is_err());
    store.inner.delete(&path).await?;
    let writes = store.registry_writes.load(Ordering::Acquire);
    ensure!(matches!(
        archive.open_root_registry(registry.id()).await,
        Err(ArchiveError::Corrupt(_))
    ));
    ensure!(registry.prepare_reserve([13; 32], None).await.is_err());
    ensure!(store.registry_writes.load(Ordering::Acquire) == writes);
    let replacement = archive.create_root_registry().await?;
    ensure!(replacement.id() != registry.id());
    ensure!(matches!(
        replacement.referenced(held).await,
        Err(ArchiveError::Conflict)
    ));
    // Transplanting a valid record under another identity is also rejected.
    let replacement_path = store
        .inner
        .list(None)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .find(|entry| entry.location.filename() == Some("registry"))
        .context("replacement registry")?
        .location;
    store.inner.put(&replacement_path, valid.into()).await?;
    ensure!(matches!(
        replacement.protection().await,
        Err(ArchiveError::Corrupt(_))
    ));
    Ok(())
}

#[tokio::test]
async fn missing_identity_and_cross_registry_tokens_refuse() -> Result<()> {
    let (store, archive, registry) = fixture().await?;
    let ticket = reserve(&registry, 14, None).await?;
    let intent = registry
        .prepare_bind(ticket, RootDescriptor::new(ticket, [14; 32], 100)?, None)
        .await?;
    let other = archive.create_root_registry().await?;
    ensure!(matches!(
        other.commit(&intent).await,
        Err(ArchiveError::Conflict)
    ));
    ensure!(matches!(
        other.abort(ticket).await,
        Err(ArchiveError::Conflict)
    ));
    let objects = store.inner.list(None).try_collect::<Vec<_>>().await?;
    for entry in objects
        .iter()
        .filter(|entry| entry.location.filename() == Some("identity"))
    {
        store.inner.delete(&entry.location).await?;
    }
    ensure!(matches!(
        archive.open_root_registry(registry.id()).await,
        Err(ArchiveError::Corrupt(_))
    ));
    ensure!(matches!(
        archive.open_root_registry(other.id()).await,
        Err(ArchiveError::Corrupt(_))
    ));
    Ok(())
}

#[tokio::test]
async fn lost_creation_response_cannot_adopt_an_existing_identity() -> Result<()> {
    let store = Arc::new(archive_faults::Store::default());
    let (_, _, archive) = archive::history(store.clone()).await?;
    store.fail_after.store(true, Ordering::Release);
    ensure!(archive.create_root_registry().await.is_err());
    let abandoned = key(&store, "identity").await?;
    // The identity PUT committed, but creation did not establish a registry.
    ensure!(store
        .inner
        .list(None)
        .try_collect::<Vec<_>>()
        .await?
        .iter()
        .all(|entry| entry.location.filename() != Some("registry")));
    store.fail_registry_after.store(true, Ordering::Release);
    let created = archive.create_root_registry().await?;
    let objects = store.inner.list(None).try_collect::<Vec<_>>().await?;
    ensure!(
        objects
            .iter()
            .filter(|entry| entry.location.filename() == Some("identity"))
            .count()
            == 2
    );
    ensure!(objects.iter().any(|entry| entry.location == abandoned));
    ensure!(created.protection().await?.revision == 1);
    Ok(())
}

#[tokio::test]
async fn intent_reconciliation_binds_the_complete_operation() -> Result<()> {
    let (_, _, registry) = fixture().await?;
    let intent = registry.prepare_reserve([15; 32], None).await?;
    let ticket = reservation(registry.commit(&intent).await?)?;
    let mut changed = serde_json::to_value(&intent)?;
    changed["operation"]["Reserve"]["image"] = serde_json::to_value([16; 32])?;
    let changed: RootIntent = serde_json::from_value(changed)?;
    ensure!(matches!(
        registry.commit(&changed).await,
        Err(ArchiveError::ReferenceExpired)
    ));
    let mark = registry.protection().await?;
    ensure!(mark.registry == registry.id());
    ensure!(mark.pending_generations == vec![ticket.generation()]);
    let newer = reserve(&registry, 17, None).await?;
    ensure!(newer.generation() > mark.revision);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an isolated RustFS prefix and explicit credentials"]
async fn rustfs_registry_survives_reopen_and_serializes_competing_holds() -> Result<()> {
    let store = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(std::env::var("CELLAR_LARGE_IMAGE_BUCKET")?)
        .with_endpoint(std::env::var("CELLAR_LARGE_IMAGE_ENDPOINT")?)
        .with_allow_http(true)
        .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
        .build()?;
    let prefix = ObjectPath::from(format!(
        "root-retention-qualification/{:032x}",
        rand::random::<u128>()
    ));
    eprintln!("root retention qualification prefix={prefix}");
    let store = Arc::new(object_store::prefix::PrefixStore::new(store, prefix));
    let (_, _, archive) = archive::history(store.clone()).await?;
    let registry = archive.create_root_registry().await?;
    let held = fresh(&registry, 18, None).await?;
    let saved_id = serde_json::to_vec(&registry.id())?;
    let other = archive.open_root_registry(registry.id()).await?;
    let first = registry.prepare_clone(held).await?;
    let second = other.prepare_clone(held).await?;
    let (left, right) = tokio::join!(registry.commit(&first), other.commit(&second));
    let (winner, winner_intent) = match (left, right) {
        (Ok(result), Err(ArchiveError::ReferenceExpired)) => (reference(result)?, first),
        (Err(ArchiveError::ReferenceExpired), Ok(result)) => (reference(result)?, second),
        (left, right) => {
            bail!("conditional registry writes did not serialize: {left:?}, {right:?}")
        }
    };
    let saved_intent = serde_json::to_vec(&winner_intent)?;
    ensure!(registry.release(held).await?);
    drop(registry);
    drop(other);
    drop(archive);
    let (_, _, archive) = archive::history(store.clone()).await?;
    let reopened = archive
        .open_root_registry(serde_json::from_slice(&saved_id)?)
        .await?;
    ensure!(
        reference(
            reopened
                .commit(&serde_json::from_slice(&saved_intent)?)
                .await?
        )? == winner
    );
    ensure!(reopened.referenced(winner).await? == held.root());
    ensure!(reopened.protection().await?.roots == vec![held.root()]);
    ensure!(reopened.release(winner).await?);
    ensure!(reopened.protection().await?.roots.is_empty());
    ensure!(matches!(
        reopened.commit(&winner_intent).await,
        Err(ArchiveError::ReferenceExpired)
    ));
    Ok(())
}

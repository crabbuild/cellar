// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real conditional object-store operations with dropped responses. No fake
//! publication successes, voter claims or Raft authority are synthesized here.

use super::*;
use anyhow::{Context, Result};
use cellar::application::consensus::verification::{
    Catalog, CatalogError, CatalogOptions, NamespaceConfig,
};
use cellar::sdk::ExecutionDomain;
use object_store::{path::Path, ObjectStore};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

struct Fixture {
    domain: ExecutionDomain,
    store: Arc<fault::Store>,
    directory: RangeDirectory,
    config: NamespaceConfig,
    limits: DirectoryLimits,
}

impl Fixture {
    async fn new(limits: DirectoryLimits) -> Result<Self> {
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let store = Arc::new(fault::Store::default());
        store.reject_listing.store(true, Ordering::Release);
        let route = route();
        let config = NamespaceConfig::new(
            route.history.cluster,
            route.history.namespace,
            cellar_logic::NamespaceMode::Raft,
            route.schema_digest,
        );
        let catalog = Catalog::new(store.clone(), CatalogOptions::default())?;
        let namespace = domain.run(catalog.open_namespace(config.clone())).await??;
        let directory = RangeDirectory::new(namespace, limits)?;
        Ok(Self {
            domain,
            store,
            directory,
            config,
            limits,
        })
    }

    async fn run<F: std::future::Future>(&self, work: F) -> F::Output {
        self.domain
            .run(work)
            .await
            .expect("fixture execution domain closed before joined shutdown")
    }

    async fn reopen(&self) -> Result<RangeDirectory> {
        let catalog = Catalog::new(self.store.clone(), CatalogOptions::default())?;
        Ok(RangeDirectory::new(
            self.run(catalog.existing_namespace(self.config.clone()))
                .await?,
            self.limits,
        )?)
    }

    fn key(&self, name: &str) -> Path {
        Path::from(format!(
            "namespaces/{}/cell-groups/{name}",
            hex::encode(self.config.namespace.bytes())
        ))
    }
}

fn operation(n: u64) -> ProposalId {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&n.to_be_bytes());
    ProposalId::new(bytes).unwrap()
}

fn assigned(id: u64, range: CellRange, epoch: u64) -> GroupRoute {
    let mut r = route();
    r.history.group = GroupId::new(operation(id).bytes()).unwrap();
    r.history.incarnation = IncarnationId::new(operation(id + 10000).bytes()).unwrap();
    r.birth_digest[..8].copy_from_slice(&id.to_be_bytes());
    r.range = range;
    r.epoch = RouteEpoch::new(epoch).unwrap();
    r
}

fn ranges(count: usize) -> Vec<GroupRoute> {
    let ns = route().history.namespace;
    let mut keys: Vec<_> = (1..count)
        .map(|i| cell_routing_key(ns, &CellId::new(format!("boundary-{i}")).unwrap()))
        .collect();
    keys.sort();
    let mut lower = CellBound::NegativeInfinity;
    let mut result = vec![];
    for (i, upper) in keys
        .into_iter()
        .map(CellBound::Key)
        .chain([CellBound::PositiveInfinity])
        .enumerate()
    {
        result.push(assigned(
            i as u64 + 1,
            CellRange::new(lower, upper.clone()).unwrap(),
            1,
        ));
        lower = upper;
    }
    result
}

fn split(parent: &GroupRoute, id: u64) -> Vec<GroupRoute> {
    let key = (0..100_000)
        .map(|i| {
            cell_routing_key(
                parent.history.namespace,
                &CellId::new(format!("split-{id}-{i}")).unwrap(),
            )
        })
        .find(|key| {
            parent.range.contains(key) && CellBound::Key(key.clone()) != *parent.range.lower()
        })
        .unwrap();
    let (left, right) = parent.range.split(key).unwrap();
    vec![
        assigned(id, left, parent.epoch.get() + 1),
        assigned(id + 1, right, parent.epoch.get() + 1),
    ]
}

pub async fn single_owner() -> Result<()> {
    let f = Fixture::new(DirectoryLimits::default()).await?;
    let d = &f.directory;
    let initial = ranges(2);
    let init = d.prepare_initialization(operation(1), &initial)?;
    for bad in [
        vec![],
        vec![initial[0].clone()],
        vec![initial[1].clone(), initial[0].clone()],
        vec![initial[0].clone(), initial[0].clone()],
    ] {
        assert!(d.prepare_initialization(operation(2), &bad).is_err());
    }
    let mut duplicate = initial.clone();
    duplicate[1].history.group = duplicate[0].history.group;
    assert!(d.prepare_initialization(operation(2), &duplicate).is_err());
    let mut wrong_epoch = initial.clone();
    wrong_epoch[0].epoch = RouteEpoch::new(2)?;
    assert!(d
        .prepare_initialization(operation(2), &wrong_epoch)
        .is_err());
    f.run(d.publish(&init)).await?;
    let view = f.run(d.observe()).await?;
    let left = split(&initial[0], 100);
    let right = split(&initial[1], 200);
    let a = d.prepare_replacement(&view, operation(2), &initial[..1], &left)?;
    let b = d.prepare_replacement(&view, operation(3), &initial[1..], &right)?;
    let mut stale_parent = initial[0].clone();
    stale_parent.birth_digest = [22; 32];
    let stale = d.prepare_replacement(&view, operation(4), &[stale_parent], &left)?;
    assert!(f.run(d.publish(&stale)).await.is_err());
    f.store.range_before.armed.store(true, Ordering::Release);
    let entered = f.store.range_before.entered.notified();
    let (loser, winner) = f
        .run(cellar::asyncrt::timeout(Duration::from_secs(15), async {
            tokio::join!(d.publish(&a), async {
                entered.await;
                let result = d.publish(&b).await;
                f.store.range_before.resume.notify_one();
                result
            })
        }))
        .await?;
    assert!(matches!(loser, Err(CatalogError::Conflict(_))));
    assert_eq!(winner?.revision(), 2);
    let after = f.run(d.observe()).await?;
    assert_eq!(after.groups(), 3);
    assert_eq!(view.groups(), 2);
    assert!(matches!(
        f.run(d.publish(&a)).await,
        Err(CatalogError::Conflict(_))
    ));
    // Previously assigned group IDs stay reserved even after range replacement.
    let mut reused = assigned(201, initial[0].range.clone(), 2);
    reused.history.incarnation = IncarnationId::new([42; 16])?;
    let reused = d.prepare_replacement(&after, operation(5), &initial[..1], &[reused])?;
    assert!(matches!(
        f.run(d.publish(&reused)).await,
        Err(CatalogError::Conflict(_))
    ));
    for i in 0..100 {
        let cell = CellId::new(format!("check-{i}"))?;
        let key = cell_routing_key(f.config.namespace, &cell);
        let old = initial.iter().find(|r| r.range.contains(&key)).unwrap();
        let expected = if old == &initial[0] {
            old
        } else {
            right.iter().find(|r| r.range.contains(&key)).unwrap()
        };
        assert_eq!(&f.run(d.lookup(&after, &cell)).await?, expected);
        assert_eq!(&f.run(d.lookup(&view, &cell)).await?, old);
    }
    assert_eq!(f.store.listing_calls.load(Ordering::Acquire), 0);
    f.domain.shutdown().await;
    Ok(())
}

pub async fn bounded_discovery() -> Result<()> {
    let limits = DirectoryLimits {
        max_cached_pages: 4,
        max_cache_bytes: 4 * 64 * 1024,
        operation_timeout_ms: 30_000,
        ..Default::default()
    };
    let f = Fixture::new(limits).await?;
    let d = &f.directory;
    let initial = ranges(128);
    let init = d.prepare_initialization(operation(1), &initial)?;
    f.run(d.publish(&init)).await?;
    let view = f.run(d.observe()).await?;
    for i in 0..300 {
        let cell = CellId::new(format!("lookup-{i}"))?;
        let (found, reads) = fault::count_reads(f.run(d.lookup(&view, &cell))).await;
        assert!(
            reads.values().sum::<usize>() <= 8,
            "lookup must read one AVL path: {reads:?}"
        );
        assert!(reads.keys().all(|k| k.contains("/cell-groups/ranges/")));
        let key = cell_routing_key(f.config.namespace, &cell);
        assert_eq!(
            &found?,
            initial.iter().find(|r| r.range.contains(&key)).unwrap()
        );
        let (pages, bytes) = d.cache_usage()?;
        assert!(pages <= 4 && bytes <= limits.max_cache_bytes);
    }
    let parent = &initial[63];
    let children = split(parent, 1000);
    let mutation =
        d.prepare_replacement(&view, operation(2), std::slice::from_ref(parent), &children)?;
    let before = f.store.range_page_puts.load(Ordering::Acquire);
    f.run(d.publish(&mutation)).await?;
    let pages = f.store.range_page_puts.load(Ordering::Acquire) - before;
    assert!(
        pages <= 64,
        "one split rewrote {pages} pages for 128 groups"
    );
    let after = f.run(d.observe()).await?;
    assert_eq!(after.groups(), 129);
    let merged = assigned(2000, parent.range.clone(), 3);
    let merge = d.prepare_replacement(
        &after,
        operation(3),
        &children,
        std::slice::from_ref(&merged),
    )?;
    f.run(d.publish(&merge)).await?;
    let final_view = f.run(d.observe()).await?;
    assert_eq!(final_view.groups(), 128);
    let catalog = Catalog::new(f.store.clone(), CatalogOptions::default())?;
    let namespace = f.run(catalog.existing_namespace(f.config.clone())).await?;
    let warm = RangeDirectory::new(namespace, DirectoryLimits::default())?;
    let warm_view = f.run(warm.observe()).await?;
    let cell = CellId::new("warm-cell")?;
    f.run(warm.lookup(&warm_view, &cell)).await?;
    let (result, reads) = fault::count_reads(f.run(warm.lookup(&warm_view, &cell))).await;
    result?;
    assert!(
        reads.is_empty(),
        "cached route lookup must not refresh S3 metadata"
    );
    assert_eq!(f.store.listing_calls.load(Ordering::Acquire), 0);
    f.domain.shutdown().await;
    Ok(())
}

pub async fn recovery() -> Result<()> {
    let f = Fixture::new(DirectoryLimits::default()).await?;
    let d = &f.directory;
    let initial = ranges(1);
    let init = d.prepare_initialization(operation(1), &initial)?;
    f.run(d.publish(&init)).await?;
    let view = f.run(d.observe()).await?;
    let children = split(&initial[0], 100);
    let first = d.prepare_replacement(&view, operation(2), &initial, &children)?;
    f.store.fail_head_after.store(true, Ordering::Release);
    f.store
        .fail_head_confirmation
        .store(true, Ordering::Release);
    assert!(matches!(
        f.run(d.publish(&first)).await,
        Err(CatalogError::Unconfirmed)
    ));
    let restarted = f.reopen().await?;
    let retry = restarted.decode_mutation(first.encode())?;
    let selected = f.run(restarted.observe()).await?;
    assert_eq!(selected.revision(), 2);
    let second_children = split(&children[0], 200);
    let second =
        restarted.prepare_replacement(&selected, operation(3), &children[..1], &second_children)?;
    let latest_publication = f.run(restarted.publish(&second)).await?;
    let journal_key = f.key(&format!(
        "journal/{}",
        hex::encode(latest_publication.journal_digest())
    ));
    assert!(
        fault::missing_read(journal_key.to_string(), f.run(restarted.observe()))
            .await
            .is_err(),
        "a head without its selected journal cannot be observed as valid"
    );
    let receipt = f.run(restarted.publish(&retry)).await?;
    assert_eq!(
        receipt.revision(),
        2,
        "retry finds its own publication after a later CAS"
    );
    assert_eq!(f.run(restarted.publication(&retry)).await?, Some(receipt));
    let latest = f.run(restarted.observe()).await?;
    let reused = restarted.prepare_replacement(
        &latest,
        operation(2),
        &children[1..],
        &split(&children[1], 300),
    )?;
    assert!(matches!(
        f.run(restarted.publish(&reused)).await,
        Err(CatalogError::Conflict(_))
    ));
    assert!(
        restarted.lookup(&view, &CellId::new("x")?).await.is_err(),
        "old process view cannot attach to a new owner"
    );
    // Cold discovery must verify the selected immutable page, not just its name.
    let cold = f.reopen().await?;
    let cold_view = f.run(cold.observe()).await?;
    let cell = CellId::new("corruption-probe")?;
    let (result, reads) = fault::count_reads(f.run(cold.lookup(&cold_view, &cell))).await;
    result?;
    let path = reads
        .keys()
        .find(|k| k.contains("/ranges/"))
        .context("range read")?
        .clone();
    let original = f
        .store
        .inner
        .get(&Path::from(path.clone()))
        .await?
        .bytes()
        .await?;
    let mut corrupt = original.to_vec();
    *corrupt.last_mut().unwrap() ^= 1;
    f.store
        .inner
        .put(&Path::from(path.clone()), corrupt.into())
        .await?;
    let fresh = f.reopen().await?;
    let fresh_view = f.run(fresh.observe()).await?;
    assert!(f.run(fresh.lookup(&fresh_view, &cell)).await.is_err());
    f.store
        .inner
        .put(&Path::from(path.clone()), original.into())
        .await?;
    assert!(
        fault::missing_read(path, f.run(fresh.lookup(&fresh_view, &cell)))
            .await
            .is_err()
    );
    f.store.inner.delete(&f.key("head")).await?;
    assert!(f.run(restarted.observe()).await.is_err());
    assert!(
        f.run(restarted.publish(&init)).await.is_err(),
        "missing occupied metadata cannot reinitialize"
    );
    assert!(matches!(
        f.store.inner.get(&f.key("head")).await,
        Err(object_store::Error::NotFound { .. })
    ));
    f.domain.shutdown().await;
    Ok(())
}

pub async fn genesis_race() -> Result<()> {
    let f = Fixture::new(DirectoryLimits::default()).await?;
    let d = &f.directory;
    let initial = ranges(1);
    let init = d.prepare_initialization(operation(1), &initial)?;
    f.store
        .range_genesis_after
        .armed
        .store(true, Ordering::Release);
    let entered = f.store.range_genesis_after.entered.notified();
    let (first, second) = f
        .run(cellar::asyncrt::timeout(Duration::from_secs(15), async {
            tokio::join!(d.publish(&init), async {
                entered.await;
                let result = async {
                    let receipt = d.publish(&init).await?;
                    let view = d.observe().await?;
                    let replacement = d.prepare_replacement(
                        &view,
                        operation(2),
                        &initial,
                        &split(&initial[0], 100),
                    )?;
                    d.publish(&replacement).await?;
                    Ok::<_, CatalogError>(receipt)
                }
                .await;
                f.store.range_genesis_after.resume.notify_one();
                result
            })
        }))
        .await?;
    assert_eq!(first?, second?);
    assert_eq!(f.run(d.observe()).await?.revision(), 2);
    f.domain.shutdown().await;
    Ok(())
}

pub async fn churn() -> Result<()> {
    let f = Fixture::new(DirectoryLimits {
        max_cached_pages: 3,
        max_cache_bytes: 3 * 64 * 1024,
        operation_timeout_ms: 30_000,
        ..Default::default()
    })
    .await?;
    let d = &f.directory;
    let initial = ranges(16);
    let init = d.prepare_initialization(operation(1), &initial)?;
    f.run(d.publish(&init)).await?;
    let original_view = f.run(d.observe()).await?;
    // The oracle is an independently maintained flat partition. Alternate
    // splitting and merging different interior ranges so deletions exercise
    // successor replacement and rotations, rather than undoing the last split.
    let mut expected = initial.clone();
    for step in 0..80_usize {
        let view = f.run(d.observe()).await?;
        let position = (step * 7 + 3) % (expected.len() - 1);
        let id = 1000 + step as u64 * 10;
        let (parents, children) = if step % 2 == 0 {
            let parent = expected[position].clone();
            let children = split(&parent, id);
            (vec![parent], children)
        } else {
            let parents = expected[position..position + 2].to_vec();
            let range = parents[0].range.merge(&parents[1].range)?;
            let epoch = parents.iter().map(|r| r.epoch.get()).max().unwrap() + 1;
            (parents, vec![assigned(id, range, epoch)])
        };
        let mutation =
            d.prepare_replacement(&view, operation(step as u64 + 2), &parents, &children)?;
        let receipt = f.run(d.publish(&mutation)).await?;
        expected.splice(position..position + parents.len(), children);
        let after = f.run(d.observe()).await?;
        assert_eq!(after.revision(), step as u64 + 2);
        assert_eq!(after.groups() as usize, expected.len());
        assert_eq!(f.run(d.publication(&mutation)).await?, Some(receipt));
        // Probe every boundary itself (half-open ownership) plus independent
        // cell keys that are not selected from the tree under test.
        let probes = expected
            .iter()
            .filter_map(|r| match r.range.lower() {
                CellBound::Key(key) => Some(key.cell().clone()),
                _ => None,
            })
            .chain((0..32).map(|i| CellId::new(format!("churn-{i}")).unwrap()));
        for cell in probes {
            let key = cell_routing_key(f.config.namespace, &cell);
            let owners: Vec<_> = expected.iter().filter(|r| r.range.contains(&key)).collect();
            assert_eq!(owners.len(), 1);
            assert_eq!(&f.run(d.lookup(&after, &cell)).await?, owners[0]);
            let old = initial.iter().find(|r| r.range.contains(&key)).unwrap();
            assert_eq!(&f.run(d.lookup(&original_view, &cell)).await?, old);
        }
        assert!(d.cache_usage()?.0 <= 3);
    }
    let restarted = f.reopen().await?;
    assert_eq!(f.run(restarted.observe()).await?.revision(), 81);
    assert_eq!(f.run(restarted.publish(&init)).await?.revision(), 1);
    assert_eq!(f.store.listing_calls.load(Ordering::Acquire), 0);
    f.domain.shutdown().await;
    Ok(())
}

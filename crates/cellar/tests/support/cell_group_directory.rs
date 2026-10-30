// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real conditional object-store operations with dropped responses. No fake
//! publication successes, voter claims or Raft authority are synthesized here.

use super::*;
use anyhow::{Context, Result};
use cellar::application::consensus::verification::{
    Catalog, CatalogError, CatalogOptions, InitialMember, NamespaceConfig,
};
use cellar::sdk::ExecutionDomain;
use cellar_logic::consensus::{MemberId, NodeId};
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
        Self::with_schema(limits, route().schema_digest).await
    }

    async fn with_schema(limits: DirectoryLimits, schema: [u8; 32]) -> Result<Self> {
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let store = Arc::new(fault::Store::default());
        store.reject_listing.store(true, Ordering::Release);
        let route = route();
        let config = NamespaceConfig::new(
            route.history.cluster,
            route.history.namespace,
            cellar_logic::NamespaceMode::Raft,
            schema,
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

    async fn reserve(&self, routes: &mut [GroupRoute]) -> Result<(), CatalogError> {
        for route in routes {
            let request = self.directory.prepare_group_birth(birth_spec(route))?;
            self.run(self.directory.reserve_group_birth(&request))
                .await?;
            *route = request.route().clone();
        }
        Ok(())
    }

    fn key(&self, name: &str) -> Path {
        Path::from(format!(
            "namespaces/{}/cell-groups/{name}",
            hex::encode(self.config.namespace.bytes())
        ))
    }
}

fn birth_spec(route: &GroupRoute) -> CellGroupBirthSpec {
    CellGroupBirthSpec {
        history: route.history,
        range: route.range.clone(),
        epoch: route.epoch,
        creation: ProposalId::new(route.history.group.bytes()).unwrap(),
        voters: (1..=3)
            .map(|n| InitialMember {
                member: MemberId::new(n).unwrap(),
                node: NodeId::new([n as u8; 16]).unwrap(),
            })
            .collect(),
        learners: vec![],
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
    let mut initial = ranges(2);
    f.reserve(&mut initial).await?;
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
    let mut left = split(&initial[0], 100);
    let mut right = split(&initial[1], 200);
    f.reserve(&mut left).await?;
    f.reserve(&mut right).await?;
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
    let mut initial = ranges(128);
    f.reserve(&mut initial).await?;
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
    let mut children = split(parent, 1000);
    f.reserve(&mut children).await?;
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
    let mut merged = assigned(2000, parent.range.clone(), 3);
    f.reserve(std::slice::from_mut(&mut merged)).await?;
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
    let mut initial = ranges(1);
    f.reserve(&mut initial).await?;
    let init = d.prepare_initialization(operation(1), &initial)?;
    f.run(d.publish(&init)).await?;
    let view = f.run(d.observe()).await?;
    let mut children = split(&initial[0], 100);
    f.reserve(&mut children).await?;
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
    let mut second_children = split(&children[0], 200);
    f.reserve(&mut second_children).await?;
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
    let mut initial = ranges(1);
    f.reserve(&mut initial).await?;
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
                    let mut children = split(&initial[0], 100);
                    f.reserve(&mut children).await?;
                    let replacement =
                        d.prepare_replacement(&view, operation(2), &initial, &children)?;
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
    let mut initial = ranges(16);
    f.reserve(&mut initial).await?;
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
        let (parents, mut children) = if step % 2 == 0 {
            let parent = expected[position].clone();
            let children = split(&parent, id);
            (vec![parent], children)
        } else {
            let parents = expected[position..position + 2].to_vec();
            let range = parents[0].range.merge(&parents[1].range)?;
            let epoch = parents.iter().map(|r| r.epoch.get()).max().unwrap() + 1;
            (parents, vec![assigned(id, range, epoch)])
        };
        f.reserve(&mut children).await?;
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

pub async fn birth_recovery() -> Result<()> {
    let f = Fixture::new(DirectoryLimits::default()).await?;
    let d = &f.directory;
    let request = d.prepare_group_birth(birth_spec(&ranges(1)[0]))?;
    let route = request.route();
    let birth_key = f.key(&format!(
        "births/{}/{}",
        hex::encode(route.history.group.bytes()),
        hex::encode(route.history.incarnation.bytes())
    ));
    let init = d.prepare_initialization(operation(99), std::slice::from_ref(route))?;
    assert!(matches!(
        f.run(d.publish(&init)).await,
        Err(CatalogError::Corrupt(_))
    ));
    assert!(matches!(
        f.store.inner.get(&f.key("head")).await,
        Err(object_store::Error::NotFound { .. })
    ));
    f.run(d.reserve_group_birth(&request)).await?;
    assert!(
        f.run(d.materialize_group_birth(&request)).await.is_err(),
        "an unselected reservation is not a birth"
    );
    f.run(d.publish(&init)).await?;
    assert!(
        matches!(
            f.store.inner.get(&birth_key).await,
            Err(object_store::Error::NotFound { .. })
        ),
        "range publication must not eagerly materialize groups"
    );
    let unknown = fault::missing_read(
        birth_key.to_string(),
        f.run(d.materialize_group_birth(&request)),
    )
    .await;
    assert!(matches!(unknown, Err(CatalogError::Unconfirmed)));
    assert_eq!(
        f.store.inner.get(&birth_key).await?.bytes().await?.as_ref(),
        request.encode(),
        "unknown confirmation follows actual immutable creation"
    );
    let restarted = f.reopen().await?;
    let retry = restarted.decode_group_birth(request.encode())?;
    assert_eq!(retry.route(), request.route());
    let birth = f.run(restarted.materialize_group_birth(&retry)).await?;
    assert_eq!(birth.route(), route);
    assert_eq!(birth.creation(), request.creation());
    assert_eq!(birth.voters(), request.voters());
    assert!(birth.learners().is_empty());
    f.run(restarted.require_current_group_birth(&birth)).await?;
    assert!(matches!(
        f.run(d.require_current_group_birth(&birth)).await,
        Err(CatalogError::Conflict(_))
    ));
    assert!(matches!(
        fault::missing_read(
            birth_key.to_string(),
            f.run(restarted.require_current_group_birth(&birth))
        )
        .await,
        Err(CatalogError::Corrupt(_))
    ));
    let mut corrupt = request.encode().to_vec();
    *corrupt.last_mut().unwrap() ^= 1;
    f.store.inner.put(&birth_key, corrupt.into()).await?;
    assert!(matches!(
        f.run(restarted.require_current_group_birth(&birth)).await,
        Err(CatalogError::Corrupt(_))
    ));
    f.store
        .inner
        .put(&birth_key, request.encode().to_vec().into())
        .await?;
    let view = f.run(restarted.observe()).await?;
    let mut children = split(route, 700);
    f.reserve(&mut children).await?;
    let replace = restarted.prepare_replacement(
        &view,
        operation(100),
        std::slice::from_ref(route),
        &children,
    )?;
    f.run(restarted.publish(&replace)).await?;
    assert!(matches!(
        f.run(restarted.require_current_group_birth(&birth)).await,
        Err(CatalogError::Fenced)
    ));
    assert!(matches!(
        f.run(restarted.materialize_group_birth(&retry)).await,
        Err(CatalogError::Fenced)
    ));
    assert_eq!(
        f.store.inner.get(&birth_key).await?.bytes().await?.as_ref(),
        request.encode()
    );
    assert_eq!(f.store.listing_calls.load(Ordering::Acquire), 0);
    f.domain.shutdown().await;
    Ok(())
}

pub async fn birth_reservations() -> Result<()> {
    let f = Fixture::new(DirectoryLimits::default()).await?;
    let d = &f.directory;
    let route = ranges(1).remove(0);
    let original = d.prepare_group_birth(birth_spec(&route))?;
    assert_eq!(&original.encode()[..9], b"CLRCAT01\xba");
    let mut wrong_domain = original.encode().to_vec();
    wrong_domain[8] = 86;
    assert!(d.decode_group_birth(&wrong_domain).is_err());
    let mut unordered = birth_spec(&route);
    unordered.voters.reverse();
    assert_eq!(
        d.prepare_group_birth(unordered)?.encode(),
        original.encode()
    );
    let key = Path::from(format!(
        "groups/{}",
        hex::encode(route.history.group.bytes())
    ));
    assert!(matches!(
        fault::missing_read(key.to_string(), f.run(d.reserve_group_birth(&original))).await,
        Err(CatalogError::Unconfirmed)
    ));
    assert_eq!(
        f.store.inner.get(&key).await?.bytes().await?.as_ref(),
        original.encode()
    );
    f.run(d.reserve_group_birth(&original)).await?;
    for which in 0..4 {
        let mut spec = birth_spec(&route);
        match which {
            0 => spec.creation = operation(555),
            1 => spec.history.incarnation = IncarnationId::new([55; 16])?,
            2 => spec.voters[0].node = NodeId::new([55; 16])?,
            _ => spec.epoch = RouteEpoch::new(2)?,
        }
        let changed = d.prepare_group_birth(spec)?;
        assert_ne!(changed.encode(), original.encode());
        assert!(matches!(
            f.run(d.reserve_group_birth(&changed)).await,
            Err(CatalogError::Conflict(_))
        ));
    }
    let mut duplicate = birth_spec(&route);
    duplicate.voters[1].node = duplicate.voters[0].node;
    assert!(d.prepare_group_birth(duplicate).is_err());
    let mut wrong_count = birth_spec(&route);
    wrong_count.voters.pop();
    assert!(d.prepare_group_birth(wrong_count).is_err());
    let mut oversized = birth_spec(&route);
    oversized.learners = vec![oversized.voters[0]; 33];
    assert!(matches!(
        d.prepare_group_birth(oversized),
        Err(CatalogError::Limit)
    ));
    for end in 0..original.encode().len() {
        assert!(d.decode_group_birth(&original.encode()[..end]).is_err());
    }
    assert!(d.decode_group_birth(&vec![0; 64 * 1024 + 1]).is_err());
    let mut config = f.config.clone();
    config.namespace = NamespaceId::new([42; 16])?;
    let catalog = Catalog::new(f.store.clone(), CatalogOptions::default())?;
    let foreign = RangeDirectory::new(
        f.run(catalog.open_namespace(config.clone())).await?,
        DirectoryLimits::default(),
    )?;
    assert!(matches!(
        foreign.decode_group_birth(original.encode()),
        Err(CatalogError::Conflict(_))
    ));
    let mut spec = birth_spec(&route);
    spec.history.namespace = config.namespace;
    let competing = foreign.prepare_group_birth(spec)?;
    assert!(matches!(
        f.run(foreign.reserve_group_birth(&competing)).await,
        Err(CatalogError::Conflict(_))
    ));
    f.run(d.reserve_group_birth(&original)).await?;
    // The original reservation survives every conflicting attempt unchanged.
    assert_eq!(
        f.store.inner.get(&key).await?.bytes().await?.as_ref(),
        original.encode()
    );
    // Configuring a managed policy does not authorize the unmanaged APIs. No
    // fleet permit is fabricated, and refusal must precede any store access.
    config.namespace = NamespaceId::new([43; 16])?;
    config.managed_fleet = Some(serde_json::from_value(serde_json::json!({
        "fleet": vec![44; 16], "catalog": vec![45; 32],
    }))?);
    let managed = RangeDirectory::new(
        f.run(catalog.open_namespace(config.clone())).await?,
        DirectoryLimits::default(),
    )?;
    let mut spec = birth_spec(&route);
    spec.history.namespace = config.namespace;
    assert!(matches!(
        managed.prepare_group_birth(spec),
        Err(CatalogError::ManagedAdmissionRequired)
    ));
    let (result, reads) = fault::count_reads(f.run(managed.reserve_group_birth(&original))).await;
    assert!(matches!(
        result,
        Err(CatalogError::ManagedAdmissionRequired)
    ));
    assert!(reads.is_empty());
    assert!(matches!(
        f.run(managed.materialize_group_birth(&original)).await,
        Err(CatalogError::ManagedAdmissionRequired)
    ));
    assert_eq!(f.store.listing_calls.load(Ordering::Acquire), 0);
    f.domain.shutdown().await;
    Ok(())
}

fn member_options(
    path: &std::path::Path,
    node: u8,
) -> cellar::application::consensus::verification::ReplicaLogOptions {
    use cellar::application::consensus::verification::{LogOpenMode, ReplicaLogOptions};
    use cellar_logic::consensus::{DiskLineage, ProcessGeneration};
    ReplicaLogOptions::new(
        path.to_owned(),
        NodeId::new([node; 16]).unwrap(),
        DiskLineage::new([node + 10; 16]).unwrap(),
        ProcessGeneration::new([1; 16]).unwrap(),
        LogOpenMode::Create,
    )
}

async fn selected_birth(f: &Fixture) -> Result<CellGroupBirth> {
    let d = &f.directory;
    let request = d.prepare_group_birth(birth_spec(&ranges(1)[0]))?;
    f.run(d.reserve_group_birth(&request)).await?;
    let initial =
        d.prepare_initialization(operation(999), std::slice::from_ref(request.route()))?;
    f.run(d.publish(&initial)).await?;
    Ok(f.run(d.materialize_group_birth(&request)).await?)
}

pub async fn member_bootstrap() -> Result<()> {
    use cellar::application::consensus::verification::{LogOpenMode, ReplicaLog};
    use cellar_logic::consensus::ProcessGeneration;
    let f = Fixture::new(DirectoryLimits::default()).await?;
    let birth = selected_birth(&f).await?;
    let temp = tempfile::tempdir()?;
    let mut options = member_options(&temp.path().join("member"), 1);
    let log = f
        .run(ReplicaLog::open(
            options.clone(),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await?;
    let history = birth.route().history;
    let claim_key = f.key(&format!(
        "claims/{}/{}/0000000000000001",
        hex::encode(history.group.bytes()),
        hex::encode(history.incarnation.bytes())
    ));
    let result = fault::missing_read(
        claim_key.to_string(),
        f.run(f.directory.bootstrap_group_member(&birth, &log)),
    )
    .await;
    assert!(matches!(result, Err(CatalogError::Unconfirmed)));
    let group = log.cell_group(&history)?;
    assert_eq!(group.group_manifest()?.unwrap().route(), birth.route());
    assert_eq!(group.group_manifest()?.unwrap().root().cells(), 0);
    let key = group.numeric_key();
    let claim = f.store.inner.get(&claim_key).await?.bytes().await?;
    assert_eq!(&claim[..9], b"CLRCAT01\xbb");
    let original_engine = log.storage_instance()?;
    log.close().await?;
    options.mode = LogOpenMode::Resume;
    options.process = ProcessGeneration::new([9; 16])?;
    let log = f
        .run(ReplicaLog::open(
            options.clone(),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await?;
    let reopened = f.reopen().await?;
    // An observation from a different catalog owner never grants admission.
    assert!(matches!(
        f.run(reopened.bootstrap_group_member(&birth, &log)).await,
        Err(CatalogError::Conflict(_))
    ));
    let request = reopened.prepare_group_birth(birth_spec(birth.route()))?;
    let current = f.run(reopened.materialize_group_birth(&request)).await?;
    let permit = f
        .run(reopened.bootstrap_group_member(&current, &log))
        .await?;
    f.run(permit.revalidate()).await?;
    assert_eq!(permit.group().numeric_key(), key);
    assert_eq!(permit.group().identity().scope, history);
    assert_eq!(permit.group().identity().process, options.process);
    assert_eq!(log.storage_instance()?, original_engine);
    assert_eq!(f.store.inner.get(&claim_key).await?.bytes().await?, claim);
    // Corrupt claims are not overwritten or interpreted as absent.
    f.store
        .inner
        .put(&claim_key, b"not-a-claim".to_vec().into())
        .await?;
    assert!(matches!(
        f.run(permit.revalidate()).await,
        Err(CatalogError::Conflict(_))
    ));
    f.store.inner.put(&claim_key, claim.clone().into()).await?;
    drop(permit);
    log.close().await?;
    // Remove only this owned fixture's group state, preserving the actual
    // engine instance and permanent node pin. This exercises the member claim
    // refusal independently of the earlier node-storage pin check.
    let dir = options.directory.join("log").to_string_lossy().into_owned();
    f.domain
        .blocking(move || -> Result<()> {
            let engine = raft_engine::Engine::open(raft_engine::Config {
                dir,
                ..Default::default()
            })?;
            let mut catalog_key = b"g:".to_vec();
            catalog_key.extend_from_slice(&history.group.bytes());
            catalog_key.extend_from_slice(&history.incarnation.bytes());
            let mut batch = raft_engine::LogBatch::default();
            batch.delete(0, catalog_key);
            batch.add_command(key, raft_engine::Command::Clean);
            engine.write(&mut batch, true)?;
            Ok(())
        })?
        .await??;
    let log = f
        .run(ReplicaLog::open(
            options,
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await?;
    assert_eq!(log.storage_instance()?, original_engine);
    assert!(log.cell_groups()?.is_empty());
    assert!(matches!(
        f.run(reopened.bootstrap_group_member(&current, &log)).await,
        Err(CatalogError::LostStableState)
    ));
    assert!(log.cell_groups()?.is_empty());
    log.close().await?;
    let blank = f
        .run(ReplicaLog::open(
            member_options(&temp.path().join("new-path"), 1),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await?;
    assert!(matches!(
        f.run(reopened.bootstrap_group_member(&current, &blank))
            .await,
        Err(CatalogError::LostStableState)
    ));
    assert!(blank.cell_groups()?.is_empty());
    blank.close().await?;
    assert_eq!(f.store.inner.get(&claim_key).await?.bytes().await?, claim);
    f.domain.shutdown().await;
    Ok(())
}

pub async fn member_fencing() -> Result<()> {
    use cellar::application::consensus::verification::ReplicaLog;
    let f = Fixture::new(DirectoryLimits::default()).await?;
    let birth = selected_birth(&f).await?;
    let temp = tempfile::tempdir()?;
    let foreign = f
        .run(ReplicaLog::open(
            member_options(&temp.path().join("foreign"), 9),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await?;
    assert!(matches!(
        f.run(f.directory.bootstrap_group_member(&birth, &foreign))
            .await,
        Err(CatalogError::NotInitialMember)
    ));
    assert!(foreign.cell_groups()?.is_empty());
    foreign.close().await?;
    let log = f
        .run(ReplicaLog::open(
            member_options(&temp.path().join("member"), 1),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await?;
    let permit = f
        .run(f.directory.bootstrap_group_member(&birth, &log))
        .await?;
    let view = f.run(f.directory.observe()).await?;
    let mut children = split(birth.route(), 700);
    f.reserve(&mut children).await?;
    let replace = f.directory.prepare_replacement(
        &view,
        operation(1000),
        std::slice::from_ref(birth.route()),
        &children,
    )?;
    f.run(f.directory.publish(&replace)).await?;
    assert!(matches!(
        f.run(permit.revalidate()).await,
        Err(CatalogError::Fenced)
    ));
    assert_eq!(log.cell_groups()?.len(), 1);
    log.close().await?;
    f.domain.shutdown().await;
    Ok(())
}

fn route_request() -> CellRouteRequest {
    use cellar_logic::consensus::{ClientId, CommandId};
    CellRouteRequest {
        cell: CellHistory {
            cluster: route().history.cluster,
            namespace: route().history.namespace,
            cell: CellId::new("counter-routing").unwrap(),
            incarnation: IncarnationId::new([81; 16]).unwrap(),
        },
        operation: CellRouteOperation::Command(
            CommandId::new(ClientId::new([82; 16]).unwrap(), 7).unwrap(),
        ),
        fingerprint: Sha256::digest(b"canonical command with input and durability").into(),
    }
}

fn routing_limits(max_attempts: u8) -> CellRoutingLimits {
    CellRoutingLimits {
        timeout: Duration::from_secs(10),
        max_attempts,
    }
}

async fn publish_route_split(
    f: &Fixture,
    parent: &GroupRoute,
    cell: &CellId,
) -> Result<GroupRoute> {
    let view = f.run(f.directory.observe()).await?;
    let mut children = split(parent, 700);
    for child in &mut children {
        child.schema_digest = parent.schema_digest;
    }
    f.reserve(&mut children).await?;
    let replace = f.directory.prepare_replacement(
        &view,
        operation(1000),
        std::slice::from_ref(parent),
        &children,
    )?;
    f.run(f.directory.publish(&replace)).await?;
    let key = cell_routing_key(parent.history.namespace, cell);
    Ok(children
        .into_iter()
        .find(|r| r.range.contains(&key))
        .unwrap())
}

pub async fn routing_authentication() -> Result<()> {
    let f = Fixture::new(DirectoryLimits::default()).await?;
    let birth = selected_birth(&f).await?;
    let request = route_request();
    let mut session =
        f.directory
            .routing_session(&f.domain, request.clone(), routing_limits(4), None)?;
    let first = session.begin().await?;
    assert_eq!(first.route(), birth.route());
    session.mark_submitted(&first)?;
    let next = publish_route_split(&f, birth.route(), &request.cell.cell).await?;
    let second = session.redirect(&first, &next).await?;
    assert_eq!(second.route(), &next);
    assert_eq!(second.request(), &request);
    assert_eq!(second.deadline_ms(), first.deadline_ms());
    assert_eq!(second.ordinal(), 2);
    let stale = session.redirect(&first, &next).await.err().unwrap();
    assert!(matches!(stale.reason, CellRoutingFailure::StaleAttempt));
    assert!(stale.ambiguous_command.is_some());
    let third = session.retry(&second).await?;
    assert_eq!(third.request(), first.request());
    assert_eq!(third.deadline_ms(), first.deadline_ms());

    // Valid-looking but unpublished history must fail catalog confirmation.
    let mut forged = next.clone();
    forged.history.group = GroupId::new([99; 16])?;
    let mut refused = f.directory.routing_session(
        &f.domain,
        request.clone(),
        routing_limits(4),
        Some(first.cached_route()),
    )?;
    let attempt = refused.begin().await?;
    refused.mark_submitted(&attempt)?;
    let (result, reads) = fault::count_reads(refused.redirect(&attempt, &forged)).await;
    let failure = result.err().unwrap();
    assert!(matches!(failure.reason, CellRoutingFailure::InvalidHint));
    assert!(!reads.is_empty());
    let CellRouteOperation::Command(id) = request.operation else {
        unreachable!()
    };
    assert_eq!(failure.ambiguous_command, Some(id));
    assert!(matches!(
        refused.retry(&attempt).await.err().unwrap().reason,
        CellRoutingFailure::StaleAttempt
    ));

    // Syntactically valid namespace/schema rebinding is rejected before I/O.
    for field in 0..4 {
        let mut wrong = next.clone();
        match field {
            0 => wrong.history.namespace = NamespaceId::new([91; 16])?,
            1 => wrong.history.cluster = ClusterId::new([92; 16])?,
            2 => wrong.schema_digest = [93; 32],
            _ => wrong.epoch = birth.route().epoch,
        }
        let mut session = f.directory.routing_session(
            &f.domain,
            request.clone(),
            routing_limits(4),
            Some(first.cached_route()),
        )?;
        let attempt = session.begin().await?;
        let (result, reads) = fault::count_reads(session.redirect(&attempt, &wrong)).await;
        assert!(matches!(
            result.err().unwrap().reason,
            CellRoutingFailure::InvalidHint
        ));
        assert!(reads.is_empty());
    }
    assert_eq!(f.store.listing_calls.load(Ordering::Acquire), 0);
    f.domain.shutdown().await;
    Ok(())
}

pub async fn routing_cache_and_budget() -> Result<()> {
    let f = Fixture::new(DirectoryLimits::default()).await?;
    let birth = selected_birth(&f).await?;
    let request = route_request();
    let mut origin =
        f.directory
            .routing_session(&f.domain, request.clone(), routing_limits(2), None)?;
    let first = origin.begin().await?;
    let cached = first.cached_route();
    let next = publish_route_split(&f, birth.route(), &request.cell.cell).await?;
    origin.mark_submitted(&first)?;
    let second = origin.redirect(&first, &next).await?;
    let error = origin.retry(&second).await.err().unwrap();
    assert!(matches!(
        error.reason,
        CellRoutingFailure::AttemptsExhausted
    ));
    assert!(error.ambiguous_command.is_some());

    // Cached attempts can use a historical route during a catalog outage.
    // This grants no serving authority: the destination must check its epoch.
    f.store.fail_read.store(true, Ordering::Release);
    let mut known = f.directory.routing_session(
        &f.domain,
        request.clone(),
        routing_limits(2),
        Some(cached.clone()),
    )?;
    let (attempt, reads) = fault::count_reads(known.begin()).await;
    let attempt = attempt?;
    assert!(reads.is_empty());
    assert!(f.store.fail_read.load(Ordering::Acquire));
    assert_eq!(attempt.route(), birth.route());
    assert_eq!(attempt.request(), &request);
    let (retry, reads) = fault::count_reads(known.retry(&attempt)).await;
    assert!(reads.is_empty());
    assert_eq!(retry?.request(), &request);
    let foreign_ticket = known.mark_submitted(&first).unwrap_err();
    assert!(matches!(
        foreign_ticket.reason,
        CellRoutingFailure::StaleAttempt
    ));
    f.store.fail_read.store(false, Ordering::Release);
    let foreign = f.reopen().await?;
    assert!(matches!(
        foreign
            .routing_session(
                &f.domain,
                request.clone(),
                routing_limits(2),
                Some(cached.clone())
            )
            .err()
            .unwrap()
            .reason,
        CellRoutingFailure::InvalidRequest
    ));
    for field in 0..3 {
        let mut wrong = request.clone();
        match field {
            0 => wrong.cell.cell = CellId::new("another-cell")?,
            1 => wrong.cell.incarnation = IncarnationId::new([93; 16])?,
            _ => wrong.fingerprint = [0; 32],
        }
        assert!(matches!(
            f.directory
                .routing_session(&f.domain, wrong, routing_limits(2), Some(cached.clone()))
                .err()
                .unwrap()
                .reason,
            CellRoutingFailure::InvalidRequest
        ));
    }
    for maximum in [0, 17, 255] {
        assert!(f
            .directory
            .routing_session(&f.domain, request.clone(), routing_limits(maximum), None)
            .is_err());
    }
    f.domain.shutdown().await;
    assert!(matches!(
        origin.retry(&second).await.err().unwrap().reason,
        CellRoutingFailure::StaleAttempt
    ));
    Ok(())
}

#[allow(clippy::disallowed_methods)]
pub async fn routing_deadline_and_cancellation() -> Result<()> {
    let f = Fixture::new(DirectoryLimits::default()).await?;
    let birth = selected_birth(&f).await?;
    let request = route_request();
    let mut initial =
        f.directory
            .routing_session(&f.domain, request.clone(), routing_limits(4), None)?;
    let observed = initial.begin().await?;
    let next = publish_route_split(&f, birth.route(), &request.cell.cell).await?;
    let limits = CellRoutingLimits {
        timeout: Duration::from_secs(5),
        max_attempts: 4,
    };
    let mut session = f.directory.routing_session(
        &f.domain,
        request.clone(),
        limits,
        Some(observed.cached_route()),
    )?;
    let attempt = session.begin().await?;
    session.mark_submitted(&attempt)?;
    // Spend most of the original budget before redirecting. The two-second
    // external guard must beat a fresh five-second timeout, but leaves a full
    // second of scheduling tolerance for the original one-second remainder.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let gate = Arc::new(fault::RegistryGate::default());
    gate.armed.store(true, Ordering::Release);
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        fault::gate_read(
            f.key("pin").to_string(),
            gate.clone(),
            session.redirect(&attempt, &next),
        ),
    )
    .await?;
    assert!(!gate.armed.load(Ordering::Acquire));
    let error = result.err().unwrap();
    assert!(matches!(error.reason, CellRoutingFailure::Deadline));
    assert!(error.ambiguous_command.is_some());
    assert!(matches!(
        session.retry(&attempt).await.err().unwrap().reason,
        CellRoutingFailure::StaleAttempt
    ));

    // Dropping discovery cannot reset its attempt budget or accept its ticket.
    let mut cancelled =
        f.directory
            .routing_session(&f.domain, request.clone(), routing_limits(3), None)?;
    let gate = Arc::new(fault::RegistryGate::default());
    gate.armed.store(true, Ordering::Release);
    {
        let call = fault::gate_read(f.key("pin").to_string(), gate.clone(), cancelled.begin());
        tokio::pin!(call);
        match tokio::time::timeout(
            Duration::from_secs(2),
            futures_util::future::select(&mut call, Box::pin(gate.entered.notified())),
        )
        .await?
        {
            futures_util::future::Either::Left((result, _)) => {
                panic!("stalled discovery completed: {}", result.is_ok())
            }
            futures_util::future::Either::Right(((), _pending)) => {}
        }
    }
    assert!(matches!(
        cancelled.begin().await.err().unwrap().reason,
        CellRoutingFailure::StaleAttempt
    ));
    let mut closed = f.directory.routing_session(
        &f.domain,
        request,
        routing_limits(2),
        Some(observed.cached_route()),
    )?;
    f.domain.shutdown().await;
    assert!(matches!(
        closed.begin().await.err().unwrap().reason,
        CellRoutingFailure::Closed
    ));
    Ok(())
}

#[path = "cell_group_routing_cache.rs"]
mod routing_cache;
pub(crate) use routing_cache::{
    bounded_route_hints, cached_operation_discovery, stale_cache_refill,
};

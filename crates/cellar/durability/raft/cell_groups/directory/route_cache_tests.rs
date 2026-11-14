// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Negative observations and a purported publication hiding unrelated changes.
use super::*;
use crate::asyncrt::ExecutionDomain;
use crate::raft::catalog::{Catalog, InitialMember, NamespaceConfig};
use cellar_logic::consensus::{ClusterId, MemberId, NodeId};

struct Fixture {
    domain: ExecutionDomain,
    directory: RangeDirectory,
    routes: Vec<GroupRoute>,
}

fn identity(id: u8) -> ProposalId {
    ProposalId::new([id; 16]).unwrap()
}

fn assigned(id: u8, range: CellRange, epoch: u64) -> GroupRoute {
    GroupRoute {
        history: GroupHistory {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            group: GroupId::new([id; 16]).unwrap(),
            incarnation: IncarnationId::new([id; 16]).unwrap(),
        },
        range,
        epoch: RouteEpoch::new(epoch).unwrap(),
        schema_digest: [9; 32],
        birth_digest: [id; 32],
    }
}

impl Fixture {
    async fn new() -> anyhow::Result<Self> {
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let catalog = Catalog::new(
            Arc::new(object_store::memory::InMemory::new()),
            Default::default(),
        )?;
        let namespace = domain
            .run(catalog.open_namespace(NamespaceConfig::new(
                ClusterId::new([1; 16])?,
                NamespaceId::new([2; 16])?,
                cellar_logic::NamespaceMode::Raft,
                [9; 32],
            )))
            .await??;
        let directory = RangeDirectory::new(namespace, Default::default())?;
        let key = cell_routing_key(NamespaceId::new([2; 16])?, &CellId::new("boundary")?);
        let (left, right) = CellRange::all().split(key)?;
        let mut fixture = Self {
            domain,
            directory,
            routes: vec![],
        };
        for route in [assigned(3, left, 1), assigned(4, right, 1)] {
            let route = fixture.reserve(route).await?;
            fixture.routes.push(route);
        }
        let mutation = fixture
            .directory
            .prepare_initialization(identity(5), &fixture.routes)?;
        fixture
            .domain
            .run(fixture.directory.publish(&mutation))
            .await??;
        Ok(fixture)
    }

    async fn reserve(&self, route: GroupRoute) -> anyhow::Result<GroupRoute> {
        let request = self.directory.prepare_group_birth(CellGroupBirthSpec {
            history: route.history,
            range: route.range,
            epoch: route.epoch,
            creation: ProposalId::new(route.history.group.bytes())?,
            voters: (1..=3)
                .map(|id| InitialMember {
                    member: MemberId::new(id).unwrap(),
                    node: NodeId::new([id as u8; 16]).unwrap(),
                })
                .collect(),
            learners: vec![],
        })?;
        self.domain
            .run(self.directory.reserve_group_birth(&request))
            .await??;
        Ok(request.route().clone())
    }

    async fn split(&self, parent: &GroupRoute, id: u8) -> anyhow::Result<DirectoryMutation> {
        let key = (0..100_000)
            .map(|i| {
                cell_routing_key(
                    parent.history.namespace,
                    &CellId::new(format!("proof-{id}-{i}")).unwrap(),
                )
            })
            .find(|key| {
                parent.range.contains(key) && &CellBound::Key(key.clone()) != parent.range.lower()
            })
            .unwrap();
        let (left, right) = parent.range.split(key)?;
        let children = vec![
            self.reserve(assigned(id, left, parent.epoch.get() + 1))
                .await?,
            self.reserve(assigned(id + 1, right, parent.epoch.get() + 1))
                .await?,
        ];
        let view = self.domain.run(self.directory.observe()).await??;
        let mutation = self.directory.prepare_replacement(
            &view,
            identity(id + 2),
            std::slice::from_ref(parent),
            &children,
        )?;
        self.domain.run(self.directory.publish(&mutation)).await??;
        Ok(mutation)
    }
}

#[tokio::test]
async fn same_revision_fence_and_its_removal_cannot_resurrect_an_old_lookup() -> anyhow::Result<()>
{
    let fixture = Fixture::new().await?;
    let view = fixture.domain.run(fixture.directory.observe()).await??;
    let cache = RouteCache::new(
        fixture.directory.clone(),
        8,
        64 * 1024,
        Budget::new(1024 * 1024, 8),
        8192,
    )?;
    let old = cache.generation()?;
    fixture
        .domain
        .run(cache.remember(&view, &fixture.routes[0], &old))
        .await??;
    let mut fenced = view.clone();
    // Model a host observing a namespace fence with no routing revision change.
    // This is a cache race test, not a synthetic successful topology publication.
    fenced.head.import = Some([81; 16]);
    fenced.digest = record::digest(&record::encode(wire::HEAD, &fenced.head, wire::LIMIT)?);
    fixture
        .domain
        .run(cache.advance(&fenced, None, None))
        .await??;
    assert_eq!(cache.usage()?.entries, 0);
    assert!(!cache.can_reuse(&fixture.routes[0], view.revision(), view.digest())?);
    let fresh = cache.generation()?;
    fixture
        .domain
        .run(cache.remember(&fenced, &fixture.routes[0], &fresh))
        .await??;
    fixture
        .domain
        .run(cache.advance(&fenced, None, None))
        .await??;
    assert_eq!(cache.usage()?.entries, 1);
    assert!(cache.can_reuse(&fixture.routes[0], fenced.revision(), fenced.digest())?);
    // Stable fence observations can keep freshly checked, unblocked routes.
    // Removing/changing the fence is a new negative boundary for older lookups.
    fixture
        .domain
        .run(cache.advance(&view, None, None))
        .await??;
    for token in [&old, &fresh] {
        assert!(matches!(
            fixture
                .domain
                .run(cache.remember(&view, &fixture.routes[0], token))
                .await?,
            Err(CatalogError::Conflict("superseded routing observation"))
        ));
        assert_eq!(cache.usage()?.entries, 0);
    }
    fixture.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn repair_cannot_take_the_last_invocation_item_or_byte_headroom() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let before = fixture.domain.run(fixture.directory.observe()).await??;
    fixture.split(&fixture.routes[0], 40).await?;
    for (bytes, items) in [(1024 * 1024, 1), (REPAIR_BYTES + 8191, 2)] {
        let bank = Budget::new(bytes, items);
        let cache = RouteCache::new(fixture.directory.clone(), 8, 64 * 1024, bank.clone(), 8192)?;
        let generation = cache.generation()?;
        fixture
            .domain
            .run(cache.remember(&before, &fixture.routes[1], &generation))
            .await??;
        assert_eq!(cache.usage()?.entries, 1);
        assert_eq!(fixture.domain.run(cache.refresh()).await??, 2);
        assert_eq!(cache.usage()?.entries, 0);
        assert_eq!(bank.usage(), (0, 0));
        // The maximum normal invocation can still enter the original bank.
        let progress = bank.acquire(8192).unwrap();
        assert_eq!(bank.usage(), (8192, 1));
        drop(progress);
    }
    fixture.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn valid_claimed_parents_and_children_do_not_hide_an_unrelated_changed_range(
) -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let before = fixture.domain.run(fixture.directory.observe()).await??;
    let first = fixture.split(&fixture.routes[0], 20).await?;
    fixture.split(&fixture.routes[1], 30).await?;
    let after = fixture.domain.run(fixture.directory.observe()).await??;
    // Construct an adversarial proof whose hashes, operation, claimed parent
    // and selected children all check, but whose tree also changed another range.
    // It is never installed as a trusted catalog head or serving authority.
    let mut journal = fixture
        .domain
        .run(fixture.directory.journal(after.head.journal))
        .await??;
    journal.previous = Some(before.head.journal);
    journal.revision = before.revision() + 1;
    journal.operation = first.operation.bytes();
    journal.request = first.digest;
    let bytes = record::encode(wire::JOURNAL, &journal, wire::LIMIT)?;
    let id = record::digest(&bytes);
    fixture
        .domain
        .run(fixture.directory.immutable("journal", id, &bytes))
        .await??;
    let mut purported = after;
    purported.head.journal = id;
    purported.head.revision = journal.revision;
    purported.digest = record::digest(&record::encode(wire::HEAD, &purported.head, wire::LIMIT)?);
    assert!(matches!(
        fixture
            .domain
            .run(fixture.directory.routing_repair(
                &purported,
                before.revision(),
                before.head.journal,
            ))
            .await?,
        Err(CatalogError::Corrupt(
            "routing proof changed outside interval"
        ))
    ));
    fixture.domain.shutdown().await;
    Ok(())
}

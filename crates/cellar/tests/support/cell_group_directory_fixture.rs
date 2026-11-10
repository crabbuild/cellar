// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Owned conditional-catalog fixture shared by routing and transition cases.
use super::*;

pub(super) struct Fixture {
    pub(super) domain: ExecutionDomain,
    pub(super) store: Arc<fault::Store>,
    pub(super) directory: RangeDirectory,
    pub(super) config: NamespaceConfig,
    pub(super) limits: DirectoryLimits,
}

impl Fixture {
    pub(super) async fn new(limits: DirectoryLimits) -> Result<Self> {
        Self::with_schema(limits, route().schema_digest).await
    }

    pub(super) async fn with_schema(limits: DirectoryLimits, schema: [u8; 32]) -> Result<Self> {
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

    pub(super) async fn run<F: std::future::Future>(&self, work: F) -> F::Output {
        self.domain
            .run(work)
            .await
            .expect("fixture execution domain closed before joined shutdown")
    }

    pub(super) async fn reopen(&self) -> Result<RangeDirectory> {
        let catalog = Catalog::new(self.store.clone(), CatalogOptions::default())?;
        Ok(RangeDirectory::new(
            self.run(catalog.existing_namespace(self.config.clone()))
                .await?,
            self.limits,
        )?)
    }

    pub(super) async fn reserve(&self, routes: &mut [GroupRoute]) -> Result<(), CatalogError> {
        for route in routes {
            let request = self.directory.prepare_group_birth(birth_spec(route))?;
            self.run(self.directory.reserve_group_birth(&request))
                .await?;
            *route = request.route().clone();
        }
        Ok(())
    }
}

pub(super) fn birth_spec(route: &GroupRoute) -> CellGroupBirthSpec {
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

pub(super) fn operation(n: u64) -> ProposalId {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&n.to_be_bytes());
    ProposalId::new(bytes).unwrap()
}

pub(super) fn assigned(id: u64, range: CellRange, epoch: u64) -> GroupRoute {
    let mut r = route();
    r.history.group = GroupId::new(operation(id).bytes()).unwrap();
    r.history.incarnation = IncarnationId::new(operation(id + 10000).bytes()).unwrap();
    r.birth_digest[..8].copy_from_slice(&id.to_be_bytes());
    r.range = range;
    r.epoch = RouteEpoch::new(epoch).unwrap();
    r
}

pub(super) fn split(parent: &GroupRoute, id: u64) -> Vec<GroupRoute> {
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

//! Exclusion admission uses real root CAS, complete inventory and native hosts.
//! It grants no shutdown, terminal evacuation or removal certificate.
use super::retirement::{discover, reserve, stopped};
use super::*;
use object_store::{path::Path, ObjectStore};

struct Setup {
    f: Fixture,
    nodes: Vec<Running>,
    cells: Vec<RaftCellHandle>,
    namespace: NamespaceCatalog,
    proposal: BirthProposal,
    authorization: FleetGroupAuthorization,
    spare_root: tempfile::TempDir,
    spare_node: cellar::sdk::Node,
    spare: RaftCells,
}

fn request(id: u8, revision: u64) -> FleetRequestIdentity {
    FleetRequestIdentity {
        id: [id; 16],
        operation: [id + 1; 16],
        principal: "evacuation-test".into(),
        expected_revision: revision,
    }
}

impl Setup {
    async fn new() -> Result<Self> {
        let f = Fixture::with_managed_births(true).await?;
        let nodes = fleet(&f).await?;
        let (namespace, _) = initial_birth(&f).await?;
        let (proposal, authorization) = birth(&f).await?;
        let cells = registered(&nodes, &authorization).await?;
        activate(&cells, &authorization).await?;
        write_read(&f, &cells, 1, 7, 7).await?;
        let (spare_root, spare_node, spare) = f.peer_host(7).await?;
        f.state(NodeId::new([7; 16])?, FleetNodeState::Active, 150)
            .await?;
        discover(
            &f,
            &[&nodes[0].host, &nodes[1].host, &nodes[2].host, &spare],
        )
        .await?;
        Ok(Self {
            f,
            nodes,
            cells,
            namespace,
            proposal,
            authorization,
            spare_root,
            spare_node,
            spare,
        })
    }

    async fn close(self) -> Result<()> {
        for node in self.nodes {
            stopped(node.node).await?;
        }
        stopped(self.spare_node).await?;
        drop(self.spare_root);
        self.f.domain.shutdown().await;
        Ok(())
    }

    async fn waiting(&self) -> Result<()> {
        self.f
            .domain
            .run(cellar::asyncrt::timeout(TIMEOUT, async {
                while !self.f.store.fleet_cas_waiting.load(Ordering::Acquire) {
                    cellar::asyncrt::sleep(Duration::from_millis(5)).await;
                }
            }))
            .await??;
        Ok(())
    }

    async fn declare_extra(&self, catalog: &FleetCatalog) -> Result<NamespaceCatalog> {
        let mut config = self.f.declaration.namespace.clone();
        config.namespace = NamespaceId::new([88; 16])?;
        config.managed_fleet = None;
        let namespace = Catalog::new(
            self.f.store.clone(),
            CatalogOptions {
                prefix: "host-enrollment/".into(),
                ..Default::default()
            },
        )?
        .open_namespace(config)
        .await?;
        let root = catalog.read(TIMEOUT).await?;
        catalog
            .declare_namespace(
                &root,
                &request(205, root.revision()),
                &namespace,
                "Counter",
                TIMEOUT,
            )
            .await?;
        Ok(namespace)
    }

    async fn adopt_empty(
        &self,
        catalog: &FleetCatalog,
        namespace: &NamespaceCatalog,
    ) -> Result<()> {
        let operation = ProposalId::new([210; 16])?;
        let cut = self
            .f
            .domain
            .run(namespace.freeze_births(operation, TIMEOUT))
            .await??;
        ensure!(cut.is_empty());
        let root = catalog.read(TIMEOUT).await?;
        let mut identity = request(211, root.revision());
        identity.operation = operation.bytes();
        catalog
            .begin_namespace_import(&root, &identity, namespace, &cut, TIMEOUT)
            .await?;
        for id in 212..=215 {
            let root = catalog.read(TIMEOUT).await?;
            let mut identity = request(id, root.revision());
            identity.operation = operation.bytes();
            catalog
                .advance_namespace_import(&root, &identity, namespace, None, TIMEOUT)
                .await?;
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_evacuation_inventory_admission() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = primary(&s.f, &s.cells).await?;
    let before = s.spare.observe_node(TIMEOUT).await?;
    ensure!(
        before.node.groups == 0
            && before.current_enrollment.as_ref().unwrap().state == FleetNodeState::Active
    );
    let target = s
        .spare
        .register_managed_history(s.authorization.clone(), TIMEOUT)
        .await
        .context("prepare dormant history before exclusion")?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let identity = request(200, root.revision());
    let admitted =
        s.f.catalog
            .begin_evacuation(
                &root,
                &identity,
                NodeId::new([7; 16])?,
                std::slice::from_ref(&s.namespace),
                TIMEOUT,
            )
            .await?;
    ensure!(admitted.applied_revision == root.revision() + 1);
    let view =
        s.f.catalog
            .evacuation_admission(
                &admitted.snapshot,
                ProposalId::new(identity.operation)?,
                TIMEOUT,
            )
            .await?;
    ensure!(view.operation().bytes() == identity.operation && view.node() == NodeId::new([7; 16])?);
    ensure!(
        view.disk() == before.node.identity.disk
            && view.storage_instance() == before.node.storage_instance
    );
    ensure!(
        view.inventory_revision() == root.revision()
            && view.namespaces() == 1
            && view.histories() == 1
    );
    ensure!(
        view.applied_revision() == admitted.applied_revision
            && view.observed_revision() == admitted.snapshot.revision()
    );
    let current =
        s.f.remote(&s.nodes[p].host, view.node(), admitted.snapshot.revision())
            .await?;
    let enrollment = current
        .view
        .current_enrollment
        .context("missing exclusion enrollment")?;
    ensure!(
        enrollment.state == FleetNodeState::Draining && enrollment.epoch == view.enrollment_epoch()
    );
    ensure!(enrollment.epoch == before.current_enrollment.unwrap().epoch + 1);
    ensure!(matches!(
        s.spare
            .register_managed_history(s.authorization.clone(), TIMEOUT)
            .await,
        Err(CommandFailure::Unavailable)
    ));
    let (plan, permit) = reserve(
        &s.f,
        &s.cells[p],
        &s.proposal.scope,
        1,
        160,
        MemberId::new(((p + 1) % 3) as u64 + 1)?,
        JoiningMember {
            member: MemberId::new(4)?,
            node: view.node(),
            disk: view.disk(),
        },
    )
    .await?;
    ensure!(matches!(
        s.cells[p].begin_managed_repair(permit, plan, TIMEOUT).await,
        Err(CommandFailure::Unavailable)
    ));
    ensure!(target.inspect().await?.occupancy == ReplicaOccupancy::Standby);
    let proof = s.cells[p]
        .observe_fleet_membership(s.authorization.clone(), TIMEOUT)
        .await?;
    ensure!(proof.status().consumed_sequence == 0 && proof.configuration().learners().is_empty());
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(s
        .f
        .catalog
        .get(
            &root,
            FleetTable::Admissions,
            &s.f.catalog.group_binding(&s.proposal.scope)?.key()?,
            TIMEOUT
        )
        .await?
        .is_some());
    write_read(&s.f, &s.cells, 2, 5, 12).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_evacuation_inventory_gate() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let extra = s.declare_extra(&s.f.catalog).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let identity = request(200, root.revision());
    let both = [s.namespace.clone(), extra.clone()];
    for namespaces in [std::slice::from_ref(&s.namespace), both.as_slice()] {
        ensure!(matches!(
            s.f.catalog
                .begin_evacuation(&root, &identity, NodeId::new([7; 16])?, namespaces, TIMEOUT)
                .await,
            Err(FleetCatalogError::InventoryIncomplete)
        ));
        ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    }
    s.adopt_empty(&s.f.catalog, &extra).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let key = s.f.catalog.group_binding(&s.proposal.scope)?.key()?;
    let history =
        s.f.catalog
            .get(&root, FleetTable::Histories, &key, TIMEOUT)
            .await?
            .context("missing fixture history")?;
    let removed =
        s.f.catalog
            .commit(
                &root,
                &FleetMutation {
                    id: [190; 16],
                    operation: [191; 16],
                    principal: "inject-incomplete-index".into(),
                    expected_revision: root.revision(),
                    changes: vec![FleetChange {
                        table: FleetTable::Histories,
                        key: key.clone(),
                        value: None,
                    }],
                },
                TIMEOUT,
            )
            .await?;
    ensure!(s.spare.observe_node(TIMEOUT).await?.node.groups == 0);
    let identity = request(202, removed.snapshot.revision());
    ensure!(matches!(
        s.f.catalog
            .begin_evacuation(
                &removed.snapshot,
                &identity,
                NodeId::new([7; 16])?,
                &both,
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::InventoryIncomplete)
    ));
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == removed.snapshot.revision());
    ensure!(s
        .f
        .catalog
        .get(
            &removed.snapshot,
            FleetTable::Operations,
            &hex::encode(identity.operation),
            TIMEOUT
        )
        .await?
        .is_none());
    let restored =
        s.f.catalog
            .commit(
                &removed.snapshot,
                &FleetMutation {
                    id: [192; 16],
                    operation: [193; 16],
                    principal: "restore-injected-index".into(),
                    expected_revision: removed.snapshot.revision(),
                    changes: vec![FleetChange {
                        table: FleetTable::Histories,
                        key,
                        value: Some(history),
                    }],
                },
                TIMEOUT,
            )
            .await?;
    let identity = request(204, restored.snapshot.revision());
    let admitted =
        s.f.catalog
            .begin_evacuation(
                &restored.snapshot,
                &identity,
                NodeId::new([7; 16])?,
                &both,
                TIMEOUT,
            )
            .await?;
    let view =
        s.f.catalog
            .evacuation_admission(
                &admitted.snapshot,
                ProposalId::new(identity.operation)?,
                TIMEOUT,
            )
            .await?;
    ensure!(view.namespaces() == 2 && view.histories() == 1);
    write_read(&s.f, &s.cells, 2, 5, 12).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_evacuation_stale_inventory_cas() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let identity = request(200, root.revision());
    let catalog = s.f.catalog.clone();
    let namespace = s.namespace.clone();
    let snapshot = root.clone();
    let original = identity.clone();
    s.f.store.fleet_cas_hang.store(true, Ordering::Release);
    s.f.store.hold_next_fleet_cas.store(true, Ordering::Release);
    let pending = s.f.domain.spawn(async move {
        catalog
            .begin_evacuation(
                &snapshot,
                &original,
                NodeId::new([7; 16]).unwrap(),
                &[namespace],
                TIMEOUT,
            )
            .await
    })?;
    s.waiting().await?;
    write_read(&s.f, &s.cells, 2, 5, 12).await?;
    let reopened = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let extra = s.declare_extra(&reopened).await?;
    s.f.store.fleet_cas_hang.store(false, Ordering::Release);
    s.f.store.fleet_cas_resume.notify_waiters();
    ensure!(matches!(pending.await?, Err(FleetCatalogError::Conflict)));
    let now = reopened.read(TIMEOUT).await?;
    ensure!(now.revision() > root.revision());
    ensure!(
        reopened
            .authorize_host(&now, NodeId::new([7; 16])?, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::Active
    );
    ensure!(reopened
        .get(
            &now,
            FleetTable::Operations,
            &hex::encode(identity.operation),
            TIMEOUT
        )
        .await?
        .is_none());
    ensure!(matches!(
        reopened
            .begin_evacuation(&now, &identity, NodeId::new([7; 16])?, &[], TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    let mut rebound = request(203, now.revision());
    rebound.operation = identity.operation;
    match reopened
        .begin_evacuation(&now, &rebound, NodeId::new([7; 16])?, &[], TIMEOUT)
        .await
    {
        Err(FleetCatalogError::PayloadConflict) => (),
        Err(error) => {
            anyhow::bail!("unadmitted operation identity was not permanently bound: {error}")
        }
        Ok(_) => anyhow::bail!("unadmitted operation identity was rebound"),
    }
    s.adopt_empty(&reopened, &extra).await?;
    let now = reopened.read(TIMEOUT).await?;
    let next = request(202, now.revision());
    let admitted = reopened
        .begin_evacuation(
            &now,
            &next,
            NodeId::new([7; 16])?,
            &[s.namespace.clone(), extra],
            TIMEOUT,
        )
        .await?;
    ensure!(admitted.applied_revision == now.revision() + 1);
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_evacuation_lost_reply_and_retained_slot() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let identity = request(200, root.revision());
    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(s
        .f
        .catalog
        .begin_evacuation(
            &root,
            &identity,
            NodeId::new([7; 16])?,
            std::slice::from_ref(&s.namespace),
            TIMEOUT
        )
        .await
        .is_err());
    s.f.store.fail_fleet_read.store(false, Ordering::Release);
    let reopened = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let _extra = s.declare_extra(&reopened).await?;
    let now = reopened.read(TIMEOUT).await?;
    let reply = reopened
        .begin_evacuation(&now, &identity, NodeId::new([7; 16])?, &[], TIMEOUT)
        .await?;
    ensure!(
        reply.applied_revision == root.revision() + 1 && reply.applied_revision < now.revision()
    );
    ensure!(reply.snapshot.revision() == now.revision());
    let view = reopened
        .evacuation_admission(&now, ProposalId::new(identity.operation)?, TIMEOUT)
        .await?;
    ensure!(view.inventory_revision() == root.revision() && view.histories() == 1);
    ensure!(matches!(
        reopened
            .begin_evacuation(&now, &identity, NodeId::new([5; 16])?, &[], TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    let mut changed = identity.clone();
    changed.principal = "other-principal".into();
    ensure!(matches!(
        reopened
            .begin_evacuation(&now, &changed, NodeId::new([7; 16])?, &[], TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    let next = request(202, now.revision());
    ensure!(
        matches!(reopened.begin_evacuation(&now,&next,NodeId::new([5;16])?,&[],TIMEOUT).await,Err(FleetCatalogError::DisruptionInProgress {operation}) if operation==identity.operation)
    );
    let key = Path::from(format!(
        "tenant/hosts/raft/{}/fleet/operation-requests/{}",
        hex::encode(s.proposal.scope.cluster.bytes()),
        hex::encode(identity.operation)
    ));
    let canonical = s.f.store.inner.get(&key).await?.bytes().await?;
    s.f.store.inner.delete(&key).await?;
    ensure!(matches!(
        reopened
            .evacuation_admission(&now, ProposalId::new(identity.operation)?, TIMEOUT)
            .await,
        Err(FleetCatalogError::Corrupt(
            "missing canonical evacuation request"
        ))
    ));
    ensure!(matches!(
        reopened
            .begin_evacuation(&now, &next, NodeId::new([5; 16])?, &[], TIMEOUT)
            .await,
        Err(FleetCatalogError::Corrupt(
            "missing canonical evacuation request"
        ))
    ));
    s.f.store.inner.put(&key, canonical.into()).await?;
    ensure!(matches!(
        reopened
            .commit(
                &now,
                &FleetMutation {
                    id: [203; 16],
                    operation: [204; 16],
                    principal: "attempt-premature-slot-release".into(),
                    expected_revision: now.revision(),
                    changes: vec![FleetChange {
                        table: FleetTable::Policy,
                        key: "active-disruption".into(),
                        value: None
                    }],
                },
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Invalid(_))
    ));
    ensure!(reopened.read(TIMEOUT).await?.revision() == now.revision());
    ensure!(matches!(
        reopened
            .begin_evacuation(
                &now,
                &next,
                NodeId::new([5; 16])?,
                &vec![s.namespace.clone(); 65],
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Limit)
    ));
    write_read(&s.f, &s.cells, 2, 5, 12).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_evacuation_one_disruption_slot() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let other = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let a = s.f.catalog.read(TIMEOUT).await?;
    let b = other.read(TIMEOUT).await?;
    let ra = request(200, a.revision());
    let rb = request(202, b.revision());
    let (a, b) = tokio::join!(
        s.f.catalog.begin_evacuation(
            &a,
            &ra,
            NodeId::new([7; 16])?,
            std::slice::from_ref(&s.namespace),
            TIMEOUT
        ),
        other.begin_evacuation(
            &b,
            &rb,
            NodeId::new([4; 16])?,
            std::slice::from_ref(&s.namespace),
            TIMEOUT
        ),
    );
    ensure!(a.is_ok() != b.is_ok());
    ensure!(matches!(
        a.as_ref().err().or(b.as_ref().err()),
        Some(FleetCatalogError::Conflict)
    ));
    let root = other.read(TIMEOUT).await?;
    let (winner, loser, op) = if a.is_ok() {
        (7, 4, ra.operation)
    } else {
        (4, 7, rb.operation)
    };
    ensure!(
        other
            .authorize_host(&root, NodeId::new([winner; 16])?, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::Draining
    );
    ensure!(
        other
            .authorize_host(&root, NodeId::new([loser; 16])?, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::Active
    );
    let next = request(204, root.revision());
    ensure!(
        matches!(other.begin_evacuation(&root,&next,NodeId::new([loser;16])?,std::slice::from_ref(&s.namespace),TIMEOUT).await,Err(FleetCatalogError::DisruptionInProgress {operation}) if operation==op)
    );
    write_read(&s.f, &s.cells, 2, 5, 12).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_evacuation_cancellation_and_deadline() -> Result<()> {
    Box::pin(cancelled(false)).await?;
    Box::pin(cancelled(true)).await
}

async fn cancelled(expire: bool) -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let identity = request(200, root.revision());
    s.f.store.fleet_cas_hang.store(true, Ordering::Release);
    s.f.store.hold_next_fleet_cas.store(true, Ordering::Release);
    let timeout = if expire {
        Duration::from_millis(250)
    } else {
        TIMEOUT
    };
    let mut pending = Box::pin(s.f.catalog.begin_evacuation(
        &root,
        &identity,
        NodeId::new([7; 16])?,
        std::slice::from_ref(&s.namespace),
        timeout,
    ));
    s.f.domain.run(async {
        cellar::asyncrt::select! {
            result=&mut pending=>anyhow::bail!("admission finished before the held CAS: {}",result.is_ok()),
            result=s.waiting()=>result,
        }
    }).await??;
    if expire {
        ensure!(matches!(
            pending.as_mut().await,
            Err(FleetCatalogError::Unconfirmed)
        ));
    }
    drop(pending);
    ensure!(!s.f.store.fleet_cas_waiting.load(Ordering::Acquire));
    s.f.store.fleet_cas_hang.store(false, Ordering::Release);
    s.f.store.fleet_cas_resume.notify_waiters();
    let now = s.f.catalog.read(TIMEOUT).await?;
    ensure!(now.revision() == root.revision());
    ensure!(
        s.f.catalog
            .authorize_host(&now, NodeId::new([7; 16])?, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::Active
    );
    let admitted =
        s.f.catalog
            .begin_evacuation(
                &now,
                &identity,
                NodeId::new([7; 16])?,
                std::slice::from_ref(&s.namespace),
                TIMEOUT,
            )
            .await?;
    ensure!(admitted.applied_revision == root.revision() + 1);
    s.close().await
}

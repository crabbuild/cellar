//! Actual quorum guards plus conditional namespace ownership and fleet progress.
//! Declared host labels are fixture inputs, not physical-isolation qualification.
use super::*;
use anyhow::Context as _;
use cellar::application::consensus::verification::{
    BirthDisposition, CatalogError, FleetImportPhase, FleetNamespaceImport, FleetSnapshot,
    NamespaceCatalog,
};
use object_store::path::Path;

const OP: [u8; 16] = [201; 16];

#[path = "raft_fleet_inventory.rs"]
mod fleet_inventory;

struct ImportFixture {
    f: Fixture,
    namespace: NamespaceCatalog,
    store: Arc<catalog_fault::Store>,
    next: u64,
}

impl ImportFixture {
    async fn new(enrolled: bool) -> Result<Self> {
        Self::with_pin(enrolled, false).await
    }

    async fn with_pin(enrolled: bool, wrong_pin: bool) -> Result<Self> {
        let store = Arc::new(catalog_fault::Store::default());
        let mut host = RaftHostOptions {
            max_groups: 8,
            ..Default::default()
        };
        host.node_profile = Some(RaftNodeProfile {
            pool: "native".into(),
            region: "local".into(),
            zone: "shared-host".into(),
            group_capacity: 8,
            byte_capacity: host.images.max_bytes * 8,
            bytes_per_group: host.images.max_bytes,
            leader_capacity: 8,
            draining: false,
        });
        let f = Fixture::configured(true, enrolled, host, store.clone()).await?;
        let namespace = Catalog::new(
            store.clone(),
            CatalogOptions {
                prefix: "partition/test".into(),
                ..Default::default()
            },
        )?
        .existing_namespace(f.registration.namespace.clone())
        .await?;
        let mut this = Self {
            f,
            namespace,
            store,
            next: 1,
        };
        let digest = RaftManagedHostOptions::capability_digest(&[RaftNamespaceRegistration {
            namespace: this.f.registration.namespace.clone(),
            class: "Counter".into(),
        }])?;
        for index in 0..3 {
            let view = this.f.cluster.services[index].observe_node(TIMEOUT).await?;
            let snapshot = this.f.catalog.read(TIMEOUT).await?;
            let request = this.request(snapshot.revision());
            this.f
                .catalog
                .register_node(
                    &snapshot,
                    &request,
                    &FleetNodeSpec {
                        node: view.node.identity.node,
                        disk: view.node.identity.disk,
                        physical_host: IncarnationId::new([index as u8 + 90; 16])?,
                        label: format!("raft-cluster-{}", index + 1),
                        server_name: "localhost".into(),
                        capability_digest: digest,
                        storage_instance: Some(if wrong_pin && index == 2 {
                            EngineInstanceId::new([223; 16])?
                        } else {
                            view.node.storage_instance
                        }),
                    },
                    TIMEOUT,
                )
                .await?;
        }
        this.store.listing_calls.store(0, Ordering::Release);
        this.store.reject_listing.store(true, Ordering::Release);
        Ok(this)
    }

    fn request(&mut self, revision: u64) -> FleetRequestIdentity {
        let mut id = [117; 16];
        id[..8].copy_from_slice(&self.next.to_be_bytes());
        self.next += 1;
        FleetRequestIdentity {
            id,
            operation: OP,
            principal: "namespace-import-test".into(),
            expected_revision: revision,
        }
    }

    async fn begin(&mut self) -> Result<FleetRequestIdentity> {
        let cut = self
            .f
            .domain
            .run(self.namespace.freeze_births(ProposalId::new(OP)?, TIMEOUT))
            .await??;
        let snapshot = self.f.catalog.read(TIMEOUT).await?;
        let request = self.request(snapshot.revision());
        self.f
            .catalog
            .begin_namespace_import(&snapshot, &request, &self.namespace, &cut, TIMEOUT)
            .await?;
        Ok(request)
    }

    async fn state(&self) -> Result<(FleetSnapshot, FleetNamespaceImport)> {
        let root = self.f.catalog.read(TIMEOUT).await?;
        let status = self
            .f
            .catalog
            .namespace_import(&root, &self.namespace, TIMEOUT)
            .await?;
        Ok((root, status))
    }

    async fn advance(
        &mut self,
        proof: Option<&FleetMembershipObservation>,
    ) -> Result<FleetRequestIdentity> {
        let root = self.f.catalog.read(TIMEOUT).await?;
        let request = self.request(root.revision());
        self.f
            .catalog
            .advance_namespace_import(&root, &request, &self.namespace, proof, TIMEOUT)
            .await?;
        Ok(request)
    }

    async fn counter_proof(&self) -> Result<FleetMembershipObservation> {
        let root = self.f.catalog.read(TIMEOUT).await?;
        let authorization = self
            .f
            .catalog
            .authorize_group(&root, &self.f.registration.birth.scope, TIMEOUT)
            .await?;
        let cell = &self.f.handles[self.f.primary];
        if cell.fleet_membership_status().await?.is_none() {
            let expected = cell.status().await?.membership_revision;
            cell.enroll_fleet(authorization.clone(), expected, TIMEOUT)
                .await?;
        }
        Ok(cell
            .observe_fleet_membership(authorization, TIMEOUT)
            .await?)
    }

    async fn complete(&mut self) -> Result<()> {
        let intents = self.state().await?.1.intents();
        for _ in 0..intents * 2 + 5 {
            let (root, status) = self.state().await?;
            if status.phase() == FleetImportPhase::Complete {
                return Ok(());
            }
            let mut proof = None;
            if status.phase() == FleetImportPhase::Coverage {
                if let Some(entry) = self
                    .f
                    .catalog
                    .next_namespace_import_birth(&root, &self.namespace, TIMEOUT)
                    .await?
                {
                    if matches!(
                        self.namespace
                            .birth_disposition(&entry.proposal().scope.cell)
                            .await?,
                        Some(BirthDisposition::Live(_))
                    ) {
                        ensure!(
                            entry.proposal().scope == self.f.registration.birth.scope,
                            "fixture has only one actual group"
                        );
                        proof = Some(self.counter_proof().await?);
                    }
                }
            }
            self.advance(proof.as_ref()).await?;
        }
        anyhow::bail!("import failed to reach its bounded terminal step")
    }

    async fn stopped(&mut self, value: bool) -> Result<()> {
        let (root, _) = self.state().await?;
        let request = self.request(root.revision());
        self.f
            .catalog
            .set_namespace_import_stopped(&root, &request, &self.namespace, value, TIMEOUT)
            .await?;
        Ok(())
    }

    fn another(&self, id: u8) -> Result<BirthProposal> {
        let mut proposal = self.f.registration.birth.clone();
        proposal.scope.cell = format!("Counter:import-{id}");
        proposal.scope.group = GroupId::new([id; 16])?;
        Ok(proposal)
    }

    async fn close(self) -> Result<()> {
        ensure!(self.store.listing_calls.load(Ordering::Acquire) == 0);
        self.f.close().await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn namespace_import_live_cut_and_handoff() -> Result<()> {
    let mut f = ImportFixture::new(false).await?;
    let begin = f.begin().await?;
    ensure!(f.state().await?.1.phase() == FleetImportPhase::Preparing);
    f.f.write(2, 2, true).await?;
    f.advance(None).await?;
    f.advance(None).await?;
    let (root, status) = f.state().await?;
    ensure!(
        status.phase() == FleetImportPhase::Coverage
            && status.inventoried() == 1
            && status.covered() == 0
    );
    let attempt = f.request(root.revision());
    ensure!(matches!(
        f.f.catalog
            .advance_namespace_import(&root, &attempt, &f.namespace, None, TIMEOUT)
            .await,
        Err(FleetCatalogError::Invalid(_))
    ));
    ensure!(f.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    let proof = f.counter_proof().await?;
    f.advance(Some(&proof)).await?;
    ensure!(f.state().await?.1.phase() == FleetImportPhase::Handoff);
    ensure!(f.stopped(true).await.is_err());
    ensure!(matches!(
        f.namespace.resolve_birth(f.another(56)?).await,
        Err(CatalogError::BirthsFrozen)
    ));
    f.advance(None).await?;
    let (root, status) = f.state().await?;
    ensure!(
        status.phase() == FleetImportPhase::Complete && status.covered() == 1 && !status.stopped()
    );
    ensure!(status.cut_digest()? != [0; 32]);
    ensure!(matches!(
        f.namespace.resolve_birth(f.another(56)?).await,
        Err(CatalogError::ManagedAdmissionRequired)
    ));
    let replay =
        f.f.catalog
            .retry_namespace_import_request(&begin, &f.namespace, TIMEOUT)
            .await?;
    ensure!(replay.applied_revision < root.revision());
    f.f.write(3, 4, true).await?;
    ensure!(f.f.strong().await? == 11);
    f.close().await
}

struct BirthRelease(Arc<catalog_fault::Store>);

impl Drop for BirthRelease {
    fn drop(&mut self) {
        self.0.birth_hang.store(false, Ordering::Release);
        self.0.birth_resume.notify_waiters();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn namespace_import_pending_stop_resume() -> Result<()> {
    let mut f = ImportFixture::new(false).await?;
    f.store.birth_hang.store(true, Ordering::Release);
    f.store.hold_next_birth.store(true, Ordering::Release);
    let release = BirthRelease(f.store.clone());
    let pending = f.another(57)?;
    let namespace = f.namespace.clone();
    let proposed = pending.clone();
    let writer =
        f.f.domain
            .spawn(async move { namespace.resolve_birth(proposed).await })?;
    f.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            while !f.store.birth_waiting.load(Ordering::Acquire) {
                cellar::asyncrt::sleep(Duration::from_millis(2)).await;
            }
        }))
        .await??;
    f.begin().await?;
    f.advance(None).await?;
    f.stopped(true).await?;
    let (root, status) = f.state().await?;
    ensure!(status.stopped() && status.intents() == 2);
    let request = f.request(root.revision());
    ensure!(matches!(
        f.f.catalog
            .advance_namespace_import(&root, &request, &f.namespace, None, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(f
        .namespace
        .birth_disposition(&pending.scope.cell)
        .await?
        .is_none());
    f.stopped(false).await?;
    f.complete().await?;
    drop(release);
    ensure!(matches!(writer.await?, Err(CatalogError::BirthCancelled)));
    let (root, status) = f.state().await?;
    ensure!(status.inventoried() == 2 && status.covered() == 2);
    let history = f.f.catalog.history(&root, &pending.scope, TIMEOUT).await?;
    ensure!(matches!(
        history.state(),
        cellar::application::consensus::verification::FleetHistoryState::Cancelled { .. }
    ));
    ensure!(matches!(
        f.f.catalog
            .authorize_group(&root, &pending.scope, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(f.f.strong().await? == 5);
    f.close().await
}

struct InventoryRelease(Arc<catalog_fault::Store>);

impl Drop for InventoryRelease {
    fn drop(&mut self) {
        self.0
            .birth_inventory_cas_hang
            .store(false, Ordering::Release);
        self.0.birth_inventory_cas_resume.notify_waiters();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn namespace_import_claim_stop_race() -> Result<()> {
    let mut f = ImportFixture::new(false).await?;
    f.begin().await?;
    let (root, _) = f.state().await?;
    let request = f.request(root.revision());
    let control = Fixture::open_catalog(&f.f.domain, f.store.clone())?;
    f.store
        .birth_inventory_cas_hang
        .store(true, Ordering::Release);
    f.store
        .hold_next_birth_inventory_cas
        .store(true, Ordering::Release);
    let release = InventoryRelease(f.store.clone());
    let namespace = f.namespace.clone();
    let catalog = f.f.catalog.clone();
    let joining = f.f.domain.spawn(async move {
        catalog
            .advance_namespace_import(&root, &request, &namespace, None, TIMEOUT)
            .await
    })?;
    f.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            while !f.store.birth_inventory_cas_waiting.load(Ordering::Acquire) {
                cellar::asyncrt::sleep(Duration::from_millis(2)).await;
            }
        }))
        .await??;
    let current = control.read(TIMEOUT).await?;
    let stop = f.request(current.revision());
    control
        .set_namespace_import_stopped(&current, &stop, &f.namespace, true, TIMEOUT)
        .await?;
    drop(release);
    ensure!(matches!(joining.await?, Err(FleetCatalogError::Conflict)));
    ensure!(f.state().await?.1.stopped());
    ensure!(matches!(
        f.namespace.resolve_birth(f.another(58)?).await,
        Err(CatalogError::BirthsFrozen)
    ));
    f.stopped(false).await?;
    f.complete().await?;
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn namespace_import_lost_reply_reopen() -> Result<()> {
    let mut f = ImportFixture::new(true).await?;
    let cut =
        f.f.domain
            .run(f.namespace.freeze_births(ProposalId::new(OP)?, TIMEOUT))
            .await??;
    let root = f.f.catalog.read(TIMEOUT).await?;
    let begin = f.request(root.revision());
    f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(f
        .f
        .catalog
        .begin_namespace_import(&root, &begin, &f.namespace, &cut, TIMEOUT)
        .await
        .is_err());
    f.f.catalog = Fixture::open_catalog(&f.f.domain, f.store.clone())?;
    let admitted =
        f.f.catalog
            .retry_namespace_import_request(&begin, &f.namespace, TIMEOUT)
            .await?;
    ensure!(admitted.applied_revision > root.revision());

    let (root, _) = f.state().await?;
    let claim = f.request(root.revision());
    f.store
        .fail_birth_inventory_confirmation
        .store(true, Ordering::Release);
    ensure!(f
        .f
        .catalog
        .advance_namespace_import(&root, &claim, &f.namespace, None, TIMEOUT)
        .await
        .is_err());
    ensure!(f.state().await?.1.phase() == FleetImportPhase::Preparing);
    ensure!(matches!(
        f.f.catalog
            .retry_namespace_import_request(&claim, &f.namespace, TIMEOUT)
            .await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    f.f.catalog
        .advance_namespace_import(&root, &claim, &f.namespace, None, TIMEOUT)
        .await?;
    f.store.fail_fleet_after.store(true, Ordering::Release);
    let inventory = f.advance(None).await?;
    ensure!(f.state().await?.1.phase() == FleetImportPhase::Coverage);
    let proof = f.counter_proof().await?;
    let (root, _) = f.state().await?;
    let coverage = f.request(root.revision());
    f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(f
        .f
        .catalog
        .advance_namespace_import(&root, &coverage, &f.namespace, Some(&proof), TIMEOUT)
        .await
        .is_err());
    drop(proof);
    f.f.catalog = Fixture::open_catalog(&f.f.domain, f.store.clone())?;
    let guarded =
        f.f.catalog
            .retry_namespace_import_request(&coverage, &f.namespace, TIMEOUT)
            .await?;
    ensure!(guarded.applied_revision > admitted.applied_revision);
    ensure!(f.state().await?.1.phase() == FleetImportPhase::Handoff);

    let (root, _) = f.state().await?;
    let finish = f.request(root.revision());
    f.store
        .fail_birth_inventory_confirmation
        .store(true, Ordering::Release);
    ensure!(f
        .f
        .catalog
        .advance_namespace_import(&root, &finish, &f.namespace, None, TIMEOUT)
        .await
        .is_err());
    ensure!(f.state().await?.1.phase() == FleetImportPhase::Handoff);
    // Namespace ownership may already have changed, but incomplete rooted
    // handoff cannot reserve a new history or fall back to ordinary creation.
    let candidate = f.another(59)?;
    let request = f.request(root.revision());
    ensure!(matches!(
        f.f.catalog
            .reserve_birth(&root, &request, &f.namespace, candidate.clone(), TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(matches!(
        f.namespace.resolve_birth(candidate).await,
        Err(CatalogError::ManagedAdmissionRequired)
    ));
    f.namespace = Catalog::new(
        f.store.clone(),
        CatalogOptions {
            prefix: "partition/test".into(),
            ..Default::default()
        },
    )?
    .existing_namespace(f.f.registration.namespace.clone())
    .await?;
    f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(f
        .f
        .catalog
        .advance_namespace_import(&root, &finish, &f.namespace, None, TIMEOUT)
        .await
        .is_err());
    let complete =
        f.f.catalog
            .retry_namespace_import_request(&finish, &f.namespace, TIMEOUT)
            .await?;
    ensure!(f.state().await?.1.phase() == FleetImportPhase::Complete);
    ensure!(
        f.f.catalog
            .retry_namespace_import_request(&inventory, &f.namespace, TIMEOUT)
            .await?
            .applied_revision
            < complete.applied_revision
    );
    let mut changed = finish.clone();
    changed.principal = "different-principal".into();
    ensure!(matches!(
        f.f.catalog
            .retry_namespace_import_request(&changed, &f.namespace, TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    f.close().await
}

fn other_catalog(f: &ImportFixture) -> Result<FleetCatalog> {
    let bucket = Bucket::from_clients(
        f.store.clone(),
        f.store.clone(),
        Arc::new(NoListing),
        StorageBackend::S3,
        "other-catalog".into(),
        "".into(),
    )?;
    Ok(FleetCatalog::new(
        &f.f.domain,
        bucket,
        f.f.registration.namespace.cluster,
        IncarnationId::new([80; 16])?,
        "other-coordination/",
    )?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn namespace_import_catalog_scope() -> Result<()> {
    let mut f = ImportFixture::new(false).await?;
    let other = other_catalog(&f)?;
    other.initialize(TIMEOUT).await?;
    let actual = f
        .namespace
        .birth(&f.f.registration.birth.scope.cell)
        .await?
        .context("actual birth")?;
    let root = other.read(TIMEOUT).await?;
    let declared = f.request(root.revision());
    other
        .declare_namespace(&root, &declared, &f.namespace, "Counter", TIMEOUT)
        .await?;
    let root = other.read(TIMEOUT).await?;
    let early = f.request(root.revision());
    other
        .enroll_history(&root, &early, &f.namespace, &actual, TIMEOUT)
        .await?;
    let root = other.read(TIMEOUT).await?;
    let stale = other
        .authorize_group(&root, actual.scope(), TIMEOUT)
        .await?;
    let left = f.f.catalog.group_binding(actual.scope())?;
    let right = other.group_binding(actual.scope())?;
    ensure!(
        left.fleet() == right.fleet()
            && left.catalog() != right.catalog()
            && left.key() != right.key()
    );
    let shared_cut =
        f.f.domain
            .run(f.namespace.freeze_births(ProposalId::new(OP)?, TIMEOUT))
            .await??;
    f.begin().await?;
    f.complete().await?;
    let cell = f.f.handles[f.f.primary].clone();
    ensure!(matches!(
        cell.enroll_fleet(
            stale.clone(),
            cell.status().await?.membership_revision,
            TIMEOUT
        )
        .await,
        Err(CommandFailure::CommandConflict)
    ));
    ensure!(matches!(
        cell.observe_fleet_membership(stale, TIMEOUT).await,
        Err(CommandFailure::CommandConflict)
    ));
    // A second root can retain a harmless Preparing intent, but its different
    // catalog scope cannot claim or replace the adopted namespace owner.
    let root = other.read(TIMEOUT).await?;
    let request = f.request(root.revision());
    other
        .begin_namespace_import(&root, &request, &f.namespace, &shared_cut, TIMEOUT)
        .await?;
    let root = other.read(TIMEOUT).await?;
    let request = f.request(root.revision());
    ensure!(matches!(
        other
            .advance_namespace_import(&root, &request, &f.namespace, None, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(
        other
            .namespace_import(&root, &f.namespace, TIMEOUT)
            .await?
            .phase()
            == FleetImportPhase::Preparing
    );
    let plan = f.f.plan(210).await?;
    let permit = FleetMembershipPermit::new(
        right.clone(),
        1,
        ProposalId::new([211; 16])?,
        ProposalId::new(OP)?,
        &plan,
    )?;
    let root = other.read(TIMEOUT).await?;
    let request = f.request(root.revision());
    let mutation = FleetMutation {
        id: request.id,
        operation: request.operation,
        principal: request.principal,
        expected_revision: request.expected_revision,
        changes: vec![FleetChange {
            table: FleetTable::Admissions,
            key: right.key()?,
            value: Some(permit.encode()?),
        }],
    };
    let saved = other.commit(&root, &mutation, TIMEOUT).await?;
    let unauthorized = other
        .authorize_membership(&saved.snapshot, actual.scope(), TIMEOUT)
        .await?;
    ensure!(matches!(
        cell.fence_fleet_permit(unauthorized, TIMEOUT).await,
        Err(CommandFailure::CommandConflict)
    ));
    ensure!(
        cell.fleet_membership_status()
            .await?
            .context("retained guard")?
            .consumed_sequence
            == 0
    );
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn namespace_import_missing_coverage_page() -> Result<()> {
    let mut f = ImportFixture::new(true).await?;
    f.begin().await?;
    f.advance(None).await?;
    f.advance(None).await?;
    let proof = f.counter_proof().await?;
    f.advance(Some(&proof)).await?;
    let (root, status) = f.state().await?;
    ensure!(status.phase() == FleetImportPhase::Handoff);
    let bytes =
        f.f.catalog
            .get(
                &root,
                FleetTable::Policy,
                &format!(
                    "namespace/{}",
                    hex::encode(f.f.registration.namespace.namespace.bytes())
                ),
                TIMEOUT,
            )
            .await?
            .context("import record")?;
    let value: serde_json::Value = serde_json::from_slice(&bytes[13..bytes.len() - 32])?;
    let digest: Vec<u8> = serde_json::from_value(value["coverage"].clone())?;
    let page = Path::from(format!(
        "coordination/raft/{}/fleet/objects/{}",
        hex::encode(f.f.registration.namespace.cluster.bytes()),
        hex::encode(digest)
    ));
    let saved = f.store.inner.get(&page).await?.bytes().await?;
    f.store.inner.delete(&page).await?;
    let request = f.request(root.revision());
    ensure!(matches!(
        f.f.catalog
            .advance_namespace_import(&root, &request, &f.namespace, None, TIMEOUT)
            .await,
        Err(FleetCatalogError::Corrupt(_))
    ));
    ensure!(f.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    ensure!(matches!(
        f.namespace.resolve_birth(f.another(60)?).await,
        Err(CatalogError::BirthsFrozen)
    ));
    f.store.inner.put(&page, saved.clone().into()).await?;
    f.advance(None).await?;
    ensure!(f.state().await?.1.phase() == FleetImportPhase::Complete);
    let root = f.f.catalog.read(TIMEOUT).await?;
    ensure!(
        f.f.catalog
            .check_inventory(&root, std::slice::from_ref(&f.namespace), TIMEOUT)
            .await?
            .histories()
            == 1
    );
    f.store.inner.delete(&page).await?;
    ensure!(f
        .f
        .catalog
        .check_inventory(&root, std::slice::from_ref(&f.namespace), TIMEOUT)
        .await
        .is_err());
    f.store.inner.put(&page, saved.clone().into()).await?;
    ensure!(
        f.f.catalog
            .check_inventory(&root, std::slice::from_ref(&f.namespace), TIMEOUT)
            .await?
            .histories()
            == 1
    );
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn namespace_import_radix_completeness() -> Result<()> {
    let mut f = ImportFixture::new(false).await?;
    for id in 61..201 {
        let proposal = f.another(id)?;
        f.f.domain
            .run(
                f.namespace
                    .cancel_birth(proposal, ProposalId::new([202; 16])?, TIMEOUT),
            )
            .await??;
    }
    f.begin().await?;
    f.complete().await?;
    let (root, status) = f.state().await?;
    ensure!(status.intents() == 141 && status.inventoried() == 141 && status.covered() == 141);
    let mut cursor = None;
    let mut histories = 0;
    loop {
        let page =
            f.f.catalog
                .page(&root, FleetTable::Histories, cursor.as_ref(), 7, TIMEOUT)
                .await?;
        histories += page.entries.len();
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    ensure!(histories == 141);
    ensure!(
        f.f.catalog
            .check_inventory(&root, std::slice::from_ref(&f.namespace), TIMEOUT)
            .await?
            .histories()
            == 141
    );
    ensure!(f.f.strong().await? == 5);
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn namespace_import_engine_pin_conflict() -> Result<()> {
    let mut f = ImportFixture::with_pin(true, true).await?;
    f.begin().await?;
    f.advance(None).await?;
    f.advance(None).await?;
    let proof = f.counter_proof().await?;
    let (root, status) = f.state().await?;
    ensure!(status.phase() == FleetImportPhase::Coverage);
    let request = f.request(root.revision());
    ensure!(matches!(
        f.f.catalog
            .advance_namespace_import(&root, &request, &f.namespace, Some(&proof), TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(f.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    ensure!(f.state().await?.1.covered() == 0);
    ensure!(matches!(
        f.namespace.resolve_birth(f.another(60)?).await,
        Err(CatalogError::BirthsFrozen)
    ));
    f.close().await
}

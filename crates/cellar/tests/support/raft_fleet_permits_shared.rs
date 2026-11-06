// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Managed voter lifecycle and exact typed permit operations for the native suite.
use super::*;
use futures_util::TryStreamExt as _;
use object_store::ObjectStore as _;

pub(super) const SPARE: usize = 3;
pub(super) const TIMEOUT: Duration = Duration::from_secs(10);
pub(super) const SHORT: Duration = Duration::from_millis(500);

pub(super) struct Fixture {
    pub(super) cluster: ManagedCluster,
    pub(super) primary: usize,
    nodes: Vec<Option<Node>>,
    calls: Vec<Arc<AtomicUsize>>,
    cell: CellHistory,
    retry_until_ms: u64,
}

pub(super) struct Reserved {
    pub(super) group: FleetGroupAuthorization,
    pub(super) permit: FleetPermitAuthorization,
    pub(super) plan: RepairPlan,
    pub(super) expected: FleetMembershipPermit,
    execution: FleetMovementExecutionRequest,
}

pub(super) async fn qualify(
    rooted: bool,
    check: impl AsyncFnOnce(&mut Fixture) -> Result<()>,
) -> Result<()> {
    let mut fixture = Box::pin(Fixture::start(rooted)).await?;
    let result = Box::pin(check(&mut fixture)).await;
    let joined = fixture.close().await;
    result.with_context(|| format!("joined permit fixture: {joined:?}"))?;
    joined
}

impl Fixture {
    async fn start(rooted: bool) -> Result<Self> {
        let calls = (0..=SPARE)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect::<Vec<_>>();
        let classes = calls
            .iter()
            .map(|count| {
                let count = count.clone();
                managed_fixture::counter_class_with_callback(move || {
                    count.fetch_add(1, Ordering::SeqCst);
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let first =
            managed_fixture::Fixture::with_class(rooted, managed_fixture::counter_class()?).await?;
        let applications = classes
            .into_iter()
            .map(|class| {
                Ok(Arc::new(
                    Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_class(class)?,
                ) as Arc<dyn Application>)
            })
            .collect::<Result<Vec<_>>>()?;
        let mut cluster = Box::pin(ManagedCluster::start_with_applications(
            first,
            &[7],
            ArchiveSetup::Initialized,
            false,
            Some(applications),
            FirstGroupSetup::Normal,
        ))
        .await?;
        let nodes = std::mem::take(&mut cluster.nodes)
            .into_iter()
            .map(Some)
            .collect();
        let cell = CellHistory {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            cell: CellId::new("Counter:fleet")?,
            incarnation: IncarnationId::new([25; 16])?,
        };
        let retry_until_ms = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis(),
        )?
        .checked_add(60_000)
        .context("retry deadline")?;
        let mut fixture = Self {
            cluster,
            nodes,
            primary: 0,
            calls,
            cell,
            retry_until_ms,
        };
        let initialized = async {
            fixture.primary = fixture.leader().await?.0;
            fixture.cluster.hosts[fixture.primary]
                .cell(fixture.cell.clone())?
                .enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([181; 16])?,
                        durability: Durability::Quorum,
                    },
                    TIMEOUT,
                )
                .await?;
            ensure!(fixture.write(1, 5, Durability::Quorum).await?.result == 5_i64.to_be_bytes());
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = initialized {
            let joined = fixture.close().await;
            return Err(error).context(format!("joined failed permit fixture: {joined:?}"));
        }
        Ok(fixture)
    }

    pub(super) fn catalog(&self) -> &FleetCatalog {
        &self.cluster.fixtures[0].catalog
    }

    pub(super) fn group(&self, index: usize) -> Result<RaftGroupHandle> {
        Ok(self.cluster.hosts[index].group(self.cluster.history)?)
    }

    pub(super) fn counts(&self) -> Vec<usize> {
        self.calls
            .iter()
            .map(|count| count.load(Ordering::SeqCst))
            .collect()
    }

    pub(super) async fn leader(&self) -> Result<(usize, Term)> {
        Ok(tokio::time::timeout(TIMEOUT, async {
            loop {
                let mut term = 0;
                let mut leader = None;
                for (index, node) in self.nodes.iter().enumerate() {
                    if node.is_some() {
                        if let Ok(group) = self.group(index) {
                            if let Ok(status) = group.status().await {
                                if status.term.get() > term {
                                    term = status.term.get();
                                    leader = None;
                                }
                                if status.term.get() == term
                                    && status.ready
                                    && status.leader == Some(index as u64 + 1)
                                {
                                    leader = Some((index, status.term));
                                }
                            }
                        }
                    }
                }
                if let Some(leader) = leader {
                    return leader;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?)
    }

    async fn discover(&self) -> Result<()> {
        let cut = self.catalog().read(TIMEOUT).await?;
        for (source, host) in self.cluster.hosts.iter().enumerate() {
            if self.nodes[source].is_none() {
                continue;
            }
            for (target, fixture) in self.cluster.fixtures.iter().enumerate() {
                if self.nodes[target].is_some() {
                    inspect_current(host, fixture.spec.node, cut.revision()).await?;
                }
            }
        }
        Ok(())
    }

    pub(super) async fn reserve(&mut self, id: u8) -> Result<Reserved> {
        self.primary = self.leader().await?.0;
        self.discover().await?;
        let before = self.group(self.primary)?.quorum_membership(TIMEOUT).await?;
        let previous = status(&before)?;
        let sequence = previous
            .consumed_sequence
            .checked_add(1)
            .context("permit sequence")?;
        drop(before);
        let (_, execution) = movement::admit_replacement(
            &self.cluster,
            movement::ReplacementTarget {
                source: self.primary,
                obsolete: (self.primary + 1) % SPARE,
                spare: SPARE,
            },
            [id, id + 1],
            false,
        )
        .await?;
        self.discover().await?;
        let cut = self.catalog().read(TIMEOUT).await?;
        let moves = self.catalog().pending_movements(&cut, TIMEOUT).await?;
        let selected = moves
            .into_iter()
            .find(|item| item.reservation == execution.reservation)
            .context("admitted movement")?;
        let group = self
            .catalog()
            .authorize_group(&cut, &self.cluster.history, TIMEOUT)
            .await?;
        let permit = self
            .catalog()
            .authorize_membership(&cut, &self.cluster.history, TIMEOUT)
            .await?;
        let expected = FleetMembershipPermit::new(
            previous.binding,
            sequence,
            ProposalId::new(execution.reservation)?,
            ProposalId::new(request(id, cut.revision()).operation)?,
            &selected.plan,
        )?;
        Ok(Reserved {
            group,
            permit,
            plan: selected.plan,
            expected,
            execution,
        })
    }

    pub(super) async fn catalog_permit(
        &self,
    ) -> Result<FleetPermitAuthorization, FleetCatalogError> {
        let cut = self.catalog().read(TIMEOUT).await?;
        self.catalog()
            .authorize_membership(&cut, &self.cluster.history, TIMEOUT)
            .await
    }

    pub(super) async fn ensure_pending(&self, reserved: &Reserved) -> Result<()> {
        let cut = self.catalog().read(TIMEOUT).await?;
        let pending = self.catalog().pending_reservations(&cut, TIMEOUT).await?;
        ensure!(
            pending.len() == 1
                && pending[0].reservation == reserved.execution.reservation
                && pending[0].boot == reserved.execution.attestation.boot,
            "active movement lost its exact debit: {pending:?}"
        );
        Ok(())
    }

    pub(super) async fn ensure_spare_unclaimed(&self) -> Result<()> {
        let history = self.cluster.history;
        let claim = format!(
            "/claims/{}/{}/0000000000000004",
            hex::encode(history.group.bytes()),
            hex::encode(history.incarnation.bytes()),
        );
        let mut objects = self.cluster.fixtures[SPARE].store.list(None);
        while let Some(object) = objects.try_next().await? {
            ensure!(
                !object.location.as_ref().ends_with(&claim),
                "unguarded startup claimed a managed learner"
            );
        }
        Ok(())
    }

    pub(super) async fn settle(
        &self,
        reserved: &Reserved,
        id: u8,
        phase: FleetPermitPhase,
    ) -> Result<()> {
        self.discover().await?;
        let cut = self.catalog().read(TIMEOUT).await?;
        let completion = FleetMovementCompletionRequest {
            identity: request(id, cut.revision()),
            reservation: reserved.execution.reservation,
            attestation: reserved.execution.attestation.clone(),
        };
        let applied = match phase {
            FleetPermitPhase::Fenced => self
                .catalog()
                .settle_fenced_movement(
                    &cut,
                    &completion,
                    &self.cluster.hosts[self.primary],
                    TIMEOUT,
                )
                .await?
                .applied_revision(),
            FleetPermitPhase::Aborted => self
                .catalog()
                .settle_aborted_movement(
                    &cut,
                    &completion,
                    &self.cluster.hosts[self.primary],
                    TIMEOUT,
                )
                .await?
                .applied_revision(),
            _ => anyhow::bail!("unexpected permit settlement {phase:?}"),
        };
        let settled = self.catalog().read(TIMEOUT).await?;
        ensure!(
            applied == settled.revision()
                && self
                    .catalog()
                    .pending_reservations(&settled, TIMEOUT)
                    .await?
                    .is_empty()
        );
        Ok(())
    }

    pub(super) async fn write(
        &self,
        sequence: u64,
        delta: i64,
        durability: Durability,
    ) -> Result<CommandReceipt> {
        let handle = self.cluster.hosts[self.primary].cell(self.cell.clone())?;
        let mut options = CommandOptions::new(
            CommandId::new(ClientId::new([7; 16])?, sequence)?,
            self.retry_until_ms,
        );
        options.durability = durability;
        Ok(handle
            .command(
                &handle.contract("Add")?,
                delta.to_be_bytes().to_vec(),
                options,
            )
            .await?)
    }

    pub(super) async fn read(
        &self,
        index: usize,
        consistency: QueryConsistency,
        timeout: Duration,
    ) -> Result<QueryResult> {
        let handle = self.cluster.hosts[index].cell(self.cell.clone())?;
        Ok(handle
            .query(
                ProposalId::new([119; 16])?,
                &handle.contract("Get")?,
                vec![],
                QueryOptions {
                    consistency,
                    timeout,
                    routing: QueryRouting::Local,
                    ..Default::default()
                },
            )
            .await?)
    }

    pub(super) async fn strong(&self, index: usize) -> Result<i64> {
        Ok(i64::from_be_bytes(
            self.read(index, QueryConsistency::Linearizable, TIMEOUT)
                .await?
                .result
                .as_slice()
                .try_into()?,
        ))
    }

    pub(super) async fn begin(
        &self,
        index: usize,
        reserved: &Reserved,
    ) -> Result<CellGroupQuorumMembership> {
        Ok(self
            .group(index)?
            .begin_managed_repair(
                reserved.group.clone(),
                reserved.permit.clone(),
                reserved.plan.clone(),
                TIMEOUT,
            )
            .await?)
    }

    pub(super) async fn advance(
        &mut self,
        reserved: &Reserved,
        step: RepairStep,
        phase: FleetPermitPhase,
    ) -> Result<FleetMembershipStatus> {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            self.primary = self.leader().await?.0;
            let group = self.group(self.primary)?;
            let expected = group.observe(TIMEOUT).await?.membership_revision;
            let remaining = deadline.saturating_duration_since(Instant::now());
            ensure!(!remaining.is_zero(), "managed repair {step:?} deadline");
            match group
                .advance_managed_repair(
                    reserved.group.clone(),
                    reserved.permit.clone(),
                    step,
                    expected,
                    remaining.min(Duration::from_secs(2)),
                )
                .await
            {
                Ok(observed) => {
                    verify_phase(&observed, reserved, phase)?;
                    return Ok(status(&observed)?);
                }
                Err(CellGroupControlError::Refused(
                    RuntimeError::NotLeader | RuntimeError::NotReady | RuntimeError::Overloaded,
                ))
                | Err(CellGroupControlError::Unconfirmed(
                    RuntimeError::DeadlineExceeded | RuntimeError::NotReady,
                )) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(CellGroupControlError::Unconfirmed(RuntimeError::NotFound))
                    if step == RepairStep::FinishJoint =>
                {
                    // A leader can apply its own removal before it returns
                    // the terminal proof. Observe the same permit on the
                    // successor, with the original invocation deadline.
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => return Err(error).context(format!("managed repair {step:?}")),
            }
        }
    }

    pub(super) async fn wait_phase(
        &self,
        index: usize,
        reserved: &Reserved,
        phase: FleetPermitPhase,
    ) -> Result<FleetMembershipStatus> {
        let group = self.group(index)?;
        let deadline = Instant::now() + TIMEOUT;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            ensure!(
                !remaining.is_zero(),
                "member {index} did not observe {phase:?}"
            );
            if let Ok(proof) = group.quorum_membership(remaining.min(SHORT)).await {
                if status(&proof)?.phase == Some(phase) {
                    verify_phase(&proof, reserved, phase)?;
                    return Ok(status(&proof)?);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub(super) fn activation(&self, source: usize, from_s3: bool) -> Result<LearnerActivation> {
        Ok(LearnerActivation {
            member: MemberId::new(4)?,
            source: MemberId::new(source as u64 + 1)?,
            source_node: self.cluster.fixtures[source].spec.node,
            from_s3,
            timeout: TIMEOUT,
        })
    }

    pub(super) async fn activate(
        &self,
        reserved: &Reserved,
        source: usize,
        from_s3: bool,
    ) -> Result<FleetMembershipStatus> {
        self.discover().await?;
        Ok(self.cluster.hosts[SPARE]
            .activate_managed_learner_group(
                reserved.group.clone(),
                reserved.permit.clone(),
                self.activation(source, from_s3)?,
            )
            .await?)
    }

    pub(super) async fn stop(&mut self, index: usize) -> Result<()> {
        let node = self.nodes[index]
            .take()
            .context("running permit fixture node")?;
        ensure!(matches!(
            node.shutdown(ShutdownMode::Preserve).await?,
            NodeOutcome::Stopped(_)
        ));
        let report = self.cluster.hosts[index].close().await?;
        ensure!(report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers);
        Ok(())
    }

    pub(super) async fn reopen(&mut self, index: usize, generation: u8) -> Result<()> {
        ensure!(
            self.nodes[index].is_none(),
            "engine reopened before joining prior node"
        );
        let fixture = &self.cluster.fixtures[index];
        let node = start_node(
            self.cluster.roots[index].path(),
            fixture.application.clone(),
            &fixture.spec.label,
        )
        .await?;
        let started = async {
            let (mut config, services) = fixture
                .config(&node, LogOpenMode::Resume, generation)
                .await?;
            archives::configure(&mut config);
            node.start_raft_cells(config, services).await
        }
        .await;
        match started {
            Ok(host) => {
                self.nodes[index] = Some(node);
                self.cluster.hosts[index] = host;
                Ok(())
            }
            Err(error) => {
                let joined = node.shutdown(ShutdownMode::Preserve).await;
                Err(error).context(format!("reopen member {}: {joined:?}", index + 1))
            }
        }
    }

    pub(super) async fn restart_voters(&mut self, generation: u8) -> Result<()> {
        for index in 0..SPARE {
            self.stop(index).await?;
        }
        for index in 0..SPARE {
            self.reopen(index, generation + index as u8).await?;
        }
        let first = &mut self.cluster.fixtures[0];
        first.catalog = managed_fixture::Fixture::open_catalog(
            &first.domain,
            first.store.clone(),
            self.cluster.history.cluster,
        )?;
        self.discover().await?;
        self.primary = self.leader().await?.0;
        Ok(())
    }

    async fn close(mut self) -> Result<()> {
        let mut errors = Vec::new();
        for index in 0..self.nodes.len() {
            if self.nodes[index].is_some() {
                if let Err(error) = self.stop(index).await {
                    errors.push(format!("node {index}: {error:#}"));
                }
            }
        }
        self.cluster.fixtures[0].domain.shutdown().await;
        ensure!(errors.is_empty(), "{errors:?}");
        Ok(())
    }
}

pub(super) fn status(proof: &CellGroupQuorumMembership) -> Result<FleetMembershipStatus> {
    proof
        .membership()
        .fleet_status()
        .context("durable fleet guard")
}

pub(super) fn verify_phase(
    proof: &CellGroupQuorumMembership,
    reserved: &Reserved,
    phase: FleetPermitPhase,
) -> Result<()> {
    let status = status(proof)?;
    ensure!(
        status.permit.as_ref() == Some(&reserved.expected)
            && status.consumed_sequence == reserved.expected.sequence()
            && status.phase == Some(phase),
        "unexpected permit guard: {status:?}"
    );
    Ok(())
}

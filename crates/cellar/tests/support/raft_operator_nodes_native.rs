// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Human grants -> exact rooted admission -> real new boot -> generated move.
use super::*;

fn descriptor(spec: &FleetNodeSpec) -> RaftFleetNodeDescriptor {
    RaftFleetNodeDescriptor {
        node_id: hex::encode(spec.node.bytes()),
        disk_id: hex::encode(spec.disk.bytes()),
        physical_host: hex::encode(spec.physical_host.bytes()),
        label: spec.label.clone(),
        server_name: spec.server_name.clone(),
        capability_digest: hex::encode(spec.capability_digest),
        storage_instance: spec.storage_instance.map(|v| hex::encode(v.bytes())),
    }
}

impl Server {
    fn register_args(&self, request: &RaftFleetRegisterNode) -> Result<Vec<String>> {
        let path = self.root.path().join("node.json");
        std::fs::write(&path, serde_json::to_vec(&request.descriptor)?)?;
        Ok(vec![
            "fleet".into(),
            "nodes".into(),
            "add".into(),
            "--descriptor".into(),
            path.to_string_lossy().into_owned(),
            "--operation-id".into(),
            request.request_id.clone(),
            "--expected-revision".into(),
            request.expected_revision.to_string(),
        ])
    }

    fn capacity_args(
        &self,
        node: &str,
        request: &RaftFleetNodeCapacity,
        activate: bool,
    ) -> Result<Vec<String>> {
        let path = self.root.path().join("capacity.json");
        std::fs::write(&path, serde_json::to_vec(&request.attestation)?)?;
        Ok(vec![
            "fleet".into(),
            "nodes".into(),
            if activate { "activate" } else { "qualify" }.into(),
            node.into(),
            "--attestation".into(),
            path.to_string_lossy().into_owned(),
            "--operation-id".into(),
            request.request_id.clone(),
            "--expected-revision".into(),
            request.expected_revision.to_string(),
        ])
    }

    async fn node_view(&self, node: &str) -> Result<RaftFleetNodeStatus> {
        let (exit, value) = self
            .cli(
                &["fleet".into(), "nodes".into(), "status".into(), node.into()],
                "reader",
            )
            .await?;
        ensure!(exit == 0, "node status {exit}: {value}");
        let view: RaftFleetNodeStatus = serde_json::from_value(value)?;
        view.validate()?;
        Ok(view)
    }
}

fn accepted(value: Value) -> Result<RaftFleetNodeAcceptance> {
    let value: RaftFleetNodeAcceptance = serde_json::from_value(value)?;
    value.admission.validate()?;
    Ok(value)
}

async fn start_added(
    f: &Fixture,
    spec: &FleetNodeSpec,
    root: &std::path::Path,
    process: Option<ProcessGeneration>,
) -> Result<(cellar::sdk::Node, RaftCells)> {
    let cut = f.catalog.read(TIMEOUT).await?;
    let authorization = f.catalog.authorize_host(&cut, spec.node, TIMEOUT).await?;
    let node = public_node::node_named(root, f.definition.application.clone(), &spec.label).await?;
    let (mut config, services) = f.config_for(&node, authorization, spec).await?;
    if let Some(process) = process {
        config.infrastructure.log.mode = LogOpenMode::Resume;
        config.infrastructure.log.process = process;
        config.peers[0].process = process;
    }
    let host = node.start_raft_cells(config, services).await?;
    Ok((node, host))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_operator_node_add_activate_rebalances_with_exact_retries() -> Result<()> {
    Box::pin(add_capacity()).await
}

async fn add_capacity() -> Result<()> {
    let mut f = Box::pin(Scenario::new(true)).await?;
    let server = Server::new(&f).await?;
    let spec = f.s.f.peer_spec(9)?;
    let node_id = hex::encode(spec.node.bytes());
    let revision = f.s.f.catalog.read(TIMEOUT).await?.revision();
    let registration = RaftFleetRegisterNode {
        catalog_id: server.control.catalog_id().into(),
        request_id: hex::encode([160; 16]),
        expected_revision: revision,
        descriptor: descriptor(&spec),
    };
    let args = server.register_args(&registration)?;
    ensure!(server.cli(&args, "capacity").await?.0 == 4);
    ensure!(server.cli(&args, "reader").await?.0 == 4);
    let (exit, wrong) = server
        .cli_for_catalog(&args, "registrar", &hex::encode([99; 32]))
        .await?;
    ensure!(exit == 3 && wrong["error"]["outcome"] == "not_submitted");
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == revision);
    f.s.f
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let (exit, unknown) = server.cli(&args, "registrar").await?;
    ensure!(
        exit == 5 && unknown["error"]["outcome"] == "unknown",
        "{exit}: {unknown}"
    );
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == revision + 1);
    let (exit, value) = server.cli(&args, "registrar").await?;
    ensure!(exit == 0, "register retry {exit}: {value}");
    let registered = accepted(value)?;
    ensure!(
        registered.reconciled
            && registered.admission.principal == "registrar"
            && registered.admission.admitted_state == RaftFleetNodeState::Registered
    );
    let view = server.node_view(&node_id).await?;
    ensure!(
        view.state == RaftFleetNodeState::Registered
            && view.capacity.is_none()
            && view.descriptor.storage_instance.is_none()
    );
    let mut changed = registration.clone();
    changed.descriptor.label = "different-node-name".into();
    let (exit, value) = server
        .cli(&server.register_args(&changed)?, "registrar")
        .await?;
    ensure!(exit == 3 && value["error"]["code"] == "payload_conflict");
    let mut forged = serde_json::to_value(&registration)?;
    forged["principal"] = json!("forged");
    let response = reqwest::Client::new()
        .post(format!("{}/api/v1/fleets/test/raft/nodes", server.url))
        .bearer_auth(REGISTRAR)
        .json(&forged)
        .send()
        .await?;
    ensure!(response.status() == reqwest::StatusCode::BAD_REQUEST);
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == revision + 1);

    // Infrastructure startup is explicit and separate from registration.
    let root = tempfile::tempdir()?;
    let (new_node, host) = start_added(&f.s.f, &spec, root.path(), None).await?;
    let evidence = attestation(&f.s.f, &f.s.nodes[0].host, &host).await?;
    let revision = f.s.f.catalog.read(TIMEOUT).await?.revision();
    let ready = RaftFleetNodeCapacity {
        catalog_id: registration.catalog_id.clone(),
        request_id: hex::encode([161; 16]),
        expected_revision: revision,
        attestation: evidence.clone(),
    };
    let standalone = Server::configured(&f.s.f, &f.s.namespace, None).await?;
    let (exit, value) = standalone
        .cli(
            &standalone.capacity_args(&node_id, &ready, false)?,
            "capacity",
        )
        .await?;
    ensure!(
        exit == 4
            && value["error"]["code"] == "unsupported"
            && value["error"]["outcome"] == "not_submitted"
    );
    standalone.close().await?;
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == revision);
    let ready_args = server.capacity_args(&node_id, &ready, false)?;
    ensure!(server.cli(&ready_args, "registrar").await?.0 == 4);
    f.s.f
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let (exit, unknown) = server.cli(&ready_args, "capacity").await?;
    ensure!(exit == 5 && unknown["error"]["outcome"] == "unknown");
    let (exit, value) = server.cli(&ready_args, "capacity").await?;
    ensure!(exit == 0, "qualify retry {exit}: {value}");
    let qualified = accepted(value)?;
    ensure!(
        qualified.reconciled && qualified.admission.admitted_state == RaftFleetNodeState::Ready
    );
    let view = server.node_view(&node_id).await?;
    ensure!(view.state == RaftFleetNodeState::Ready && view.capacity.as_ref() == Some(&evidence));
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(!f
        .s
        .f
        .catalog
        .authorize_capacity(&cut, spec.node, TIMEOUT)
        .await?
        .receive_replicas());
    f.s.f
        .remote(&f.s.nodes[0].host, spec.node, cut.revision())
        .await?;
    let active = RaftFleetNodeCapacity {
        request_id: hex::encode([162; 16]),
        expected_revision: cut.revision(),
        ..ready.clone()
    };
    let active_args = server.capacity_args(&node_id, &active, true)?;
    f.s.f
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let (exit, unknown) = server.cli(&active_args, "capacity").await?;
    ensure!(exit == 5 && unknown["error"]["outcome"] == "unknown");
    let (exit, value) = server.cli(&active_args, "capacity").await?;
    ensure!(exit == 0, "activate retry {exit}: {value}");
    let activated = accepted(value)?;
    ensure!(
        activated.reconciled
            && activated.admission.principal == "capacity"
            && activated.admission.admitted_state == RaftFleetNodeState::Active
    );
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(f
        .s
        .f
        .catalog
        .authorize_capacity(&cut, spec.node, TIMEOUT)
        .await?
        .receive_replicas());
    let view = server.node_view(&node_id).await?;
    ensure!(view.state == RaftFleetNodeState::Active && view.capacity.as_ref() == Some(&evidence));
    // Old registrations/qualification receipts cannot rewrite the active state.
    let (exit, value) = server
        .cli(&server.register_args(&registration)?, "registrar")
        .await?;
    ensure!(
        exit == 0
            && accepted(value)?.admission.applied_revision == registered.admission.applied_revision
    );
    let (exit, value) = server
        .cli(&server.capacity_args(&node_id, &ready, false)?, "capacity")
        .await?;
    ensure!(
        exit == 0
            && accepted(value)?.admission.applied_revision == qualified.admission.applied_revision
    );
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());

    let begin = RaftFleetBegin {
        catalog_id: registration.catalog_id.clone(),
        operation_id: hex::encode([164; 16]),
        expected_revision: cut.revision(),
        keep: [4_u8, 5, 6, 7, 8, 9].map(|n| hex::encode([n; 16])).to_vec(),
        policy: FleetTopologyPolicy {
            revision: 1,
            distinct_zones: false,
            distinct_regions: false,
            max_work: 1_000_000,
        },
    };
    let (exit, value) = server
        .cli(&server.begin_args(&begin, "added-policy.json")?, "owner")
        .await?;
    ensure!(exit == 0, "begin added-node rebalance {exit}: {value}");
    let mut inputs = Inputs::new(&f).await?;
    inputs.hosts.push((spec.node, host.clone()));
    let mut hosts = f.hosts();
    hosts.push(&host);
    discover(&f.s.f, &hosts).await?;
    let inputs = Arc::new(inputs);
    // This add-capacity workflow retains the whole fleet. The old four-node
    // request also excluded both spares and could not prove recovery once an
    // old binding was retained. Preserve that exact refusal without admitting
    // a movement or changing any safety/headroom bound.
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let operation =
        f.s.f
            .catalog
            .active_topology_operation(&cut, TIMEOUT)
            .await?
            .context("active new-capacity operation")?;
    let mut reduced = FleetTopologyEvidence::refresh(inputs.as_ref(), &operation, TIMEOUT).await?;
    reduced.request.keep = [4_u8, 5, 6, 9]
        .map(|n| NodeId::new([n; 16]))
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut walk =
        f.s.f
            .catalog
            .prepare_topology_verification(
                &cut,
                &reduced.request,
                &f.s.nodes[0].host,
                &reduced.namespaces,
                TIMEOUT,
            )
            .await?;
    while walk.verify_next(TIMEOUT).await? {}
    ensure!(matches!(
        walk.preview(TIMEOUT).await,
        Err(FleetCatalogError::Topology(
            cellar_logic::raft_fleet::TopologyError::BalanceUnproven
        ))
    ));
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    ensure!(f
        .s
        .f
        .catalog
        .pending_movements(&cut, TIMEOUT)
        .await?
        .is_empty());
    let service =
        f.s.f
            .catalog
            .start_topology_service_with_collection(
                f.s.nodes[0].host.clone(),
                inputs.clone(),
                inputs,
                options(),
                FleetTopologyCollectionOptions::default(),
            )
            .await?;
    let mut args = op_args("wait", &begin.operation_id);
    args.extend(["--timeout-seconds".into(), "40".into()]);
    let (exit, value) = server.cli(&args, "reader").await?;
    ensure!(
        exit == 0 && value["phase"] == "complete" && value["converged"] == true,
        "rebalance {exit}: {value}"
    );
    service.shutdown().await;
    let page = server
        .control
        .movements(&begin.operation_id, None, 128, TIMEOUT)
        .await?;
    ensure!(page.entries.len() == 3);
    let mut targets = std::collections::BTreeSet::new();
    for entry in page.entries {
        targets.insert(entry.target.clone());
        let target = if entry.target == node_id {
            &host
        } else if entry.target == hex::encode([7; 16]) {
            &f.s.target
        } else {
            ensure!(entry.target == hex::encode([8; 16]));
            &f.spare
        };
        let index =
            f.s.proposals
                .iter()
                .position(|p| hex::encode(p.scope.group.bytes()) == entry.group)
                .context("moved history")?;
        let old = f.live[index]
            .iter_mut()
            .find(|(n, _)| hex::encode(n.bytes()) == entry.source)
            .context("old voter")?;
        *old = (
            NodeId::new(
                hex::decode(&entry.target)?
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("target ID width"))?,
            )?,
            target.cell(f.s.proposals[index].scope.namespace, &entry.cell)?,
        );
    }
    ensure!(
        targets
            == [hex::encode([7; 16]), hex::encode([8; 16]), node_id.clone()]
                .into_iter()
                .collect()
    );
    for (index, live) in f.live.iter().enumerate() {
        ensure!(live.len() == 3);
        let cells = live.iter().map(|(_, h)| h.clone()).collect::<Vec<_>>();
        let cut = f.s.f.catalog.read(TIMEOUT).await?;
        let authorization =
            f.s.f
                .catalog
                .authorize_group(&cut, &f.s.proposals[index].scope, TIMEOUT)
                .await?;
        let proof = cells[0]
            .observe_fleet_membership(authorization, TIMEOUT)
            .await?;
        ensure!(
            proof.configuration().voters().len() == 3
                && proof.configuration().outgoing_voters().is_empty()
                && proof.configuration().learners().is_empty()
                && proof.configuration().next_learners().is_empty()
        );
        if live.iter().any(|(node, _)| *node == spec.node) {
            let mut found = false;
            for member in proof.configuration().voters() {
                found |= proof.joined_member(*member)?.is_some_and(|(binding, _)| {
                    binding.node == spec.node && binding.disk == spec.disk
                });
            }
            ensure!(found, "new node is absent from fresh quorum binding");
        }
        write_read(&f.s.f, &cells, 2, 5, 12).await?;
        write_read(&f.s.f, &cells, 3, 9, 21).await?;
    }
    let audit = reqwest::Client::new()
        .get(format!("{}/api/v1/fleets/test/operations", server.url))
        .bearer_auth(READER)
        .send()
        .await?
        .json::<Value>()
        .await?;
    let audit = audit.to_string();
    ensure!(
        audit.contains("raft_node_register")
            && audit.contains("raft_node_qualify")
            && audit.contains("raft_node_activate")
            && audit.contains("registrar")
            && audit.contains("capacity")
    );
    server.close().await?;
    joined(new_node).await?;
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_operator_node_capacity_fences_reboot_and_retains_receipts() -> Result<()> {
    Box::pin(reboot()).await
}

async fn reboot() -> Result<()> {
    let f = Fixture::with_managed_births(true).await?;
    let (_origin_root, origin_node, origin) = f.empty_host().await?;
    let namespace = Catalog::new(
        f.store.clone(),
        CatalogOptions {
            prefix: "host-enrollment/".into(),
            ..Default::default()
        },
    )?
    .existing_namespace(f.declaration.namespace.clone())
    .await?;
    let server = Server::configured(&f, &namespace, Some(origin.clone())).await?;
    let spec = f.peer_spec(9)?;
    let node_id = hex::encode(spec.node.bytes());
    let request = RaftFleetRegisterNode {
        catalog_id: server.control.catalog_id().into(),
        request_id: hex::encode([160; 16]),
        expected_revision: f.catalog.read(TIMEOUT).await?.revision(),
        descriptor: descriptor(&spec),
    };
    ensure!(
        server
            .cli(&server.register_args(&request)?, "registrar")
            .await?
            .0
            == 0
    );
    let root = tempfile::tempdir()?;
    let (node, host) = start_added(&f, &spec, root.path(), None).await?;
    let evidence = attestation(&f, &origin, &host).await?;
    let active = RaftFleetNodeCapacity {
        catalog_id: request.catalog_id,
        request_id: hex::encode([161; 16]),
        expected_revision: f.catalog.read(TIMEOUT).await?.revision(),
        attestation: evidence.clone(),
    };
    let (exit, value) = server
        .cli(&server.capacity_args(&node_id, &active, true)?, "capacity")
        .await?;
    ensure!(exit == 0, "initial activation {exit}: {value}");
    let admitted = accepted(value)?;
    joined(node).await?;
    let (restarted_node, restarted) = start_added(
        &f,
        &spec,
        root.path(),
        Some(ProcessGeneration::new([135; 16])?),
    )
    .await?;
    let fresh = attestation(&f, &origin, &restarted).await?;
    ensure!(fresh.boot != evidence.boot);
    let current = server.node_view(&node_id).await?;
    ensure!(current.capacity.is_none());
    let revision = f.catalog.read(TIMEOUT).await?.revision();
    let stale = RaftFleetNodeCapacity {
        request_id: hex::encode([162; 16]),
        expected_revision: revision,
        ..active.clone()
    };
    let (exit, value) = server
        .cli(&server.capacity_args(&node_id, &stale, true)?, "capacity")
        .await?;
    ensure!(
        exit == 3 && value["error"]["code"] == "conflict",
        "stale boot {exit}: {value}"
    );
    let (exit, value) = server
        .cli(&server.capacity_args(&node_id, &active, true)?, "capacity")
        .await?;
    ensure!(
        exit == 0
            && accepted(value)?.admission.applied_revision == admitted.admission.applied_revision
    );
    ensure!(server.node_view(&node_id).await?.capacity.is_none());
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == revision);
    let requalified = RaftFleetNodeCapacity {
        request_id: hex::encode([163; 16]),
        expected_revision: revision,
        attestation: fresh.clone(),
        ..active
    };
    let (exit, value) = server
        .cli(
            &server.capacity_args(&node_id, &requalified, true)?,
            "capacity",
        )
        .await?;
    ensure!(exit == 0, "new boot qualification {exit}: {value}");
    ensure!(server.node_view(&node_id).await?.capacity.as_ref() == Some(&fresh));
    server.close().await?;
    joined(restarted_node).await?;
    joined(origin_node).await?;
    f.domain.shutdown().await;
    Ok(())
}

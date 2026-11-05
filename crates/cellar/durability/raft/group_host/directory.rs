// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One bounded physical-node discovery owner, joined by the shared SDK service.
//! Endpoints and rooted membership are independent of current-term Raft authority.
use super::*;
use crate::application::consensus::group_client::ControlServiceKind;
use crate::raft::{
    catalog::NodeRegistration,
    fleet_catalog::{FleetHostAuthorization, FleetTrustReader, FleetTrustSnapshot},
    host::RaftNodeDirectoryOptions,
    storage::ReplicaLog,
    transport::RaftTransport,
    transport_options::RaftPeerNode,
};
use rand::RngCore;
use std::{collections::BTreeMap, time::Duration};

const METADATA_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone)]
enum Trusted {
    Configured(RaftPeerNode),
    Enrolled(Box<FleetHostAuthorization>),
}

pub(super) struct Directory {
    options: RaftNodeDirectoryOptions,
    registrations: Vec<NodeRegistration>,
    trusted: Vec<Trusted>,
    reader: Option<FleetTrustReader>,
    rooted: Option<FleetTrustSnapshot>,
    cursor: usize,
    own_revision: u64,
    routers: Option<super::router_directory::RouterDirectory>,
}

pub(super) fn remaining(log: &ReplicaLog, deadline: u64) -> anyhow::Result<Duration> {
    let ms = deadline
        .checked_sub(log.domain().mono_ms()?)
        .filter(|ms| *ms > 0)
        .ok_or_else(|| anyhow::anyhow!("node directory original deadline"))?;
    Ok(Duration::from_millis(ms))
}

fn trusted(trust: &FleetTrustSnapshot) -> Vec<Trusted> {
    trust
        .nodes()
        .iter()
        .filter(|n| n.permits_transport())
        .cloned()
        .map(|node| Trusted::Enrolled(Box::new(node)))
        .collect()
}

pub(super) async fn prepare(
    prepared: &mut Prepared,
    options: &mut crate::raft::host::RaftHostOptions,
    log: &ReplicaLog,
    transport: &RaftTransport,
) -> anyhow::Result<Option<Directory>> {
    let Some(settings) = options.node_directory.clone() else {
        return Ok(None);
    };
    let local = log.limits();
    let mut peers: BTreeMap<_, _> = prepared
        .peers
        .iter()
        .cloned()
        .map(|p| (p.node, p))
        .collect();
    let own = peers
        .get(&local.node)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("node directory requires an explicit self endpoint"))?;
    anyhow::ensure!(
        own.disk == local.disk
            && own.process == local.process
            && own.label == transport.local_label(),
        "directory self endpoint disagrees with engine owner"
    );
    for peer in peers.values() {
        peer.validate_endpoint()?;
    }
    if let Some(managed) = &options.managed {
        // Check the rooted descriptor before either a permanent storage pin or
        // a boot CAS. Advertised addresses may differ from local bind addresses.
        managed.authorization.check_boot_endpoint(&own)?;
    }
    let deadline = log
        .domain()
        .mono_ms()?
        .checked_add(settings.timeout_ms)
        .ok_or_else(|| anyhow::anyhow!("directory deadline overflow"))?;
    let mut namespaces = BTreeMap::new();
    for ns in &prepared.namespaces {
        namespaces
            .entry(ns.directory.namespace_config().cluster)
            .or_insert_with(|| ns.directory.namespace().clone());
    }
    let mut registrations = Vec::new();
    for namespace in namespaces.into_values() {
        registrations.push(
            namespace
                .register_node_storage(log, remaining(log, deadline)?)
                .await?,
        );
    }
    let mut rooted = None;
    let mut reader = None;
    // Bind once for the complete host, before publishing any process endpoint or
    // attaching a group. The engine update must actually sync before admission.
    if let Some(managed) = options.managed.as_mut() {
        let mut id = [0; 16];
        crate::asyncrt::rng("fleet-host-boot").fill_bytes(&mut id);
        let id = cellar_logic::consensus::ProposalId::new(id)?;
        let join_key = crate::asyncrt::services()
            .peer_probe_signer()?
            .public_key_bytes();
        let host = &managed.authorization;
        let (bound, update) =
            if host.state() == crate::raft::fleet_catalog::FleetNodeState::RestartPending {
                host.bind_restart_boot(log, id, join_key, deadline, remaining(log, deadline)?)
                    .await?
            } else {
                let bound = host
                    .bind_boot(
                        log.storage_instance()?,
                        local.process,
                        id,
                        join_key,
                        remaining(log, deadline)?,
                    )
                    .await?;
                let update = crate::raft::fleet_stop::LifecycleUpdate::Boot {
                    boot: bound.boot_binding()?,
                    deadline,
                };
                (bound, update)
            };
        log.update_lifecycle(update).await?;
        remaining(log, deadline)?;
        managed.authorization = bound;
        if let Some(selector) = log.lifecycle_guard()?.and_then(|g| g.quiescing().cloned()) {
            crate::asyncrt::services()
                .raft_lifecycle()
                .install_startup_maintenance(&selector)?;
        }
        let trust = managed
            .authorization
            .trust_snapshot(remaining(log, deadline)?)
            .await?;
        require_local(&trust, log)?;
        anyhow::ensure!(
            trust.nodes().len() <= transport.peer_limit(),
            "fleet exceeds transport peer bound"
        );
        transport.install_fleet_trust(trust.clone())?;
        reader = Some(managed.authorization.trust_reader());
        rooted = Some(trust);
    }
    let mut own_revision = 0;
    for registration in &registrations {
        let view = registration
            .publication(own.clone(), remaining(log, deadline)?)
            .await?
            .publish(remaining(log, deadline)?)
            .await?;
        own_revision = own_revision.max(view.revision());
        transport.accept_node_view(view, &own)?;
        if let Some(trust) = &rooted {
            for node in trust
                .nodes()
                .iter()
                .filter(|n| n.node() != local.node && n.permits_transport())
            {
                if let Some(view) = registration
                    .observe_fleet_node(node, remaining(log, deadline)?)
                    .await?
                {
                    let peer = view.peer().clone();
                    transport.accept_node_view(view, &peer)?;
                    peers.insert(peer.node, peer);
                }
            }
        }
    }
    let selected = rooted.as_ref().map_or_else(
        || peers.values().cloned().map(Trusted::Configured).collect(),
        trusted,
    );
    let routers = reader.as_ref().and_then(|_| {
        super::router_directory::RouterDirectory::new(
            prepared
                .groups
                .iter()
                .filter(|assignment| assignment.placement == RaftGroupPlacement::QueryRouter)
                .map(|assignment| assignment.birth.route().history),
        )
    });
    prepared.peers = peers.into_values().collect();
    Ok(Some(Directory {
        options: settings,
        registrations,
        trusted: selected,
        reader,
        rooted,
        cursor: 0,
        own_revision,
        routers,
    }))
}

fn require_local(trust: &FleetTrustSnapshot, log: &ReplicaLog) -> anyhow::Result<()> {
    let limits = log.limits();
    let boot = log
        .lifecycle_guard()?
        .ok_or_else(|| anyhow::anyhow!("missing synced managed boot"))?
        .boot;
    let host = trust
        .node(limits.node)
        .ok_or_else(|| anyhow::anyhow!("local node removed from fleet"))?;
    anyhow::ensure!(
        host.permits_transport()
            && host.boot_binding()? == boot
            && boot.disk == limits.disk.bytes()
            && boot.process == limits.process.bytes()
            && boot.engine == log.storage_instance()?.bytes(),
        "local fleet boot superseded"
    );
    Ok(())
}

impl Directory {
    pub(super) fn start(
        self,
        service: &RaftCells,
        log: ReplicaLog,
        transport: RaftTransport,
    ) -> anyhow::Result<()> {
        let lease =
            service.register_control_service(ControlServiceKind::Directory, METADATA_BYTES)?;
        let mut stop = lease.stop_receiver();
        let observer = service.internal_observer();
        log.domain().spawn_cleanup(async move {
            let _lease = lease;
            let mut directory = self;
            loop {
                if *stop.borrow() { break; }
                // Entered discovery work is retained until its original bounded
                // operation finishes. Shutdown never drops it at a select edge.
                if let Err(error) = directory.refresh(&log, &transport, &observer).await {
                    tracing::debug!(%error, "shared node directory fenced local owner");
                    drop(observer.close()); // requests independently owned joined closure
                    break;
                }
                crate::asyncrt::select! {
                    _ = stop.changed() => break,
                    _ = crate::asyncrt::sleep(Duration::from_millis(directory.options.poll_ms)) => (),
                }
            }
        })?.detach();
        Ok(())
    }

    async fn refresh(
        &mut self,
        log: &ReplicaLog,
        transport: &RaftTransport,
        observer: &RaftCells,
    ) -> anyhow::Result<()> {
        let deadline = log
            .domain()
            .mono_ms()?
            .checked_add(self.options.timeout_ms)
            .ok_or_else(|| anyhow::anyhow!("directory deadline overflow"))?;
        if let Some(reader) = &self.reader {
            let result = transport
                .seed_control_until(deadline, async {
                    let timeout = remaining(log, deadline)
                        .map_err(|_| crate::raft::transport::TransportError::Timeout)?;
                    Ok(reader.refresh(self.rooted.as_ref(), timeout).await)
                })
                .await;
            match result {
                Ok(Ok(trust)) => {
                    require_local(&trust, log)?;
                    transport.install_fleet_trust(trust.clone())?;
                    self.trusted = trusted(&trust);
                    self.rooted = Some(trust);
                }
                // Missing metadata is not revocation or loss of a live quorum.
                Ok(Err(
                    error @ (crate::raft::fleet_catalog::FleetCatalogError::Conflict
                    | crate::raft::fleet_catalog::FleetCatalogError::Corrupt(_)),
                )) => return Err(error.into()),
                Ok(Err(error)) => tracing::debug!(%error, "fleet trust read unavailable"),
                Err(error) => tracing::debug!(%error, "fleet trust read refused"),
            }
        }
        let total = self
            .registrations
            .len()
            .checked_mul(self.trusted.len())
            .ok_or_else(|| anyhow::anyhow!("directory inventory overflow"))?;
        if total == 0 {
            return Ok(());
        }
        let mut batch = Vec::new();
        for _ in 0..self.options.batch_nodes.min(total) {
            self.cursor %= total;
            batch.push((
                self.registrations[self.cursor / self.trusted.len()].clone(),
                self.trusted[self.cursor % self.trusted.len()].clone(),
            ));
            self.cursor = (self.cursor + 1) % total;
        }
        let results = futures_util::future::join_all(batch.into_iter().map(
            |(registration, trusted)| async move {
                let result = transport
                    .seed_control_until(deadline, async {
                        let timeout = remaining(log, deadline)
                            .map_err(|_| crate::raft::transport::TransportError::Timeout)?;
                        Ok(match &trusted {
                            Trusted::Configured(peer) => {
                                registration.observe_node(peer, timeout).await
                            }
                            Trusted::Enrolled(node) => {
                                registration.observe_fleet_node(node, timeout).await
                            }
                        })
                    })
                    .await;
                (trusted, result)
            },
        ))
        .await;
        for (trusted, result) in results {
            let view = match result {
                Ok(Ok(Some(view))) => view,
                Ok(Ok(None)) => continue,
                Ok(Err(error)) => {
                    tracing::debug!(%error, "node view unavailable");
                    continue;
                }
                Err(error) => {
                    tracing::debug!(%error, "node discovery refused");
                    continue;
                }
            };
            let peer = view.peer().clone();
            let local = log.limits();
            anyhow::ensure!(
                peer.node != local.node
                    || (view.storage_instance() == log.storage_instance()?
                        && !(peer.process != local.process && view.revision() > self.own_revision)),
                "local process view superseded"
            );
            let expected = match &trusted {
                Trusted::Configured(p) => p,
                Trusted::Enrolled(_) => &peer,
            };
            if let Err(error) = transport.accept_node_view(view, expected) {
                tracing::debug!(%error, "node process refresh refused");
            }
        }
        if let (Some(routers), Some(reader), Some(trust)) =
            (&mut self.routers, &self.reader, &self.rooted)
        {
            if let Err(error) = routers
                .refresh(log, transport, observer, reader, trust, deadline)
                .await
            {
                tracing::debug!(%error, "router membership refresh unavailable");
            }
        }
        Ok(())
    }
}

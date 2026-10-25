// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Owned exact-key discovery. Directory observations refresh endpoints only;
//! existing quorum authority survives missing metadata or an object-store outage.
use super::super::fleet_catalog::{FleetHostAuthorization, FleetTrustReader, FleetTrustSnapshot};
use super::*;
use std::time::Duration;

const FLEET_READ_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone)]
enum DiscoveryNode {
    Configured(RaftPeerNode),
    Enrolled(Box<FleetHostAuthorization>),
}

impl DiscoveryNode {
    fn node(&self) -> NodeId {
        match self {
            Self::Configured(peer) => peer.node,
            Self::Enrolled(node) => node.node(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RaftNodeDirectoryOptions {
    /// Independent host timer interval; no application traffic is required.
    pub poll_ms: u64,
    /// Original deadline shared by every operation in one discovery batch.
    pub timeout_ms: u64,
    /// At most this many bounded node records are observed concurrently.
    pub batch_nodes: usize,
}

impl Default for RaftNodeDirectoryOptions {
    fn default() -> Self {
        Self {
            poll_ms: 250,
            timeout_ms: 5000,
            batch_nodes: 4,
        }
    }
}

impl RaftNodeDirectoryOptions {
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (10..=60_000).contains(&self.poll_ms)
                && (1..=60_000).contains(&self.timeout_ms)
                && (1..=8).contains(&self.batch_nodes),
            "invalid node directory bounds"
        );
        Ok(())
    }
}

pub(super) struct Directory {
    options: RaftNodeDirectoryOptions,
    registrations: Vec<NodeRegistration>,
    trusted: Vec<DiscoveryNode>,
    enrollment: Option<FleetTrustReader>,
    rooted: Option<FleetTrustSnapshot>,
    namespace: usize,
    node: usize,
    next_at: u64,
    busy: bool,
    own_revision: u64,
}

fn remaining(domain: &crate::asyncrt::DomainToken, deadline: u64) -> anyhow::Result<Duration> {
    let millis = deadline
        .checked_sub(domain.mono_ms()?)
        .filter(|ms| *ms > 0)
        .ok_or_else(|| anyhow::anyhow!("node directory original deadline"))?;
    Ok(Duration::from_millis(millis))
}

pub(super) async fn prepare(
    options: Option<&RaftNodeDirectoryOptions>,
    cells: &[Registration],
    declared: &[NamespaceRegistration],
    mut enrollment: Option<&mut super::super::fleet_catalog::FleetHostAuthorization>,
    peers: &mut BTreeMap<NodeId, RaftPeerNode>,
    service: &crate::daemon::NodeRaftService,
    label: &str,
) -> anyhow::Result<Option<Directory>> {
    let Some(options) = options else {
        return Ok(None);
    };
    options.validate()?;
    anyhow::ensure!(
        !declared.is_empty()
            || cells
                .iter()
                .any(|cell| !matches!(cell.placement, RaftCellPlacement::QueryRouter { .. })),
        "node directory requires stable replica or standby storage"
    );
    let local = service.log.limits();
    let own = peers
        .get(&local.node)
        .ok_or_else(|| anyhow::anyhow!("node directory requires an explicit self endpoint"))?
        .clone();
    anyhow::ensure!(
        own.node == local.node
            && own.disk == local.disk
            && own.process == local.process
            && own.label == label,
        "node directory self endpoint disagrees with engine owner"
    );
    for peer in peers.values() {
        peer.validate_endpoint()?;
    }
    let mut namespaces = BTreeMap::new();
    for (namespace, class) in cells
        .iter()
        .map(|c| (&c.namespace, &c.class))
        .chain(declared.iter().map(|c| (&c.namespace, &c.class)))
    {
        namespace.require_mode(cellar_logic::NamespaceMode::Raft)?;
        anyhow::ensure!(
            namespace.config().schema_digest == class.schema_digest(),
            "registered schema disagrees with namespace"
        );
        namespaces
            .entry(namespace.config().cluster)
            .or_insert_with(|| namespace.clone());
    }
    let domain = service.log.domain();
    let deadline = domain.mono_ms()?.saturating_add(options.timeout_ms);
    let mut registrations = Vec::with_capacity(namespaces.len());
    let mut own_revision = 0;
    let mut rooted = None;
    for namespace in namespaces.into_values() {
        let registration = namespace
            .register_node_storage(&service.log, remaining(&domain, deadline)?)
            .await?;
        if let Some(authorization) = enrollment.as_mut() {
            **authorization = authorization
                .bind_storage(
                    service.log.storage_instance()?,
                    remaining(&domain, deadline)?,
                )
                .await?;
            let trust = authorization
                .trust_snapshot(remaining(&domain, deadline)?)
                .await?;
            service.transport.install_fleet_trust(trust.clone())?;
            anyhow::ensure!(
                trust.nodes().len() <= service.transport.peer_limit(),
                "fleet exceeds transport peer bound"
            );
            rooted = Some(trust);
        }
        let publication = registration
            .publication(own.clone(), remaining(&domain, deadline)?)
            .await?;
        let view = publication.publish(remaining(&domain, deadline)?).await?;
        own_revision = own_revision.max(view.revision());
        service.transport.accept_node_view(view, &own)?;
        // Cold managed members have only a self hint. Resolve exact rooted
        // process views before constructing their current membership routes;
        // periodic discovery then refreshes restarts without static addresses.
        if let Some(trust) = &rooted {
            for node in trust
                .nodes()
                .iter()
                .filter(|node| node.node() != local.node)
            {
                let result = registration
                    .observe_fleet_node(node, remaining(&domain, deadline)?)
                    .await;
                match result {
                    Ok(Some(view)) => {
                        let peer = view.peer().clone();
                        service.transport.accept_node_view(view, &peer)?;
                        peers.insert(peer.node, peer);
                    }
                    Ok(None) => (),
                    Err(error) => tracing::debug!(%error, "startup node view unavailable"),
                }
            }
        }
        registrations.push(registration);
    }
    Ok(Some(Directory {
        options: options.clone(),
        registrations,
        trusted: rooted.as_ref().map_or_else(
            || {
                peers
                    .values()
                    .cloned()
                    .map(DiscoveryNode::Configured)
                    .collect()
            },
            trusted_nodes,
        ),
        enrollment: enrollment
            .as_deref()
            .map(FleetHostAuthorization::trust_reader),
        rooted,
        namespace: 0,
        node: 0,
        next_at: domain.mono_ms()?,
        busy: false,
        own_revision,
    }))
}

enum DiscoveryError {
    Catalog(CatalogError),
    Transport(super::super::transport::TransportError),
}

struct PeerObservation {
    trusted: DiscoveryNode,
    result: Result<Option<NodeView>, DiscoveryError>,
}

pub(super) struct Observation {
    peers: Vec<PeerObservation>,
    trust: Option<Result<FleetTrustSnapshot, String>>,
    _metadata: Option<super::super::budget::Permit>,
}

fn trusted_nodes(trust: &FleetTrustSnapshot) -> Vec<DiscoveryNode> {
    trust
        .nodes()
        .iter()
        .filter(|node| node.permits_transport())
        .cloned()
        .map(|node| DiscoveryNode::Enrolled(Box::new(node)))
        .collect()
}

impl Directory {
    pub(super) fn authorization(&self, node: NodeId) -> Option<&FleetHostAuthorization> {
        self.rooted.as_ref()?.node(node)
    }

    pub(super) fn rooted_revision(&self) -> Option<u64> {
        self.rooted.as_ref().map(FleetTrustSnapshot::revision)
    }
}

impl Driver {
    pub(super) fn pump_directory(&mut self) {
        let Some(directory) = self.directory.as_mut() else {
            return;
        };
        let Ok(now) = self.log.domain().mono_ms() else {
            return;
        };
        if directory.busy || now < directory.next_at {
            return;
        }
        let metadata = if directory.enrollment.is_some() {
            let Some(permit) = self.control_budget.acquire(FLEET_READ_BYTES) else {
                directory.next_at = now.saturating_add(directory.options.poll_ms);
                return;
            };
            Some(permit)
        } else {
            None
        };
        directory.busy = true;
        directory.next_at = now.saturating_add(directory.options.poll_ms);
        let deadline = now.saturating_add(directory.options.timeout_ms);
        let count = directory.options.batch_nodes.min(
            directory
                .registrations
                .len()
                .saturating_mul(directory.trusted.len()),
        );
        let mut batch = Vec::with_capacity(count);
        for _ in 0..count {
            batch.push((
                directory.registrations[directory.namespace].clone(),
                directory.trusted[directory.node].clone(),
            ));
            directory.node += 1;
            if directory.node == directory.trusted.len() {
                directory.node = 0;
                directory.namespace = (directory.namespace + 1) % directory.registrations.len();
            }
        }
        let transport = self.transport.clone();
        let domain = self.log.domain();
        let enrollment = directory.enrollment.clone();
        let previous = directory.rooted.clone();
        self.jobs.push(Box::pin(async move {
            let trust = if let Some(enrollment) = enrollment {
                Some(
                    transport
                        .seed_control_until(deadline, async {
                            let timeout = remaining(&domain, deadline)
                                .map_err(|_| super::super::transport::TransportError::Timeout)?;
                            Ok(enrollment.refresh(previous.as_ref(), timeout).await)
                        })
                        .await
                        .map_err(|error| error.to_string())
                        .and_then(|result| result.map_err(|error| error.to_string())),
                )
            } else {
                None
            };
            let observations =
                futures_util::future::join_all(batch.into_iter().map(|(registration, trusted)| {
                    let transport = transport.clone();
                    let domain = domain.clone();
                    async move {
                        let result = transport
                            .seed_control_until(deadline, async {
                                let timeout = remaining(&domain, deadline).map_err(|_| {
                                    super::super::transport::TransportError::Timeout
                                })?;
                                Ok(match &trusted {
                                    DiscoveryNode::Configured(peer) => {
                                        registration.observe_node(peer, timeout).await
                                    }
                                    DiscoveryNode::Enrolled(node) => {
                                        registration.observe_fleet_node(node, timeout).await
                                    }
                                })
                            })
                            .await
                            .map_err(DiscoveryError::Transport)
                            .and_then(|result| result.map_err(DiscoveryError::Catalog));
                        PeerObservation { trusted, result }
                    }
                }))
                .await;
            Job::Directory(Box::new(Observation {
                peers: observations,
                trust,
                _metadata: metadata,
            }))
        }));
    }

    pub(super) fn on_directory(&mut self, observation: Observation) {
        if let Some(directory) = self.directory.as_mut() {
            directory.busy = false;
        }
        if let Some(result) = observation.trust {
            match result {
                Ok(trust) => {
                    let local = self.log.limits();
                    if trust
                        .node(local.node)
                        .is_none_or(|node| !node.permits_transport())
                    {
                        self.closing.store(true, Ordering::Release);
                        return;
                    }
                    match self.transport.install_fleet_trust(trust.clone()) {
                        Ok(()) => {
                            if let Some(directory) = &mut self.directory {
                                if directory.rooted_revision() != Some(trust.revision()) {
                                    directory.rooted = Some(trust.clone());
                                    directory.trusted = trusted_nodes(&trust);
                                    directory.node = directory
                                        .node
                                        .min(directory.trusted.len().saturating_sub(1));
                                }
                            }
                            self.peers.retain(|node, peer| {
                                trust
                                    .node(*node)
                                    .is_some_and(|entry| entry.matches_peer(peer))
                            });
                        }
                        Err(error) => tracing::debug!(%error, "Raft fleet trust cut refused"),
                    }
                }
                Err(error) => tracing::debug!(%error, "Raft fleet trust refresh unavailable"),
            }
        }
        for observation in observation.peers {
            let view = match observation.result {
                Ok(Some(view)) => view,
                // No observation supplies authority or invalidates a live
                // quorum. Keep the last accepted routes and retry boundedly.
                Ok(None) => continue,
                Err(DiscoveryError::Catalog(error)) => {
                    tracing::debug!(node = ?observation.trusted.node(), %error, "Raft node directory observation refused");
                    continue;
                }
                Err(DiscoveryError::Transport(error)) => {
                    tracing::debug!(node = ?observation.trusted.node(), %error, "Raft node directory observation cancelled");
                    continue;
                }
            };
            let peer = view.peer().clone();
            let local = self.log.limits();
            let newer_boot = self.directory.as_ref().is_some_and(|directory| {
                peer.process != local.process && view.revision() > directory.own_revision
            });
            if peer.node == local.node
                && (newer_boot || self.log.storage_instance().ok() != Some(view.storage_instance()))
            {
                // The shared closing flag immediately fences retained handles;
                // the host loop then joins every owned service and physical job.
                self.closing.store(true, Ordering::Release);
                return;
            }
            let trusted = match &observation.trusted {
                DiscoveryNode::Configured(peer) => peer,
                // The transport's installed rooted cut independently checks
                // registration identity and storage pin before accepting this.
                DiscoveryNode::Enrolled(_) => &peer,
            };
            match self.transport.accept_node_view(view, trusted) {
                Ok(()) => {
                    self.peers.insert(peer.node, peer);
                }
                Err(error) => {
                    tracing::debug!(node = ?peer.node, %error, "Raft node process refresh refused")
                }
            }
        }
    }
}

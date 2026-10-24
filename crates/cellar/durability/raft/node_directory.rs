// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cluster-wide storage pins and advisory process endpoints. Neither record
//! grants group membership, seeding, a vote or a fresh-quorum proof.
use super::*;
use crate::raft::transport_options::RaftPeerNode;
use cellar_logic::consensus::{EngineInstanceId, ProcessGeneration};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{future::Future, time::Duration};

const NODE_PIN: u8 = 6;
const NODE_VIEW: u8 = 7;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pin {
    cluster: [u8; 16],
    node: [u8; 16],
    disk: [u8; 16],
    storage_instance: [u8; 16],
}

impl Pin {
    fn validate(
        &self,
        cluster: ClusterId,
        node: NodeId,
        disk: DiskLineage,
    ) -> Result<EngineInstanceId, CatalogError> {
        if self.cluster != cluster.bytes() || self.node != node.bytes() || self.disk != disk.bytes()
        {
            return Err(CatalogError::Conflict("node storage registration"));
        }
        EngineInstanceId::new(self.storage_instance)
            .map_err(|_| CatalogError::Corrupt("node storage instance"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct View {
    pin: Pin,
    revision: String,
    process: [u8; 16],
    boot_attempt: [u8; 16],
    protocol: [u8; 32],
    label: String,
    server_name: String,
    control_address: std::net::SocketAddr,
    data_address: std::net::SocketAddr,
    query_address: std::net::SocketAddr,
}

/// Exact-key discovery observation, without any group authority. Its storage
/// identity was checked against the immutable pin and configured node/disk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeView {
    cluster: ClusterId,
    revision: u64,
    instance: EngineInstanceId,
    boot_attempt: [u8; 16],
    peer: RaftPeerNode,
}

impl NodeView {
    pub fn cluster(&self) -> ClusterId {
        self.cluster
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn storage_instance(&self) -> EngineInstanceId {
        self.instance
    }

    pub fn peer(&self) -> &RaftPeerNode {
        &self.peer
    }
}

impl View {
    fn decode(
        &self,
        cluster: ClusterId,
        expected: &RaftPeerNode,
        pin: &Pin,
    ) -> Result<NodeView, CatalogError> {
        let instance = self.pin.validate(cluster, expected.node, expected.disk)?;
        if self.pin != *pin
            || self.protocol != crate::application::consensus::protocol::protocol_identity()
            || self.label != expected.label
            || self.server_name != expected.server_name
            || self.boot_attempt == [0; 16]
        {
            return Err(CatalogError::Conflict("node process registration"));
        }
        let revision = self
            .revision
            .parse::<u64>()
            .map_err(|_| CatalogError::Corrupt("node view revision"))?;
        if revision == 0 || revision.to_string() != self.revision {
            return Err(CatalogError::Corrupt("node view revision"));
        }
        let peer = RaftPeerNode {
            node: expected.node,
            disk: expected.disk,
            process: ProcessGeneration::new(self.process)
                .map_err(|_| CatalogError::Corrupt("node process identity"))?,
            label: self.label.clone(),
            server_name: self.server_name.clone(),
            control_address: self.control_address,
            data_address: self.data_address,
            query_address: self.query_address,
        };
        peer.validate_endpoint()
            .map_err(|_| CatalogError::Corrupt("node process endpoints"))?;
        Ok(NodeView {
            cluster,
            revision,
            instance,
            boot_attempt: self.boot_attempt,
            peer,
        })
    }
}

/// Bound to one exact live engine owner. Closing that engine revokes further
/// operations; a reopened engine does not revive an old registration object.
#[derive(Clone)]
pub struct NodeRegistration {
    inner: Arc<Inner>,
    pin: Pin,
    cluster: ClusterId,
    log: ReplicaLog,
}

/// Retain this object when retrying an ambiguous publication. The synced
/// revision and boot attempt stay identical; discovery never invents a vote.
#[derive(Clone)]
pub struct NodePublication {
    registration: NodeRegistration,
    peer: RaftPeerNode,
    view: View,
}

impl NamespaceCatalog {
    pub async fn register_node_storage(
        &self,
        log: &ReplicaLog,
        timeout: Duration,
    ) -> Result<NodeRegistration, CatalogError> {
        self.require_mode(NamespaceMode::Raft)?;
        let instance = log.storage_instance().map_err(CatalogError::Log)?;
        let local = log.limits();
        let registration = NodeRegistration {
            inner: self.inner.clone(),
            pin: Pin {
                cluster: self.config.cluster.bytes(),
                node: local.node.bytes(),
                disk: local.disk.bytes(),
                storage_instance: instance.bytes(),
            },
            cluster: self.config.cluster,
            log: log.clone(),
        };
        let deadline = registration.deadline(timeout)?;
        let key = registration.key(local.node, "pin");
        let bytes = wire::encode(NODE_PIN, &registration.pin, registration.inner.limit)?;
        let mut found = registration
            .until(deadline, registration.inner.read(&key))
            .await?;
        if found.is_none() {
            let view_key = registration.key(local.node, "view");
            if registration
                .until(deadline, registration.inner.read(&view_key))
                .await?
                .is_some()
                && registration
                    .until(deadline, registration.inner.read(&key))
                    .await?
                    .is_none()
            {
                return Err(CatalogError::Corrupt("process view has no storage pin"));
            }
        }
        if found.is_none() {
            found = Some(
                registration
                    .until(
                        deadline,
                        registration.inner.create_and_observe(&key, &bytes),
                    )
                    .await?,
            );
        }
        let found = found.ok_or(CatalogError::Unconfirmed)?;
        let pin: Pin = wire::decode(NODE_PIN, &found, registration.inner.limit)?;
        let previous = pin.validate(registration.cluster, local.node, local.disk)?;
        if previous != instance {
            return Err(CatalogError::LostStableState);
        }
        Ok(registration)
    }

    pub(crate) async fn require_node_storage(
        &self,
        log: &ReplicaLog,
    ) -> Result<NodeRegistration, CatalogError> {
        self.register_node_storage(log, Duration::from_millis(self.inner.node_timeout_ms))
            .await
    }
}

impl NodeRegistration {
    fn check(&self) -> Result<(), CatalogError> {
        let local = self.log.limits();
        let instance = self.log.storage_instance().map_err(CatalogError::Log)?;
        if self.pin.validate(self.cluster, local.node, local.disk)? != instance {
            return Err(CatalogError::LostStableState);
        }
        Ok(())
    }

    fn key(&self, node: NodeId, leaf: &str) -> Path {
        Path::from(format!(
            "{}raft/{}/nodes/{}/{}",
            self.inner.prefix,
            hex::encode(self.cluster.bytes()),
            hex::encode(node.bytes()),
            leaf
        ))
    }

    fn deadline(&self, timeout: Duration) -> Result<u64, CatalogError> {
        self.check()?;
        if timeout.is_zero() || timeout > Duration::from_secs(60) {
            return Err(CatalogError::Invalid("node directory timeout"));
        }
        self.log
            .domain()
            .mono_ms()
            .map_err(|_| CatalogError::Unconfirmed)?
            .checked_add(timeout.as_millis() as u64)
            .ok_or(CatalogError::Limit)
    }

    async fn until<T>(
        &self,
        deadline: u64,
        future: impl Future<Output = Result<T, CatalogError>>,
    ) -> Result<T, CatalogError> {
        self.check()?;
        let domain = self.log.domain();
        if domain.mono_ms().map_err(|_| CatalogError::Unconfirmed)? >= deadline {
            return Err(CatalogError::Unconfirmed);
        }
        let timer = domain
            .sleep_until(deadline)
            .map_err(|_| CatalogError::Unconfirmed)?;
        let result = crate::asyncrt::select! { _=timer=>Err(CatalogError::Unconfirmed), result=future=>result }?;
        self.check()?;
        Ok(result)
    }

    pub async fn publication(
        &self,
        peer: RaftPeerNode,
        timeout: Duration,
    ) -> Result<NodePublication, CatalogError> {
        let deadline = self.deadline(timeout)?;
        peer.validate_endpoint()
            .map_err(|_| CatalogError::Invalid("node process endpoints"))?;
        let local = self.log.limits();
        if peer.node != local.node || peer.disk != local.disk || peer.process != local.process {
            return Err(CatalogError::Conflict("publication engine owner"));
        }
        let revision = self
            .until(deadline, async {
                self.log
                    .reserve_node_revision()
                    .await
                    .map_err(CatalogError::Log)
            })
            .await?;
        let mut hash = Sha256::new();
        hash.update(b"cellar-node-boot-attempt-1");
        hash.update(self.pin.storage_instance);
        hash.update(self.cluster.bytes());
        hash.update(peer.process.bytes());
        hash.update(revision.to_be_bytes());
        let digest = hash.finalize();
        let mut boot_attempt = [0; 16];
        boot_attempt.copy_from_slice(&digest[..16]);
        if boot_attempt == [0; 16] {
            return Err(CatalogError::Invalid("node boot attempt"));
        }
        let view = View {
            pin: self.pin.clone(),
            revision: revision.to_string(),
            process: peer.process.bytes(),
            boot_attempt,
            protocol: crate::application::consensus::protocol::protocol_identity(),
            label: peer.label.clone(),
            server_name: peer.server_name.clone(),
            control_address: peer.control_address,
            data_address: peer.data_address,
            query_address: peer.query_address,
        };
        Ok(NodePublication {
            registration: self.clone(),
            peer,
            view,
        })
    }

    pub async fn observe_node(
        &self,
        expected: &RaftPeerNode,
        timeout: Duration,
    ) -> Result<Option<NodeView>, CatalogError> {
        let deadline = self.deadline(timeout)?;
        expected
            .validate_endpoint()
            .map_err(|_| CatalogError::Invalid("trusted node endpoint"))?;
        let pin_path = self.key(expected.node, "pin");
        let view_path = self.key(expected.node, "view");
        let pin = self.until(deadline, self.inner.read(&pin_path)).await?;
        let view = self.until(deadline, self.inner.read(&view_path)).await?;
        match (pin, view) {
            (None, None) => Ok(None),
            (Some(pin), None) => {
                let pin: Pin = wire::decode(NODE_PIN, &pin, self.inner.limit)?;
                pin.validate(self.cluster, expected.node, expected.disk)?;
                Ok(None)
            }
            (None, Some(_)) => Err(CatalogError::Corrupt("process view has no storage pin")),
            (Some(pin), Some(view)) => {
                let pin: Pin = wire::decode(NODE_PIN, &pin, self.inner.limit)?;
                pin.validate(self.cluster, expected.node, expected.disk)?;
                let view: View = wire::decode(NODE_VIEW, &view, self.inner.limit)?;
                Ok(Some(view.decode(self.cluster, expected, &pin)?))
            }
        }
    }
}

impl NodePublication {
    pub async fn publish(&self, timeout: Duration) -> Result<NodeView, CatalogError> {
        let registration = &self.registration;
        let deadline = registration.deadline(timeout)?;
        let pin_path = registration.key(self.peer.node, "pin");
        let pin = registration
            .until(deadline, registration.inner.read(&pin_path))
            .await?
            .ok_or(CatalogError::Corrupt("publication storage pin missing"))?;
        let pin: Pin = wire::decode(NODE_PIN, &pin, registration.inner.limit)?;
        if pin != registration.pin {
            return Err(CatalogError::Conflict("publication storage pin changed"));
        }
        let path = registration.key(self.peer.node, "view");
        let desired = wire::encode(NODE_VIEW, &self.view, registration.inner.limit)?;
        let selected = self
            .view
            .decode(registration.cluster, &self.peer, &registration.pin)?;
        for _ in 0..8 {
            let previous = registration
                .until(deadline, registration.inner.read_versioned(&path))
                .await?;
            let mode = if let Some((bytes, version)) = previous {
                let view: View = wire::decode(NODE_VIEW, &bytes, registration.inner.limit)?;
                let observed = view.decode(registration.cluster, &self.peer, &registration.pin)?;
                if view == self.view {
                    return Ok(observed);
                }
                if observed.peer.process == self.peer.process
                    || observed.revision >= selected.revision
                {
                    return Err(CatalogError::Conflict("superseded or reused node process"));
                }
                PutMode::Update(version)
            } else {
                PutMode::Create
            };
            registration.check()?;
            let written = registration
                .until(deadline, async {
                    Ok(registration
                        .inner
                        .store
                        .put_opts(&path, desired.clone().into(), PutOptions::from(mode))
                        .await)
                })
                .await?;
            let observed = registration
                .until(deadline, registration.inner.read(&path))
                .await?;
            if written.is_ok() && observed.as_ref() != Some(&desired) {
                return Err(CatalogError::Conflict(
                    "conditional node view/read disagreement",
                ));
            }
            if let Some(bytes) = observed {
                let view: View = wire::decode(NODE_VIEW, &bytes, registration.inner.limit)?;
                let current = view.decode(registration.cluster, &self.peer, &registration.pin)?;
                if view == self.view {
                    return Ok(current);
                }
                if current.revision >= selected.revision
                    || current.peer.process == self.peer.process
                {
                    return Err(CatalogError::Conflict("concurrent node publication"));
                }
                // A late older conditional write may win before our CAS. It
                // cannot have reserved a revision above this live engine fence.
                // Retry the exact immutable payload against its new version.
            }
            match written {
                Ok(_)
                | Err(
                    object_store::Error::AlreadyExists { .. }
                    | object_store::Error::Precondition { .. },
                ) => (),
                Err(error) => return Err(CatalogError::Storage(error)),
            }
        }
        Err(CatalogError::Unconfirmed)
    }
}

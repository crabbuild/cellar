// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Controller-supplied boot identities, private credentials and fixed capacities.
use super::*;
use cellar::sdk::fleet_operations::{RaftFleetConnection, RaftFleetNamespaceScope};
use std::net::{IpAddr, SocketAddr};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Configuration {
    pub retained: bool,
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    pub access: String,
    pub secret: String,
    pub bucket_prefix: String,
    pub connection: RaftFleetConnection,
    pub principal: String,
    pub token: String,
    pub node: [u8; 16],
    pub disk: [u8; 16],
    pub process: [u8; 16],
    pub label: String,
    pub server_name: String,
    pub bind: IpAddr,
    pub advertise: IpAddr,
    pub peer_ports: [u16; 3],
    pub api_port: u16,
    pub ca: PathBuf,
    pub certificate: PathBuf,
    pub key: PathBuf,
    pub fence_public: [u8; 32],
}

impl Configuration {
    pub fn load(path: &std::path::Path, class: &RaftClass) -> Result<Self> {
        let bytes = std::fs::read(path)?;
        ensure!(
            bytes.len() <= 256 * 1024,
            "managed voter configuration too large"
        );
        let config: Self = serde_json::from_slice(&bytes)?;
        ensure!(
            !config.access.is_empty() && !config.secret.is_empty(),
            "explicit S3 credentials required"
        );
        ensure!(
            config.connection.namespaces.len() == 1,
            "one declared namespace required"
        );
        let namespace = &config.connection.namespaces[0];
        ensure!(
            namespace.managed && namespace.schema_digest == hex::encode(class.schema_digest()),
            "compiled Counter contract must match the existing managed namespace"
        );
        ensure!(
            config.token.len() == 64
                && config
                    .token
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "private 32-byte hexadecimal bearer required"
        );
        ensure!(
            !config.advertise.is_unspecified() && !config.advertise.is_multicast(),
            "explicit peer advertisement required"
        );
        let mut ports = config.peer_ports.to_vec();
        ports.push(config.api_port);
        ports.sort_unstable();
        ensure!(
            ports[0] != 0 && ports.windows(2).all(|p| p[0] != p[1]),
            "distinct fixed ports required"
        );
        for path in [&config.ca, &config.certificate, &config.key] {
            ensure!(path.is_absolute(), "absolute private TLS paths required");
        }
        NodeId::new(config.node)?;
        DiskLineage::new(config.disk)?;
        ProcessGeneration::new(config.process)?;
        CellGroupPermanentFenceAuthority::new(config.fence_public)?;
        Ok(config)
    }

    pub fn namespace(&self) -> &cellar::sdk::fleet_operations::RaftFleetNamespace {
        &self.connection.namespaces[0]
    }

    pub fn rooted(&self) -> bool {
        self.namespace().image_storage == RaftImageStorage::Extents
    }

    pub fn bucket(&self) -> Result<cellar::bucket::Bucket> {
        crate::managed_store::open(
            &self.endpoint,
            &self.bucket,
            &self.prefix,
            &self.access,
            &self.secret,
            &self.bucket_prefix,
        )
    }

    pub fn store(&self, bucket: &cellar::bucket::Bucket) -> Arc<dyn object_store::ObjectStore> {
        match self.namespace().scope {
            RaftFleetNamespaceScope::Bucket => bucket.raft_store(),
            RaftFleetNamespaceScope::StoreRoot => bucket.cas_store.clone(),
        }
    }

    pub fn peer(&self) -> Result<RaftPeerNode> {
        Ok(RaftPeerNode {
            node: NodeId::new(self.node)?,
            disk: DiskLineage::new(self.disk)?,
            process: ProcessGeneration::new(self.process)?,
            label: self.label.clone(),
            server_name: self.server_name.clone(),
            control_address: SocketAddr::new(self.advertise, self.peer_ports[0]),
            data_address: SocketAddr::new(self.advertise, self.peer_ports[1]),
            query_address: SocketAddr::new(self.advertise, self.peer_ports[2]),
        })
    }
}

pub(super) fn recovery_pool() -> Result<CellGroupRecoveryPool> {
    Ok(CellGroupRecoveryPool::new(CellGroupRecoveryLimits {
        max_cells: 4,
        max_image_bytes: 1024 * 1024,
        max_restore_bytes: 4 * 1024 * 1024,
        ..Default::default()
    })?)
}

pub(super) fn limits() -> CellGroupNodeLimits {
    CellGroupNodeLimits {
        payloads: None,
        namespaces: CellGroupHostLimits {
            max_namespaces: 1,
            namespace_bytes: 16 * 1024 * 1024,
        },
        controller: CellGroupControllerLimits {
            max_groups: 2,
            max_parallel_apply: 2,
            retry_interval_ms: 5,
            max_commands: 2,
            max_command_bytes: 64 * 1024,
            checkpoints: None,
        },
        sql: CellSqlLimits {
            workers: 2,
            queue_per_worker: 4,
            max_pending_cells: 8,
            group_pending_cells: 8,
            node_bytes: 512 * 1024 * 1024,
            group_bytes: 512 * 1024 * 1024,
        },
        provisioning: CellGroupProvisionLimits {
            max_preparations: 1,
            max_preparation_bytes: 512 * 1024 * 1024,
            max_parallel_preparations: 1,
            max_waiters: 2,
            peer_revision: 1,
        },
        directory: Default::default(),
        dispatch: CellDispatchLimits {
            max_cached_routes: 32,
            route_cache_bytes: 256 * 1024,
            max_invocations: 8,
            retained_bytes: 1024 * 1024,
            max_input_bytes: 1024,
        },
        manifest: ManifestLimits {
            max_cells: 32,
            max_depth: 16,
        },
        entries: Default::default(),
        manifest_pages: 1024,
        manifest_bytes: 16 * 1024 * 1024,
        retained_roots: 16,
        max_effect_rows: 100,
        checkpoints: Some(CellGroupCheckpointLimits {
            max_sources: 2,
            max_retained_bytes: 32 * 1024 * 1024,
            max_cells: 32,
            max_transfer_bytes: 32 * 1024 * 1024,
            max_chunk_bytes: 4093,
        }),
        archive: Some(CellGroupNodeArchiveOptions {
            io: Default::default(),
            uploads: Default::default(),
            tails: Default::default(),
        }),
    }
}

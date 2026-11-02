// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::consensus::ReplicaIdentity;
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    prefix::PrefixStore,
    ObjectStore,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Peer {
    pub host: String,
    pub disk: [u8; 16],
    pub process: [u8; 16],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    pub rooted: bool,
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    pub access: String,
    pub secret: String,
    pub token: String,
    pub peers: [Vec<Peer>; 2],
    pub fence_public: [u8; 32],
    pub certificate: Vec<u8>,
    pub recovery: Vec<u8>,
}

pub(super) struct Options {
    pub config: PathBuf,
    pub data: PathBuf,
    pub action: String,
    pub node: usize,
    pub generation: usize,
    pub retained: bool,
}

impl Options {
    pub fn load() -> Result<Self> {
        let value =
            |name: &str, fallback: &str| std::env::var(name).unwrap_or_else(|_| fallback.into());
        let options = Self {
            config: value("CELLAR_GROUP_CONFIG", "/config/config.json").into(),
            data: value("CELLAR_GROUP_DATA", "/data").into(),
            action: value("CELLAR_GROUP_ACTION", "node"),
            node: value("CELLAR_GROUP_NODE", "0").parse()?,
            generation: value("CELLAR_GROUP_GENERATION", "0").parse()?,
            retained: value("CELLAR_GROUP_RETAINED", "false").parse()?,
        };
        ensure!(
            options.node < 3 && options.generation < 2,
            "invalid disposable voter identity"
        );
        ensure!(
            options.config.is_absolute() && options.data.is_absolute(),
            "absolute private paths required"
        );
        Ok(options)
    }
}

impl Config {
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let bytes = std::fs::read(path)?;
        ensure!(bytes.len() <= 256 * 1024, "config too large");
        let config: Self = serde_json::from_slice(&bytes)?;
        ensure!(
            config.endpoint == "http://rustfs:9000"
                || config.endpoint.starts_with("http://127.0.0.1:"),
            "isolated RustFS endpoint required"
        );
        ensure!(
            config.prefix.starts_with("cellar-cg-dr-") && config.token.len() == 64,
            "isolated run required"
        );
        for peers in &config.peers {
            ensure!(peers.len() == 3, "three peers required");
            for peer in peers {
                let _: std::net::Ipv4Addr = peer.host.parse()?;
            }
        }
        Ok(config)
    }

    pub fn store(&self) -> Result<Arc<dyn ObjectStore>> {
        let store = AmazonS3Builder::new()
            .with_endpoint(&self.endpoint)
            .with_bucket_name(&self.bucket)
            .with_region("us-east-1")
            .with_access_key_id(&self.access)
            .with_secret_access_key(&self.secret)
            .with_allow_http(true)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()?;
        Ok(Arc::new(PrefixStore::new(store, self.prefix.as_str())))
    }

    pub fn replica(&self, generation: usize, node: usize) -> Result<ReplicaIdentity<GroupHistory>> {
        let peer = &self.peers[generation][node];
        Ok(ReplicaIdentity {
            scope: history(generation)?,
            node: node_id(generation, node)?,
            member: member(generation, node)?,
            disk: DiskLineage::new(peer.disk)?,
            process: ProcessGeneration::new(peer.process)?,
        })
    }

    pub async fn directory(
        &self,
        domain: &ExecutionDomain,
        schema: [u8; 32],
    ) -> Result<RangeDirectory> {
        let catalog = Catalog::new(self.store()?, Default::default())?;
        let namespace = domain
            .run(catalog.existing_namespace(namespace(schema)?))
            .await??;
        Ok(RangeDirectory::new(namespace, Default::default())?)
    }

    pub fn spec(&self, generation: usize) -> Result<CellGroupBirthSpec> {
        Ok(CellGroupBirthSpec {
            history: history(generation)?,
            range: CellRange::all(),
            epoch: RouteEpoch::new(generation as u64 + 1)?,
            creation: ProposalId::new([5 + generation as u8; 16])?,
            voters: (0..3)
                .map(|n| {
                    Ok(InitialMember {
                        member: member(generation, n)?,
                        node: node_id(generation, n)?,
                    })
                })
                .collect::<Result<_>>()?,
            learners: vec![],
        })
    }

    pub fn log_options(&self, options: &Options, mode: LogOpenMode) -> Result<ReplicaLogOptions> {
        let identity = self.replica(options.generation, options.node)?;
        Ok(ReplicaLogOptions::new(
            options.data.join("log"),
            identity.node,
            identity.disk,
            identity.process,
            mode,
        ))
    }
}

pub(super) fn namespace(schema: [u8; 32]) -> Result<NamespaceConfig> {
    Ok(NamespaceConfig::new(
        ClusterId::new([1; 16])?,
        NamespaceId::new([2; 16])?,
        NamespaceMode::Raft,
        schema,
    ))
}

pub(super) fn history(generation: usize) -> Result<GroupHistory> {
    Ok(GroupHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        group: GroupId::new([if generation == 0 { 3 } else { 70 }; 16])?,
        incarnation: IncarnationId::new([if generation == 0 { 4 } else { 71 }; 16])?,
    })
}

pub(super) fn node_id(generation: usize, node: usize) -> Result<NodeId> {
    Ok(NodeId::new(
        [node as u8 + if generation == 0 { 1 } else { 111 }; 16],
    )?)
}

pub(super) fn member(generation: usize, node: usize) -> Result<MemberId> {
    Ok(MemberId::new(
        node as u64 + if generation == 0 { 1 } else { 11 },
    )?)
}

pub(super) fn cell(generation: usize, name: &str) -> Result<CellHistory> {
    let group = history(generation)?;
    Ok(CellHistory {
        cluster: group.cluster,
        namespace: group.namespace,
        cell: CellId::new(name)?,
        incarnation: IncarnationId::new([if generation == 0 { 8 } else { 71 }; 16])?,
    })
}

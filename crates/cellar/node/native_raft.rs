// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Compiled-application Raft startup at the process boundary.
use crate::application::consensus::{
    RaftCells, RaftCellsConfig, RaftCellsServices, RaftTls, VersionKeys,
};
use futures_util::future::BoxFuture;
use std::{path::PathBuf, sync::Arc};

/// Actual process identity and persistent storage selected by the node CLI.
/// Prepare peer listeners, TLS, catalog storage and explicit Raft policies here.
/// This context grants no authority to initialize a retained group history.
pub struct NativeRaftNode {
    pub node: String,
    pub data_dir: PathBuf,
}

/// Explicit resources for one production cell host. The daemon supplies the
/// application instance from its active deployment, rather than constructing a
/// second instance for consensus. Create/resume and member placement stay explicit.
pub struct NativeRaftCells {
    pub config: RaftCellsConfig,
    pub listeners: super::NodeRaftListeners,
    pub tls: RaftTls,
    pub store: Arc<dyn object_store::ObjectStore>,
    pub versions: VersionKeys,
}

type Prepare =
    Box<dyn FnOnce(NativeRaftNode) -> BoxFuture<'static, anyhow::Result<NativeRaftCells>> + Send>;
type Bind = Box<dyn FnOnce(RaftCells) -> anyhow::Result<()> + Send>;

pub(super) struct Startup {
    pub script: String,
    pub prepare: Prepare,
    pub bind: Bind,
}

impl Startup {
    pub async fn start(
        self,
        app: &super::AppHandle,
        data_dir: PathBuf,
        node: String,
        shared_peer_auth: bool,
    ) -> anyhow::Result<()> {
        let application = app
            .runtime
            .as_ref()
            .and_then(|runtime| {
                runtime
                    .generation()
                    .native_applications
                    .get(&self.script)
                    .cloned()
            })
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Raft application {} is absent from the active deployment",
                    self.script
                )
            })?;
        let prepared = (self.prepare)(NativeRaftNode {
            node: node.clone(),
            data_dir: data_dir.clone(),
        })
        .await?;
        let local_node = prepared.config.infrastructure.log.node;
        let remote_routes = prepared
            .config
            .peers
            .iter()
            .any(|peer| peer.node != local_node)
            || prepared
                .config
                .query_origins
                .iter()
                .any(|origin| origin.peer.node != local_node)
            || prepared
                .config
                .host
                .inspection_origins
                .iter()
                .any(|origin| origin.peer.node != local_node);
        anyhow::ensure!(
            !remote_routes || shared_peer_auth,
            "multi-node Raft CLI startup requires a fleet bucket for shared peer authentication"
        );
        let services = crate::asyncrt::services();
        let service = super::raft_startup::Startup {
            domain: services
                .domain_token()
                .ok_or(crate::asyncrt::DomainClosed)?,
            data_dir: &data_dir,
            node: &node,
            auth: app.peer_auth.clone(),
        }
        .start(
            prepared.config,
            RaftCellsServices {
                listeners: prepared.listeners,
                tls: prepared.tls,
                store: prepared.store,
                application,
                versions: prepared.versions,
            },
        )
        .await?;
        (self.bind)(service)
    }
}

// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::cell_groups::{ManifestPageStore, PreparedGroupManifest};
use crate::raft::transport::{GroupRoute as PhysicalRoute, RaftPeer};
use cellar_logic::consensus::ReplicaIdentity;

#[derive(Clone)]
pub(super) struct Prepared {
    pub applier: CellGroupApplier,
    pub current: PreparedGroupManifest,
}

pub(super) async fn recover(
    directory: RangeDirectory,
    route: GroupRoute,
    config: Arc<Config>,
    runtime: CellGroupRuntime,
    existing: Option<Prepared>,
) -> Result<CellGroupRegistration, CellGroupControllerError> {
    let _parallel = config
        .parallel
        .acquire()
        .await
        .map_err(|_| CellGroupControllerError::Closed)?;
    let birth = directory
        .materialize_selected_group(&route)
        .await
        .map_err(catalog_error)?;
    // Check complete trusted placement before creating genesis or page files.
    let mut peers = Vec::new();
    for member in birth.voters().iter().chain(birth.learners()) {
        let node = config
            .settings
            .peers
            .iter()
            .find(|p| p.node == member.node)
            .ok_or(CellGroupControllerError::Invalid(
                "unconfigured birth member",
            ))?;
        peers.push(RaftPeer {
            replica: ReplicaIdentity {
                scope: route.history,
                member: member.member,
                node: node.node,
                disk: node.disk,
                process: node.process,
            },
            label: node.label.clone(),
            server_name: node.server_name.clone(),
            control_address: node.control_address,
            data_address: node.data_address,
            query_address: node.query_address,
        });
    }
    let permit = directory
        .bootstrap_group_member(&birth, runtime.log())
        .await
        .map_err(catalog_error)?;
    let prepared = match existing {
        Some(prepared) => prepared,
        None => {
            let config = config.clone();
            let group = permit.group().clone();
            runtime
                .log()
                .domain()
                .blocking(move || {
                    let settings = &config.settings;
                    let history = group.identity().scope;
                    let path = settings.directory.join(format!(
                        "{}-{}-{}-{}",
                        hex::encode(history.cluster.bytes()),
                        hex::encode(history.namespace.bytes()),
                        hex::encode(history.group.bytes()),
                        hex::encode(history.incarnation.bytes())
                    ));
                    let pages = ManifestPageStore::open(
                        &path,
                        settings.filesystem.clone(),
                        settings.max_pages,
                        settings.max_page_bytes,
                        settings.max_retained_roots,
                    )
                    .map_err(crate::raft::execution::storage)?;
                    let (applier, current) = CellGroupApplier::recover(
                        group,
                        pages,
                        settings.images.clone(),
                        settings.manifest,
                        settings.entries,
                        settings.max_effect_rows,
                    )?;
                    Ok::<_, ExecutionError>(Prepared { applier, current })
                })
                .map_err(|_| CellGroupControllerError::Closed)?
                .await
                .map_err(|_| CellGroupControllerError::Closed)?
                .map_err(apply_error)?
        }
    };
    Ok(CellGroupRegistration {
        permit,
        current: prepared.current,
        applier: prepared.applier,
        transport: PhysicalRoute {
            scope: route.history,
            schema_digest: route.schema_digest,
            revision: config.settings.peer_revision,
            peers,
        },
    })
}

// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::{
    cell_groups::{GroupManifest, TransferChildQuorum},
    fleet_catalog::ManagedTransitionPermit,
};
use serde::{Deserialize, Serialize};

const CHILD: u8 = 205;
const SET: u8 = 206;
const CHILDREN: &str = "transfer-child-quorums";
const SETS: &str = "transfer-child-quorum-sets";

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ChildQuorum {
    marker: GroupManifest,
    member: u64,
    node: [u8; 16],
    disk: [u8; 16],
    engine: [u8; 16],
}

impl From<TransferChildQuorum> for ChildQuorum {
    fn from(proof: TransferChildQuorum) -> Self {
        let (marker, leader) = proof.into_parts();
        Self {
            marker,
            member: leader.member.get(),
            node: leader.node.bytes(),
            disk: leader.disk.bytes(),
            engine: leader.engine.bytes(),
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ChildQuorumSet {
    namespace: [u8; 32],
    request: [u8; 32],
    children: Vec<[u8; 32]>,
}

impl RangeDirectory {
    /// Root one durable fresh-term witness for every child in plan order.
    /// The selected set records installation. The parent routes stay selected.
    pub async fn record_transfer_child_quorums(
        &self,
        plan: &DirectoryMutation,
        proofs: Vec<TransferChildQuorum>,
    ) -> Result<TransitionJournal, CatalogError> {
        self.unmanaged_birth()?;
        self.record_transfer_child_quorums_inner(plan, proofs, None)
            .await
    }

    pub(crate) async fn record_managed_transfer_child_quorums(
        &self,
        plan: &DirectoryMutation,
        proofs: Vec<TransferChildQuorum>,
        permit: &ManagedTransitionPermit,
    ) -> Result<TransitionJournal, CatalogError> {
        permit.check(self, plan)?;
        self.record_transfer_child_quorums_inner(plan, proofs, Some(permit))
            .await
    }

    async fn record_transfer_child_quorums_inner(
        &self,
        plan: &DirectoryMutation,
        proofs: Vec<TransferChildQuorum>,
        permit: Option<&ManagedTransitionPermit>,
    ) -> Result<TransitionJournal, CatalogError> {
        let (_, children) = self.transition_routes(plan)?;
        if proofs.len() != children.len() {
            return Err(CatalogError::Invalid(
                "incomplete transfer child quorum set",
            ));
        }
        let digest = self
            .bounded(async {
                let mut set = ChildQuorumSet {
                    namespace: self.namespace.root_digest,
                    request: plan.digest(),
                    children: Vec::with_capacity(children.len()),
                };
                for (route, proof) in children.iter().zip(proofs) {
                    let proof = ChildQuorum::from(proof);
                    self.check_child_quorum(plan, route, &proof, permit).await?;
                    let bytes = record::encode(CHILD, &proof, wire::LIMIT)?;
                    let digest = record::digest(&bytes);
                    self.immutable(CHILDREN, digest, &bytes).await?;
                    set.children.push(digest);
                }
                let bytes = record::encode(SET, &set, wire::LIMIT)?;
                let digest = record::digest(&bytes);
                self.immutable(SETS, digest, &bytes).await?;
                Ok(digest)
            })
            .await?;
        self.record_verified_child_quorums(plan, digest, permit)
            .await
    }

    /// Recover the exact child markers selected by the installation CAS.
    /// Revalidates the physical install bindings and keeps admission fenced.
    pub async fn installed_transfer_children(
        &self,
        plan: &DirectoryMutation,
    ) -> Result<Option<Vec<GroupManifest>>, CatalogError> {
        self.installed_transfer_children_inner(plan, None).await
    }

    pub(super) async fn installed_transfer_children_inner(
        &self,
        plan: &DirectoryMutation,
        permit: Option<&ManagedTransitionPermit>,
    ) -> Result<Option<Vec<GroupManifest>>, CatalogError> {
        match permit {
            Some(permit) => permit.check(self, plan)?,
            None => self.unmanaged_birth()?,
        }
        let (_, children) = self.transition_routes(plan)?;
        self.bounded(async {
            let (view, _) = self.current().await?;
            let journal = self
                .matching_transition(&view.head, plan)
                .await?
                .ok_or(CatalogError::Fenced)?;
            let Some(digest) = journal.installed_children() else {
                return Ok(None);
            };
            let bytes = self.read_child_quorum_record(SETS, digest).await?;
            let set: ChildQuorumSet = record::decode(SET, &bytes, wire::LIMIT)?;
            if set.namespace != self.namespace.root_digest
                || set.request != plan.digest()
                || set.children.len() != children.len()
            {
                return Err(CatalogError::Corrupt("transfer child quorum set binding"));
            }
            let mut markers = Vec::with_capacity(children.len());
            for (route, digest) in children.iter().zip(set.children) {
                let bytes = self.read_child_quorum_record(CHILDREN, digest).await?;
                let proof: ChildQuorum = record::decode(CHILD, &bytes, wire::LIMIT)?;
                self.check_child_quorum(plan, route, &proof, permit).await?;
                markers.push(proof.marker);
            }
            Ok(Some(markers))
        })
        .await
    }

    async fn read_child_quorum_record(
        &self,
        category: &str,
        digest: [u8; 32],
    ) -> Result<Vec<u8>, CatalogError> {
        let bytes = self
            .read(&format!("{category}/{}", hex::encode(digest)))
            .await?
            .ok_or(CatalogError::Corrupt(
                "missing transfer child quorum record",
            ))?;
        if record::digest(&bytes) != digest {
            return Err(CatalogError::Corrupt("transfer child quorum digest"));
        }
        Ok(bytes)
    }

    async fn check_child_quorum(
        &self,
        plan: &DirectoryMutation,
        route: &GroupRoute,
        proof: &ChildQuorum,
        permit: Option<&ManagedTransitionPermit>,
    ) -> Result<(), CatalogError> {
        let marker = &proof.marker;
        let position = marker.position();
        if marker.route() != route
            || marker.is_active()
            || marker.membership_revision() != 1
            || position.term.get() <= 1
            || position.index.get() <= 1
        {
            return Err(CatalogError::Fenced);
        }
        let installed = match permit {
            Some(permit) => {
                self.observe_managed_transfer_install_quorum(plan, permit, route.history)
                    .await?
            }
            None => {
                self.observe_transfer_install_quorum(plan, route.history)
                    .await?
            }
        }
        .ok_or(CatalogError::Unconfirmed)?;
        if !marker.preserves_transfer(&installed.genesis)
            || !installed.voters.iter().any(|voter| {
                voter.member.get() == proof.member
                    && voter.node.bytes() == proof.node
                    && voter.disk.bytes() == proof.disk
                    && voter.engine.bytes() == proof.engine
            })
        {
            return Err(CatalogError::Fenced);
        }
        Ok(())
    }
}

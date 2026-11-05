// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Namespace import first closes admission, then CAS-locks topology. Delayed
//! topology writers lose their exact expected head; neither effect auto-expires.
use super::*;
use crate::raft::catalog::ManagedBirthOwner;
use crate::raft::catalog::{CellGroupImportCut, FrozenCellGroupInventory};
use serde::{Deserialize, Serialize};

const CANCELLED: u8 = 197;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cancelled {
    namespace: [u8; 32],
    birth: [u8; 32],
    operation: [u8; 16],
}

/// An immutable exact-key decision. Cancellation retains the admission and
/// permanent identity; it grants no removal or physical-release permission.
pub enum CellGroupBirthDisposition {
    Live(CellGroupBirth),
    Cancelled {
        history: GroupHistory,
        operation: ProposalId,
    },
}

impl RangeDirectory {
    /// Settle an unselected managed reservation at the same immutable decision
    /// key as materialization. A selected or already live birth remains Live;
    /// cancellation never erases a possible replica or releases its resources.
    pub async fn cancel_fleet_group_birth(
        &self,
        authorization: &crate::raft::fleet_catalog::FleetBirthAuthorization,
        operation: ProposalId,
    ) -> Result<CellGroupBirthDisposition, CatalogError> {
        authorization.check_namespace(&self.namespace)?;
        self.bounded(async {
            if self.namespace.cell_group_owner().await?.as_ref() != Some(authorization.owner()) {
                return Err(CatalogError::ManagedAdmissionRequired);
            }
            let request = authorization.proposal();
            self.verify_birth_reservation(request.route()).await?;
            if let Some(winner) = self.group_birth_disposition(&request).await? {
                return Ok(winner);
            }
            let (view, _) = self.current().await?;
            match self.selected_route_in(&view, request.route()).await {
                Ok(()) => {
                    return self
                        .materialize_import_birth(&request)
                        .await
                        .map(CellGroupBirthDisposition::Live)
                }
                Err(CatalogError::Fenced) => (),
                Err(error) => return Err(error),
            }
            let cancelled = record::encode(
                CANCELLED,
                &Cancelled {
                    namespace: self.namespace.root_digest,
                    birth: request.route().birth_digest,
                    operation: operation.bytes(),
                },
                wire::LIMIT,
            )?;
            let winner = self
                .namespace
                .inner
                .create_and_observe_bounded(
                    &self.group_birth_key(request.route()),
                    &cancelled,
                    wire::LIMIT,
                )
                .await?;
            self.decode_group_disposition(&request, &winner)
        })
        .await
    }

    /// Freeze the complete admitted group set, including unselected and lazy
    /// births. Retry the same operation after loss. Already running groups keep
    /// their Raft write path; new topology and birth admission remain closed.
    pub async fn freeze_group_births(
        &self,
        operation: ProposalId,
    ) -> Result<FrozenCellGroupInventory, CatalogError> {
        self.unmanaged_birth()?;
        self.bounded(async {
            let (initial, _) = self.current().await?;
            if self.transition_active(&initial.head).await?.is_some() {
                return Err(CatalogError::Fenced);
            }
            if self
                .transition_record(&initial.head, operation)
                .await?
                .is_some()
            {
                return Err(CatalogError::Conflict("transition operation reused"));
            }
            self.namespace.begin_cell_group_freeze(operation).await?;
            for _ in 0..8 {
                let (mut view, version) = self.current().await?;
                if view.head.recovery.is_some()
                    || self.transition_active(&view.head).await?.is_some()
                {
                    return Err(CatalogError::Fenced);
                }
                if let Some(prior) = view.head.import {
                    if prior != operation.bytes() {
                        return Err(CatalogError::Conflict("directory import operation"));
                    }
                    let bytes = record::encode(wire::HEAD, &view.head, wire::LIMIT)?;
                    let digest = record::digest(&bytes);
                    self.immutable("import-cuts", digest, &bytes).await?;
                    return self
                        .namespace
                        .finish_cell_group_freeze(operation, digest)
                        .await;
                }
                if self
                    .transition_record(&view.head, operation)
                    .await?
                    .is_some()
                {
                    return Err(CatalogError::Conflict("transition operation reused"));
                }
                let operation_key = hex::encode(operation.bytes());
                if Index::new(self)
                    .lookup(view.head.operations, &operation_key)
                    .await?
                    .is_some()
                {
                    return Err(CatalogError::Conflict("directory import operation reused"));
                }
                let revision = view
                    .head
                    .revision
                    .checked_add(1)
                    .ok_or(CatalogError::Limit)?;
                let journal = wire::Journal {
                    namespace: self.namespace.root_digest,
                    operation: operation.bytes(),
                    // The previous authenticated head is the exact freeze
                    // input. Its record domain differs from a range request.
                    request: view.digest,
                    revision,
                    previous: Some(view.head.journal),
                    transfer_archive: None,
                    tree: view.head.tree.clone(),
                };
                let journal_bytes = record::encode(wire::JOURNAL, &journal, wire::LIMIT)?;
                let journal_id = record::digest(&journal_bytes);
                self.immutable("journal", journal_id, &journal_bytes)
                    .await?;
                view.head.operations = Index::new(self)
                    .update(
                        view.head.operations,
                        &operation_key,
                        Some(journal_id.to_vec()),
                        0,
                    )
                    .await?;
                view.head.import = Some(operation.bytes());
                view.head.revision = revision;
                view.head.journal = journal_id;
                self.check_head(&view.head)?;
                let bytes = record::encode(wire::HEAD, &view.head, wire::LIMIT)?;
                let _ = self
                    .namespace
                    .inner
                    .store
                    .put_opts(
                        &self.key("head"),
                        bytes.into(),
                        PutOptions::from(PutMode::Update(version)),
                    )
                    .await;
                // Read back even after a lost CAS response. A missing head is
                // ambiguous; it never reopens admission or chooses another ID.
                let (observed, _) = self.current().await?;
                if observed
                    .head
                    .import
                    .is_some_and(|prior| prior != operation.bytes())
                {
                    return Err(CatalogError::Conflict("directory import winner"));
                }
            }
            Err(CatalogError::Unconfirmed)
        })
        .await
    }

    pub(crate) async fn import_directory_view(
        &self,
        cut: &CellGroupImportCut,
    ) -> Result<DirectoryView, CatalogError> {
        cut.validate(&self.namespace)?;
        let bytes = self
            .read(&format!("import-cuts/{}", hex::encode(cut.directory)))
            .await?
            .ok_or(CatalogError::Corrupt("missing frozen group directory"))?;
        if record::digest(&bytes) != cut.directory {
            return Err(CatalogError::Corrupt("frozen group directory digest"));
        }
        let head: wire::Head = record::decode(wire::HEAD, &bytes, wire::LIMIT)?;
        self.check_head(&head)?;
        if head.import != Some(cut.operation) || head.recovery.is_some() {
            return Err(CatalogError::Corrupt("frozen group directory operation"));
        }
        Ok(DirectoryView {
            head,
            digest: cut.directory,
            owner: self.namespace.inner.owner.clone(),
        })
    }

    pub(crate) async fn check_group_import(
        &self,
        cut: &CellGroupImportCut,
        owner: &ManagedBirthOwner,
        adopted: bool,
    ) -> Result<(), CatalogError> {
        self.namespace
            .check_cell_group_import(cut, owner, adopted)
            .await?;
        self.import_directory_view(cut).await?;
        if !adopted {
            let (current, _) = self.current().await?;
            if current.digest != cut.directory || current.head.import != Some(cut.operation) {
                // A Handoff retry may observe the already adopted and unlocked
                // directory after losing the final fleet-root response.
                if current.head.import.is_some() {
                    return Err(CatalogError::Conflict("changed import topology"));
                }
                self.namespace
                    .check_cell_group_import(cut, owner, true)
                    .await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn finish_group_import(
        &self,
        cut: &CellGroupImportCut,
        owner: &ManagedBirthOwner,
    ) -> Result<(), CatalogError> {
        self.namespace
            .change_cell_group_import(cut, owner, true)
            .await?;
        for _ in 0..8 {
            let (mut view, version) = self.current().await?;
            if view.head.import.is_none() {
                return self.check_group_import(cut, owner, true).await;
            }
            if view.digest != cut.directory || view.head.import != Some(cut.operation) {
                return Err(CatalogError::Conflict("import handoff directory"));
            }
            view.head.import = None;
            view.head.revision = view
                .head
                .revision
                .checked_add(1)
                .ok_or(CatalogError::Limit)?;
            self.check_head(&view.head)?;
            let bytes = record::encode(wire::HEAD, &view.head, wire::LIMIT)?;
            let _ = self
                .namespace
                .inner
                .store
                .put_opts(
                    &self.key("head"),
                    bytes.into(),
                    PutOptions::from(PutMode::Update(version)),
                )
                .await;
        }
        Err(CatalogError::Unconfirmed)
    }

    pub(crate) fn decode_group_disposition(
        &self,
        request: &CellGroupBirthRequest,
        bytes: &[u8],
    ) -> Result<CellGroupBirthDisposition, CatalogError> {
        if bytes == request.encode() {
            return Ok(CellGroupBirthDisposition::Live(CellGroupBirth {
                request: Arc::new(request.clone()),
                owner: self.namespace.inner.owner.clone(),
            }));
        }
        let cancelled: Cancelled = record::decode(CANCELLED, bytes, wire::LIMIT)?;
        if cancelled.namespace != self.namespace.root_digest
            || cancelled.birth != request.route().birth_digest
        {
            return Err(CatalogError::Corrupt("cancelled group birth identity"));
        }
        let operation = ProposalId::new(cancelled.operation)
            .map_err(|_| CatalogError::Corrupt("group cancellation operation"))?;
        Ok(CellGroupBirthDisposition::Cancelled {
            history: request.route().history,
            operation,
        })
    }

    pub(crate) async fn group_birth_disposition(
        &self,
        request: &CellGroupBirthRequest,
    ) -> Result<Option<CellGroupBirthDisposition>, CatalogError> {
        self.verify_birth_reservation(request.route()).await?;
        let bytes = self
            .namespace
            .inner
            .read_versioned_bounded(&self.group_birth_key(request.route()), wire::LIMIT)
            .await?;
        bytes
            .map(|(bytes, _)| self.decode_group_disposition(request, &bytes))
            .transpose()
    }

    pub(crate) async fn settle_import_group(
        &self,
        request: &CellGroupBirthRequest,
        cut: &CellGroupImportCut,
        owner: &ManagedBirthOwner,
    ) -> Result<CellGroupBirthDisposition, CatalogError> {
        self.check_group_import(cut, owner, false).await?;
        self.namespace.verify_cell_group_intent(request).await?;
        let view = self.import_directory_view(cut).await?;
        // Every selected range must remain live. A lazy selected birth is
        // materialized, never confused with an unselected cancelled intent.
        match self.selected_route_in(&view, request.route()).await {
            Ok(()) => {
                return self
                    .materialize_import_birth(request)
                    .await
                    .map(CellGroupBirthDisposition::Live)
            }
            Err(CatalogError::Fenced) => (),
            Err(error) => return Err(error),
        }
        let cancelled = record::encode(
            CANCELLED,
            &Cancelled {
                namespace: self.namespace.root_digest,
                birth: request.route().birth_digest,
                operation: cut.operation,
            },
            wire::LIMIT,
        )?;
        let bytes = self
            .namespace
            .inner
            .create_and_observe_bounded(
                &self.group_birth_key(request.route()),
                &cancelled,
                wire::LIMIT,
            )
            .await?;
        // A materializer that won before the topology lock keeps its original
        // immutable live decision. It must be covered too, even if unselected.
        self.decode_group_disposition(request, &bytes)
    }
}

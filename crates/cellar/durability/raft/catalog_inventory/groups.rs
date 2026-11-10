// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical-group admission shares the namespace CAS with disaster exclusion.
//! Frozen intents include unpublished groups; listing never defines coverage.
use super::*;
use crate::raft::catalog::ManagedBirthOwner;
use crate::raft::cell_groups::{CellGroupBirthRequest, RangeDirectory};
use serde::{Deserialize, Serialize};

const CUT_KIND: u8 = 196;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CellGroupImportCut {
    pub root: [u8; 32],
    pub operation: [u8; 16],
    pub revision: u64,
    pub count: u64,
    #[serde(deserialize_with = "required_index")]
    pub index: Option<[u8; 32]>,
    pub directory: [u8; 32],
}

fn required_index<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<[u8; 32]>, D::Error> {
    Option::deserialize(d)
}

impl CellGroupImportCut {
    pub(crate) fn validate(&self, namespace: &NamespaceCatalog) -> Result<(), CatalogError> {
        if self.root != namespace.root_digest
            || self.revision <= self.count
            || self.revision < 3
            || self.index.is_none() != (self.count == 0)
            || self.directory == [0; 32]
            || namespace.config.managed_fleet.is_some()
            || ProposalId::new(self.operation).is_err()
        {
            return Err(CatalogError::Corrupt("group import cut"));
        }
        Ok(())
    }

    pub(crate) fn digest(&self) -> Result<[u8; 32], CatalogError> {
        Ok(record::digest(&record::encode(CUT_KIND, self, 4096)?))
    }
}

/// A frozen, authoritative admission set. This is neither complete enrollment
/// nor permission to start, activate, remove or release a physical replica.
#[derive(Clone)]
pub struct FrozenCellGroupInventory {
    owner: Arc<Inner>,
    cut: CellGroupImportCut,
}

impl FrozenCellGroupInventory {
    pub fn len(&self) -> u64 {
        self.cut.count
    }

    pub fn is_empty(&self) -> bool {
        self.cut.count == 0
    }

    pub fn operation(&self) -> ProposalId {
        // Construction validates the durable cut. Retain no unchecked public
        // constructor, including for a resumed inventory.
        ProposalId::new(self.cut.operation).expect("validated group inventory operation")
    }

    pub(crate) fn import_cut(
        &self,
        directory: &RangeDirectory,
    ) -> Result<CellGroupImportCut, CatalogError> {
        if !Arc::ptr_eq(&self.owner, &directory.namespace().inner) {
            return Err(CatalogError::Conflict("foreign frozen group inventory"));
        }
        self.cut.validate(directory.namespace())?;
        Ok(self.cut.clone())
    }
}

pub(crate) struct ImportCellGroupStep {
    pub entry: Option<CellGroupBirthRequest>,
    pub next: Option<String>,
}

pub(crate) fn group_key(request: &CellGroupBirthRequest) -> String {
    hex::encode(record::digest(&request.route().history.group.bytes()))
}

impl NamespaceCatalog {
    pub(crate) async fn cell_group_owner(&self) -> Result<Option<ManagedBirthOwner>, CatalogError> {
        let (head, _) = Store(self).current().await?;
        Ok(match head.mode {
            Mode::Owned { fleet, catalog }
            | Mode::GroupImporting { fleet, catalog, .. }
            | Mode::GroupAdopted { fleet, catalog, .. } => {
                Some(ManagedBirthOwner { fleet, catalog })
            }
            Mode::Open | Mode::GroupFreezing { .. } | Mode::GroupFrozen { .. } => None,
            _ => return Err(CatalogError::Conflict("not a shared-group namespace")),
        })
    }

    pub(crate) async fn require_cell_group_unmanaged(&self) -> Result<(), CatalogError> {
        if self.cell_group_owner().await?.is_some() {
            return Err(CatalogError::ManagedAdmissionRequired);
        }
        Ok(())
    }

    /// Called after the permanent global claim, before any topology publication
    /// or materialization. A losing CAS can retain a harmless identity claim;
    /// without this admission it can never become a group birth.
    pub(crate) async fn admit_cell_group(
        &self,
        request: &CellGroupBirthRequest,
    ) -> Result<(), CatalogError> {
        let store = Store(self);
        let index = Index::new(&store);
        let key = group_key(request);
        let id = request.route().birth_digest;
        if request.route().history.cluster != self.config.cluster
            || request.route().history.namespace != self.config.namespace
            || request.route().schema_digest != self.config.schema_digest
            || record::digest(request.encode()) != id
        {
            return Err(CatalogError::Conflict("group admission namespace"));
        }
        for _ in 0..MAX_ATTEMPTS {
            let (mut head, token) = store.current().await?;
            if let Some(prior) = index.lookup(head.index, &key).await? {
                if pointer(&prior)? != id {
                    return Err(CatalogError::Conflict("group admission identity"));
                }
                return self.verify_cell_group_intent(request).await;
            }
            match head.mode {
                Mode::Open => (),
                Mode::GroupFreezing { .. }
                | Mode::GroupFrozen { .. }
                | Mode::GroupImporting { .. }
                | Mode::Frozen { .. }
                | Mode::Importing { .. } => return Err(CatalogError::BirthsFrozen),
                _ => return Err(CatalogError::ManagedAdmissionRequired),
            }
            let intent_key = store.key(&format!("group-intents/{}", hex::encode(id)));
            if self
                .inner
                .create_and_observe_bounded(&intent_key, request.encode(), INDEX_PAGE_LIMIT)
                .await?
                != request.encode()
            {
                return Err(CatalogError::Corrupt("group admission intent collision"));
            }
            head.index = index.update(head.index, &key, Some(id.to_vec()), 0).await?;
            head.count = head.count.checked_add(1).ok_or(CatalogError::Limit)?;
            head.revision = head.revision.checked_add(1).ok_or(CatalogError::Limit)?;
            head.validate(self)?;
            let result = store.cas(&head, token).await;
            let (observed, _) = store.current().await?;
            if index.lookup(observed.index, &key).await?.as_deref() == Some(id.as_slice()) {
                return self.verify_cell_group_intent(request).await;
            }
            result?;
        }
        Err(CatalogError::Unconfirmed)
    }

    pub(crate) async fn verify_cell_group_intent(
        &self,
        request: &CellGroupBirthRequest,
    ) -> Result<(), CatalogError> {
        let store = Store(self);
        let (head, _) = store.current().await?;
        let value = Index::new(&store)
            .lookup(head.index, &group_key(request))
            .await?
            .ok_or(CatalogError::Corrupt("untracked group birth"))?;
        let id = pointer(&value)?;
        let bytes = self
            .inner
            .read_versioned_bounded(
                &store.key(&format!("group-intents/{}", hex::encode(id))),
                INDEX_PAGE_LIMIT,
            )
            .await?
            .ok_or(CatalogError::Corrupt("missing group intent"))?
            .0;
        if id != request.route().birth_digest
            || record::digest(&bytes) != id
            || bytes != request.encode()
        {
            return Err(CatalogError::Corrupt("group intent identity"));
        }
        Ok(())
    }

    pub(crate) async fn begin_cell_group_freeze(
        &self,
        operation: ProposalId,
    ) -> Result<(), CatalogError> {
        let store = Store(self);
        for _ in 0..MAX_ATTEMPTS {
            let (mut head, token) = store.current().await?;
            if head.disaster {
                return Err(CatalogError::Conflict(
                    "disaster namespace cannot be imported",
                ));
            }
            match &head.mode {
                Mode::GroupFreezing { operation: prior } if *prior == operation.bytes() => {
                    return Ok(());
                }
                Mode::GroupFrozen { cut } if cut.operation == operation.bytes() => return Ok(()),
                Mode::Open => (),
                _ => return Err(CatalogError::Conflict("group freeze owner/operation")),
            }
            head.mode = Mode::GroupFreezing {
                operation: operation.bytes(),
            };
            head.revision = head.revision.checked_add(1).ok_or(CatalogError::Limit)?;
            head.validate(self)?;
            let result = store.cas(&head, token).await;
            if store.current().await?.0.mode == head.mode {
                return Ok(());
            }
            result?;
        }
        Err(CatalogError::Unconfirmed)
    }

    pub(crate) async fn finish_cell_group_freeze(
        &self,
        operation: ProposalId,
        directory: [u8; 32],
    ) -> Result<FrozenCellGroupInventory, CatalogError> {
        let store = Store(self);
        for _ in 0..MAX_ATTEMPTS {
            let (mut head, token) = store.current().await?;
            if let Mode::GroupFrozen { cut } = &head.mode {
                if cut.operation == operation.bytes() && cut.directory == directory {
                    cut.validate(self)?;
                    return Ok(FrozenCellGroupInventory {
                        owner: self.inner.clone(),
                        cut: cut.clone(),
                    });
                }
            }
            if head.mode
                != (Mode::GroupFreezing {
                    operation: operation.bytes(),
                })
            {
                return Err(CatalogError::Conflict("group freeze continuation"));
            }
            head.revision = head.revision.checked_add(1).ok_or(CatalogError::Limit)?;
            let cut = CellGroupImportCut {
                root: self.root_digest,
                operation: operation.bytes(),
                revision: head.revision,
                count: head.count,
                index: head.index,
                directory,
            };
            cut.validate(self)?;
            head.mode = Mode::GroupFrozen { cut };
            let result = store.cas(&head, token).await;
            if store.current().await?.0.mode != head.mode {
                result?;
            }
        }
        Err(CatalogError::Unconfirmed)
    }

    pub(crate) async fn change_cell_group_import(
        &self,
        cut: &CellGroupImportCut,
        owner: &ManagedBirthOwner,
        adopted: bool,
    ) -> Result<(), CatalogError> {
        cut.validate(self)?;
        let store = Store(self);
        let desired = if adopted {
            Mode::GroupAdopted {
                fleet: owner.fleet,
                catalog: owner.catalog,
                cut: cut.clone(),
            }
        } else {
            Mode::GroupImporting {
                fleet: owner.fleet,
                catalog: owner.catalog,
                cut: cut.clone(),
            }
        };
        for _ in 0..MAX_ATTEMPTS {
            let (mut head, token) = store.current().await?;
            if head.mode == desired
                || !adopted
                    && matches!(&head.mode,
                Mode::GroupAdopted { fleet, catalog, cut: prior }
                if *fleet == owner.fleet && *catalog == owner.catalog && prior == cut)
            {
                return Ok(());
            }
            let allowed = if adopted {
                matches!(&head.mode, Mode::GroupImporting { fleet, catalog, cut: prior }
                    if *fleet == owner.fleet && *catalog == owner.catalog && prior == cut)
            } else {
                matches!(&head.mode, Mode::GroupFrozen { cut: prior } if prior == cut)
            };
            if !allowed {
                return Err(CatalogError::Conflict("group import owner/cut"));
            }
            head.mode = desired.clone();
            head.revision = head.revision.checked_add(1).ok_or(CatalogError::Limit)?;
            head.validate(self)?;
            let result = store.cas(&head, token).await;
            if store.current().await?.0.mode == desired {
                return Ok(());
            }
            result?;
        }
        Err(CatalogError::Unconfirmed)
    }

    pub(crate) async fn check_cell_group_import(
        &self,
        cut: &CellGroupImportCut,
        owner: &ManagedBirthOwner,
        adopted: bool,
    ) -> Result<(), CatalogError> {
        cut.validate(self)?;
        let valid = match Store(self).current().await?.0.mode {
            Mode::GroupAdopted {
                fleet,
                catalog,
                cut: prior,
            } => fleet == owner.fleet && catalog == owner.catalog && prior == *cut,
            Mode::GroupImporting {
                fleet,
                catalog,
                cut: prior,
            } if !adopted => fleet == owner.fleet && catalog == owner.catalog && prior == *cut,
            _ => false,
        };
        if !valid {
            return Err(CatalogError::Conflict("group import marker"));
        }
        Ok(())
    }

    pub(crate) async fn cell_group_import_entry(
        &self,
        directory: &RangeDirectory,
        cut: &CellGroupImportCut,
        owner: &ManagedBirthOwner,
        key: &str,
    ) -> Result<CellGroupBirthRequest, CatalogError> {
        self.check_cell_group_import(cut, owner, false).await?;
        let store = Store(self);
        let pointer = Index::new(&store)
            .lookup(cut.index, key)
            .await?
            .ok_or(CatalogError::Corrupt("untracked group import witness"))?;
        let id = super::pointer(&pointer)?;
        let bytes = self
            .inner
            .read_versioned_bounded(
                &store.key(&format!("group-intents/{}", hex::encode(id))),
                INDEX_PAGE_LIMIT,
            )
            .await?
            .ok_or(CatalogError::Corrupt("missing group import intent"))?
            .0;
        if record::digest(&bytes) != id {
            return Err(CatalogError::Corrupt("group import intent digest"));
        }
        let request = directory.decode_group_birth(&bytes)?;
        if request.route().birth_digest != id || group_key(&request) != key {
            return Err(CatalogError::Corrupt("group import index identity"));
        }
        Ok(request)
    }

    pub(crate) async fn cell_group_import_step(
        &self,
        directory: &RangeDirectory,
        cut: &CellGroupImportCut,
        owner: &ManagedBirthOwner,
        after: Option<&str>,
    ) -> Result<ImportCellGroupStep, CatalogError> {
        self.check_cell_group_import(cut, owner, false).await?;
        let cursor = after
            .map(|key| IndexCursor::after(cut.index, key.to_owned()))
            .transpose()?;
        let page = Index::new(&Store(self))
            .scan(cut.index, cursor.as_ref(), 2)
            .await?;
        let more = page.entries.len() > 1;
        let Some(first) = page.entries.into_iter().next() else {
            return Ok(ImportCellGroupStep {
                entry: None,
                next: None,
            });
        };
        let entry = self
            .cell_group_import_entry(directory, cut, owner, &first.key)
            .await?;
        Ok(ImportCellGroupStep {
            entry: Some(entry),
            next: more.then_some(first.key),
        })
    }
}

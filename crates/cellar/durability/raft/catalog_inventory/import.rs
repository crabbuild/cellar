//! A frozen admission cut can be claimed once and handed to its exact fleet.
//! Only the fleet root decides when all histories are enrolled. This component
//! executes its namespace effects; it never issues a completeness certificate.
use super::*;
use crate::raft::catalog::ManagedBirthOwner;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BirthImportCut {
    pub root: [u8; 32],
    pub operation: [u8; 16],
    pub revision: u64,
    pub count: u64,
    pub index: Option<[u8; 32]>,
}

impl BirthImportCut {
    pub(crate) fn validate(&self, namespace: &NamespaceCatalog) -> Result<(), CatalogError> {
        if self.root != namespace.root_digest
            || self.revision <= self.count
            || self.revision <= 1
            || self.index.is_none() != (self.count == 0)
            || namespace.config.managed_fleet.is_some()
        {
            return Err(CatalogError::Corrupt("import birth cut"));
        }
        ProposalId::new(self.operation).map_err(|_| CatalogError::Corrupt("import operation"))?;
        Ok(())
    }

    pub(crate) fn digest(&self) -> Result<[u8; 32], CatalogError> {
        Ok(record::digest(&record::encode(8, self, 4096)?))
    }
}

pub(crate) struct ImportBirthStep {
    pub entry: Option<BirthInventoryEntry>,
    pub next: Option<String>,
}

impl FrozenBirthInventory {
    pub(crate) fn import_cut(
        &self,
        namespace: &NamespaceCatalog,
    ) -> Result<BirthImportCut, CatalogError> {
        if !Arc::ptr_eq(&self.owner, &namespace.inner) || self.root != namespace.root_digest {
            return Err(CatalogError::Conflict("foreign frozen import cut"));
        }
        let cut = BirthImportCut {
            root: self.root,
            operation: self.operation.bytes(),
            revision: self.revision,
            count: self.count,
            index: self.index,
        };
        cut.validate(namespace)?;
        Ok(cut)
    }
}

impl NamespaceCatalog {
    pub(crate) async fn import_birth_entry(
        &self,
        cut: &BirthImportCut,
        owner: &ManagedBirthOwner,
        key: &str,
    ) -> Result<BirthInventoryEntry, CatalogError> {
        self.check_birth_import(cut, owner, false).await?;
        let store = Store(self);
        let bytes = Index::new(&store)
            .lookup(cut.index, key)
            .await?
            .ok_or(CatalogError::Corrupt("untracked import witness"))?;
        let id = pointer(&bytes)?;
        let intent = store.intent(id).await?;
        if key != name_key(intent.scope.cell()) {
            return Err(CatalogError::Corrupt("import witness name"));
        }
        Ok(BirthInventoryEntry {
            proposal: store.proposal(&intent)?,
            digest: id,
        })
    }

    pub(crate) async fn verify_import_member_claim(
        &self,
        birth: &crate::raft::catalog::GroupBirth,
        member: cellar_logic::consensus::MemberId,
        disk: cellar_logic::consensus::DiskLineage,
    ) -> Result<(), CatalogError> {
        self.check_birth(birth)?;
        let claim = self
            .read_claim(&self.claim_key(birth, member), birth)
            .await?
            .ok_or(CatalogError::Conflict("missing import member claim"))?;
        if claim.member != member
            || claim.disk != disk
            || claim.birth_digest != birth.digest()
            || claim.scope != *birth.scope()
        {
            return Err(CatalogError::Conflict("import member claim identity"));
        }
        Ok(())
    }

    pub(crate) async fn claim_birth_import(
        &self,
        cut: &BirthImportCut,
        owner: &ManagedBirthOwner,
    ) -> Result<(), CatalogError> {
        self.change_import_owner(cut, owner, false).await
    }

    pub(crate) async fn finish_birth_import(
        &self,
        cut: &BirthImportCut,
        owner: &ManagedBirthOwner,
    ) -> Result<(), CatalogError> {
        self.change_import_owner(cut, owner, true).await
    }

    async fn change_import_owner(
        &self,
        cut: &BirthImportCut,
        owner: &ManagedBirthOwner,
        adopt: bool,
    ) -> Result<(), CatalogError> {
        cut.validate(self)?;
        let store = Store(self);
        let desired = if adopt {
            Mode::Adopted {
                fleet: owner.fleet,
                catalog: owner.catalog,
                cut: cut.clone(),
            }
        } else {
            Mode::Importing {
                fleet: owner.fleet,
                catalog: owner.catalog,
                cut: cut.clone(),
            }
        };
        for _ in 0..MAX_ATTEMPTS {
            let (mut head, token) = store.current().await?;
            if head.mode == desired
                || (!adopt
                    && matches!(&head.mode, Mode::Adopted { fleet, catalog, cut: prior } if *fleet==owner.fleet && *catalog==owner.catalog && prior==cut))
            {
                return Ok(());
            }
            let allowed = if adopt {
                matches!(&head.mode, Mode::Importing { fleet, catalog, cut: prior } if *fleet==owner.fleet && *catalog==owner.catalog && prior==cut)
            } else {
                head.mode
                    == (Mode::Frozen {
                        operation: cut.operation,
                    })
                    && head.revision == cut.revision
                    && head.count == cut.count
                    && head.index == cut.index
            };
            if !allowed {
                return Err(CatalogError::Conflict("namespace import owner/cut"));
            }
            head.mode = desired.clone();
            head.revision = head.revision.checked_add(1).ok_or(CatalogError::Limit)?;
            head.validate(self)?;
            if let Err(error) = store.cas(&head, token).await {
                let (observed, _) = store.current().await?;
                if observed.mode != desired {
                    return Err(error);
                }
            }
        }
        Err(CatalogError::Unconfirmed)
    }

    pub(crate) async fn check_birth_import(
        &self,
        cut: &BirthImportCut,
        owner: &ManagedBirthOwner,
        adopted: bool,
    ) -> Result<(), CatalogError> {
        cut.validate(self)?;
        let (head, _) = Store(self).current().await?;
        let valid = match head.mode {
            Mode::Adopted {
                fleet,
                catalog,
                cut: prior,
            } => fleet == owner.fleet && catalog == owner.catalog && prior == *cut,
            Mode::Importing {
                fleet,
                catalog,
                cut: prior,
            } if !adopted => fleet == owner.fleet && catalog == owner.catalog && prior == *cut,
            _ => false,
        };
        if !valid {
            return Err(CatalogError::Conflict("namespace import marker"));
        }
        Ok(())
    }

    pub(crate) async fn import_birth_step(
        &self,
        cut: &BirthImportCut,
        owner: &ManagedBirthOwner,
        after: Option<&str>,
    ) -> Result<ImportBirthStep, CatalogError> {
        self.check_birth_import(cut, owner, false).await?;
        let cursor = after
            .map(|key| IndexCursor::after(cut.index, key.to_owned()))
            .transpose()?;
        let store = Store(self);
        // The shared cursor means "resume after the last returned entry", not
        // "there is definitely another entry". One bounded lookahead proves
        // the terminal cut without weakening the import count checks.
        let page = Index::new(&store)
            .scan(cut.index, cursor.as_ref(), 2)
            .await?;
        let more = page.entries.len() > 1;
        let mut next = None;
        let entry = if let Some(entry) = page.entries.into_iter().next() {
            if more {
                next = Some(entry.key.clone());
            }
            let id = pointer(&entry.value)?;
            let intent = store.intent(id).await?;
            if entry.key != name_key(intent.scope.cell()) {
                return Err(CatalogError::Corrupt("import intent index name"));
            }
            Some(BirthInventoryEntry {
                proposal: store.proposal(&intent)?,
                digest: id,
            })
        } else {
            None
        };
        Ok(ImportBirthStep { entry, next })
    }
}

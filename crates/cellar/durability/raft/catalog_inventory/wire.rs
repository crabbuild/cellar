use super::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) enum Mode {
    Open,
    Owned {
        fleet: [u8; 16],
        catalog: [u8; 32],
    },
    Frozen {
        operation: [u8; 16],
    },
    Importing {
        fleet: [u8; 16],
        catalog: [u8; 32],
        cut: BirthImportCut,
    },
    Adopted {
        fleet: [u8; 16],
        catalog: [u8; 32],
        cut: BirthImportCut,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Head {
    pub root: [u8; 32],
    pub protocol: [u8; 32],
    pub revision: u64,
    pub count: u64,
    pub index: Option<[u8; 32]>,
    pub mode: Mode,
    /// A permanent exclusion from fleet import after disaster recovery admission.
    pub disaster: bool,
}

impl Head {
    pub fn genesis(namespace: &NamespaceCatalog) -> Self {
        Self {
            root: namespace.root_digest,
            protocol: crate::application::consensus::protocol::protocol_identity(),
            revision: 1,
            count: 0,
            index: None,
            disaster: false,
            mode: namespace
                .config
                .managed_fleet
                .as_ref()
                .map_or(Mode::Open, |owner| Mode::Owned {
                    fleet: owner.fleet,
                    catalog: owner.catalog,
                }),
        }
    }

    pub fn validate(&self, namespace: &NamespaceCatalog) -> Result<(), CatalogError> {
        if self.root != namespace.root_digest
            || self.protocol != crate::application::consensus::protocol::protocol_identity()
            || self.revision == 0
            || self.count >= self.revision
            || self.index.is_none() != (self.count == 0)
            || (self.disaster && !matches!(self.mode, Mode::Open))
        {
            return Err(CatalogError::Corrupt("birth inventory identity/count"));
        }
        match (&self.mode, &namespace.config.managed_fleet) {
            (Mode::Open, None) => (),
            (Mode::Owned { fleet, catalog }, Some(owner)) => {
                if fleet != &owner.fleet || catalog != &owner.catalog {
                    return Err(CatalogError::Corrupt("birth inventory owner"));
                }
            }
            (Mode::Frozen { operation }, None) if self.revision > 1 => {
                ProposalId::new(*operation)
                    .map_err(|_| CatalogError::Corrupt("birth freeze operation"))?;
            }
            (
                Mode::Importing {
                    fleet,
                    catalog,
                    cut,
                },
                None,
            ) => {
                cut.validate(namespace)?;
                validate_owner(*fleet, *catalog)?;
                if self.revision != cut.revision.checked_add(1).ok_or(CatalogError::Limit)?
                    || self.count != cut.count
                    || self.index != cut.index
                {
                    return Err(CatalogError::Corrupt("changed import admission set"));
                }
            }
            (
                Mode::Adopted {
                    fleet,
                    catalog,
                    cut,
                },
                None,
            ) => {
                cut.validate(namespace)?;
                validate_owner(*fleet, *catalog)?;
                if self.revision < cut.revision.checked_add(2).ok_or(CatalogError::Limit)?
                    || self.count < cut.count
                    || (self.count == cut.count && self.index != cut.index)
                {
                    return Err(CatalogError::Corrupt("regressed adopted admission set"));
                }
            }
            _ => return Err(CatalogError::Corrupt("birth inventory owner")),
        }
        Ok(())
    }
}

fn validate_owner(fleet: [u8; 16], catalog: [u8; 32]) -> Result<(), CatalogError> {
    cellar_logic::consensus::IncarnationId::new(fleet)
        .map_err(|_| CatalogError::Corrupt("import fleet owner"))?;
    if catalog == [0; 32] {
        return Err(CatalogError::Corrupt("import catalog owner"));
    }
    Ok(())
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Pin {
    pub root: [u8; 32],
    pub genesis: [u8; 32],
}

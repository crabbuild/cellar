use super::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) enum Mode {
    Open,
    Owned { fleet: [u8; 16], catalog: [u8; 32] },
    Frozen { operation: [u8; 16] },
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
}

impl Head {
    pub fn genesis(namespace: &NamespaceCatalog) -> Self {
        Self {
            root: namespace.root_digest,
            protocol: crate::application::consensus::protocol::protocol_identity(),
            revision: 1,
            count: 0,
            index: None,
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
            _ => return Err(CatalogError::Corrupt("birth inventory owner")),
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Pin {
    pub root: [u8; 32],
    pub genesis: [u8; 32],
}

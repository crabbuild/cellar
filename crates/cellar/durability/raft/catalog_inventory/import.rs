//! A frozen admission cut can be claimed once and handed to its exact fleet.
//! Only the fleet root decides when all histories are enrolled. This component
//! executes its namespace effects; it never issues a completeness certificate.
use super::*;
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
}

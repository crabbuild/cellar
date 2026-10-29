use super::*;
use serde::{Deserialize, Serialize};

pub(super) const KIND: u8 = 49;
pub(super) const WITNESS_KIND: u8 = 50;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FleetImportPhase {
    Preparing,
    Inventory,
    Coverage,
    Handoff,
    Complete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Record {
    pub namespace: [u8; 16],
    pub schema: [u8; 32],
    pub catalog: [u8; 32],
    pub cut: BirthImportCut,
    pub phase: FleetImportPhase,
    pub stopped: bool,
    pub inventoried: u64,
    pub covered: u64,
    #[serde(deserialize_with = "required_nullable")]
    pub cursor: Option<String>,
    #[serde(deserialize_with = "required_nullable")]
    pub coverage: Option<[u8; 32]>,
}

fn required_nullable<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::<T>::deserialize(d)
}

impl Record {
    pub fn validate(&self, catalog: &FleetCatalog, namespace: &NamespaceCatalog) -> Result<()> {
        self.cut.validate(namespace).map_err(catalog_error)?;
        if self.namespace != namespace.config().namespace.bytes()
            || namespace.config().cluster.bytes() != catalog.inner.cluster
            || namespace.config().voters != VoterPolicy::ThreeVoters
            || self.schema != namespace.config().schema_digest
            || self.catalog != catalog.birth_owner()?.catalog
            || self.cut.count > HISTORY_LIMIT
            || self.inventoried > self.cut.count
            || self.covered > self.inventoried
            || self.coverage.is_none() != (self.covered == 0)
            || self.cursor.as_ref().is_some_and(|k| {
                k.len() != 64
                    || !k
                        .bytes()
                        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            })
        {
            return Err(FleetCatalogError::Corrupt("namespace import record"));
        }
        let valid = match self.phase {
            FleetImportPhase::Preparing => {
                self.inventoried == 0 && self.covered == 0 && self.cursor.is_none()
            }
            FleetImportPhase::Inventory => {
                self.covered == 0
                    && ((self.inventoried == 0 && self.cursor.is_none())
                        || (self.inventoried > 0
                            && self.inventoried < self.cut.count
                            && self.cursor.is_some()))
            }
            FleetImportPhase::Coverage => {
                self.inventoried == self.cut.count
                    && ((self.covered == 0 && self.cursor.is_none())
                        || (self.covered > 0
                            && self.covered < self.cut.count
                            && self.cursor.is_some()))
            }
            FleetImportPhase::Handoff | FleetImportPhase::Complete => {
                !self.stopped
                    && self.inventoried == self.cut.count
                    && self.covered == self.cut.count
                    && self.cursor.is_none()
            }
        };
        if !valid {
            return Err(FleetCatalogError::Corrupt(
                "namespace import phase/progress",
            ));
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        encode(KIND, self, 4096)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Voter {
    pub member: u64,
    pub node: [u8; 16],
    pub disk: [u8; 16],
    pub instance: [u8; 16],
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) enum Decision {
    Cancelled {
        operation: [u8; 16],
    },
    Guarded {
        node: [u8; 16],
        disk: [u8; 16],
        process: [u8; 16],
        member: u64,
        barrier: u64,
        applied: u64,
        term: u64,
        membership_revision: u64,
        consumed_sequence: u64,
        phase: Option<crate::raft::member_state::FleetPermitPhase>,
        voters: Vec<Voter>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Witness {
    pub history: String,
    pub birth: [u8; 32],
    pub decision: Decision,
}

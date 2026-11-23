// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::filesystem::MAX_ENGINE_DIRECTORY_ENTRIES;
use crate::raft::storage::RaftSegmentFile;

// The canonical tuple encoding has at most 48 bytes per file, including
// separators and both 20-digit u64 values.
pub(super) const INVENTORY_LIMIT: usize = 1024 + 48 * MAX_ENGINE_DIRECTORY_ENTRIES;
const KIND: u8 = 242;
const RECEIPT_KIND: u8 = 243;

#[derive(Clone, Copy, Serialize, Deserialize)]
struct File(u8, u64, u64);

impl File {
    fn id(self) -> (u8, u64) {
        (self.0, self.1)
    }
}

impl From<&RaftSegmentFile> for File {
    fn from(file: &RaftSegmentFile) -> Self {
        Self(file.id.queue as u8, file.id.seq, file.bytes)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Charges {
    unattributed: u64,
    files: Vec<File>,
}

impl Charges {
    pub(super) fn observe(inventory: &RaftSegmentInventory) -> Result<Self> {
        let charges = Self {
            unattributed: 0,
            files: ordered(inventory.files()),
        };
        if charges.floor()? != inventory.bytes {
            return Err(FleetCatalogError::InventoryIncomplete);
        }
        Ok(charges)
    }

    pub(super) fn floor(&self) -> Result<u64> {
        validate(&self.files)?;
        self.files
            .iter()
            .try_fold(self.unattributed, |total, file| add(total, file.2))
    }

    pub(super) fn settle(
        &self,
        receipt: &RaftSegmentPurgeReceipt,
        inventory: &RaftSegmentInventory,
    ) -> Result<Self> {
        self.reconcile(&ordered(receipt.removed_files()), Self::observe(inventory)?)
    }

    fn reconcile(&self, removed: &[File], mut after: Self) -> Result<Self> {
        validate(removed)?;
        after.unattributed = self.unattributed;
        for file in removed {
            if find(&after.files, file.id()).is_ok() {
                return Err(FleetCatalogError::InventoryIncomplete);
            }
        }
        for &charged in &self.files {
            match find(&after.files, charged.id()) {
                Ok(index) => {
                    let current = &mut after.files[index];
                    current.2 = current.2.max(charged.2);
                }
                Err(_) => {
                    // Only an exact receipt for this charged file releases its
                    // proven length. Missing files and any unproven remainder
                    // stay charged after their file IDs leave the inventory.
                    let released = find(removed, charged.id())
                        .map_or(0, |index| charged.2.min(removed[index].2));
                    after.unattributed = add(after.unattributed, charged.2 - released)?;
                }
            }
        }
        after.floor()?;
        Ok(after)
    }

    pub(super) async fn read(catalog: &FleetCatalog, digest: [u8; 32]) -> Result<Self> {
        let key = key(digest);
        let (charges, _) = catalog
            .inner
            .read::<Self>(&key, KIND, INVENTORY_LIMIT)
            .await?
            .ok_or(FleetCatalogError::Corrupt("missing segment charges"))?;
        if hash(&encode(KIND, &charges, INVENTORY_LIMIT)?) != digest {
            return Err(FleetCatalogError::Corrupt("segment charges digest"));
        }
        Ok(charges)
    }

    pub(super) async fn write(&self, catalog: &FleetCatalog) -> Result<[u8; 32]> {
        let encoded = encode(KIND, self, INVENTORY_LIMIT)?;
        let digest = hash(&encoded);
        catalog
            .inner
            .create_exact(&key(digest), &encoded, INVENTORY_LIMIT)
            .await?;
        Ok(digest)
    }
}

pub(super) fn receipt_digest(receipt: &RaftSegmentPurgeReceipt) -> Result<[u8; 32]> {
    let record = (
        receipt.number,
        receipt.node.bytes(),
        receipt.disk.bytes(),
        receipt.engine.bytes(),
        receipt.prepared_process.bytes(),
        receipt.prepared_sequence,
        receipt.completed_process.bytes(),
        receipt.completed_sequence,
        ordered(receipt.removed_files()),
    );
    Ok(hash(&encode(RECEIPT_KIND, &record, INVENTORY_LIMIT)?))
}

fn key(digest: [u8; 32]) -> String {
    format!("segment-charges/{}", hex::encode(digest))
}

fn ordered(files: &[RaftSegmentFile]) -> Vec<File> {
    let mut files = files.iter().map(File::from).collect::<Vec<_>>();
    files.sort_unstable_by_key(|file| file.id());
    files
}

fn validate(files: &[File]) -> Result<()> {
    if files.len() >= MAX_ENGINE_DIRECTORY_ENTRIES
        || files.iter().any(|file| file.0 > 1 || file.1 == 0)
        || files.windows(2).any(|pair| pair[0].id() >= pair[1].id())
    {
        return Err(FleetCatalogError::Corrupt("invalid segment charges"));
    }
    Ok(())
}

fn find(files: &[File], id: (u8, u64)) -> std::result::Result<usize, usize> {
    files.binary_search_by_key(&id, |file| file.id())
}

fn add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right).ok_or(FleetCatalogError::Limit)
}

#[cfg(test)]
#[path = "charges_tests.rs"]
mod tests;

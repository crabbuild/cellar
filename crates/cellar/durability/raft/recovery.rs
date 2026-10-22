//! Engine-selected recovery references. Inventory and object storage never
//! choose progress, votes or membership; marker validation precedes reclamation.

use super::apply::{marker_id, marker_state};
use super::cache::GroupLog;
use super::snapshot::{snapshot_image, SnapshotError, SnapshotLimits};
use cellar_ltx::generation::{GenerationPin, GenerationStore};

pub(crate) const SNAPSHOT_REFERENCE: u64 = 1;
pub(crate) const PENDING_SNAPSHOT_REFERENCE: u64 = 2;

/// Run on the image store's owning blocking domain. Read one coherent engine
/// cut and validate the snapshot/applied boundary before adopting any image.
pub(crate) fn snapshot_pin(
    group: &GroupLog,
    images: &GenerationStore,
    schema: [u8; 32],
) -> Result<Option<GenerationPin>, SnapshotError> {
    let (snapshot, applied) = {
        let cache = group.read()?;
        (cache.snapshot.clone(), cache.applied.clone())
    };
    if snapshot.get_metadata().index == 1 && snapshot.data.is_empty() {
        return Ok(None);
    }
    let image = snapshot_image(group, schema, &snapshot, SnapshotLimits::default())?;
    if applied.position.index.get() == snapshot.get_metadata().index && applied.image != image {
        return Err(SnapshotError::Invalid(
            "snapshot/applied image at the same cut",
        ));
    }
    let Some(image) = &image else {
        return Ok(None);
    };
    let pin = images.adopt(marker_id(image)?, marker_state(image))?;
    pin.retain_reference(SNAPSHOT_REFERENCE, snapshot.get_metadata().index)?;
    Ok(Some(pin))
}

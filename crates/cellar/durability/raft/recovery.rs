// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

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
    // Retained snapshot metadata must use the destination's captured image
    // policy, including on joined-member restart. No transfer buffer is
    // allocated here; physical adoption still verifies the store's own bounds.
    let apply = images.apply_limits();
    let encoded = cellar_ltx::ltx::full_snapshot_bound(apply.decode.max_image_bytes, 512)
        .ok_or(SnapshotError::Limit)?;
    let limits = SnapshotLimits {
        max_image_bytes: apply.decode.max_image_bytes,
        max_encoded_bytes: SnapshotLimits::default().max_encoded_bytes.max(encoded),
        ..Default::default()
    };
    let image = snapshot_image(group, schema, &snapshot, limits)?;
    if applied.position.index.get() == snapshot.get_metadata().index && applied.image != image {
        return Err(SnapshotError::Invalid(
            "snapshot/applied image at the same cut",
        ));
    }
    let Some(image) = &image else {
        return Ok(None);
    };
    let pin = images.adopt(marker_id(image)?, marker_state(image)?)?;
    pin.retain_reference(SNAPSHOT_REFERENCE, snapshot.get_metadata().index)?;
    Ok(Some(pin))
}

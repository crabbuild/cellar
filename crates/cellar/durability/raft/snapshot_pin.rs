// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical retention under the runtime's existing exact snapshot ticket.
//! Root pins refer to the destination inventory; a source root is insufficient.

use super::*;
use cellar_ltx::internal::{NodeExtentStore, SealedImageRoot};
use std::sync::Arc;

#[derive(Clone)]
pub(super) enum SnapshotPin {
    Materialized(GenerationPin),
    Rooted {
        image: Arc<SealedImageRoot>,
        store: NodeExtentStore,
    },
}

impl SnapshotPin {
    pub(super) fn marker(&self) -> CommittedImage {
        match self {
            Self::Materialized(pin) => marker(pin),
            Self::Rooted { image, .. } => super::super::rooted::marker::encode(image),
        }
    }

    pub(super) fn retain_pending(
        &self,
        group: &GroupLog,
        revision: u64,
    ) -> Result<(), SnapshotError> {
        self.retain_pending_for(&group.identity().scope, group.birth_digest, revision)
    }

    pub(super) fn retain_pending_for(
        &self,
        scope: &GroupScope,
        birth: [u8; 32],
        revision: u64,
    ) -> Result<(), SnapshotError> {
        match self {
            Self::Materialized(pin) => {
                if !pin.retain_reference(PENDING_SNAPSHOT_REFERENCE, revision)? {
                    return Err(SnapshotError::Invalid("stale prospective install"));
                }
            }
            Self::Rooted { image, store } => {
                store.retain_image_reference(
                    super::super::rooted::marker::reference_key_for(scope, birth, 3),
                    revision,
                    image,
                )?;
            }
        }
        Ok(())
    }

    pub(super) fn retain(&self, group: &GroupLog, revision: u64) -> Result<(), SnapshotError> {
        self.retain_for(&group.identity().scope, group.birth_digest, revision)
    }

    pub(super) fn retain_for(
        &self,
        scope: &GroupScope,
        birth: [u8; 32],
        revision: u64,
    ) -> Result<(), SnapshotError> {
        match self {
            Self::Materialized(pin) => {
                pin.retain_reference(SNAPSHOT_REFERENCE, revision)?;
                pin.release_reference(PENDING_SNAPSHOT_REFERENCE, revision)?;
            }
            Self::Rooted { image, store } => {
                store.retain_image_reference(
                    super::super::rooted::marker::reference_key_for(scope, birth, 2),
                    revision,
                    image,
                )?;
                store.release_reference(
                    super::super::rooted::marker::reference_key_for(scope, birth, 3),
                    revision,
                )?;
            }
        }
        Ok(())
    }
}

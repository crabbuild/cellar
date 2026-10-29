// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Complete group-state storage. A synced page is data, not Raft authority.

mod enrollment;
mod image_pin;
mod manifest;
mod page_store;
mod page_wire;
mod pins;
mod retention;
mod root;

pub use manifest::{
    CellImageGeometry, CellManifestLeaf, EffectSummary, ManifestError, ManifestLimits,
    ManifestRoot, ManifestTree,
};
pub use page_store::{ManifestPageStore, ManifestReclaimReport, ManifestStoreUsage};

pub use enrollment::{
    decide_enrollment, EnrollmentDecision, EnrollmentError, EnrollmentPhase, EnrollmentRecord,
};
pub use image_pin::{cell_image_history_digest, CellImagePin};
pub(crate) use pins::ManifestPins;
pub use root::{GroupManifest, PreparedGroupManifest};

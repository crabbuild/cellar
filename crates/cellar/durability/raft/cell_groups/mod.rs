// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Complete group-state storage. A synced page is data, not Raft authority.

pub use super::host_sql::cell_groups::{
    CellGroupPreparedApply, CellGroupSqlPool, CellSqlEnrollment, CellSqlEnrollmentResolution,
    CellSqlLimits, CellSqlOptions, CellSqlOutcome, CellSqlPrepared, CellSqlQueryResult,
    CellSqlReceipt, CellSqlReceiptLookup, CellSqlReconciliation, CellSqlRejection,
    CellSqlRootOptions, CellSqlSubmission, CellSqlUsage,
};

pub use crate::raft::transport::{
    CellCommandGateway, CellQueryGateway, CellRemoteCommandResponse, CellRemoteEnrollmentResponse,
    CellRemoteQueryResponse, CellRemoteReceiptResponse,
};

mod apply;
mod archive;
pub use archive::{
    CellGroupArchive, CellGroupArchiveCheckpointReceiver, CellGroupArchiveCollectionLimits,
    CellGroupArchiveCollectionReport, CellGroupArchiveCoverage, CellGroupArchiveCreation,
    CellGroupArchiveError, CellGroupArchiveExecution, CellGroupArchiveHold, CellGroupArchiveIntent,
    CellGroupArchiveLimits, CellGroupArchivePool, CellGroupArchiveReference,
    CellGroupArchiveRegistry, CellGroupArchiveStore, CellGroupArchiveTailLimits,
    CellGroupArchiveTailPool, CellGroupArchiveTailReference, CellGroupArchiveUpload,
    CellGroupArchiveUploadLimits, CellGroupArchiveUploadPool, CellGroupArchiveUploadTicket,
    CellGroupArchiveUploadedTail, CellGroupArchiveUsage, PreparedCellGroupArchiveCheckpoint,
    PreparedCellGroupArchiveCreation, PreparedCellGroupArchivePublication,
    VerifiedCellGroupArchiveCheckpoint, VerifiedCellGroupArchiveRecovery,
};
pub(crate) mod checkpoint;
pub use checkpoint::{
    CellGroupCheckpoint, CellGroupCheckpointChunk, CellGroupCheckpointError,
    CellGroupCheckpointLimits, CellGroupCheckpointPart, CellGroupCheckpointPool,
    CellGroupCheckpointReceiver, CellGroupCheckpointUsage, PreparedCellGroupCheckpoint,
    PreparedLocalCellGroupCheckpoint,
};
mod controller;
pub use controller::{
    CellCommandAdmissionError, CellCommandCall, CellEnrollmentAdmissionError, CellEnrollmentCall,
    CellGroupArchiveCall, CellGroupArchiveControlError, CellGroupArchiveRegistration,
    CellGroupArchiveStatus, CellGroupCheckpointPolicy, CellGroupController,
    CellGroupControllerError, CellGroupControllerLimits, CellGroupControllerUsage,
    CellGroupProvisionCall, CellGroupProvisioning, CellGroupRegistration,
    CellGroupRegistrationCall, CellGroupRegistrationError,
};
mod dispatch;
pub use apply::{CellGroupApplier, CellGroupApplyImages};
pub use dispatch::{
    CellCommandError, CellCommandInvocation, CellCommandPreparation, CellCommandRequest,
    CellCommandResponse, CellCommandSettlement, CellCommandSubmission, CellDispatchError,
    CellDispatchLimits, CellEnrollmentError, CellEnrollmentInvocation, CellEnrollmentRequest,
    CellGroupDispatcher, CellQueryConsistency, CellQueryInvocation, CellQueryRequest,
    CellQueryResponse, CellReceiptInvocation, CellReceiptRequest, CellReceiptResponse,
};

mod execution_authority;
pub use execution_authority::{CellExecutionAuthority, CellReconciliationAuthority};

mod enrollment_authority;
pub use enrollment_authority::{CellEnrollmentAuthority, CellEnrollmentState};

mod enrollment;
mod enrollment_apply;
mod image_pin;
mod manifest;
pub(crate) mod membership;
pub use membership::CellGroupMembershipChange;
mod owner;
mod page_store;
mod page_wire;
mod pins;
mod proposal_admission;
mod read_authority;
pub(crate) mod reclaim;
pub use read_authority::{CellReadAuthority, CellReadConsistency};
pub use reclaim::{CellGroupReclaimError, CellGroupReclaimLimits, CellGroupReclaimReport};
mod seed_authority;
pub use seed_authority::{CellGroupSeedAuthority, CellGroupSeedSource};
mod retention;
mod root;
mod runtime;

pub use runtime::{
    CellGroupAdmissionUsage, CellGroupRuntime, CellGroupRuntimeEvent, CellGroupRuntimeEvents,
    CellGroupRuntimeUsage,
};

pub use manifest::{
    CellImageGeometry, CellManifestLeaf, EffectSummary, ManifestError, ManifestLimits,
    ManifestRoot, ManifestTree,
};
pub use page_store::{ManifestPageStore, ManifestReclaimReport, ManifestStoreUsage};

pub use crate::raft::catalog::{
    CellGroupArchiveBindingError, CellGroupBirth, CellGroupBirthRequest, CellGroupBirthSpec,
    CellGroupBootstrapPermit, CellRouteAttempt, CellRouteCacheUsage, CellRouteOperation,
    CellRouteRequest, CellRoutingError, CellRoutingFailure, CellRoutingLimits, CellRoutingSession,
    DirectoryLimits, DirectoryMutation, DirectoryPublication, DirectoryView,
    PreparedCellGroupArchiveBinding, RangeDirectory, ResolvedCellRoute,
};
pub use enrollment::{
    decide_enrollment, EnrollmentDecision, EnrollmentError, EnrollmentPhase, EnrollmentRecord,
};
pub use enrollment_apply::{EnrollmentApply, EnrollmentApplyError, EnrollmentImage};
pub use image_pin::{cell_image_history_digest, CellImagePin};
pub(crate) use pins::ManifestPins;
pub use root::{GroupManifest, PreparedGroupManifest};

pub use owner::{
    CellGroupApply, CellGroupEffect, CellGroupOwner, CellGroupPersist, CellGroupPersisted,
    CellGroupProposal, CellGroupSnapshotAdmission, CellGroupStatus, CellGroupTicket,
};

pub(crate) use dispatch::RemoteMutationRoute;

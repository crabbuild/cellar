// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Complete group-state storage. A synced page is data, not Raft authority.

pub(crate) use super::host_sql::cell_groups::CellSqlStageMode;
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
pub(crate) use archive::CellGroupArchiveAcquireOutcome;
pub(crate) use archive::ControllerExecution as ArchiveExecution;
pub(crate) mod archive_compaction;
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
    VerifiedFleetGroupArchiveCoverage,
};
pub(crate) mod checkpoint;
pub use checkpoint::{
    CellGroupCheckpoint, CellGroupCheckpointChunk, CellGroupCheckpointError,
    CellGroupCheckpointLimits, CellGroupCheckpointPart, CellGroupCheckpointPool,
    CellGroupCheckpointReceiver, CellGroupCheckpointUsage, PreparedCellGroupCheckpoint,
    PreparedLocalCellGroupCheckpoint,
};
mod controller;
pub(crate) use controller::CellGroupArchiveProvisioning;
pub(crate) use controller::PartitionGroupPage;
pub(crate) use controller::{ArchiveSeedClient, ArchiveSeedDescriptor};
pub use controller::{
    CellCommandAdmissionError, CellCommandCall, CellEnrollmentAdmissionError, CellEnrollmentCall,
    CellGroupArchiveCall, CellGroupArchiveControlError, CellGroupArchiveRegistration,
    CellGroupArchiveStatus, CellGroupCheckpointPolicy, CellGroupController,
    CellGroupControllerError, CellGroupControllerLimits, CellGroupControllerUsage,
    CellGroupEffectRegistration, CellGroupProvisionCall, CellGroupProvisionLimits,
    CellGroupProvisionNamespace, CellGroupProvisioning, CellGroupRegistration,
    CellGroupRegistrationCall, CellGroupRegistrationError, CellQueryCall,
};
pub(crate) use controller::{RetiredSource, S3Client, S3OutputProof};
mod disaster;
mod dispatch;
pub use disaster::CellGroupPermanentFenceAuthority;
mod host;
pub use apply::{CellGroupApplier, CellGroupApplyImages};
pub use dispatch::{
    CellCommandError, CellCommandInvocation, CellCommandPreparation, CellCommandRequest,
    CellCommandResponse, CellCommandSettlement, CellCommandSubmission, CellDispatchError,
    CellDispatchLimits, CellEnrollmentError, CellEnrollmentInvocation, CellEnrollmentRequest,
    CellEnrollmentResponse, CellGroupDispatcher, CellQueryConsistency, CellQueryInvocation,
    CellQueryRequest, CellQueryResponse, CellQueryRouting, CellReceiptInvocation,
    CellReceiptRequest, CellReceiptResponse, CellReceiptSelection,
};
pub(crate) use dispatch::{MutationRouting, PendingCellCommand};
pub use host::{
    CellGroupCellHandle, CellGroupHost, CellGroupHostLimits, CellGroupHostRegistration,
    CellGroupHostUsage, CellGroupNamespaceStorage,
};

mod effect_index;
mod execution_authority;
pub(crate) mod member_reclaim;
pub(crate) mod source_reclaim;
pub(crate) use execution_authority::CellActivationFence;
pub(crate) use execution_authority::CellEffectAuthority;
pub use execution_authority::{CellExecutionAuthority, CellReconciliationAuthority};

mod enrollment_authority;
pub use enrollment_authority::{CellEnrollmentAuthority, CellEnrollmentState};

mod enrollment;
mod enrollment_apply;
mod hibernate;
pub use hibernate::GroupParkCut;
pub(crate) use hibernate::ParkPrepareRequest;
pub(crate) use hibernate::{
    LocalParkChange, LocalParkRecord, ParkAcknowledgement, MAX_PARK_NEGOTIATION_MS,
};
mod image_pin;
mod manifest;
pub(crate) mod membership;
pub use membership::CellGroupMembershipChange;
mod log_admission;
pub(crate) mod log_retention;
mod membership_observation;
mod observation;
mod owner;
pub use membership_observation::CellGroupQuorumMembership;
pub use observation::{
    CellGroupObservation, CellGroupPartitionCell, CellGroupPartitionContext,
    CellGroupPartitionObservation, CellGroupPartitionWriter, CellGroupWorkload,
};
mod park_certificate;
pub(crate) use park_certificate::ParkCertificate;
mod page_store;
mod page_wire;
pub(crate) mod partition;
mod pins;
mod proposal_admission;
pub use log_retention::{
    CellGroupLogCapacity, CellGroupLogRetentionLimits, CellGroupLogRetentionUsage,
};
mod read_authority;
mod recovery;
pub(crate) use recovery::StagedCellGroupPartition;
pub use recovery::{
    CellGroupRecoveryLimits, CellGroupRecoveryPool, CellGroupTransferTarget,
    PreparedCellGroupRecovery, PreparedCellGroupTransfer, PreparedCellGroupTransferChild,
    SeededCellGroupRecovery,
};
pub(crate) use recovery::{PreparedRecoveryData, RecoverySeed, TransferSeed};
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
    CellGroupArchiveBindingError, CellGroupBirth, CellGroupBirthDisposition, CellGroupBirthRequest,
    CellGroupBirthSpec, CellGroupBootstrapPermit, CellGroupDirectoryCursor, CellGroupDirectoryPage,
    CellGroupDisasterFenceRequest, CellGroupRecoveryActivationAck,
    CellGroupRecoveryActivationPermit, CellGroupRecoveryError, CellGroupRecoveryRequest,
    CellGroupSealPermit, CellGroupSourceRetirementAck, CellGroupSourceRetirementPermit,
    CellGroupTransferActivationAck, CellGroupTransferActivationPermit, CellGroupTransferArchive,
    CellGroupTransitionPreparationInputs, CellGroupTransitionSource, CellRouteAttempt,
    CellRouteCacheUsage, CellRouteOperation, CellRouteRequest, CellRoutingError,
    CellRoutingFailure, CellRoutingLimits, CellRoutingSession, DirectoryLimits, DirectoryMutation,
    DirectoryPublication, DirectoryView, FencedCellGroupRecovery, FrozenCellGroupInventory,
    InstalledTransferVoter, PreparedCellGroupArchiveBinding, PreparedManagedCellGroupTransition,
    RangeDirectory, ResolvedCellRoute, TransferInstallQuorum, TransitionJournal, TransitionPhase,
    VerifiedCellGroupTransitionSources,
};
pub use enrollment::{
    decide_enrollment, EnrollmentDecision, EnrollmentError, EnrollmentPhase, EnrollmentRecord,
};
pub use enrollment_apply::{EnrollmentApply, EnrollmentApplyError, EnrollmentImage};
pub use image_pin::{cell_image_history_digest, CellImagePin};
pub(crate) use pins::ManifestPins;
pub use root::{
    GroupManifest, GroupSourceRetirement, GroupSourceSeal, GroupTransferOrigin,
    PreparedGroupManifest,
};

pub(crate) use owner::SelectedParkCertificate;
pub use owner::{
    CellGroupApply, CellGroupEffect, CellGroupOwner, CellGroupPersist, CellGroupPersisted,
    CellGroupProposal, CellGroupRecoveryActivation, CellGroupSealOutcome,
    CellGroupSnapshotAdmission, CellGroupSourceRetirement, CellGroupStatus, CellGroupTicket,
    CellGroupTransferActivation, TransferChildQuorum,
};

pub(crate) use dispatch::RemoteMutationRoute;

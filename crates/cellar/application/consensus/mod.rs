// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Command/query consensus contracts for opt-in Raft namespaces.
//!
//! These values describe durable identities and physical update envelopes.
//! Transport, storage and command registration are supplied by the host.

pub(crate) mod admission;
mod archive_status;
pub use archive_status::{ArchiveFailure, ArchiveReadiness, ArchiveStatus};
mod class;
pub(crate) use class::{
    callback_active, framework_dispatch, require_framework_dispatch, CommandTarget,
};
pub(crate) mod group_client;
mod group_contracts;
pub(crate) use group_contracts::cell_routing_key;
pub(crate) mod group_protocol;
mod native_applications;
pub use native_applications::RaftApplications;
mod native_cell;
mod native_configuration;
pub(crate) mod native_ingress;
mod native_services;
pub(crate) mod native_work;
pub(crate) mod sql_ingress_identity;
pub(in crate::application) use native_services::{kv_service, queue_service, sql_service};
pub(crate) mod native_rpc;
pub(crate) use native_cell::CellRpcActivation;
pub(in crate::application) use native_configuration::AdmittedCellConfiguration;
pub(crate) use native_configuration::NativeCellConfiguration;
mod performance;
pub use performance::{
    RaftDurationBucket, RaftOperationPerformance, RaftPerformanceCounter, RaftPerformanceSnapshot,
};
mod readiness;
pub use readiness::{RaftQueryReadiness, RaftReplicaRole};
mod resource_metrics;
mod resources;
pub use resources::{
    RaftBudgetUsage, RaftConnectionReservations, RaftControllerResources, RaftExtentUsage,
    RaftGroupResources, RaftImageUsage, RaftQueryPinUsage, RaftResourceUsage, RaftSqlUsage,
    RaftTransportBanks, RaftTransportResources,
};
pub(crate) mod protocol;

pub use crate::raft::archive::ArchiveLimits;
pub use crate::raft::completion::{
    LeadershipTransfer, MemberProgress, ReplicationStatus, RuntimeError,
};
pub use crate::raft::effects::{
    RaftAlarmLimits, RaftBlobLimits, RaftBlobReference, RaftDeliveryRequest, RaftEffectOptions,
    RaftEffectSigningKey, RaftEffectSink, RaftEffectSourceGrant, RaftEffectTarget,
    RaftEffectVerificationKey, RaftInbox, RaftNativeEffectSink, RaftOutbox, RaftOutboxLimits,
    RaftOutboxProgress,
};
pub use crate::raft::physical_inventory::{RaftPhysicalHistory, RaftPhysicalInventory};
pub use crate::raft::receipt_maintenance::ReceiptMaintenanceLimits;

pub use crate::raft::archive::ArchiveReference;
pub use crate::raft::catalog::{
    BirthDisposition, BirthProposal, CatalogOptions, DisasterFenceAuthority, DisasterFenceRequest,
    DisasterRecoveryRequest, InitialMember, ManagedBirthOwner, NamespaceConfig, RaftImageStorage,
    VoterPolicy,
};
pub use crate::raft::catalog::{Catalog, CatalogError};
pub use crate::raft::disaster::S3DisasterRecovery;
pub use cellar_logic::cell_group_placement::{
    HotCell, PartitionBasis, PartitionDecision, PartitionLimits, PartitionLoad, PartitionPolicy,
    PartitionSample, PartitionTracker, SplitReason, WaitReason,
};
pub use cellar_logic::raft_leadership::LeaderPolicy;
pub use cellar_logic::raft_placement::PlacementPolicy;

pub use crate::raft::cell_groups::{
    CellCommandError, CellDispatchError, CellDispatchLimits, CellEnrollmentError,
    CellEnrollmentRequest, CellEnrollmentResponse, CellGroupArchiveControlError,
    CellGroupArchiveLimits, CellGroupArchivePool, CellGroupArchiveStatus,
    CellGroupArchiveTailLimits, CellGroupArchiveTailPool, CellGroupArchiveUploadLimits,
    CellGroupBirthSpec, CellGroupCheckpointLimits, CellGroupCheckpointPolicy,
    CellGroupControllerError, CellGroupControllerLimits, CellGroupDisasterFenceRequest,
    CellGroupHostLimits, CellGroupObservation, CellGroupPartitionCell, CellGroupPartitionContext,
    CellGroupPartitionObservation, CellGroupPartitionWriter, CellGroupPayloadLimits,
    CellGroupPayloadUsage, CellGroupPermanentFenceAuthority, CellGroupProposal,
    CellGroupProvisionLimits, CellGroupQuorumMembership, CellGroupRecoveryLimits,
    CellGroupRecoveryPool, CellGroupSealOutcome, CellGroupSourceRetirement,
    CellGroupSourceRetirementAck, CellGroupTransferActivation, CellGroupTransferActivationAck,
    CellGroupTransferArchive, CellGroupTransitionSource, CellGroupWorkload, CellReceiptRequest,
    CellReceiptResponse, CellSqlLimits, DirectoryLimits, DirectoryPublication, GroupSourceSeal,
    InstalledTransferVoter, ManifestLimits, TransferChildQuorum, TransferInstallQuorum,
    VerifiedCellGroupTransitionSources,
};
pub use crate::raft::fleet_catalog::{
    fleet_capacity_profile_digest, FleetAbortedMovementCompletion, FleetCapacityAction,
    FleetCapacityAdmission, FleetCapacityAttestation, FleetCapacityAuthorization,
    FleetCapacityRequest, FleetCatalog, FleetCatalogError, FleetFencedMovementCompletion,
    FleetGroupArchiveReplayOptions, FleetGroupArchiveVerifier, FleetGroupAuthorization,
    FleetHostAuthorization, FleetIsolationAttestation, FleetMovementAdmission,
    FleetMovementCompletion, FleetMovementCompletionRequest, FleetMovementExecutionRequest,
    FleetMovementPhase, FleetMovementProgress, FleetMovementRequest, FleetNodeEligibility,
    FleetNodeIsolation, FleetNodeIsolationRequest, FleetNodeSpec, FleetNodeState,
    FleetPendingMovement, FleetPendingReservation, FleetPermitAuthorization,
    FleetReplicaSeparation, FleetRequestIdentity, FleetResourceAdmission, FleetResourceAttestation,
    FleetResourceBounds, FleetResourceRequest,
};
pub use crate::raft::fleet_catalog::{
    FleetAutomaticRecoveryOptions, FleetAutomaticTopologyOptions, FleetAutomaticTopologyStatus,
    FleetAutomaticTopologyWait, FleetDockerIsolation, FleetDockerResourcePolicy,
    FleetDockerResources, FleetFailureDetectionOptions, FleetFailureDetectionStatus,
    FleetFailureRecoveryContext, FleetFailureRecoveryOptions, FleetFailureRecoveryPhase,
    FleetFailureRecoveryPlatform, FleetFailureRecoveryStatus, FleetIsolatedRepairContext,
    FleetIsolatedRepairEvidence, FleetIsolatedRepairInputs, FleetIsolatedRepairServiceStatus,
    FleetMovementEvidence, FleetMovementService, FleetMovementServiceEntry,
    FleetMovementServiceFailure, FleetMovementServiceOptions, FleetMovementServiceStatus,
    FleetNodeFailureStatus, FleetNodeHealth, FleetRestartService, FleetRestartServiceOptions,
    FleetRestartServiceStatus, FleetTopologyCollectionOptions, FleetTopologyEvidence,
    FleetTopologyServiceInputs, FleetTopologyServiceStatus,
};
pub use crate::raft::fleet_catalog::{
    FleetTopologyHistoryResources, FleetTopologyNodeResources, FleetTopologyPlan,
    FleetTopologyPolicy, FleetTopologyPreview, FleetTopologyRequest, FleetTopologyVerification,
};
pub use crate::raft::fleet_catalog::{
    FleetTopologyIntentRequest, FleetTopologyMovementEntry, FleetTopologyMovementPage,
    FleetTopologyOperation, FleetTopologyPhase,
};
pub use crate::raft::fleet_catalog::{
    RaftManagedArchiveProvisioning, RaftManagedFirstGroup, RaftManagedFirstGroupRequest,
    ReopenedManagedCellGroupTransition,
};
pub use crate::raft::fleet_stop::{
    FleetBootBinding, FleetStopAcceptance, FleetStopAction, FleetStopRequest, VerifiedFleetClosure,
};
pub use crate::raft::group_host::{
    CellGroupNodeArchiveOptions, CellGroupNodeLimits, RaftCellsConfig,
    RaftGroupArchiveInitialization, RaftGroupAssignment, RaftGroupPlacement, RaftNamespaceConfig,
    RaftNamespaceInitialization, RaftNamespaceProvisioning,
};
pub use crate::raft::host::{
    FleetImageState, FleetMembershipConfiguration, FleetMembershipObservation, FleetPublishedCut,
    FleetPublishedImage, RaftArchiveOptions, RaftCellsServices, RaftControllerOptions,
    RaftControllerStatus, RaftExtentCacheLimits, RaftExtentLimits, RaftExtentOptions,
    RaftHostEnrollmentObservation, RaftHostObservation, RaftHostOptions, RaftHostStartupBounds,
    RaftInspectionScanOptions, RaftManagedHostOptions, RaftNamespaceRegistration,
    RaftNodeDirectoryOptions, RaftPeerFleetMembershipObservation, RaftPeerNode,
    RaftPeerNodeObservation,
};
pub use crate::raft::inspection::{
    CellInspection, GroupInspection, InspectionError, LeaderLoad, MemberInspection, NodeInspection,
    RaftNodeProfile, ReplicaOccupancy,
};
pub use crate::raft::member_state::{
    FleetGroupBinding, FleetMembershipPermit, FleetMembershipStatus, FleetPermitPhase,
    JoiningMember, RepairPhase, RepairPlan, RepairStatus, RepairStep,
};
pub use crate::raft::operations::{
    RaftCloseReportLimits, RaftDrainReport, RaftNodeShutdownError, RaftNodeShutdownReport,
};
pub use crate::raft::options::{LogOpenMode, RaftRuntimeOptions, ReplicaLogOptions};
pub use crate::raft::transport_options::{
    NamespaceCommandOrigin, NamespaceInspectionOrigin, NamespaceQueryOrigin, RaftTls,
    TransportOptions,
};
pub use crate::raft::version::{
    VersionError, VersionKeys, VersionSigningKey, VersionToken, VersionVerificationKey,
};
pub use crate::storage::{CommandInputs, CommandTransaction};
pub use cellar_logic::cell_group::{CellBound, CellRange, RouteEpoch};
pub use class::{
    command_request_digest, CommandContext, OperationContract, OperationKind, QueryContext,
    QueryVersion, RaftClass, RegistrationError,
};
pub use group_protocol::GroupEntryLimits;

pub use crate::raft::version::CellVersionToken;
pub use cellar_logic::cell_group::{CommitWitness, GroupHistory};
pub use group_client::{
    CellGroupCollectionFailure, CellGroupCollectionUsage, CellGroupControlError,
    CellGroupControlServiceUsage, CellGroupImageUsage, CellGroupResourceUsage,
    CellGroupStartupError, CommandOptions, CommandReceipt, LearnerActivation, MutationOutcomes,
    QueryConsistency, QueryOptions, QueryResult, QueryRouting, RaftCellHandle, RaftCells,
    RaftCloseError, RaftCloseReport, RaftGroupCloseReport, RaftGroupHandle, StaleReadPolicy,
};

pub use crate::raft::version::CellVersionObservation;
pub use cellar_logic::cell_group::{CellHistory, CellId, CellVersion};
pub use cellar_logic::consensus::{
    ClientId, ClusterId, CommandFailure, CommandId, CommittedVersion, DiskLineage, Durability,
    EngineInstanceId, GroupId, GroupScope, IncarnationId, LogIndex, LogPosition, MemberId,
    NamespaceId, NodeId, NodeIdentity, ProcessGeneration, ProposalId, Term,
};
pub use cellar_logic::NamespaceMode;
pub use protocol::{
    CellUpdate, EnvelopeLimits, ImageCommitment, LtxImage, LtxUpdate, ProtocolError,
};

/// Unstable host-storage interfaces for integration verification and embedding.
#[doc(hidden)]
pub mod verification {
    pub use crate::raft::apply::{ImageApplier, ImageApplyError, PublishedImage};
    pub use crate::raft::archive::{
        ArchiveDependency, ArchiveError, ArchiveLimits, ArchiveMetadata, ArchiveRecoverySet,
        ArchiveReference, ArchiveVerifier, CellArchive, CommittedArchiveCut, RootAllocation,
        RootArchiveReference, RootArchiveSnapshot, RootDescriptor, RootIntent, RootProtection,
        RootReference, RootRegistry, RootRegistryId, RootReservation,
    };
    pub use crate::raft::cache::{
        CommittedImage, DurableApply, GroupLog, GroupWrite, SnapshotChange,
    };
    pub use crate::raft::catalog::{
        BirthDisposition, BirthInventoryCursor, BirthInventoryEntry, BirthInventoryPage,
        BirthProposal, BootstrapPermit, Catalog, CatalogError, CatalogOptions,
        FrozenBirthInventory, GroupBirth, InitialMember, NamespaceCatalog, NamespaceConfig,
        NodePublication, NodeRegistration, NodeView, RaftImageStorage, VoterPolicy,
    };
    pub use crate::raft::completion::{
        AcceptedProposal, ApplyCompletion, ApplyTicket, GroupStatus, RuntimeError, RuntimeEvent,
        RuntimeEventKind, SnapshotCompletion,
    };
    pub use crate::raft::execution::{
        query_registered, AppliedExecution, CellCommittedReceipt, CellEnrollmentAttempt,
        CellEnrollmentResolution, CellPreparedCommand, CellPrivateEnrollment, CellPrivateExecutor,
        CellProposalRejection, CellReconciliation, CellStageOutcome, CellSubmissionOutcome,
        CommandAttempt, CommandAuthority, CommandRequest, CommittedReceipt, ExecutionError,
        ExecutionLimits, ExecutionStatus, PreparedCommand, PrivateExecutor, PrivateOptions,
        RegisteredQueryOutput, RootPrivateOptions, StageOutcome, StoredReceipt,
    };
    pub use crate::raft::fleet_catalog::{
        FleetBirthAuthorization, FleetBirthReservation, FleetCatalog, FleetCatalogError,
        FleetChange, FleetCommit, FleetCursor, FleetEntry, FleetEvacuationAdmission,
        FleetEvacuationArchive, FleetEvacuationCancelFence, FleetEvacuationCancelRequest,
        FleetEvacuationCancellation, FleetEvacuationCandidate, FleetEvacuationVerification,
        FleetFinalizationAdmission, FleetHistory, FleetHistoryState, FleetImportPhase,
        FleetInventory, FleetIsolatedRemovalAdmission, FleetIsolatedRemovalRequest, FleetMutation,
        FleetNamespaceImport, FleetPage, FleetRemovalAdmission, FleetRemovalRequest,
        FleetRestartAdmission, FleetRestartBirthAction, FleetRestartBirthResolution,
        FleetRestartGateAdmission, FleetRestartGateCancelRequest, FleetRestartGateRequest,
        FleetRestartHistoryProgress, FleetRestartOfflineAdmission, FleetRestartOfflineRequest,
        FleetRestartOperatorAction, FleetRestartOperatorActionAdmission,
        FleetRestartOperatorActionRequest, FleetRestartOperatorAdmission,
        FleetRestartOperatorCancelAdmission, FleetRestartOperatorCancelRequest,
        FleetRestartOperatorCancelled, FleetRestartOperatorCompleted, FleetRestartOperatorDriver,
        FleetRestartOperatorPhase, FleetRestartOperatorProgress, FleetRestartOperatorRequest,
        FleetRestartOperatorStatus, FleetRestartPendingWork, FleetRestartReadyAdmission,
        FleetRestartReadyRequest, FleetRestartRequest, FleetRestartResumeAdmission,
        FleetRestartResumeCompleteRequest, FleetRestartResumeCompletion, FleetRestartResumePhase,
        FleetRestartResumeProgress, FleetRestartResumeRequest, FleetRestartResumeVerification,
        FleetRestartReturnAdmission, FleetRestartReturnRequest, FleetRestartRuntimeCandidate,
        FleetRestartRuntimeProgress, FleetRestartSettlementBlocker, FleetRestartSettlementProgress,
        FleetRestartStopAdmission, FleetRestartStopRefreshRequest, FleetRestartStopRequest,
        FleetRestartVerification, FleetSnapshot, FleetTable,
    };
    pub use crate::raft::inspection::{
        CellInspection, LeaderLoad, MemberInspection, NodeInspection, RaftNodeProfile,
        ReplicaOccupancy,
    };
    pub use crate::raft::member_state::{GroupMembership, JoiningMember};
    pub use crate::raft::options::RaftRuntimeOptions;
    pub use crate::raft::options::{LogOpenMode, ReplicaLogOptions};
    pub use crate::raft::rooted::{
        query_registered_rooted, RootImageApplier, RootPublishedImage, RootQueryOptions,
    };
    pub use crate::raft::runtime::{RaftRuntime, RuntimeEvents};
    pub use crate::raft::snapshot::{
        PreparedSnapshot, RootSnapshotSet, RootSnapshotSource, SnapshotCheckpoint,
        SnapshotDownload, SnapshotError, SnapshotLimits, SnapshotPackage, SnapshotReference,
    };
    pub use crate::raft::storage::{
        CellGroupBootstrap, GroupBootstrap, LogError, ReplicaLog, SyncReceipt,
    };
    pub use crate::raft::transport::{
        CellGroupCheckpointRegistration, CellGroupSeedError, CellGroupSeedSession, Delivery,
        GroupRoute, Lane, RaftPeer, RaftTransport, TransportError, TransportUsage,
    };
    pub use crate::raft::transport_options::{
        NamespaceCommandOrigin, NamespaceInspectionOrigin, NamespaceQueryOrigin, RaftTls,
        TransportOptions,
    };
    /// Contracts under integration into the replacement group runtime.
    pub mod cell_groups {
        pub use super::super::group_contracts::cell_routing_key;
        pub use super::super::group_protocol::*;
        pub use crate::raft::cell_groups::*;
        pub use crate::raft::version::{CellVersionObservation, CellVersionToken};
        pub use cellar_logic::cell_group::{
            CellBound, CellHistory, CellId, CellImage, CellImageCommitment, CellKey, CellRange,
            CellVersion, CommitWitness, GroupHistory, GroupRoute, MembershipRevision,
            MutationSequence, ParkGeneration, RouteEpoch, TransitionId,
        };
    }
}

// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Host/engine-lifetime observations with bounded storage and short lock scopes.
use crate::application::consensus::{
    RaftDurationBucket, RaftOperationPerformance, RaftPerformanceCounter, RaftPerformanceSnapshot,
};
use crate::asyncrt::DomainToken;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};

pub(super) const BOUNDS_US: [u64; 22] = [
    50,
    100,
    250,
    500,
    1000,
    2500,
    5000,
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    1_000_000,
    5_000_000,
    30_000_000,
    60_000_000,
    120_000_000,
    300_000_000,
    900_000_000,
    1_800_000_000,
    3_600_000_000,
    u64::MAX,
];

#[derive(Clone, Copy)]
pub(crate) enum Operation {
    EngineQueue,
    EngineFileSync,
    EngineDirectorySync,
    EnginePersist,
    SqlQueue,
    SqlStage,
    SqlPrepare,
    SqlPublish,
    SqlQuery,
    SqlArchiveCapture,
    SqlOther,
    ObjectGet,
    ObjectPutCreate,
    ObjectPutUpdate,
    ObjectPutOverwrite,
    ControllerProbe,
    ControllerAction,
    ControlQueue,
    DataQueue,
    ControlExchange,
    DataExchange,
    ControlAdmit,
    DataAdmit,
    ArchiveAdmissionCheck,
    ArchiveAdmissionWait,
    ArchivePublicationBatch,
    SnapshotEncode,
    RecoveryAuthorize,
    RecoveryPeerFetch,
    RecoveryPeerReceive,
    RecoveryAppendQueue,
    RecoveryAppend,
    RecoveryArchiveFetch,
    RecoveryPeerVerify,
    RecoveryArchiveReplay,
    RecoveryInstall,
    RecoveryArchiveWait,
    SnapshotEncodeQueue,
    SnapshotSeedEncode,
    SnapshotCheckpointEncode,
    SnapshotTransferEncode,
    ArchivePhysicalAdmission,
    ArchivePhysicalQueue,
    EnginePurge,
    ImageStageLeaderSql,
    ImageStageLeaderInventoryWait,
    ImageStageLeaderLtxDecode,
    ImageStageLeaderExtentRead,
    ImageStageLeaderExtentHash,
    ImageStageLeaderExtentWrite,
    ImageStageLeaderFileSync,
    ImageStageLeaderDirectorySync,
    ImageStageLeaderBarrierWait,
    ImageStageLeaderOther,
    ImagePrepareLeaderSql,
    ImagePrepareLeaderInventoryWait,
    ImagePrepareLeaderLtxDecode,
    ImagePrepareLeaderExtentRead,
    ImagePrepareLeaderExtentHash,
    ImagePrepareLeaderExtentWrite,
    ImagePrepareLeaderFileSync,
    ImagePrepareLeaderDirectorySync,
    ImagePrepareLeaderBarrierWait,
    ImagePrepareLeaderOther,
    ImagePrepareFollowerSql,
    ImagePrepareFollowerInventoryWait,
    ImagePrepareFollowerLtxDecode,
    ImagePrepareFollowerExtentRead,
    ImagePrepareFollowerExtentHash,
    ImagePrepareFollowerExtentWrite,
    ImagePrepareFollowerFileSync,
    ImagePrepareFollowerDirectorySync,
    ImagePrepareFollowerBarrierWait,
    ImagePrepareFollowerOther,
    ImagePublishLeaderSql,
    ImagePublishLeaderInventoryWait,
    ImagePublishLeaderLtxDecode,
    ImagePublishLeaderExtentRead,
    ImagePublishLeaderExtentHash,
    ImagePublishLeaderExtentWrite,
    ImagePublishLeaderFileSync,
    ImagePublishLeaderDirectorySync,
    ImagePublishLeaderBarrierWait,
    ImagePublishLeaderOther,
    ImagePublishFollowerSql,
    ImagePublishFollowerInventoryWait,
    ImagePublishFollowerLtxDecode,
    ImagePublishFollowerExtentRead,
    ImagePublishFollowerExtentHash,
    ImagePublishFollowerExtentWrite,
    ImagePublishFollowerFileSync,
    ImagePublishFollowerDirectorySync,
    ImagePublishFollowerBarrierWait,
    ImagePublishFollowerOther,
    SqlPrepareLeader,
    SqlPrepareFollower,
    SqlPublishLeader,
    SqlPublishFollower,
}

const OPERATIONS: [Operation; 98] = [
    Operation::EngineQueue,
    Operation::EngineFileSync,
    Operation::EngineDirectorySync,
    Operation::EnginePersist,
    Operation::SqlQueue,
    Operation::SqlStage,
    Operation::SqlPrepare,
    Operation::SqlPublish,
    Operation::SqlQuery,
    Operation::SqlArchiveCapture,
    Operation::SqlOther,
    Operation::ObjectGet,
    Operation::ObjectPutCreate,
    Operation::ObjectPutUpdate,
    Operation::ObjectPutOverwrite,
    Operation::ControllerProbe,
    Operation::ControllerAction,
    Operation::ControlQueue,
    Operation::DataQueue,
    Operation::ControlExchange,
    Operation::DataExchange,
    Operation::ControlAdmit,
    Operation::DataAdmit,
    Operation::ArchiveAdmissionCheck,
    Operation::ArchiveAdmissionWait,
    Operation::ArchivePublicationBatch,
    Operation::SnapshotEncode,
    Operation::RecoveryAuthorize,
    Operation::RecoveryPeerFetch,
    Operation::RecoveryPeerReceive,
    Operation::RecoveryAppendQueue,
    Operation::RecoveryAppend,
    Operation::RecoveryArchiveFetch,
    Operation::RecoveryPeerVerify,
    Operation::RecoveryArchiveReplay,
    Operation::RecoveryInstall,
    Operation::RecoveryArchiveWait,
    Operation::SnapshotEncodeQueue,
    Operation::SnapshotSeedEncode,
    Operation::SnapshotCheckpointEncode,
    Operation::SnapshotTransferEncode,
    Operation::ArchivePhysicalAdmission,
    Operation::ArchivePhysicalQueue,
    Operation::EnginePurge,
    Operation::ImageStageLeaderSql,
    Operation::ImageStageLeaderInventoryWait,
    Operation::ImageStageLeaderLtxDecode,
    Operation::ImageStageLeaderExtentRead,
    Operation::ImageStageLeaderExtentHash,
    Operation::ImageStageLeaderExtentWrite,
    Operation::ImageStageLeaderFileSync,
    Operation::ImageStageLeaderDirectorySync,
    Operation::ImageStageLeaderBarrierWait,
    Operation::ImageStageLeaderOther,
    Operation::ImagePrepareLeaderSql,
    Operation::ImagePrepareLeaderInventoryWait,
    Operation::ImagePrepareLeaderLtxDecode,
    Operation::ImagePrepareLeaderExtentRead,
    Operation::ImagePrepareLeaderExtentHash,
    Operation::ImagePrepareLeaderExtentWrite,
    Operation::ImagePrepareLeaderFileSync,
    Operation::ImagePrepareLeaderDirectorySync,
    Operation::ImagePrepareLeaderBarrierWait,
    Operation::ImagePrepareLeaderOther,
    Operation::ImagePrepareFollowerSql,
    Operation::ImagePrepareFollowerInventoryWait,
    Operation::ImagePrepareFollowerLtxDecode,
    Operation::ImagePrepareFollowerExtentRead,
    Operation::ImagePrepareFollowerExtentHash,
    Operation::ImagePrepareFollowerExtentWrite,
    Operation::ImagePrepareFollowerFileSync,
    Operation::ImagePrepareFollowerDirectorySync,
    Operation::ImagePrepareFollowerBarrierWait,
    Operation::ImagePrepareFollowerOther,
    Operation::ImagePublishLeaderSql,
    Operation::ImagePublishLeaderInventoryWait,
    Operation::ImagePublishLeaderLtxDecode,
    Operation::ImagePublishLeaderExtentRead,
    Operation::ImagePublishLeaderExtentHash,
    Operation::ImagePublishLeaderExtentWrite,
    Operation::ImagePublishLeaderFileSync,
    Operation::ImagePublishLeaderDirectorySync,
    Operation::ImagePublishLeaderBarrierWait,
    Operation::ImagePublishLeaderOther,
    Operation::ImagePublishFollowerSql,
    Operation::ImagePublishFollowerInventoryWait,
    Operation::ImagePublishFollowerLtxDecode,
    Operation::ImagePublishFollowerExtentRead,
    Operation::ImagePublishFollowerExtentHash,
    Operation::ImagePublishFollowerExtentWrite,
    Operation::ImagePublishFollowerFileSync,
    Operation::ImagePublishFollowerDirectorySync,
    Operation::ImagePublishFollowerBarrierWait,
    Operation::ImagePublishFollowerOther,
    Operation::SqlPrepareLeader,
    Operation::SqlPrepareFollower,
    Operation::SqlPublishLeader,
    Operation::SqlPublishFollower,
];

impl Operation {
    fn name(self) -> &'static str {
        match self {
            Self::EngineQueue => "engine_queue",
            Self::EngineFileSync => "engine_file_sync",
            Self::EngineDirectorySync => "engine_directory_sync",
            Self::EnginePersist => "engine_persist",
            Self::SqlQueue => "sql_queue",
            Self::SqlStage => "sql_stage",
            Self::SqlPrepare => "sql_prepare",
            Self::SqlPublish => "sql_publish",
            Self::SqlQuery => "sql_query",
            Self::SqlArchiveCapture => "sql_archive_capture",
            Self::SqlOther => "sql_other",
            Self::ObjectGet => "object_get",
            Self::ObjectPutCreate => "object_put_create",
            Self::ObjectPutUpdate => "object_put_update",
            Self::ObjectPutOverwrite => "object_put_overwrite",
            Self::ControllerProbe => "controller_probe",
            Self::ControllerAction => "controller_action",
            Self::ControlQueue => "transport_control_queue",
            Self::DataQueue => "transport_data_queue",
            Self::ControlExchange => "transport_control_exchange",
            Self::DataExchange => "transport_data_exchange",
            Self::ControlAdmit => "transport_control_admit",
            Self::DataAdmit => "transport_data_admit",
            Self::ArchiveAdmissionCheck => "archive_admission_check",
            Self::ArchiveAdmissionWait => "archive_admission_wait",
            Self::ArchivePublicationBatch => "archive_publication_batch",
            Self::SnapshotEncode => "snapshot_encode",
            Self::RecoveryAuthorize => "recovery_authorize",
            Self::RecoveryPeerFetch => "recovery_peer_fetch",
            Self::RecoveryPeerReceive => "recovery_peer_receive",
            Self::RecoveryAppendQueue => "recovery_append_queue",
            Self::RecoveryAppend => "recovery_append",
            Self::RecoveryArchiveFetch => "recovery_archive_fetch",
            Self::RecoveryPeerVerify => "recovery_peer_verify",
            Self::RecoveryArchiveReplay => "recovery_archive_replay",
            Self::RecoveryInstall => "recovery_install",
            Self::RecoveryArchiveWait => "recovery_archive_wait",
            Self::SnapshotEncodeQueue => "snapshot_encode_queue",
            Self::SnapshotSeedEncode => "snapshot_seed_encode",
            Self::SnapshotCheckpointEncode => "snapshot_checkpoint_encode",
            Self::SnapshotTransferEncode => "snapshot_transfer_encode",
            Self::ArchivePhysicalAdmission => "archive_physical_admission",
            Self::ArchivePhysicalQueue => "archive_physical_queue",
            Self::EnginePurge => "engine_purge",
            Self::ImageStageLeaderSql => "image_stage_leader_sql",
            Self::ImageStageLeaderInventoryWait => "image_stage_leader_inventory_wait",
            Self::ImageStageLeaderLtxDecode => "image_stage_leader_ltx_decode",
            Self::ImageStageLeaderExtentRead => "image_stage_leader_extent_read",
            Self::ImageStageLeaderExtentHash => "image_stage_leader_extent_hash",
            Self::ImageStageLeaderExtentWrite => "image_stage_leader_extent_write",
            Self::ImageStageLeaderFileSync => "image_stage_leader_file_sync",
            Self::ImageStageLeaderDirectorySync => "image_stage_leader_directory_sync",
            Self::ImageStageLeaderBarrierWait => "image_stage_leader_barrier_wait",
            Self::ImageStageLeaderOther => "image_stage_leader_other",
            Self::ImagePrepareLeaderSql => "image_prepare_leader_sql",
            Self::ImagePrepareLeaderInventoryWait => "image_prepare_leader_inventory_wait",
            Self::ImagePrepareLeaderLtxDecode => "image_prepare_leader_ltx_decode",
            Self::ImagePrepareLeaderExtentRead => "image_prepare_leader_extent_read",
            Self::ImagePrepareLeaderExtentHash => "image_prepare_leader_extent_hash",
            Self::ImagePrepareLeaderExtentWrite => "image_prepare_leader_extent_write",
            Self::ImagePrepareLeaderFileSync => "image_prepare_leader_file_sync",
            Self::ImagePrepareLeaderDirectorySync => "image_prepare_leader_directory_sync",
            Self::ImagePrepareLeaderBarrierWait => "image_prepare_leader_barrier_wait",
            Self::ImagePrepareLeaderOther => "image_prepare_leader_other",
            Self::ImagePrepareFollowerSql => "image_prepare_follower_sql",
            Self::ImagePrepareFollowerInventoryWait => "image_prepare_follower_inventory_wait",
            Self::ImagePrepareFollowerLtxDecode => "image_prepare_follower_ltx_decode",
            Self::ImagePrepareFollowerExtentRead => "image_prepare_follower_extent_read",
            Self::ImagePrepareFollowerExtentHash => "image_prepare_follower_extent_hash",
            Self::ImagePrepareFollowerExtentWrite => "image_prepare_follower_extent_write",
            Self::ImagePrepareFollowerFileSync => "image_prepare_follower_file_sync",
            Self::ImagePrepareFollowerDirectorySync => "image_prepare_follower_directory_sync",
            Self::ImagePrepareFollowerBarrierWait => "image_prepare_follower_barrier_wait",
            Self::ImagePrepareFollowerOther => "image_prepare_follower_other",
            Self::ImagePublishLeaderSql => "image_publish_leader_sql",
            Self::ImagePublishLeaderInventoryWait => "image_publish_leader_inventory_wait",
            Self::ImagePublishLeaderLtxDecode => "image_publish_leader_ltx_decode",
            Self::ImagePublishLeaderExtentRead => "image_publish_leader_extent_read",
            Self::ImagePublishLeaderExtentHash => "image_publish_leader_extent_hash",
            Self::ImagePublishLeaderExtentWrite => "image_publish_leader_extent_write",
            Self::ImagePublishLeaderFileSync => "image_publish_leader_file_sync",
            Self::ImagePublishLeaderDirectorySync => "image_publish_leader_directory_sync",
            Self::ImagePublishLeaderBarrierWait => "image_publish_leader_barrier_wait",
            Self::ImagePublishLeaderOther => "image_publish_leader_other",
            Self::ImagePublishFollowerSql => "image_publish_follower_sql",
            Self::ImagePublishFollowerInventoryWait => "image_publish_follower_inventory_wait",
            Self::ImagePublishFollowerLtxDecode => "image_publish_follower_ltx_decode",
            Self::ImagePublishFollowerExtentRead => "image_publish_follower_extent_read",
            Self::ImagePublishFollowerExtentHash => "image_publish_follower_extent_hash",
            Self::ImagePublishFollowerExtentWrite => "image_publish_follower_extent_write",
            Self::ImagePublishFollowerFileSync => "image_publish_follower_file_sync",
            Self::ImagePublishFollowerDirectorySync => "image_publish_follower_directory_sync",
            Self::ImagePublishFollowerBarrierWait => "image_publish_follower_barrier_wait",
            Self::ImagePublishFollowerOther => "image_publish_follower_other",
            Self::SqlPrepareLeader => "sql_prepare_leader",
            Self::SqlPrepareFollower => "sql_prepare_follower",
            Self::SqlPublishLeader => "sql_publish_leader",
            Self::SqlPublishFollower => "sql_publish_follower",
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Counter {
    EngineReadBytes,
    EngineWriteRequestedBytes,
    EngineWriteCompletedBytes,
    PersistGroups,
    PersistEntries,
    PersistEntryBytes,
    SqlStageInputBytes,
    SqlPreparedProposalBytes,
    SqlPreparedLtxBytes,
    SqlQueryInputBytes,
    SqlQueryOutputBytes,
    ObjectReadBytes,
    ObjectPutRequestedBytes,
    ObjectPutConfirmedBytes,
    ControlReadBytes,
    ControlWriteBytes,
    DataReadBytes,
    DataWriteBytes,
    QueryReadBytes,
    QueryWriteBytes,
    ArchiveAdmissionGranted,
    ArchiveAdmissionObservationRefused,
    ArchiveAdmissionRetainedBytesRefused,
    ArchiveAdmissionRetainedEntriesRefused,
    ArchiveAdmissionReservedBytesDeferred,
    ArchiveAdmissionReservedEntriesDeferred,
    ArchivePublicationCutSucceeded,
    ArchivePublicationCutFailed,
    ArchiveCoverageAdvanced,
    ArchiveCoverageIndexAdvance,
    ArchiveCoverageImagePending,
    ArchiveCoverageRejected,
    ArchiveCoverageUnchanged,
    ArchiveCoverageMissingCell,
    RecoveryPeerReceivedBytes,
    RecoveryPeerAppendedBytes,
    SnapshotSeedEncodedBytes,
    SnapshotCheckpointEncodedBytes,
    SnapshotTransferEncodedBytes,
    SnapshotPreparedHits,
    SnapshotPreparedMisses,
    SnapshotPreparedReusedBytes,
    ImageSqlCalls,
    ImageInventoryWaitCalls,
    ImageLtxDecodeCalls,
    ImageExtentReadCalls,
    ImageExtentHashCalls,
    ImageExtentWriteCalls,
    ImageFileSyncCalls,
    ImageDirectorySyncCalls,
    ImageBarrierWaitCalls,
    ImageOtherCalls,
    ImageExtentReadBytes,
    ImageExtentWriteBytes,
    ImageUnattributedRequests,
}

const COUNTERS: [Counter; 55] = [
    Counter::EngineReadBytes,
    Counter::EngineWriteRequestedBytes,
    Counter::EngineWriteCompletedBytes,
    Counter::PersistGroups,
    Counter::PersistEntries,
    Counter::PersistEntryBytes,
    Counter::SqlStageInputBytes,
    Counter::SqlPreparedProposalBytes,
    Counter::SqlPreparedLtxBytes,
    Counter::SqlQueryInputBytes,
    Counter::SqlQueryOutputBytes,
    Counter::ObjectReadBytes,
    Counter::ObjectPutRequestedBytes,
    Counter::ObjectPutConfirmedBytes,
    Counter::ControlReadBytes,
    Counter::ControlWriteBytes,
    Counter::DataReadBytes,
    Counter::DataWriteBytes,
    Counter::QueryReadBytes,
    Counter::QueryWriteBytes,
    Counter::ArchiveAdmissionGranted,
    Counter::ArchiveAdmissionObservationRefused,
    Counter::ArchiveAdmissionRetainedBytesRefused,
    Counter::ArchiveAdmissionRetainedEntriesRefused,
    Counter::ArchiveAdmissionReservedBytesDeferred,
    Counter::ArchiveAdmissionReservedEntriesDeferred,
    Counter::ArchivePublicationCutSucceeded,
    Counter::ArchivePublicationCutFailed,
    Counter::ArchiveCoverageAdvanced,
    Counter::ArchiveCoverageIndexAdvance,
    Counter::ArchiveCoverageImagePending,
    Counter::ArchiveCoverageRejected,
    Counter::ArchiveCoverageUnchanged,
    Counter::ArchiveCoverageMissingCell,
    Counter::RecoveryPeerReceivedBytes,
    Counter::RecoveryPeerAppendedBytes,
    Counter::SnapshotSeedEncodedBytes,
    Counter::SnapshotCheckpointEncodedBytes,
    Counter::SnapshotTransferEncodedBytes,
    Counter::SnapshotPreparedHits,
    Counter::SnapshotPreparedMisses,
    Counter::SnapshotPreparedReusedBytes,
    Counter::ImageSqlCalls,
    Counter::ImageInventoryWaitCalls,
    Counter::ImageLtxDecodeCalls,
    Counter::ImageExtentReadCalls,
    Counter::ImageExtentHashCalls,
    Counter::ImageExtentWriteCalls,
    Counter::ImageFileSyncCalls,
    Counter::ImageDirectorySyncCalls,
    Counter::ImageBarrierWaitCalls,
    Counter::ImageOtherCalls,
    Counter::ImageExtentReadBytes,
    Counter::ImageExtentWriteBytes,
    Counter::ImageUnattributedRequests,
];

impl Counter {
    fn name(self) -> &'static str {
        match self {
            Self::EngineReadBytes => "engine_file_read_bytes",
            Self::EngineWriteRequestedBytes => "engine_file_write_requested_bytes",
            Self::EngineWriteCompletedBytes => "engine_file_write_completed_bytes",
            Self::PersistGroups => "engine_persist_groups",
            Self::PersistEntries => "engine_persist_entries",
            Self::PersistEntryBytes => "engine_persist_entry_bytes",
            Self::SqlStageInputBytes => "sql_stage_input_bytes",
            Self::SqlPreparedProposalBytes => "sql_prepared_proposal_bytes",
            Self::SqlPreparedLtxBytes => "sql_prepared_ltx_bytes",
            Self::SqlQueryInputBytes => "sql_query_input_bytes",
            Self::SqlQueryOutputBytes => "sql_query_output_bytes",
            Self::ObjectReadBytes => "object_get_body_bytes",
            Self::ObjectPutRequestedBytes => "object_put_requested_bytes",
            Self::ObjectPutConfirmedBytes => "object_put_confirmed_bytes",
            Self::ControlReadBytes => "transport_control_read_bytes",
            Self::ControlWriteBytes => "transport_control_write_bytes",
            Self::DataReadBytes => "transport_data_read_bytes",
            Self::DataWriteBytes => "transport_data_write_bytes",
            Self::QueryReadBytes => "transport_query_read_bytes",
            Self::QueryWriteBytes => "transport_query_write_bytes",
            Self::ArchiveAdmissionGranted => "archive_admission_granted",
            Self::ArchiveAdmissionObservationRefused => "archive_admission_observation_refused",
            Self::ArchiveAdmissionRetainedBytesRefused => {
                "archive_admission_retained_bytes_refused"
            }
            Self::ArchiveAdmissionRetainedEntriesRefused => {
                "archive_admission_retained_entries_refused"
            }
            Self::ArchiveAdmissionReservedBytesDeferred => {
                "archive_admission_reserved_bytes_deferred"
            }
            Self::ArchiveAdmissionReservedEntriesDeferred => {
                "archive_admission_reserved_entries_deferred"
            }
            Self::ArchivePublicationCutSucceeded => "archive_publication_cut_succeeded",
            Self::ArchivePublicationCutFailed => "archive_publication_cut_failed",
            Self::ArchiveCoverageAdvanced => "archive_coverage_advanced",
            Self::ArchiveCoverageIndexAdvance => "archive_coverage_index_advance",
            Self::ArchiveCoverageImagePending => "archive_coverage_image_pending",
            Self::ArchiveCoverageRejected => "archive_coverage_rejected",
            Self::ArchiveCoverageUnchanged => "archive_coverage_unchanged",
            Self::ArchiveCoverageMissingCell => "archive_coverage_missing_cell",
            Self::RecoveryPeerReceivedBytes => "recovery_peer_received_bytes",
            Self::RecoveryPeerAppendedBytes => "recovery_peer_appended_bytes",
            Self::SnapshotSeedEncodedBytes => "snapshot_seed_encoded_bytes",
            Self::SnapshotCheckpointEncodedBytes => "snapshot_checkpoint_encoded_bytes",
            Self::SnapshotTransferEncodedBytes => "snapshot_transfer_encoded_bytes",
            Self::SnapshotPreparedHits => "snapshot_prepared_hits",
            Self::SnapshotPreparedMisses => "snapshot_prepared_misses",
            Self::SnapshotPreparedReusedBytes => "snapshot_prepared_reused_bytes",
            Self::ImageSqlCalls => "image_sql_calls",
            Self::ImageInventoryWaitCalls => "image_inventory_wait_calls",
            Self::ImageLtxDecodeCalls => "image_ltx_decode_calls",
            Self::ImageExtentReadCalls => "image_extent_read_calls",
            Self::ImageExtentHashCalls => "image_extent_hash_calls",
            Self::ImageExtentWriteCalls => "image_extent_write_calls",
            Self::ImageFileSyncCalls => "image_file_sync_calls",
            Self::ImageDirectorySyncCalls => "image_directory_sync_calls",
            Self::ImageBarrierWaitCalls => "image_barrier_wait_calls",
            Self::ImageOtherCalls => "image_other_calls",
            Self::ImageExtentReadBytes => "image_extent_read_bytes",
            Self::ImageExtentWriteBytes => "image_extent_write_bytes",
            Self::ImageUnattributedRequests => "image_unattributed_requests",
        }
    }
}

// Originating-replica classification is observational, never an admission proof.
#[derive(Clone, Copy)]
#[repr(usize)]
pub(crate) enum ImageWork {
    StageLeader,
    PrepareLeader,
    PrepareFollower,
    PublishLeader,
    PublishFollower,
}

impl ImageWork {
    pub(crate) fn total_operation(self) -> Option<Operation> {
        match self {
            Self::StageLeader => None,
            Self::PrepareLeader => Some(Operation::SqlPrepareLeader),
            Self::PrepareFollower => Some(Operation::SqlPrepareFollower),
            Self::PublishLeader => Some(Operation::SqlPublishLeader),
            Self::PublishFollower => Some(Operation::SqlPublishFollower),
        }
    }
}

const IMAGE_OPERATIONS: [[Operation; cellar_ltx::internal::image_timing::PHASES.len()]; 5] = [
    [
        Operation::ImageStageLeaderSql,
        Operation::ImageStageLeaderInventoryWait,
        Operation::ImageStageLeaderLtxDecode,
        Operation::ImageStageLeaderExtentRead,
        Operation::ImageStageLeaderExtentHash,
        Operation::ImageStageLeaderExtentWrite,
        Operation::ImageStageLeaderFileSync,
        Operation::ImageStageLeaderDirectorySync,
        Operation::ImageStageLeaderBarrierWait,
        Operation::ImageStageLeaderOther,
    ],
    [
        Operation::ImagePrepareLeaderSql,
        Operation::ImagePrepareLeaderInventoryWait,
        Operation::ImagePrepareLeaderLtxDecode,
        Operation::ImagePrepareLeaderExtentRead,
        Operation::ImagePrepareLeaderExtentHash,
        Operation::ImagePrepareLeaderExtentWrite,
        Operation::ImagePrepareLeaderFileSync,
        Operation::ImagePrepareLeaderDirectorySync,
        Operation::ImagePrepareLeaderBarrierWait,
        Operation::ImagePrepareLeaderOther,
    ],
    [
        Operation::ImagePrepareFollowerSql,
        Operation::ImagePrepareFollowerInventoryWait,
        Operation::ImagePrepareFollowerLtxDecode,
        Operation::ImagePrepareFollowerExtentRead,
        Operation::ImagePrepareFollowerExtentHash,
        Operation::ImagePrepareFollowerExtentWrite,
        Operation::ImagePrepareFollowerFileSync,
        Operation::ImagePrepareFollowerDirectorySync,
        Operation::ImagePrepareFollowerBarrierWait,
        Operation::ImagePrepareFollowerOther,
    ],
    [
        Operation::ImagePublishLeaderSql,
        Operation::ImagePublishLeaderInventoryWait,
        Operation::ImagePublishLeaderLtxDecode,
        Operation::ImagePublishLeaderExtentRead,
        Operation::ImagePublishLeaderExtentHash,
        Operation::ImagePublishLeaderExtentWrite,
        Operation::ImagePublishLeaderFileSync,
        Operation::ImagePublishLeaderDirectorySync,
        Operation::ImagePublishLeaderBarrierWait,
        Operation::ImagePublishLeaderOther,
    ],
    [
        Operation::ImagePublishFollowerSql,
        Operation::ImagePublishFollowerInventoryWait,
        Operation::ImagePublishFollowerLtxDecode,
        Operation::ImagePublishFollowerExtentRead,
        Operation::ImagePublishFollowerExtentHash,
        Operation::ImagePublishFollowerExtentWrite,
        Operation::ImagePublishFollowerFileSync,
        Operation::ImagePublishFollowerDirectorySync,
        Operation::ImagePublishFollowerBarrierWait,
        Operation::ImagePublishFollowerOther,
    ],
];
const IMAGE_CALLS: [Counter; cellar_ltx::internal::image_timing::PHASES.len()] = [
    Counter::ImageSqlCalls,
    Counter::ImageInventoryWaitCalls,
    Counter::ImageLtxDecodeCalls,
    Counter::ImageExtentReadCalls,
    Counter::ImageExtentHashCalls,
    Counter::ImageExtentWriteCalls,
    Counter::ImageFileSyncCalls,
    Counter::ImageDirectorySyncCalls,
    Counter::ImageBarrierWaitCalls,
    Counter::ImageOtherCalls,
];

#[derive(Default)]
struct Samples {
    started: u64,
    succeeded: u64,
    failed: u64,
    abandoned: u64,
    unavailable: u64,
    sum: u64,
    buckets: [u64; BOUNDS_US.len()],
}

pub(crate) struct Registry {
    clock: DomainToken,
    source: String,
    available: AtomicBool,
    saturated: AtomicBool,
    counters: [AtomicU64; COUNTERS.len()],
    operations: [Mutex<Samples>; OPERATIONS.len()],
}

impl Registry {
    pub(crate) fn new(clock: DomainToken) -> Arc<Self> {
        use rand::RngCore as _;
        let mut source = [0; 16];
        crate::asyncrt::rng("raft-performance-source").fill_bytes(&mut source);
        Arc::new(Self {
            clock,
            source: hex::encode(source),
            available: AtomicBool::new(true),
            saturated: AtomicBool::new(false),
            counters: std::array::from_fn(|_| AtomicU64::new(0)),
            operations: std::array::from_fn(|_| Mutex::new(Samples::default())),
        })
    }

    pub(crate) fn mark_unavailable(&self) {
        self.available.store(false, Ordering::Relaxed);
    }

    fn add(&self, value: &mut u64, amount: u64) {
        match value.checked_add(amount) {
            Some(next) => *value = next,
            None => {
                *value = u64::MAX;
                self.saturated.store(true, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn increment(&self, counter: Counter, amount: u64) {
        let _ = self.counters[counter as usize].fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |value| {
                Some(value.checked_add(amount).unwrap_or_else(|| {
                    self.saturated.store(true, Ordering::Relaxed);
                    u64::MAX
                }))
            },
        );
    }

    fn update(&self, operation: Operation, f: impl FnOnce(&mut Samples)) {
        match self.operations[operation as usize].lock() {
            Ok(mut sample) => f(&mut sample),
            Err(_) => self.available.store(false, Ordering::Relaxed),
        }
    }

    pub(crate) fn timer(self: &Arc<Self>, operation: Operation) -> Timer {
        let start = self.clock.mono_us().ok();
        self.update(operation, |sample| self.add(&mut sample.started, 1));
        Timer {
            registry: self.clone(),
            operation,
            start,
            outcome: None,
            secondary: None,
        }
    }

    /// The timer follows the future's lifetime. Errors count as failures;
    /// cancellation drops the timer as abandoned rather than inventing success.
    pub(crate) async fn measure<T, E>(
        self: &Arc<Self>,
        operation: Operation,
        future: impl std::future::Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        let timer = self.timer(operation);
        let result = future.await;
        timer.finish(result.is_ok());
        result
    }

    /// One exclusive phase-sum sample per request (including zero work).
    /// Separate counters count actual primitive calls and byte amplification.
    pub(crate) fn record_image(
        &self,
        work: ImageWork,
        timings: &cellar_ltx::internal::image_timing::Timings,
        succeeded: bool,
    ) {
        use cellar_ltx::internal::image_timing::Phase;
        if timings.unavailable {
            self.mark_unavailable();
            return;
        }
        for (index, sample) in timings.phases.iter().enumerate() {
            let operation = IMAGE_OPERATIONS[work as usize][index];
            self.update(operation, |s| self.add(&mut s.started, 1));
            self.record(operation, Some(succeeded), Some(sample.micros));
            self.increment(IMAGE_CALLS[index], sample.calls);
        }
        self.increment(
            Counter::ImageExtentReadBytes,
            timings.phases[Phase::ExtentRead as usize].bytes,
        );
        self.increment(
            Counter::ImageExtentWriteBytes,
            timings.phases[Phase::ExtentWrite as usize].bytes,
        );
    }

    fn record(&self, operation: Operation, outcome: Option<bool>, elapsed: Option<u64>) {
        self.update(operation, |sample| {
            match outcome {
                Some(true) => self.add(&mut sample.succeeded, 1),
                Some(false) => self.add(&mut sample.failed, 1),
                None => self.add(&mut sample.abandoned, 1),
            };
            if let Some(us) = elapsed {
                let bucket = BOUNDS_US.partition_point(|bound| *bound < us);
                self.add(&mut sample.buckets[bucket], 1);
                self.add(&mut sample.sum, us);
            } else {
                self.add(&mut sample.unavailable, 1);
            }
        });
    }

    pub(crate) fn snapshot(&self) -> RaftPerformanceSnapshot {
        let sampled_at_us = self.clock.mono_us().ok();
        let operations = OPERATIONS
            .iter()
            .map(|operation| {
                let sample = match self.operations[*operation as usize].lock() {
                    Ok(sample) => sample,
                    Err(poison) => {
                        self.available.store(false, Ordering::Relaxed);
                        poison.into_inner()
                    }
                };
                let mut count = 0u64;
                let buckets = BOUNDS_US
                    .iter()
                    .zip(sample.buckets)
                    .map(|(bound, n)| {
                        self.add(&mut count, n);
                        RaftDurationBucket {
                            upper_us: (*bound != u64::MAX).then_some(*bound),
                            count,
                        }
                    })
                    .collect();
                RaftOperationPerformance {
                    name: operation.name(),
                    started: sample.started,
                    succeeded: sample.succeeded,
                    failed: sample.failed,
                    abandoned: sample.abandoned,
                    timing_unavailable: sample.unavailable,
                    count,
                    sum_us: sample.sum,
                    buckets,
                }
            })
            .collect();
        RaftPerformanceSnapshot {
            source: self.source.clone(),
            available: self.available.load(Ordering::Relaxed),
            saturated: self.saturated.load(Ordering::Relaxed),
            sampled_at_us,
            counters: COUNTERS
                .iter()
                .map(|counter| RaftPerformanceCounter {
                    name: counter.name(),
                    value: self.counters[*counter as usize].load(Ordering::Relaxed),
                })
                .collect(),
            operations,
        }
    }
}

pub(crate) struct Timer {
    registry: Arc<Registry>,
    operation: Operation,
    start: Option<u64>,
    outcome: Option<bool>,
    secondary: Option<Operation>,
}

impl Timer {
    pub(crate) fn finish_as(mut self, succeeded: bool, secondary: Option<Operation>) {
        self.outcome = Some(succeeded);
        self.secondary = secondary;
        if let Some(operation) = secondary {
            self.registry
                .update(operation, |s| self.registry.add(&mut s.started, 1));
        }
    }

    pub(crate) fn finish(mut self, succeeded: bool) {
        self.outcome = Some(succeeded);
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let elapsed = self
            .start
            .zip(self.registry.clock.mono_us().ok())
            .and_then(|(start, end)| end.checked_sub(start));
        self.registry.record(self.operation, self.outcome, elapsed);
        if let Some(operation) = self.secondary {
            self.registry.record(operation, self.outcome, elapsed);
        }
    }
}

#[cfg(test)]
#[path = "performance_tests.rs"]
mod tests;

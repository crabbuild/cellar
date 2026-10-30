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
mod controller;
pub use controller::{
    CellCommandAdmissionError, CellCommandCall, CellEnrollmentAdmissionError, CellEnrollmentCall,
    CellGroupController, CellGroupControllerError, CellGroupControllerLimits,
    CellGroupControllerUsage,
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
mod owner;
mod page_store;
mod page_wire;
mod pins;
mod proposal_admission;
mod read_authority;
pub use read_authority::{CellReadAuthority, CellReadConsistency};
mod retention;
mod root;
mod runtime;

pub use runtime::{CellGroupRuntime, CellGroupRuntimeEvent, CellGroupRuntimeEvents};

pub use manifest::{
    CellImageGeometry, CellManifestLeaf, EffectSummary, ManifestError, ManifestLimits,
    ManifestRoot, ManifestTree,
};
pub use page_store::{ManifestPageStore, ManifestReclaimReport, ManifestStoreUsage};

pub use crate::raft::catalog::{
    CellGroupBirth, CellGroupBirthRequest, CellGroupBirthSpec, CellGroupBootstrapPermit,
    CellRouteAttempt, CellRouteCacheUsage, CellRouteOperation, CellRouteRequest, CellRoutingError,
    CellRoutingFailure, CellRoutingLimits, CellRoutingSession, DirectoryLimits, DirectoryMutation,
    DirectoryPublication, DirectoryView, RangeDirectory, ResolvedCellRoute,
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
    CellGroupProposal, CellGroupStatus, CellGroupTicket,
};

pub(crate) use dispatch::RemoteMutationRoute;

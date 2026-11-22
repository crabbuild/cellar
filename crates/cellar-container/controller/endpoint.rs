// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use cellar::sdk::{containers::*, HandlerFuture};
use std::sync::Arc;

/// The ordinary host protocol. Implementations must preserve original operation
/// identities and uncertain outcomes; transport errors never prove no effect.
/// Recovery administration and controller shutdown are deliberately separate.
pub trait ControllerEndpoint: ExecutionFenceAuthority {
    fn limits(&self) -> ExecutionControllerLimits;

    fn execute(
        self: Arc<Self>,
        claim: ExecutionClaim,
        operation: ExecutionOperation,
        payload: Vec<u8>,
    ) -> HandlerFuture<ExecutionOutcome>;

    fn reconcile(
        self: Arc<Self>,
        claim: ExecutionClaim,
        original: ExecutionPendingOperation,
    ) -> HandlerFuture<ExecutionOutcome>;

    fn pending(
        self: Arc<Self>,
        claim: ExecutionClaim,
    ) -> HandlerFuture<Vec<ExecutionPendingOperation>>;

    fn lifecycle(
        self: Arc<Self>,
        claim: ExecutionClaim,
        kind: ExecutionOperationKind,
    ) -> HandlerFuture<Option<ExecutionPendingOperation>>;

    fn seal_unsubmitted(
        self: Arc<Self>,
        claim: ExecutionClaim,
        request: ExecutionOperation,
    ) -> HandlerFuture<ExecutionPendingOperation>;
}

impl ControllerEndpoint for ExecutionController {
    fn limits(&self) -> ExecutionControllerLimits {
        self.options().limits
    }

    fn execute(
        self: Arc<Self>,
        claim: ExecutionClaim,
        operation: ExecutionOperation,
        payload: Vec<u8>,
    ) -> HandlerFuture<ExecutionOutcome> {
        Box::pin(
            async move { ExecutionController::execute(&self, claim, operation, payload).await },
        )
    }

    fn reconcile(
        self: Arc<Self>,
        claim: ExecutionClaim,
        original: ExecutionPendingOperation,
    ) -> HandlerFuture<ExecutionOutcome> {
        Box::pin(async move { ExecutionController::reconcile(&self, claim, original).await })
    }

    fn pending(
        self: Arc<Self>,
        claim: ExecutionClaim,
    ) -> HandlerFuture<Vec<ExecutionPendingOperation>> {
        Box::pin(async move { ExecutionController::pending(&self, claim).await })
    }

    fn lifecycle(
        self: Arc<Self>,
        claim: ExecutionClaim,
        kind: ExecutionOperationKind,
    ) -> HandlerFuture<Option<ExecutionPendingOperation>> {
        Box::pin(async move { ExecutionController::lifecycle(&self, claim, kind).await })
    }

    fn seal_unsubmitted(
        self: Arc<Self>,
        claim: ExecutionClaim,
        request: ExecutionOperation,
    ) -> HandlerFuture<ExecutionPendingOperation> {
        Box::pin(async move { ExecutionController::seal_unsubmitted(&self, claim, request).await })
    }
}

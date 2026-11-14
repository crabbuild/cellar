// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Capture namespace admission now; defer all controller dispatch until proof.

use super::*;

impl CellGroupCellHandle {
    pub(crate) fn prepare_sdk_command(
        &self,
        request: CellCommandRequest,
        limits: CellRoutingLimits,
    ) -> Result<
        impl std::future::Future<Output = Result<CellCommandResponse, Arc<CellCommandError>>>
            + Send
            + 'static,
        CellCommandError,
    > {
        let id = request.id;
        let failure = move |cause| CellCommandError {
            command: id,
            ambiguous: false,
            cause: Box::new(cause),
        };
        self.host.check_lifecycle().map_err(failure)?;
        let pending = self.binding.dispatcher.prepare_command(
            self.history.clone(),
            request,
            limits,
            None,
            None,
            false,
        )?;
        let handle = self.clone();
        Ok(async move {
            handle
                .host
                .check_open()
                .map_err(|cause| Arc::new(failure(cause)))?;
            let options = handle
                .binding
                .options(&handle.history)
                .map_err(|cause| Arc::new(failure(cause)))?;
            let call = handle
                .host
                .controller
                .dispatch_prepared_command(pending, options)
                .map_err(Arc::new)?;
            call.response().await
        })
    }

    pub(crate) fn prepare_sdk_query(
        &self,
        request: CellQueryRequest,
        limits: CellRoutingLimits,
        routing: CellQueryRouting,
    ) -> Result<
        impl std::future::Future<Output = Result<CellQueryResponse, Arc<CellDispatchError>>>
            + Send
            + 'static,
        CellDispatchError,
    > {
        self.host.check_lifecycle()?;
        let invocation = self.binding.dispatcher.query_routed(
            self.history.clone(),
            request,
            limits,
            None,
            routing,
        )?;
        let handle = self.clone();
        Ok(async move {
            handle.host.check_open().map_err(Arc::new)?;
            let options = handle.binding.options(&handle.history).map_err(Arc::new)?;
            let call = handle
                .host
                .controller
                .query(invocation, options)
                .map_err(Arc::new)?;
            call.response().await
        })
    }

    pub(crate) fn prepare_sdk_receipt(
        &self,
        request: CellReceiptRequest,
        limits: CellRoutingLimits,
        transport: RaftTransport,
    ) -> Result<
        impl std::future::Future<Output = Result<CellReceiptResponse, Arc<CellDispatchError>>>
            + Send
            + 'static,
        CellDispatchError,
    > {
        self.host.check_lifecycle()?;
        let invocation =
            self.binding
                .dispatcher
                .lookup_receipt(self.history.clone(), request, limits, None)?;
        let handle = self.clone();
        Ok(async move {
            handle.host.check_open().map_err(Arc::new)?;
            let options = handle.binding.options(&handle.history).map_err(Arc::new)?;
            handle
                .host
                .controller
                .lookup_receipt(invocation, options, Some(&transport))
                .await
                .map_err(Arc::new)
        })
    }

    pub(crate) fn prepare_sdk_enrollment(
        &self,
        request: CellEnrollmentRequest,
        limits: CellRoutingLimits,
    ) -> Result<
        impl std::future::Future<Output = Result<CellEnrollmentResponse, Arc<CellEnrollmentError>>>
            + Send
            + 'static,
        CellEnrollmentError,
    > {
        let creation = request.creation;
        let failure = move |cause| CellEnrollmentError {
            creation,
            ambiguous: false,
            cause: Box::new(cause),
        };
        self.host.check_lifecycle().map_err(failure)?;
        let prepared = self.binding.dispatcher.capture_routed_enrollment(
            self.history.clone(),
            request,
            limits,
        )?;
        let handle = self.clone();
        Ok(async move {
            handle
                .host
                .check_open()
                .map_err(|cause| Arc::new(failure(cause)))?;
            let invocation = prepared.await.map_err(Arc::new)?;
            let options = handle
                .binding
                .options(&handle.history)
                .map_err(|cause| Arc::new(failure(cause)))?;
            let call = handle
                .host
                .controller
                .enroll(invocation, options)
                .map_err(|error| Arc::new(error.error))?;
            call.response().await
        })
    }
}

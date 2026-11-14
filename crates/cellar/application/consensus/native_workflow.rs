// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The ordinary Workflow journal runs inside one shared-group command owner.

use super::{AdmittedCellConfiguration, RaftClass, RegistrationError};
use crate::application::{
    Application, Context, HandlerFuture, Invocation, Output, WorkflowHandler,
};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::Arc};

struct Handlers(HashMap<String, WorkflowHandler>);

#[cfg(test)]
#[path = "native_workflow_tests.rs"]
mod tests;

impl Application for Handlers {
    fn supports_cell(&self, _: &str) -> bool {
        false
    }

    fn create_cell(
        &self,
        _: Context,
        _: &crate::application::CellIdentity,
    ) -> anyhow::Result<Arc<dyn crate::application::Cell>> {
        anyhow::bail!("Workflow catalogue cannot construct ordinary cells")
    }

    fn invoke(self: Arc<Self>, _: Context, _: Invocation) -> HandlerFuture<Output> {
        Box::pin(async { anyhow::bail!("Workflow catalogue cannot serve application ingress") })
    }

    fn run_workflow(
        self: Arc<Self>,
        class: String,
        context: Context,
        event: crate::application::workflow::Event,
        steps: crate::application::workflow::Step,
    ) -> HandlerFuture<cellar_native::clone::StructuredValue> {
        match self.0.get(&class) {
            Some(handler) => handler(context, event, steps),
            None => Box::pin(async move { anyhow::bail!("undeclared Workflow handler {class}") }),
        }
    }
}

pub(in crate::application) fn workflow_service(
    configuration: &AdmittedCellConfiguration,
    handlers: HashMap<String, WorkflowHandler>,
    digests: &std::collections::BTreeMap<String, [u8; 32]>,
) -> Result<RaftClass, RegistrationError> {
    let classes: HashMap<_, _> = configuration
        .workflow_bindings
        .values()
        .map(|b| (b.workflow.clone(), b.class.clone()))
        .collect();
    if classes
        .values()
        .any(|class| !handlers.contains_key(class) || !digests.contains_key(class))
    {
        return Err(RegistrationError::Invalid("missing Workflow handler"));
    }
    let compiled: std::collections::BTreeMap<_, _> = classes
        .values()
        .map(|class| (class, digests[class]))
        .collect();
    let mut class = RaftClass::workflow_service(
        &configuration.script,
        Sha256::digest(
            serde_json::to_vec(&compiled)
                .map_err(|_| RegistrationError::Invalid("Workflow digests"))?,
        )
        .into(),
    )?
    .initializer(
        Sha256::digest(b"cellar-raft-workflow-initializer-1").into(),
        |_, _| Ok(()),
    )?
    .durable_callbacks()?;
    for method in [
        "__wfCreate",
        "__wfStatus",
        "__wfDelete",
        "__wfPause",
        "__wfResume",
        "__wfTerminate",
        "__wfRestart",
        "__wfSendEvent",
        "__wfHistory",
        "__wfStreamOpen",
        "__wfStreamRead",
        crate::application::workflow::OPERATOR,
    ] {
        let mut digest = Sha256::new();
        digest.update(b"cellar-raft-workflow-quorum-journal-1");
        digest.update(method);
        class = class.cell_rpc(method, digest.finalize().into())?;
    }
    for method in [
        "__wfStatus",
        "__wfHistory",
        "__wfStreamOpen",
        "__wfStreamRead",
    ] {
        let operation = crate::application::workflow::read_operation(method)
            .ok_or(RegistrationError::Invalid("Workflow read operation"))?;
        let script = configuration.script.clone();
        let name = class.name().to_owned();
        let mut digest = Sha256::new();
        digest.update(b"cellar-raft-workflow-pinned-read-1");
        digest.update(method);
        class = class.query(operation, digest.finalize().into(), move |query, input| {
            let request = super::native_rpc::decode_request(input)?;
            super::native_rpc::validate_identity(
                &script,
                &name,
                query.identity().cell.as_str(),
                request.name.as_deref(),
            )?;
            let value =
                crate::application::workflow::read_query(&query.storage(), method, &request.args)?;
            super::native_rpc::encode_response(&value)
        })?;
    }
    class = class.query(
        super::sql_ingress_identity::OPERATION,
        super::sql_ingress_identity::native_digest(),
        |query, input| query.sql_ingress_sequence(input),
    )?;
    let application: Arc<dyn Application> = Arc::new(Handlers(handlers));
    class = class.with_cell_factory(Arc::new(move |context, _| {
        crate::application::workflow::create_cell(&context, application.clone(), classes.clone())
    }))?;
    Ok(class.configured_cell(configuration))
}

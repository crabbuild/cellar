//! Native Workflow bindings and the retained instance journal/RPC protocol.
//!
//! Handlers receive a [`Step`] for named durable work. Rust reserves `do`, so
//! Cloudflare's `step.do` is spelled `step.run`; use `run_with` for configuration.
//! Sleeps accept duration strings, numeric milliseconds, or `std::time::Duration`.
//!
//! ```no_run
//! use cellar::sdk::{
//!     clone::StructuredValue,
//!     workflow::{Event, Retries, Step, StepConfig, WaitOptions},
//!     Context,
//! };
//!
//! async fn report(_: Context, event: Event, step: Step) -> anyhow::Result<StructuredValue> {
//!     let config = StepConfig::default()
//!         .retries(Retries::new(3, "5 seconds"))
//!         .timeout("1 minute");
//!     let prepared = step.run_with("prepare", config, move |_| async move {
//!         Ok(event.payload)
//!     }).await?;
//!     step.sleep("delay", "1 minute").await?;
//!     step.wait_for_event("approval", WaitOptions::new("approve").timeout("1 hour")).await?;
//!     step.run("publish", move |_| async move { Ok(prepared) }).await
//! }
//! ```

mod batch;
mod cell;
mod configuration;
mod definition;
mod history;
mod operator;
mod replay;
mod rollback;
mod steps;
mod stream;
mod types;

pub use batch::{BatchCreateError, BatchCreateOptions, BatchCreateResult};
pub(crate) use cell::create_cell;
pub use configuration::{BindingSettings, CronSchedule};
pub use definition::{Definition, DefinitionEdge, DefinitionKind, DefinitionNode};
pub use history::{InstanceEvent, InstanceEventType, SubscribeOptions, Subscription};
pub use operator::OperatorAction;
pub use replay::{Step, Steps};
pub use rollback::{RollbackContext, RollbackOptions};
pub use steps::{
    Attempt, Backoff, Delay, Event, Retries, StepConfig, StepFailure, StepName, WaitOptions,
};
pub use stream::{StepSelector, StreamOutput};
pub use types::{
    CreateOptions, DeleteBatchResult, DeleteError, Deleted, Duration, ErrorRecord, RestartFrom,
    RestartOptions, Retention, SendEvent, State, Status, StepKind, TerminateOptions,
    RETENTION_MAX_MS, VALUE_LIMIT,
};

use super::{decode_rpc_result, encode_rpc, Context};
use anyhow::{anyhow, ensure, Result};
use cellar_native::clone::{StructuredValue, Value as CloneValue};
use indexmap::IndexSet;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

fn error(message: impl std::fmt::Display) -> anyhow::Error {
    anyhow!("WORKFLOW_ERROR: {message}")
}

fn name_valid(name: &str, max: usize) -> bool {
    let mut bytes = name.bytes();
    name.len() <= max
        && bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

fn limits() -> crate::application::limits::WorkflowLimits {
    crate::application::limits::current().workflow
}

fn check_event_type(name: &str) -> Result<()> {
    ensure!(
        name_valid(name, limits().max_event_type_bytes),
        "WORKFLOW_ERROR: invalid event type"
    );
    Ok(())
}

fn check_horizon(ms: f64, what: &str) -> Result<()> {
    ensure!(
        ms.is_finite()
            && ms >= 0.0
            && ms < i64::MAX.saturating_sub(cell::now()) as f64
            && limits()
                .max_sleep_ms
                .is_none_or(|maximum| ms <= maximum as f64),
        "WORKFLOW_ERROR: {what} exceeds the host limit or timestamp range"
    );
    Ok(())
}

fn check_id(id: &str) -> Result<()> {
    ensure!(name_valid(id, limits().max_id_bytes), "WORKFLOW_ERROR: invalid instance id {id:?}: use at most {} letters, digits, hyphens, or underscores, not starting with a hyphen", limits().max_id_bytes);
    Ok(())
}

pub(crate) fn check_step_name(name: &[u16]) -> Result<()> {
    ensure!(
        name.len() <= limits().max_step_name_units && !name.iter().any(|unit| *unit <= 0x1f),
        "WORKFLOW_ERROR: invalid step name: use at most {} UTF-16 units and no control characters",
        limits().max_step_name_units
    );
    Ok(())
}

fn check_value(value: &StructuredValue, what: &str) -> Result<()> {
    let bytes = cellar_native::clone::encode(value).map_err(|cause| {
        error(format!(
            "{what} is not serializable: {cause}; it must survive structured clone"
        ))
    })?;
    check_encoded(&bytes, what)
}

fn check_encoded(bytes: &[u8], what: &str) -> Result<()> {
    ensure!(
        bytes.len() <= limits().max_value_bytes,
        "WORKFLOW_ERROR: {what} is {} bytes, above the host limit of {} bytes",
        bytes.len(),
        limits().max_value_bytes
    );
    Ok(())
}

fn property(graph: &StructuredValue, name: &str) -> Result<Option<StructuredValue>> {
    Ok(graph
        .property(&name.encode_utf16().collect::<Vec<_>>())?
        .filter(|graph| !matches!(graph.root, CloneValue::Undefined)))
}

fn set(graph: &mut StructuredValue, name: &str, value: &StructuredValue) -> Result<()> {
    graph.set_property(name.encode_utf16().collect(), value)
}

#[derive(Clone, Debug)]
pub struct Workflow {
    script: Arc<str>,
    name: Arc<str>,
    pub(crate) settings: BindingSettings,
}

#[derive(Clone, Debug)]
pub struct Instance {
    workflow: Workflow,
    id: Arc<str>,
}

impl Workflow {
    pub(crate) fn new(script: &str, name: &str) -> Self {
        Self {
            script: Arc::from(script),
            name: Arc::from(name),
            settings: BindingSettings::default(),
        }
    }

    pub(crate) fn with_settings(mut self, settings: BindingSettings) -> Self {
        self.settings = settings;
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn instance(&self, id: String) -> Instance {
        Instance {
            workflow: self.clone(),
            id: Arc::from(id),
        }
    }

    fn options(&self, mut options: CreateOptions) -> Result<(Instance, StructuredValue)> {
        options.retention.success = options
            .retention
            .success
            .or_else(|| self.settings.default_retention.success.clone());
        options.retention.error = options
            .retention
            .error
            .or_else(|| self.settings.default_retention.error.clone());
        let id = options
            .id
            .map(Ok)
            .unwrap_or_else(cellar_native::crypto::random_uuid)?;
        check_id(&id)?;
        if let Some(hint) = &options.location_hint {
            ensure!(
                [
                    "wnam", "enam", "sam", "weur", "eeur", "apac", "apac-ne", "apac-se", "oc",
                    "afr", "me"
                ]
                .contains(&hint.as_str()),
                "WORKFLOW_ERROR: invalid locationHint {hint:?}"
            );
        }
        let mut data = StructuredValue::from_json(json!({
            "workflowName":self.name.as_ref(),"instanceId":id,"retention":options.retention.wire(limits(), cell::now())?,
        }))?;
        if let Some(hint) = options.location_hint {
            set(
                &mut data,
                "locationHint",
                &StructuredValue::from_json(json!(hint))?,
            )?;
        }
        if let Some(params) = options.params {
            set(&mut data, "params", &params)?;
        }
        Ok((self.instance(id), data))
    }

    pub async fn create(&self, context: &Context, mut options: CreateOptions) -> Result<Instance> {
        let params = options.params.take();
        let (instance, mut data) = self.options(options)?;
        if let Some(params) = params {
            check_value(&params, "the workflow params")?;
            set(&mut data, "params", &params)?;
        }
        set(
            &mut data,
            "skipExisting",
            &StructuredValue::from_json(json!(false))?,
        )?;
        instance.call(context, "__wfCreate", Some(data)).await?;
        Ok(instance)
    }

    pub async fn create_batch(
        &self,
        context: &Context,
        batch: Vec<CreateOptions>,
    ) -> Result<Vec<Instance>> {
        ensure!(
            !batch.is_empty() && batch.len() <= limits().max_batch_size,
            "WORKFLOW_ERROR: createBatch() accepts 1 to {} instances, got {}",
            limits().max_batch_size,
            batch.len()
        );
        // Every structural error and every encoded size error is discovered
        // before the first call. A clone failure only filters that one item.
        let mut candidates = Vec::new();
        let mut shapes = Vec::new();
        for mut options in batch {
            let params = options.params.take();
            let (instance, data) = self.options(options)?;
            shapes.push((instance, data, params));
        }
        for (instance, mut data, params) in shapes {
            if let Some(params) = &params {
                let Ok(bytes) = cellar_native::clone::encode(params) else {
                    continue;
                };
                check_encoded(&bytes, "the workflow params")?;
                set(&mut data, "params", params)?;
            }
            candidates.push((instance, data));
        }
        let mut created = Vec::new();
        for (instance, mut data) in candidates {
            set(
                &mut data,
                "skipExisting",
                &StructuredValue::from_json(json!(true))?,
            )?;
            let reply = instance.call(context, "__wfCreate", Some(data)).await?;
            if property(&reply, "created")?
                .is_some_and(|value| value.root == CloneValue::Boolean(true))
            {
                created.push(instance);
            }
        }
        Ok(created)
    }

    pub async fn get(&self, context: &Context, id: &str) -> Result<Instance> {
        check_id(id)?;
        let instance = self.instance(id.into());
        instance.status(context).await?;
        Ok(instance)
    }

    pub async fn delete_batch(
        &self,
        context: &Context,
        ids: Vec<String>,
    ) -> Result<DeleteBatchResult> {
        ensure!(
            !ids.is_empty() && ids.len() <= limits().max_batch_size,
            "WORKFLOW_ERROR: deleteBatch() accepts 1 to {} instance ids, got {}",
            limits().max_batch_size,
            ids.len()
        );
        for id in &ids {
            check_id(id)?;
        }
        let unique = ids.iter().cloned().collect::<IndexSet<_>>();
        let outcomes = futures_util::future::join_all(unique.iter().map(|id| async move {
            let outcome = self.instance(id.clone()).delete(context).await;
            (id.clone(), outcome)
        }))
        .await
        .into_iter()
        .collect::<HashMap<_, _>>();
        let mut output = DeleteBatchResult::default();
        for id in ids {
            match &outcomes[&id] {
                Ok(()) => output.deleted.push(Deleted { id }),
                Err(error) => {
                    let absent = error.to_string().contains("instance does not exist");
                    output.errors.push(DeleteError {
                        id,
                        code: if absent { 10400 } else { 10001 },
                        message: if absent {
                            "workflows.api.error.instance.not_found"
                        } else {
                            "workflows.api.error.internal_server"
                        }
                        .into(),
                    });
                }
            }
        }
        Ok(output)
    }
}

impl Instance {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn scope(&self) -> String {
        crate::engine_api::workflow_cell_scope(&self.workflow.script, &self.workflow.name, &self.id)
    }

    async fn call(
        &self,
        context: &Context,
        method: &str,
        argument: Option<StructuredValue>,
    ) -> Result<StructuredValue> {
        self.call_internal(context, method, argument, false).await
    }

    async fn resource_call(
        &self,
        context: &Context,
        method: &str,
        argument: Option<StructuredValue>,
    ) -> Result<StructuredValue> {
        self.call_internal(context, method, argument, true).await
    }

    async fn call_internal(
        &self,
        context: &Context,
        method: &str,
        argument: Option<StructuredValue>,
        continuation: bool,
    ) -> Result<StructuredValue> {
        context.check_turn()?;
        let arguments = StructuredValue::array(argument)?;
        let name = Some(format!("{}/{}", self.workflow.name, self.id));
        let args = encode_rpc(&arguments)?;
        let reply = if continuation {
            crate::native_worker::rpc_cell_resource(
                context,
                self.scope(),
                name,
                method.into(),
                args,
            )
        } else {
            context.rpc_cell(self.scope(), name, method.into(), args)
        }
        .await?;
        decode_rpc_result(&reply)
    }

    pub async fn status(&self, context: &Context) -> Result<Status> {
        let reply = self.call(context, "__wfStatus", None).await?;
        let state = serde_json::from_value(
            property(&reply, "status")?
                .ok_or_else(|| error("status reply has no status"))?
                .to_json()?,
        )?;
        let rollback =
            property(&reply, "rollback")?.filter(|value| !matches!(value.root, CloneValue::Null));
        Ok(Status {
            state,
            output: property(&reply, "output")?,
            error: property(&reply, "error")?
                .map(ErrorRecord::from_value)
                .transpose()?,
            rollback,
        })
    }

    pub async fn send_event(&self, context: &Context, event: SendEvent) -> Result<()> {
        check_event_type(&event.event_type)?;
        let mut data = StructuredValue::from_json(json!({"type":event.event_type}))?;
        if let Some(payload) = event.payload {
            check_value(&payload, "the event payload")?;
            set(&mut data, "payload", &payload)?;
        }
        self.call(context, "__wfSendEvent", Some(data)).await?;
        Ok(())
    }

    pub async fn pause(&self, context: &Context) -> Result<()> {
        self.call(context, "__wfPause", None).await?;
        Ok(())
    }

    pub async fn resume(&self, context: &Context) -> Result<()> {
        self.call(context, "__wfResume", None).await?;
        Ok(())
    }

    pub async fn restart(&self, context: &Context, options: RestartOptions) -> Result<()> {
        let mut data = StructuredValue::from_json(json!({}))?;
        if let Some(from) = options.from {
            check_step_name(&from.name)?;
            ensure!(
                from.count > 0,
                "WORKFLOW_ERROR: restart() from.count must be a positive integer"
            );
            let mut target =
                StructuredValue::from_json(json!({"count":from.count,"type":from.kind.wire()}))?;
            set(
                &mut target,
                "name",
                &StructuredValue {
                    root: CloneValue::String(from.name),
                    objects: Vec::new(),
                },
            )?;
            set(&mut data, "from", &target)?;
        }
        self.call(context, "__wfRestart", Some(data)).await?;
        Ok(())
    }

    pub async fn terminate(&self, context: &Context) -> Result<()> {
        self.terminate_with_options(context, TerminateOptions::default())
            .await
    }

    pub async fn terminate_with_options(
        &self,
        context: &Context,
        options: TerminateOptions,
    ) -> Result<()> {
        self.call(
            context,
            "__wfTerminate",
            Some(StructuredValue::from_json(
                json!({"rollback":options.rollback}),
            )?),
        )
        .await?;
        Ok(())
    }

    pub async fn delete(&self, context: &Context) -> Result<()> {
        match self.call(context, "__wfDelete", None).await {
            Ok(_) => Ok(()),
            Err(error) if error.to_string().contains("WORKFLOW_INSTANCE_DELETED") => Ok(()),
            Err(error) => Err(error),
        }
    }
}

use super::*;
use crate::application::{
    Application, Cell, HandlerFuture, Invocation, ListOptions, Output, Storage,
};
use crate::engine_api::RpcData;
use std::sync::Mutex;

pub(super) const META: &str = "__wf.meta";
pub(super) fn now() -> i64 {
    crate::ownership_store::now_ms() as i64
}
pub(super) fn data(value: serde_json::Value) -> Result<StructuredValue> {
    StructuredValue::from_json(value)
}
pub(super) fn text(value: &StructuredValue, key: &str) -> Result<String> {
    match property(value, key)?.map(|value| value.root) {
        Some(CloneValue::String(value)) => {
            String::from_utf16(&value).map_err(|_| error(format!("invalid {key}")))
        }
        _ => Err(error(format!("missing {key}"))),
    }
}
pub(super) fn number(value: &StructuredValue, key: &str) -> Result<f64> {
    match property(value, key)?.map(|value| value.root) {
        Some(CloneValue::Number(value)) => Ok(value),
        _ => Err(error(format!("invalid {key}"))),
    }
}
pub(super) fn put_field(
    value: &mut StructuredValue,
    key: &str,
    field: serde_json::Value,
) -> Result<()> {
    set(value, key, &data(field)?)
}
pub(super) fn put_number(value: &mut StructuredValue, key: &str, number: f64) -> Result<()> {
    set(
        value,
        key,
        &StructuredValue {
            root: CloneValue::Number(number),
            objects: Vec::new(),
        },
    )
}
pub(super) fn remove(value: &mut StructuredValue, key: &str) -> Result<()> {
    value.remove_property(&key.encode_utf16().collect::<Vec<_>>())?;
    Ok(())
}
pub(super) fn read(storage: &Storage, key: &str) -> Result<Option<StructuredValue>> {
    Ok(storage.get_clone(key)?.map(|value| value.value))
}
pub(super) fn list(storage: &Storage, prefix: &str) -> Result<Vec<(String, StructuredValue)>> {
    Ok(storage
        .list_clone(&ListOptions {
            prefix: Some(prefix.into()),
            ..Default::default()
        })?
        .into_iter()
        .map(|(key, value)| (key, value.value))
        .collect())
}
pub(super) fn required(storage: &Storage) -> Result<StructuredValue> {
    read(storage, META)?.ok_or_else(|| error("instance does not exist"))
}
pub(super) fn terminal(meta: &StructuredValue) -> Result<bool> {
    Ok(matches!(
        text(meta, "status")?.as_str(),
        "complete" | "errored" | "terminated"
    ))
}
pub(super) fn prefix(generation: &str) -> String {
    format!("__wf.{generation}.")
}
fn clear(storage: &Storage) -> Result<()> {
    for (key, _) in list(storage, "__wf.")? {
        storage.delete(&key)?;
    }
    storage.delete_alarm()
}
pub(super) fn finish_retention(meta: &mut StructuredValue, storage: &Storage) -> Result<()> {
    let retention = property(meta, "retention")?;
    let field = if text(meta, "status")? == "complete" {
        "successMs"
    } else {
        "errorMs"
    };
    let ms = retention
        .as_ref()
        .map(|value| number(value, field))
        .transpose()?
        .unwrap_or(RETENTION_MAX_MS);
    let ended = now();
    put_field(meta, "endedMs", json!(ended))?;
    let expires = ended as f64 + ms;
    put_number(meta, "expiresMs", expires)?;
    storage.set_alarm(expires as i64)
}
fn expire(storage: &Storage, meta: &mut StructuredValue) -> Result<bool> {
    if !terminal(meta)? {
        return Ok(false);
    }
    if property(meta, "expiresMs")?.is_none() {
        finish_retention(meta, storage)?;
        storage.put_clone(META, meta)?;
    }
    if number(meta, "expiresMs")? > now() as f64 {
        return Ok(false);
    }
    clear(storage)?;
    Ok(true)
}
pub(super) fn settle(
    storage: &Storage,
    generation: &str,
    mutate: impl FnOnce(&mut StructuredValue, &Storage) -> Result<()>,
) -> Result<bool> {
    storage.transaction_sync(|transaction| {
        let Some(mut meta) = read(transaction, META)? else {
            return Ok(false);
        };
        if terminal(&meta)? || text(&meta, "generation")? != generation {
            return Ok(false);
        }
        mutate(&mut meta, transaction)?;
        transaction.put_clone(META, &meta)?;
        Ok(true)
    })
}

pub(super) struct InstanceCell {
    pub(super) storage: Storage,
    pub(super) application: Arc<dyn Application>,
    pub(super) classes: HashMap<String, String>,
    pub(super) drive: Mutex<DriveState>,
}
#[derive(Default)]
pub(super) struct DriveState {
    pub(super) active: bool,
    pub(super) nudge: Option<i64>,
    pub(super) waiters: Vec<tokio::sync::oneshot::Sender<()>>,
}
pub(crate) fn create_cell(
    context: &Context,
    application: Arc<dyn Application>,
    classes: HashMap<String, String>,
) -> Result<Arc<dyn Cell>> {
    Ok(Arc::new(InstanceCell {
        storage: context.storage()?,
        application,
        classes,
        drive: Mutex::new(DriveState::default()),
    }))
}
impl InstanceCell {
    fn control(
        &self,
        context: &Context,
        method: &str,
        argument: StructuredValue,
    ) -> Result<StructuredValue> {
        let storage = &self.storage;
        match method {
            "__wfCreate" => {
                if let Some(params) = property(&argument, "params")? {
                    check_value(&params, "the workflow params")?;
                }
                storage.transaction_sync(|transaction| {
                    if let Some(mut meta)=read(transaction,META)? {
                        if !expire(transaction,&mut meta)? {
                            if property(&argument,"skipExisting")?.is_some_and(|value|value.root==CloneValue::Boolean(true)) {
                                return data(json!({"id":text(&argument,"instanceId")?,"created":false}));
                            }
                            return Err(error(format!("instance {:?} already exists with status {:?}",text(&argument,"instanceId")?,text(&meta,"status")?)));
                        }
                    }
                    let mut meta=data(json!({"workflowName":text(&argument,"workflowName")?,"instanceId":text(&argument,"instanceId")?,"generation":cellar_native::crypto::random_uuid()?,"createdMs":now(),"status":"queued"}))?;
                    for key in ["params","retention","locationHint"] {if let Some(value)=property(&argument,key)? {set(&mut meta,key,&value)?;}}
                    transaction.put_clone(META,&meta)?;
                    transaction.set_alarm(now())?;
                    data(json!({"id":text(&argument,"instanceId")?,"created":true}))
                })
            }
            "__wfStatus" => {
                let meta = storage
                    .transaction_sync(|transaction| {
                        let Some(mut meta) = read(transaction, META)? else {
                            return Ok(None);
                        };
                        Ok(if expire(transaction, &mut meta)? {
                            None
                        } else {
                            Some(meta)
                        })
                    })?
                    .ok_or_else(|| error("instance does not exist"))?;
                let mut status = data(json!({"status":text(&meta,"status")?,"rollback":null}))?;
                for key in ["error", "output"] {
                    if let Some(value) = property(&meta, key)? {
                        set(&mut status, key, &value)?;
                    }
                }
                Ok(status)
            }
            "__wfDelete" => {
                let deleted = storage.transaction_sync(|transaction| {
                    let Some(mut meta) = read(transaction, META)? else {
                        return Ok(false);
                    };
                    if expire(transaction, &mut meta)? {
                        return Ok(false);
                    }
                    clear(transaction)?;
                    Ok(true)
                })?;
                if !deleted {
                    return Err(error("instance does not exist"));
                }
                context
                    .io
                    .native_activation
                    .as_ref()
                    .expect("workflow activation")
                    .fail("WORKFLOW_INSTANCE_DELETED".into());
                Err(anyhow!("WORKFLOW_INSTANCE_DELETED"))
            }
            "__wfPause" | "__wfResume" | "__wfTerminate" => {
                storage.transaction_sync(|transaction| {
                    let mut meta = required(transaction)?;
                    let status = text(&meta, "status")?;
                    match method {
                        "__wfPause" => {
                            if terminal(&meta)? || matches!(status.as_str(), "paused" | "waitingForPause") {
                                return Ok(());
                            }
                            if status == "running" {
                                put_field(&mut meta, "status", json!("waitingForPause"))?;
                                transaction.set_alarm(now())?;
                            } else {
                                put_field(&mut meta, "status", json!("paused"))?;
                                put_field(&mut meta, "pausedMs", json!(now()))?;
                                transaction.delete_alarm()?;
                            }
                        }
                        "__wfResume" => {
                            if status == "waitingForPause" {
                                put_field(&mut meta, "status", json!("running"))?;
                                transaction.set_alarm(now())?;
                            } else {
                                ensure!(status == "paused", "WORKFLOW_ERROR: cannot resume an instance with status {status:?}");
                                let paused = property(&meta, "pausedMs")?
                                    .map(|value| match value.root {
                                        CloneValue::Number(value) => Ok(value),
                                        _ => Err(error("invalid paused timestamp")),
                                    }).transpose()?.unwrap_or(now() as f64);
                                let offset = (now() as f64 - paused).max(0.0);
                                let ledger = format!("{}step.", prefix(&text(&meta, "generation")?));
                                for (key, mut record) in list(transaction, &ledger)? {
                                    match text(&record, "status")?.as_str() {
                                        "retrying" => {
                                            let at = number(&record, "nextAt")?;
                                            put_number(&mut record, "nextAt", at + offset)?;
                                        }
                                        "sleeping" | "waiting" => {
                                            let at = number(&record, "deadline")?;
                                            put_number(&mut record, "deadline", at + offset)?;
                                        }
                                        _ => {}
                                    }
                                    transaction.put_clone(&key, &record)?;
                                }
                                remove(&mut meta, "pausedMs")?;
                                put_field(&mut meta, "status", json!("queued"))?;
                                transaction.set_alarm(now())?;
                            }
                        }
                        _ => {
                            ensure!(!terminal(&meta)?, "WORKFLOW_ERROR: cannot terminate an instance with status {status:?}");
                            put_field(&mut meta, "status", json!("terminated"))?;
                            finish_retention(&mut meta, transaction)?;
                        }
                    }
                    transaction.put_clone(META, &meta)
                })?;
                Ok(StructuredValue::undefined())
            }
            "__wfRestart" => {
                storage.transaction_sync(|transaction| {
                    let mut meta = required(transaction)?;
                    let old = prefix(&text(&meta, "generation")?);
                    let history = list(transaction, &format!("{old}step."))?;
                    let mut ordinal = None;
                    if let Some(from) = property(&argument, "from")? {
                        let name = property(&from, "name")?
                            .ok_or_else(|| error("invalid restart name"))?;
                        let CloneValue::String(name) = name.root else {
                            return Err(error("invalid restart name"));
                        };
                        check_step_name(&name)?;
                        let count = number(&from, "count")?;
                        ensure!(
                            count >= 1.0 && count.fract() == 0.0,
                            "WORKFLOW_ERROR: restart count must be positive integer"
                        );
                        let kind = text(&from, "type")?;
                        let kind = if kind == "waitForEvent" {
                            "event"
                        } else {
                            kind.as_str()
                        };
                        ensure!(
                            ["do", "event", "sleep"].contains(&kind),
                            "WORKFLOW_ERROR: invalid restart type"
                        );
                        let mut ordered = history
                            .iter()
                            .filter_map(|(key, record)| {
                                number(record, "ordinal")
                                    .ok()
                                    .filter(|number| number.fract() == 0.0)
                                    .map(|ordinal| (key, record, ordinal))
                            })
                            .collect::<Vec<_>>();
                        ordered.sort_by(|left, right| left.2.total_cmp(&right.2));
                        let mut occurrence = 0.0;
                        for (key, record, at) in ordered {
                            let key_name = key
                                .strip_prefix(&format!("{old}step."))
                                .and_then(|suffix| suffix.split_once('.'))
                                .map(|(_, name)| name)
                                .unwrap_or("");
                            if text(record, "kind")? == kind
                                && key_name.encode_utf16().eq(name.iter().copied())
                            {
                                occurrence += 1.0;
                                if occurrence == count {
                                    ordinal = Some(at);
                                    break;
                                }
                            }
                        }
                        ensure!(
                            ordinal.is_some(),
                            "WORKFLOW_ERROR: restart() could not find the selected step occurrence"
                        );
                    }
                    let generation = cellar_native::crypto::random_uuid()?;
                    let new = prefix(&generation);
                    if let Some(target) = ordinal {
                        for (key, record) in &history {
                            if number(record, "ordinal")
                                .is_ok_and(|ordinal| ordinal.fract() == 0.0 && ordinal < target)
                            {
                                transaction.put_clone(
                                    &format!("{new}{}", key.strip_prefix(&old).unwrap()),
                                    record,
                                )?;
                            }
                        }
                    }
                    for (key, _) in list(transaction, &old)? {
                        transaction.delete(&key)?;
                    }
                    put_field(&mut meta, "generation", json!(generation))?;
                    put_field(&mut meta, "status", json!("queued"))?;
                    for key in ["pausedMs", "output", "error"] {
                        remove(&mut meta, key)?;
                    }
                    transaction.put_clone(META, &meta)?;
                    transaction.set_alarm(now())
                })?;
                Ok(StructuredValue::undefined())
            }
            "__wfSendEvent" => {
                let kind = text(&argument, "type")?;
                ensure!(name_valid(&kind, 100), "WORKFLOW_ERROR: invalid event type");
                let payload = property(&argument, "payload")?;
                if let Some(payload) = &payload {
                    check_value(payload, "the event payload")?;
                }
                storage.transaction_sync(|transaction| {
                    let meta = required(transaction)?;
                    ensure!(
                        !terminal(&meta)?,
                        "WORKFLOW_ERROR: cannot send an event to a terminal instance"
                    );
                    let prefix = prefix(&text(&meta, "generation")?);
                    let sequence = read(transaction, &format!("{prefix}eventSeq"))?
                        .map(|value| match value.root {
                            CloneValue::Number(value) => Ok(value),
                            _ => Err(error("invalid event sequence")),
                        })
                        .transpose()?
                        .unwrap_or(0.0);
                    transaction
                        .put_clone(&format!("{prefix}eventSeq"), &data(json!(sequence + 1.0))?)?;
                    let mut event = data(json!({"type":kind,"timestampMs":now()}))?;
                    if let Some(payload) = payload {
                        set(&mut event, "payload", &payload)?;
                    }
                    transaction
                        .put_clone(&format!("{prefix}event.{:09}", sequence as u64), &event)?;
                    if !matches!(
                        text(&meta, "status")?.as_str(),
                        "paused" | "waitingForPause"
                    ) {
                        transaction.set_alarm(now())?;
                    }
                    Ok(())
                })?;
                Ok(StructuredValue::undefined())
            }
            _ => Err(error(format!("unknown instance method {method}"))),
        }
    }
    pub(super) fn prepare_alarm(&self) -> Result<Option<StructuredValue>> {
        let Some(mut meta) = read(&self.storage, META)? else {
            self.storage.delete_alarm()?;
            return Ok(None);
        };
        if terminal(&meta)? {
            self.storage.transaction_sync(|transaction| {
                if !expire(transaction, &mut meta)? {
                    transaction.set_alarm(number(&meta, "expiresMs")? as i64)?;
                }
                Ok(())
            })?;
            return Ok(None);
        }
        let generation = text(&meta, "generation")?;
        match text(&meta, "status")?.as_str() {
            "paused" => {
                self.storage.delete_alarm()?;
                Ok(None)
            }
            "waitingForPause" => {
                settle(&self.storage, &generation, |meta, transaction| {
                    if text(meta, "status")? == "waitingForPause" {
                        put_field(meta, "status", json!("paused"))?;
                        put_field(meta, "pausedMs", json!(now()))?;
                        transaction.delete_alarm()?;
                    }
                    Ok(())
                })?;
                Ok(None)
            }
            _ => Ok(Some(meta)),
        }
    }
}
impl Cell for InstanceCell {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            match invocation {
                Invocation::Alarm { .. } => {
                    super::replay::alarm(self, context).await?;
                    Ok(Output::Done)
                }
                Invocation::CellRpc { method, args } => {
                    let arguments = super::super::decode_rpc(&args)?;
                    let argument = match &arguments.root {
                        CloneValue::Reference(id) => match arguments.objects.get(*id as usize) {
                            Some(cellar_native::clone::Object::DenseArray { elements, .. }) => {
                                StructuredValue {
                                    root: elements
                                        .first()
                                        .cloned()
                                        .unwrap_or(CloneValue::Undefined),
                                    objects: arguments.objects.clone(),
                                }
                            }
                            Some(cellar_native::clone::Object::SparseArray {
                                properties, ..
                            }) => {
                                let root = properties.iter().find_map(|(key, value)| {
                                    if matches!(key, CloneValue::Number(number) if *number == 0.0) || matches!(key, CloneValue::String(name) if name.as_slice() == [u16::from(b'0')]) { Some(value.clone()) } else { None }
                                }).unwrap_or(CloneValue::Undefined);
                                StructuredValue {
                                    root,
                                    objects: arguments.objects.clone(),
                                }
                            }
                            _ => return Err(error("invalid instance arguments")),
                        },
                        _ => return Err(error("invalid instance arguments")),
                    };
                    match self.control(&context, &method, argument) {
                        Ok(value) => Ok(Output::Rpc(match args {
                            RpcData::Structured(_) => encode_rpc(&value)?,
                            RpcData::Json(_) => {
                                RpcData::Json(serde_json::to_string(&value.to_json()?)?)
                            }
                        })),
                        Err(error)
                            if method == "__wfDelete"
                                && error.to_string().contains("WORKFLOW_INSTANCE_DELETED") =>
                        {
                            Err(error)
                        }
                        Err(error) if matches!(args, RpcData::Structured(_)) => {
                            Ok(Output::Rpc(RpcData::Structured(
                                cellar_native::rpc::encode_error(
                                    &error.to_string(),
                                    "Error",
                                    None,
                                )?
                                .into(),
                            )))
                        }
                        Err(error) => Err(error),
                    }
                }
                _ => Err(error("Workflow instance received an unsupported event")),
            }
        })
    }
}

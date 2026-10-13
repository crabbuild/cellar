use super::cell::*;
use super::*;

const HISTORY: &str = "__wf.history.";
const PAGE_SIZE: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceEventType {
    WorkflowQueued,
    WorkflowStarted,
    WorkflowRunning,
    WorkflowPaused,
    WorkflowWaitingForPause,
    WorkflowWaiting,
    WorkflowCompleted,
    WorkflowErrored,
    WorkflowTerminated,
    StepStarted,
    StepCompleted,
    StepErrored,
    AttemptStarted,
    AttemptCompleted,
    AttemptErrored,
    SleepStarted,
    SleepCompleted,
    WaitStarted,
    WaitCompleted,
    WaitTimedOut,
    RollbackStarted,
    RollbackStepStarted,
    RollbackStepCompleted,
    RollbackStepErrored,
    RollbackAttemptStarted,
    RollbackAttemptCompleted,
    RollbackAttemptErrored,
    RollbackCompleted,
    RollbackErrored,
}

#[derive(Clone, Debug)]
pub struct InstanceEvent {
    pub instance_id: String,
    pub event_id: u64,
    pub timestamp_ms: i64,
    pub event_type: InstanceEventType,
    /// Variant-specific fields, such as `stepName`, `output`, or `error`.
    pub data: StructuredValue,
}

impl InstanceEvent {
    pub fn to_json(&self) -> Result<serde_json::Value> {
        self.data.to_json()
    }

    fn from_value(data: StructuredValue) -> Result<Self> {
        Ok(Self {
            instance_id: text(&data, "instanceId")?,
            event_id: number(&data, "eventId")? as u64,
            timestamp_ms: number(&data, "timestamp")? as i64,
            event_type: serde_json::from_value(json!(text(&data, "type")?))?,
            data,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct SubscribeOptions {
    pub cursor: Option<u64>,
    pub filter: Option<Vec<InstanceEventType>>,
}

/// An event-owned cursor. Dropping it releases all client-side resources.
pub struct Subscription {
    instance: Instance,
    context: Context,
    cursor: u64,
    filter: Option<Vec<InstanceEventType>>,
    buffered: std::collections::VecDeque<InstanceEvent>,
    ended: bool,
}

impl Subscription {
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    pub fn close(&mut self) {
        self.ended = true;
        self.buffered.clear();
    }

    pub async fn next(&mut self) -> Result<Option<InstanceEvent>> {
        self.context.check_turn()?;
        loop {
            if let Some(event) = self.buffered.pop_front() {
                self.cursor = event.event_id;
                if matches!(
                    event.event_type,
                    InstanceEventType::WorkflowCompleted
                        | InstanceEventType::WorkflowErrored
                        | InstanceEventType::WorkflowTerminated
                ) {
                    self.ended = true;
                    self.buffered.clear();
                }
                if self
                    .filter
                    .as_ref()
                    .is_none_or(|filter| filter.contains(&event.event_type))
                {
                    return Ok(Some(event));
                }
                continue;
            }
            if self.ended {
                return Ok(None);
            }
            let reply = self
                .instance
                .resource_call(
                    &self.context,
                    "__wfHistory",
                    Some(data(json!({"cursor":self.cursor}))?),
                )
                .await?;
            let mut values =
                property(&reply, "events")?.ok_or_else(|| error("missing workflow history"))?;
            let roots = match values.root {
                CloneValue::Reference(index) => match values.objects.get(index as usize) {
                    Some(cellar_native::clone::Object::DenseArray { elements, .. }) => {
                        elements.clone()
                    }
                    _ => return Err(error("invalid workflow history")),
                },
                _ => return Err(error("invalid workflow history")),
            };
            for root in roots {
                values.root = root;
                // Round-trip only the reachable event graph. Cycles and typed values
                // survive, without retaining every other event's object table.
                let event = cellar_native::clone::decode(&cellar_native::clone::encode(&values)?)?;
                self.buffered.push_back(InstanceEvent::from_value(event)?);
            }
            self.ended =
                property(&reply, "done")?.is_some_and(|v| v.root == CloneValue::Boolean(true));
            if self.buffered.is_empty() && !self.ended {
                crate::asyncrt::sleep(std::time::Duration::from_millis(1)).await;
                self.context.check_turn()?;
            }
        }
    }
}

impl Instance {
    pub async fn subscribe(
        &self,
        context: &Context,
        options: SubscribeOptions,
    ) -> Result<Subscription> {
        ensure!(
            options.cursor.unwrap_or(0) <= 9_007_199_254_740_991,
            "WORKFLOW_ERROR: invalid subscription cursor"
        );
        self.status(context).await?;
        Ok(Subscription {
            instance: self.clone(),
            context: context.clone(),
            cursor: options.cursor.unwrap_or(0),
            filter: options.filter,
            buffered: Default::default(),
            ended: false,
        })
    }
}

/// Called inside the same transaction as the corresponding journal mutation.
pub(super) fn append(
    storage: &crate::application::Storage,
    meta: &mut StructuredValue,
    event_type: InstanceEventType,
    mut fields: StructuredValue,
) -> Result<()> {
    let previous = property(meta, "historySeq")?
        .map(|v| v.to_json())
        .transpose()?
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let event_id = previous
        .checked_add(1)
        .filter(|id| *id <= 9_007_199_254_740_991)
        .ok_or_else(|| error("workflow event history exhausted"))?;
    set(
        &mut fields,
        "type",
        &data(serde_json::to_value(event_type)?)?,
    )?;
    set(
        &mut fields,
        "instanceId",
        &data(json!(text(meta, "instanceId")?))?,
    )?;
    set(&mut fields, "eventId", &data(json!(event_id))?)?;
    set(&mut fields, "timestamp", &data(json!(now()))?)?;
    storage.put_clone(&format!("{HISTORY}{event_id:020}"), &fields)?;
    put_field(meta, "historySeq", json!(event_id))
}

pub(super) fn transition(
    storage: &crate::application::Storage,
    before: &StructuredValue,
    after: &mut StructuredValue,
) -> Result<()> {
    use InstanceEventType::*;
    let status = text(after, "status")?;
    if text(before, "status")? == status {
        return Ok(());
    }
    let kind = match status.as_str() {
        "queued" => WorkflowQueued,
        "running" => WorkflowRunning,
        "paused" => WorkflowPaused,
        "waitingForPause" => WorkflowWaitingForPause,
        "waiting" => WorkflowWaiting,
        "complete" => WorkflowCompleted,
        "errored" => WorkflowErrored,
        "terminated" => WorkflowTerminated,
        "rollingBack" => {
            if property(after, "rollbackStarted")?.is_some() {
                WorkflowRunning
            } else {
                put_field(after, "rollbackStarted", json!(true))?;
                RollbackStarted
            }
        }
        _ => return Err(error("invalid workflow status transition")),
    };
    if status == "running" && property(after, "started")?.is_none() {
        let mut fields = data(json!({}))?;
        if let Some(params) = property(after, "params")? {
            set(&mut fields, "params", &params)?;
        }
        append(storage, after, WorkflowStarted, fields)?;
        put_field(after, "started", json!(true))?;
    }
    let mut fields = data(json!({}))?;
    for field in ["output", "error"] {
        if let Some(value) = property(after, field)? {
            set(&mut fields, field, &value)?;
        }
    }
    append(storage, after, kind, fields)
}

pub(super) fn page(
    storage: &crate::application::Storage,
    argument: &StructuredValue,
) -> Result<StructuredValue> {
    let meta = required(storage)?;
    let cursor = number(argument, "cursor")?;
    ensure!(
        cursor >= 0.0 && cursor.fract() == 0.0 && cursor <= 9_007_199_254_740_991.0,
        "WORKFLOW_ERROR: invalid history cursor"
    );
    let records = storage.list_clone(&crate::application::ListOptions {
        prefix: Some(HISTORY.into()),
        start_after: Some(format!("{HISTORY}{:020}", cursor as u64)),
        limit: Some(PAGE_SIZE),
        ..Default::default()
    })?;
    let mut selected = Vec::new();
    let mut encoded_size = 0_usize;
    for record in records {
        let size = cellar_native::clone::encode(&record.1.value)?.len();
        if !selected.is_empty() && encoded_size.saturating_add(size) > 8 << 20 {
            break;
        }
        encoded_size = encoded_size.saturating_add(size);
        selected.push(record);
    }
    let records = selected;
    let last = records
        .last()
        .map(|(_, v)| number(&v.value, "eventId"))
        .transpose()?
        .unwrap_or(cursor);
    let sequence = property(&meta, "historySeq")?
        .map(|_| number(&meta, "historySeq"))
        .transpose()?
        .unwrap_or(0.0);
    let mut reply =
        data(json!({"done":terminal(&meta)? && last >= sequence,"count":records.len()}))?;
    set(
        &mut reply,
        "events",
        &StructuredValue::array(records.into_iter().map(|(_, v)| v.value))?,
    )?;
    Ok(reply)
}

pub(super) fn step_change(
    storage: &crate::application::Storage,
    meta: &mut StructuredValue,
    key: &str,
    old: Option<&StructuredValue>,
    new: &StructuredValue,
) -> Result<()> {
    use InstanceEventType::*;
    let generation = prefix(&text(meta, "generation")?);
    let Some(suffix) = key.strip_prefix(&generation) else {
        return Ok(());
    };
    let Some((group, suffix)) = suffix.split_once('.') else {
        return Ok(());
    };
    if !matches!(group, "step" | "rollback") {
        return Ok(());
    }
    let Some((count, name)) = suffix.split_once('.') else {
        return Ok(());
    };
    let status = text(new, "status")?;
    if let Some(old) = old {
        if text(old, "status")? == status
            && property(old, "lastAttempt")?.map(|v| v.root)
                == property(new, "lastAttempt")?.map(|v| v.root)
        {
            return Ok(());
        }
    }
    let kind = text(new, "kind")?;
    let attempt = property(new, "lastAttempt")?
        .map(|_| number(new, "lastAttempt"))
        .transpose()?
        .unwrap_or(1.0);
    let mut fields =
        data(json!({"stepName":name,"count":count.parse::<u64>()?,"attempt":attempt}))?;
    for field in ["error", "config"] {
        if let Some(value) = property(new, field)? {
            set(&mut fields, field, &value)?;
        }
    }
    if let Some(value) = property(new, "value")? {
        let value =
            if property(new, "sensitive")?.is_some_and(|v| v.root == CloneValue::Boolean(true)) {
                data(json!("[REDACTED]"))?
            } else {
                value
            };
        set(&mut fields, "output", &value)?;
    }
    if status == "retrying" {
        put_number(
            &mut fields,
            "retryDelayMs",
            (number(new, "nextAt")? - now() as f64).max(0.0),
        )?;
    }
    let mut events = Vec::new();
    match (group, kind.as_str(), status.as_str()) {
        ("rollback", _, "running") => {
            if old.is_none() {
                events.push(RollbackStepStarted);
            }
            events.push(RollbackAttemptStarted);
        }
        ("rollback", _, "completed") => {
            events.extend([RollbackAttemptCompleted, RollbackStepCompleted])
        }
        ("rollback", _, "retrying") => events.push(RollbackAttemptErrored),
        ("rollback", _, "failed") => events.extend([RollbackAttemptErrored, RollbackStepErrored]),
        (_, "do", "running") => {
            if old.is_none() {
                events.push(StepStarted);
            }
            events.push(AttemptStarted);
        }
        (_, "do", "completed") => events.extend([AttemptCompleted, StepCompleted]),
        (_, "do", "retrying") => events.push(AttemptErrored),
        (_, "do", "failed") => events.extend([AttemptErrored, StepErrored]),
        (_, "sleep", "sleeping") if old.is_none() => {
            put_number(
                &mut fields,
                "durationMs",
                (number(new, "deadline")? - now() as f64).max(0.0),
            )?;
            events.push(SleepStarted);
        }
        (_, "sleep", "completed") => events.push(SleepCompleted),
        (_, "event", "waiting") if old.is_none() => {
            put_field(&mut fields, "eventType", json!(text(new, "type")?))?;
            events.push(WaitStarted);
        }
        (_, "event", "completed") => events.push(WaitCompleted),
        (_, "event", "failed") => events.push(WaitTimedOut),
        _ => {}
    }
    for kind in events {
        append(storage, meta, kind, fields.clone())?;
    }
    Ok(())
}

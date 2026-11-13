// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Every turn rereads committed state. Discovery hints never authorize sends.

use super::*;
use crate::raft::cell_groups::{
    CellCommandRequest, CellCommandResponse, CellEffectAuthority, CellReadConsistency,
    CellRoutingLimits,
};
use crate::raft::effects::{outbox, scan::DueEffect, RaftDeliveryRequest, RaftEffectSink};
use cellar_logic::consensus::{CommandFailure, CommandId, Durability};
use std::time::Duration;

#[derive(Debug)]
pub(super) enum Error {
    Runtime(RuntimeError),
    Execution(ExecutionError),
    Command(Arc<crate::raft::cell_groups::CellCommandError>),
    BindingRetirement(anyhow::Error),
    BindingAllocator(Box<crate::raft::cell_groups::CellDispatchError>),
    Effect(CommandFailure),
    Invalid(&'static str),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Runtime(e) => std::fmt::Display::fmt(e, f),
            Self::Execution(e) => std::fmt::Display::fmt(e, f),
            Self::Command(e) => std::fmt::Display::fmt(e, f),
            Self::BindingRetirement(e) => write!(f, "binding retirement: {e:#}"),
            Self::BindingAllocator(e) => write!(f, "binding settlement allocator: {e}"),
            Self::Effect(e) => std::fmt::Display::fmt(e, f),
            Self::Invalid(e) => f.write_str(e),
        }
    }
}

fn open(shared: &Shared) -> Result<(), Error> {
    if shared.closing.load(Ordering::Acquire) {
        Err(Error::Runtime(RuntimeError::Closed))
    } else {
        Ok(())
    }
}

pub(super) fn clock(shared: &Shared) -> Result<u64, Error> {
    shared
        .domain
        .mono_ms()
        .map_err(|_| Error::Runtime(RuntimeError::Closed))
}

fn wall() -> Result<u64, Error> {
    u64::try_from(crate::asyncrt::wall_ms()).map_err(|_| Error::Runtime(RuntimeError::Closed))
}

fn effect(error: anyhow::Error) -> Error {
    Error::Effect(
        error
            .downcast_ref::<CommandFailure>()
            .cloned()
            .unwrap_or(CommandFailure::CorruptState),
    )
}

pub(super) async fn run(
    group: GroupHistory,
    after: Option<CellId>,
    registration: &Registration,
    shared: &Arc<Shared>,
    reservation: crate::raft::budget::Permit,
    cursor: &mut Option<CellId>,
) -> Result<(), Error> {
    open(shared)?;
    let Some(execution) = shared
        .runtime
        .next_effect(group, wall()?, after)
        .await
        .map_err(Error::Runtime)?
    else {
        return Ok(());
    };
    if execution.route().schema_digest != registration.schema {
        return Err(Error::Invalid("effect group schema"));
    }
    let cell = execution.cell().version.history.clone();
    *cursor = Some(cell.cell.clone());
    // Retain this coherent image and original leader fence through delivery.
    let sql = shared
        .sql
        .upgrade()
        .ok_or(Error::Runtime(RuntimeError::Closed))?;
    let factory = registration.sql.clone();
    let factory_cell = cell.clone();
    let options = shared
        .domain
        .blocking(move || factory(&factory_cell))
        .map_err(|_| Error::Runtime(RuntimeError::Closed))?
        .await
        .map_err(|_| Error::Runtime(RuntimeError::Closed))?
        .map_err(Error::Execution)?;
    options.limits().validate().map_err(Error::Execution)?;
    let limits = options.limits();
    if registration.options.retry_window_ms > limits.max_retry_window_ms {
        return Err(Error::Invalid(
            "effect retry window exceeds execution limit",
        ));
    }
    if let Some(alarms) = registration.class.alarm_limits() {
        if limits.envelope.max_payload_bytes < 1024
            || limits.max_client_identities < 2
            || limits.max_retained_receipts < 2
            || alarms
                .max_payload_bytes
                .checked_mul(alarms.max_alarms)
                .and_then(|n| n.checked_add(128 * 1024))
                .is_none_or(|n| n as u64 > limits.max_image_bytes)
        {
            return Err(Error::Invalid("alarm execution capacity"));
        }
    }
    if let Some(outbox) = registration.class.outbox_definition() {
        if limits.envelope.max_payload_bytes < 32 * 1024
            || limits.max_result_bytes < 33 + crate::raft::effects::delivery::MAX_REQUEST
            || limits.max_client_identities < 2
            || limits.max_retained_receipts < 3
            || (outbox.limits.max_total_bytes as u64)
                .checked_add(128 * 1024)
                .is_none_or(|n| n > limits.max_image_bytes)
        {
            return Err(Error::Invalid("effect execution capacity"));
        }
    }
    open(shared)?;
    let deadline = clock(shared)?
        .checked_add(registration.options.operation_timeout_ms)
        .ok_or(Error::Invalid("effect deadline"))?;
    let authority = shared
        .runtime
        .read_authority(
            group,
            cell.clone(),
            CellReadConsistency::Linearizable,
            deadline,
        )
        .await
        .map_err(Error::Runtime)?;
    let scan = sql
        .scan_effects(
            options.clone(),
            authority,
            registration.class.clone(),
            wall()?,
            reservation,
        )
        .await
        .map_err(Error::Execution)?;
    let crate::raft::effects::scan::EffectScan {
        next,
        due,
        _reservation,
        ..
    } = *scan.into_scan();
    let _metadata = _reservation;
    let Some(due) = due else {
        return Ok(());
    };
    let minimum = due.minimum().map_err(effect)?;
    let (operation, input) = due.command().map_err(effect)?;
    let retry_until = wall()?
        .checked_add(registration.options.retry_window_ms)
        .ok_or(Error::Invalid("effect retry window"))?;
    let receipt = command(
        shared,
        registration,
        &cell,
        options.clone(),
        request(registration, next, &operation, input, minimum, retry_until)?,
    )
    .await?;
    let local = receipt
        .local_receipt()
        .ok_or(Error::Invalid("local effect receipt"))?;
    match due {
        DueEffect::Binding(_) => {
            let pending: Option<crate::storage::raft_binding_recovery::Pending> =
                serde_json::from_slice(receipt.result())
                    .map_err(|_| Error::Invalid("binding claim result"))?;
            let Some(pending) = pending else {
                return Ok(());
            };
            let route = execution.route().clone();
            let authority = execution
                .effect_authority(&local.version, local.position.term)
                .map_err(Error::Runtime)?;
            // Release the source proof's SQL/archive response slot before a
            // loopback target needs the same admission bank.
            drop(receipt);
            authority.check().map_err(Error::Runtime)?;
            let host = registration
                .dispatcher
                .bindings
                .get()
                .ok_or(Error::Invalid("binding recovery host"))?;
            let candidate_lost = host
                .retire(
                    pending.clone(),
                    Duration::from_millis(registration.options.operation_timeout_ms),
                )
                .await
                .map_err(Error::BindingRetirement)?;
            authority.check().map_err(Error::Runtime)?;
            // Self-calls and concurrent target maintenance may have consumed the
            // same system client's next sequence. Read its allocator anew.
            let deadline = clock(shared)?
                .checked_add(registration.options.operation_timeout_ms)
                .ok_or(Error::Invalid("binding settlement deadline"))?;
            let next = super::super::binding_recovery::next_system(
                shared,
                &registration.dispatcher,
                options.clone(),
                cell.clone(),
                route,
                deadline,
            )
            .await
            .map_err(|error| Error::BindingAllocator(Box::new(error)))?;
            let input = serde_json::to_vec(&crate::storage::raft_binding_recovery::Settlement {
                key: pending.key,
                digest: pending.digest,
                candidate_lost,
            })
            .map_err(|_| Error::Invalid("binding settlement input"))?;
            let receipt = command(
                shared,
                registration,
                &cell,
                options,
                request(
                    registration,
                    next,
                    crate::storage::raft_binding_recovery::SETTLE,
                    input,
                    minimum,
                    retry_until,
                )?,
            )
            .await?;
            check_result(receipt.result())
        }
        DueEffect::Alarm(_) | DueEffect::Work(_) => check_result(receipt.result()),
        DueEffect::Delivery {
            selected,
            definition,
            ..
        } => {
            let Some(claimed) = outbox::claimed(
                &definition,
                &cell,
                registration.schema,
                &selected,
                receipt.result(),
            )
            .map_err(effect)?
            else {
                return Ok(());
            };
            let authority = execution
                .effect_authority(&local.version, local.position.term)
                .map_err(Error::Runtime)?;
            // The committed claim has been consumed into bounded delivery
            // metadata and fenced image custody. Release its response admission
            // before a loopback receiver or source completion needs the same
            // one-slot SQL/archive lane. Published S3 durability survives a
            // consumer dropping its response; the durable Dispatched row stays.
            drop(receipt);
            let proof = deliver(
                shared,
                authority,
                claimed.request.clone(),
                claimed.sink,
                registration.options.operation_timeout_ms,
            )
            .await?;
            claimed.request.verify_receipt(&proof).map_err(effect)?;
            let input =
                outbox::completion_input(selected, claimed.generation, &proof).map_err(effect)?;
            let next = CommandId::new(
                next.client,
                next.sequence()
                    .checked_add(1)
                    .ok_or(Error::Effect(CommandFailure::Overloaded))?,
            )
            .map_err(|_| Error::Effect(CommandFailure::Overloaded))?;
            let receipt = command(
                shared,
                registration,
                &cell,
                options,
                request(
                    registration,
                    next,
                    outbox::COMPLETE,
                    input,
                    minimum,
                    retry_until,
                )?,
            )
            .await?;
            check_result(receipt.result())
        }
    }
}

fn check_result(result: &[u8]) -> Result<(), Error> {
    if result == [0] || result == [1] {
        Ok(())
    } else {
        Err(Error::Effect(CommandFailure::CorruptState))
    }
}

fn request(
    registration: &Registration,
    id: CommandId,
    operation: &str,
    input: Vec<u8>,
    durability: Durability,
    retry_until_ms: u64,
) -> Result<CellCommandRequest, Error> {
    let contract = registration
        .class
        .system_contract(operation)
        .ok_or(Error::Invalid("effect system operation"))?;
    Ok(CellCommandRequest {
        id,
        contract,
        input,
        durability,
        retry_until_ms,
    })
}

async fn command(
    shared: &Arc<Shared>,
    registration: &Registration,
    cell: &CellHistory,
    options: CellSqlOptions,
    request: CellCommandRequest,
) -> Result<CellCommandResponse, Error> {
    open(shared)?;
    if wall()? >= request.retry_until_ms {
        return Err(Error::Effect(CommandFailure::Unavailable));
    }
    let invocation = registration
        .dispatcher
        .system_command(
            cell.clone(),
            request,
            CellRoutingLimits {
                timeout: Duration::from_millis(registration.options.operation_timeout_ms),
                max_attempts: 4,
            },
        )
        .await
        .map_err(|error| Error::Command(Arc::new(error)))?;
    let call = commands::admit(shared, invocation, options)
        .map_err(|error| Error::Command(Arc::new(error.error)))?;
    let response = call.response();
    tokio::pin!(response);
    loop {
        open(shared)?;
        let timer = shared
            .domain
            .sleep_until(clock(shared)?.saturating_add(100))
            .map_err(|_| Error::Runtime(RuntimeError::Closed))?;
        crate::asyncrt::select! {
            result = &mut response => return result.map_err(Error::Command),
            _ = timer => (),
        }
    }
}

async fn deliver(
    shared: &Shared,
    authority: CellEffectAuthority,
    request: RaftDeliveryRequest,
    sink: Arc<dyn RaftEffectSink>,
    timeout: u64,
) -> Result<Vec<u8>, Error> {
    let deadline = clock(shared)?
        .checked_add(timeout)
        .ok_or(Error::Invalid("delivery deadline"))?;
    let mut send = None;
    let mut proof = None;
    loop {
        open(shared)?;
        if clock(shared)? >= deadline {
            return Err(Error::Effect(CommandFailure::Unavailable));
        }
        // Apply lag and unrelated business mutations do not revoke this
        // committed dispatch. Term, route, activation, membership and cell
        // lineage still must cover the claim before send and reply acceptance.
        authority.check().map_err(Error::Runtime)?;
        if let Some(proof) = proof {
            return Ok(proof);
        }
        if send.is_none() {
            send = Some(sink.deliver(request.clone()));
        }
        let timer = shared
            .domain
            .sleep_until(clock(shared)?.saturating_add(100).min(deadline))
            .map_err(|_| Error::Runtime(RuntimeError::Closed))?;
        let pending = send.as_mut().ok_or(Error::Invalid("delivery waiter"))?;
        crate::asyncrt::select_biased! { "source authority is rechecked before accepting a late delivery";
            _ = timer => (),
            result = pending => { proof = Some(result.map_err(Error::Effect)?); }
        }
    }
}

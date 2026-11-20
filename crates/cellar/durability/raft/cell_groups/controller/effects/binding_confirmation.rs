// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded receipt-only rediscovery after a private settlement loses its output.
use super::{turn, Registration, Shared};
use crate::raft::cell_groups::{
    CellCommandResponse, CellEffectAuthority, CellExecutionAuthority, CellReceiptRequest,
    CellRoutingLimits, CellSqlOptions,
};
use crate::storage::raft_binding_recovery::{
    self as recovery, Confirmation, Pending, ReceiptReference,
};
use cellar_logic::{
    cell_group::{CellHistory, GroupRoute},
    consensus::{ClientId, CommandId, Durability},
};
use std::{sync::Arc, time::Duration};

/// Created only from an actual command response or fresh receipt proof.
struct Proven(Confirmation);

pub(super) async fn settled(
    shared: &Arc<Shared>,
    registration: &Registration,
    cell: &CellHistory,
    options: CellSqlOptions,
    owner: (GroupRoute, CellEffectAuthority),
    pending: Pending,
    (response, candidate_lost): (CellCommandResponse, bool),
) -> Result<(), turn::Error> {
    turn::check_result(response.result())?;
    let local = response
        .local_receipt()
        .ok_or(turn::Error::Invalid("local settlement receipt"))?;
    if local.version.history != *cell
        || response.durability()
            < pending
                .minimum()
                .map_err(|_| turn::Error::Invalid("settlement minimum"))?
    {
        return Err(turn::Error::Invalid("settlement response proof"));
    }
    let receipt = ReceiptReference {
        client: local.receipt.command().client.bytes(),
        sequence: local.receipt.command().sequence(),
        retry_until_ms: local.receipt.retry_until_ms(),
        durability: if local.receipt.minimum_durability() == Durability::S3 {
            2
        } else {
            1
        },
        candidate_lost,
    };
    let proven = Proven(Confirmation {
        key: pending.key,
        digest: pending.digest,
        receipt,
    });
    // The next private mutation needs the same SQL/archive response capacity.
    drop(response);
    finish(shared, registration, cell, options, owner, proven).await
}

pub(super) async fn resume(
    shared: &Arc<Shared>,
    registration: &Registration,
    cell: &CellHistory,
    options: CellSqlOptions,
    execution: CellExecutionAuthority,
    pending: &Pending,
) -> Result<(), turn::Error> {
    let receipt = pending
        .confirmation
        .clone()
        .ok_or(turn::Error::Invalid("missing settlement reference"))?;
    let minimum = recovery::durability(receipt.durability)
        .map_err(|_| turn::Error::Invalid("settlement policy"))?;
    if minimum
        < pending
            .minimum()
            .map_err(|_| turn::Error::Invalid("source settlement policy"))?
    {
        return Err(turn::Error::Invalid("downgraded settlement reference"));
    }
    let route = execution.route().clone();
    let version = execution.cell().version.clone();
    let term = execution.term();
    let authority = execution
        .effect_authority(&version, term)
        .map_err(turn::Error::Runtime)?;
    let input = serde_json::to_vec(&recovery::Settlement {
        key: pending.key.clone(),
        digest: pending.digest,
        candidate_lost: receipt.candidate_lost,
    })
    .map_err(|_| turn::Error::Invalid("settlement receipt input"))?;
    let id = CommandId::new(
        ClientId::new(receipt.client).map_err(|_| turn::Error::Invalid("settlement client"))?,
        receipt.sequence,
    )
    .map_err(|_| turn::Error::Invalid("settlement sequence"))?;
    let invocation = registration
        .dispatcher
        .lookup_system_receipt(
            cell.clone(),
            CellReceiptRequest {
                id,
                contract: registration
                    .class
                    .system_contract(recovery::SETTLE)
                    .ok_or(turn::Error::Invalid("settlement contract"))?,
                input,
                durability: minimum,
            },
            CellRoutingLimits {
                timeout: Duration::from_millis(registration.options.operation_timeout_ms),
                max_attempts: 4,
            },
        )
        .map_err(|error| turn::Error::BindingReceipt(Box::new(error)))?;
    authority.check().map_err(turn::Error::Runtime)?;
    // One ordinary bounded read per scheduler turn. Failure retains the row and
    // its receipt pin; other groups keep their normal scheduling opportunity.
    let response = super::super::receipt_routes::execute(shared, invocation, options.clone(), None)
        .await
        .map_err(|error| turn::Error::BindingReceipt(Box::new(error)))?;
    authority.check().map_err(turn::Error::Runtime)?;
    let stored = response.receipt().ok_or(turn::Error::Invalid(
        "settlement receipt absence remains unknown",
    ))?;
    if response.command() != id
        || stored.command() != id
        || response.durability() < minimum
        || stored.minimum_durability() != minimum
        || stored.retry_until_ms() != receipt.retry_until_ms
    {
        return Err(turn::Error::Invalid("settlement receipt identity/policy"));
    }
    turn::check_result(stored.result())?;
    let proven = Proven(Confirmation {
        key: pending.key.clone(),
        digest: pending.digest,
        receipt,
    });
    drop(response);
    finish(
        shared,
        registration,
        cell,
        options,
        (route, authority),
        proven,
    )
    .await
}

async fn finish(
    shared: &Arc<Shared>,
    registration: &Registration,
    cell: &CellHistory,
    options: CellSqlOptions,
    (route, authority): (GroupRoute, CellEffectAuthority),
    Proven(confirmation): Proven,
) -> Result<(), turn::Error> {
    authority.check().map_err(turn::Error::Runtime)?;
    // Historical proof observation, not authority or an exact row count. Logical
    // settlement already released its lane; only bounded physical GC remains.
    shared.runtime.log().performance().increment(
        crate::raft::performance::Counter::CellBindingRecoverySettlements,
        1,
    );
    let deadline = turn::clock(shared)?
        .checked_add(registration.options.operation_timeout_ms)
        .ok_or(turn::Error::Invalid("binding confirmation deadline"))?;
    let next = super::super::binding_recovery::next_system(
        shared,
        &registration.dispatcher,
        options.clone(),
        cell.clone(),
        route,
        deadline,
    )
    .await
    .map_err(|error| turn::Error::BindingAllocator(Box::new(error)))?;
    authority.check().map_err(turn::Error::Runtime)?;
    let minimum = recovery::durability(confirmation.receipt.durability)
        .map_err(|_| turn::Error::Invalid("confirmation minimum"))?;
    let input = serde_json::to_vec(&confirmation)
        .map_err(|_| turn::Error::Invalid("binding confirmation input"))?;
    let retry_until = turn::wall()?
        .checked_add(registration.options.retry_window_ms)
        .ok_or(turn::Error::Invalid("binding confirmation retry horizon"))?;
    let response = turn::command(
        shared,
        registration,
        cell,
        options,
        turn::request(
            registration,
            next,
            recovery::CONFIRM,
            input,
            minimum,
            retry_until,
        )?,
    )
    .await?;
    turn::check_result(response.result())
}

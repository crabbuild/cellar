// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Platform state sampling is read-only and executes under the original owner gates.
use crate::application::event_stream::{DeliveryError, ErrorCode};
use crate::application::{Context, Invocation, OutboxOptions, Output};
use crate::event_state::{Gauge, StateSample};
use anyhow::{ensure, Context as _, Result};

pub(crate) const METHOD: &str = "__eventState";
pub(crate) const MAX_REPLY_BYTES: usize = 16 << 10;

fn sample(context: &Context) -> Result<StateSample> {
    let scope = context
        .identity()
        .context("state sample has no cell")?
        .scope
        .clone();
    let db = context.storage()?;
    let mut sample = StateSample::new(scope.clone(), crate::asyncrt::wall_ms().max(0));
    let source = db.outbox(&OutboxOptions {
        limit: 1,
        cursor: None,
    })?;
    source.validate(
        &scope,
        &OutboxOptions {
            limit: 1,
            cursor: None,
        },
    )?;
    sample.sampled_ms = sample.sampled_ms.max(source.observation.observed_ms);
    sample.set(Gauge::Sources, i64::from(source.incarnation.is_some()))?;
    sample.set(Gauge::OutboxRows, source.pending_rows)?;
    sample.set(Gauge::OutboxBytes, source.pending_bytes)?;
    sample.set(
        Gauge::OutboxOldestAgeMs,
        source.observation.oldest_pending_age_ms.unwrap_or(0),
    )?;
    for blocked in source.observation.blocked_lanes {
        sample.set(
            match blocked.reason {
                ErrorCode::Forbidden => Gauge::BlockedForbidden,
                ErrorCode::DestinationDeleted => Gauge::BlockedDestinationDeleted,
                ErrorCode::SequenceGap => Gauge::BlockedSequenceGap,
                ErrorCode::PayloadTooLarge => Gauge::BlockedPayloadTooLarge,
                ErrorCode::UnsupportedProtocol => Gauge::BlockedUnsupportedProtocol,
                ErrorCode::ConfigConflict => Gauge::BlockedConfigConflict,
                ErrorCode::ProducerSealed => Gauge::BlockedProducerSealed,
                _ => anyhow::bail!("unsupported blocked reason in state sample"),
            },
            blocked.lanes,
        )?;
    }
    let (allocated, used) = db.sqlite_allocation()?;
    sample.set(Gauge::SqliteAllocatedBytes, allocated)?;
    sample.set(Gauge::SqliteUsedBytes, used)?;
    if scope.starts_with(&format!("{}:", crate::deploy::EVENT_STREAM_CLASS)) {
        let present = db.system_sql(
            "SELECT 1 AS n FROM sqlite_schema WHERE type='table' AND name='__sys_stream'",
            &[],
        )?;
        if !present.is_empty() {
            let stream = crate::application::event_stream::statistics::sample(context)?;
            ensure!(
                crate::application::event_stream::cell_scope(&stream.resource)? == scope,
                "stored stream identity conflicts with owner scope"
            );
            let lifecycle =
                db.system_sql("SELECT lifecycle FROM __sys_stream WHERE singleton=1", &[])?;
            let retired = matches!(lifecycle.first().and_then(|row|row.get("lifecycle")),Some(crate::application::storage::SqlValue::Text(state)) if state=="deleted");
            sample.set(Gauge::RetiredStreams, i64::from(retired))?;
            sample.sampled_ms = sample.sampled_ms.max(stream.observation.observed_ms);
            for (g, n) in [
                (Gauge::Streams, 1),
                (Gauge::StreamRows, stream.rows),
                (Gauge::StreamBytes, stream.bytes),
                (Gauge::StreamEarliestOffset, stream.earliest),
                (Gauge::StreamEndOffset, stream.end),
                (Gauge::ActiveLeases, stream.observation.active_leases),
                (
                    Gauge::ExpiredSubscriptions,
                    stream.observation.expired_subscriptions,
                ),
                (
                    Gauge::ProtectedRecords,
                    stream.observation.protected_records,
                ),
                (Gauge::ProtectedBytes, stream.observation.protected_bytes),
                (
                    Gauge::RetentionBlockedBytes,
                    stream.observation.retention_blocked_bytes,
                ),
            ] {
                sample.set(g, n)?;
            }
            // The live definition is capped at 128 subscriptions. Tombstones are
            // excluded by the lifecycle index; they cannot expand this query.
            let subscriptions=db.system_sql("SELECT checkpoint FROM __sys_stream_subscriptions INDEXED BY __sys_stream_subscription_state WHERE state IN ('active','paused','expired') LIMIT 129",&[])?;
            ensure!(
                subscriptions.len() <= 128,
                "live subscription state exceeds schema bound"
            );
            sample.set(Gauge::Subscriptions, subscriptions.len() as i64)?;
            let mut lag = 0u128;
            let mut retained = 0u128;
            let mut oldest = 0i64;
            for row in subscriptions {
                let checkpoint = match row.get("checkpoint") {
                    Some(crate::application::storage::SqlValue::Integer(n)) => *n,
                    _ => anyhow::bail!("invalid subscription checkpoint"),
                };
                let distance = cellar_logic::event_stream::subscription_lag(
                    stream.earliest,
                    stream.end,
                    checkpoint,
                )?;
                lag = lag
                    .checked_add(distance.records as u128)
                    .context("subscription lag sum overflow")?;
                retained = retained
                    .checked_add(distance.retained_records as u128)
                    .context("retained lag sum overflow")?;
                if distance.retained_records > 0 {
                    let rows = db.system_sql(
                        "SELECT append_ms FROM __sys_stream_events WHERE offset=?",
                        &[distance.first_retained.into()],
                    )?;
                    let append = match rows.first().and_then(|r| r.get("append_ms")) {
                        Some(crate::application::storage::SqlValue::Integer(n)) => *n,
                        _ => anyhow::bail!("retained lag record missing"),
                    };
                    ensure!(append >= 0, "invalid retained lag time");
                    oldest = oldest.max(sample.sampled_ms.saturating_sub(append).max(0));
                }
            }
            sample.set_wide(Gauge::SubscriptionLagRecords, lag);
            sample.set_wide(Gauge::SubscriptionRetainedLagRecords, retained);
            sample.set(Gauge::SubscriptionOldestAgeMs, oldest)?;
        }
    }
    sample.values(&scope)?;
    Ok(sample)
}

pub(crate) async fn invoke(
    context: Context,
    invocation: Invocation,
    authority: super::rpc_authority::StateAuthority,
) -> Result<Output> {
    context.check_turn()?;
    let Invocation::CellRpc { method, args } = invocation else {
        anyhow::bail!("state sampling requires RPC")
    };
    authority.validate_target(
        &context
            .identity()
            .context("state sample has no cell")?
            .scope,
        &method,
    )?;
    let bytes = args.payload_bytes();
    ensure!(bytes <= 1024, "state sample request exceeds limit");
    let body = crate::application::decode_rpc(&args)?.to_json()?;
    ensure!(
        body == serde_json::json!({}),
        "state sample request must be empty"
    );
    let result = sample(&context);
    let value = match result {
        Ok(sample) => serde_json::json!({"result":sample}),
        Err(error) => {
            let error = error
                .downcast_ref::<DeliveryError>()
                .cloned()
                .unwrap_or_else(|| {
                    DeliveryError::new(ErrorCode::Unavailable, "owner state sample failed")
                });
            serde_json::json!({"error":error})
        }
    };
    crate::application::event_stream::encoding::json(&value, MAX_REPLY_BYTES)?;
    Ok(Output::Rpc(crate::application::encode_rpc(
        &cellar_native::clone::StructuredValue::from_json(value)?,
    )?))
}

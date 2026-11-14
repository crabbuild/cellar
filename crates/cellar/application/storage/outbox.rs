// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Transaction-scoped event emission into the owning cell's durable outbox.

use super::*;
use crate::application::event_stream::{DeliveryError, ErrorCode, Event, SealStatus, Stream};

pub(crate) struct EventSourceConfiguration {
    script: String,
    bindings: Arc<[crate::application::event_stream::BindingConfig]>,
    readiness: Arc<crate::application::event_stream::readiness::Readiness>,
}

impl EventSourceConfiguration {
    pub(crate) fn new(config: &crate::worker_config::WorkerConfig) -> Self {
        Self {
            script: config.script_name.clone(),
            bindings: config.stream_bindings.clone().into(),
            readiness: config.event_readiness.clone(),
        }
    }
}

fn authorize<E>(
    script: &str,
    bindings: &[crate::application::event_stream::BindingConfig],
    readiness: &crate::application::event_stream::readiness::Readiness,
    stream: &Stream<E>,
    ready: bool,
) -> Result<()> {
    stream.check_domain()?;
    if ready {
        stream.check_ready()?;
        readiness.check()?;
    }
    if !(stream.binding.publish
        && script == stream.binding.principal
        && bindings.iter().any(|current| {
            current.environment == stream.binding.environment
                && current.principal == stream.binding.principal
                && current.grant == stream.binding.grant
                && current.publish
                && readiness
                    .binding(current)
                    .is_ok_and(|resolved| resolved.resource.same_identity(stream.resource()))
        }))
    {
        return Err(DeliveryError::new(
            ErrorCode::Forbidden,
            "stream publication is not bound to this cell's application configuration",
        )
        .into());
    }
    Ok(())
}

impl Storage {
    fn authorize_outbox<E>(&self, stream: &Stream<E>, ready: bool) -> Result<()> {
        self.write_scope()?;
        if self.is_raft() {
            anyhow::ensure!(
                self.resources().durable_callbacks.load(Ordering::Acquire),
                consensus::CommandFailure::UnsupportedCapability
            );
            let config = self
                .resources()
                .event_source
                .get()
                .ok_or(consensus::CommandFailure::UnsupportedCapability)?;
            authorize(
                &config.script,
                &config.bindings,
                &config.readiness,
                stream,
                ready,
            )
        } else {
            let config = &self.native_activation()?.worker_config;
            authorize(
                &config.script_name,
                &config.stream_bindings,
                &config.event_readiness,
                stream,
                ready,
            )
        }
    }

    pub(crate) fn ensure_outbox_schema(&self) -> Result<()> {
        crate::storage::outbox::ensure_schema(self.write_scope()?)
    }

    /// Disable further emission and commit a recoverable terminal seal request.
    /// The lane must have no pending rows. Like emit, this composes with the
    /// caller's business transaction; its return is not a destination receipt.
    pub fn seal<E: Event>(&self, stream: &Stream<E>, operation: &str) -> Result<SealStatus> {
        let scope = self.write_scope()?;
        self.authorize_outbox(stream, false)?;
        anyhow::ensure!(
            crate::storage::facet_opening(scope).is_none(),
            "facets cannot seal durable producers"
        );
        if self.is_raft() {
            let history = crate::storage::capture_raft_dispatch()?
                .ok_or(consensus::CommandFailure::UnsupportedCapability)?
                .source_history()?;
            crate::storage::outbox::seal_raft(&stream.binding, operation, &history)
        } else {
            crate::storage::outbox::seal(scope, &stream.binding, operation)
        }
    }

    /// Retry this cell's pending delivery after repairing an unavailable or
    /// revoked destination. Event bytes and identities remain unchanged.
    pub fn retry_outbox(&self) -> Result<()> {
        let scope = self.write_scope()?;
        anyhow::ensure!(
            crate::storage::facet_opening(scope).is_none(),
            "facets cannot retry durable source lanes"
        );
        let present = self.system_sql(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name='__sys_outbox_lanes'",
            &[],
        )?;
        if present.is_empty() {
            return Ok(());
        }
        self.ensure_outbox_schema()?;
        self.tx(|tx| {
            let generations=tx.system_sql("SELECT max(dispatch_generation) AS generation FROM __sys_outbox_lanes AS l WHERE EXISTS(SELECT 1 FROM __sys_outbox WHERE lane=l.lane) OR EXISTS(SELECT 1 FROM __sys_outbox_seals WHERE lane=l.lane AND state!='accepted')",&[])?;
            if let Some(SqlValue::Integer(generation)) = generations.first().and_then(|row|row.get("generation")) {
                cellar_logic::event_stream::next_dispatch_generation(*generation).map_err(|_|DeliveryError::new(ErrorCode::OperationLimit,"outbox dispatch generation exhausted"))?;
            }
            // Re-arming work invalidates replies from a dispatch that began
            // before this retry, including the unconditional SDK retry path.
            tx.system_sql("UPDATE __sys_outbox_lanes AS l SET dispatch_generation=dispatch_generation+1 WHERE EXISTS(SELECT 1 FROM __sys_outbox WHERE lane=l.lane) OR EXISTS(SELECT 1 FROM __sys_outbox_seals WHERE lane=l.lane AND state!='accepted')",&[])?;
            tx.system_sql("UPDATE __sys_outbox_lanes SET status='pending',retry_at_ms=?,attempts=0,blocked_reason=NULL WHERE status!='sealed'",
                &[crate::asyncrt::wall_ms().into()])?;
            let rows=tx.system_sql("SELECT 1 AS pending FROM __sys_outbox LIMIT 1",&[])?;
            tx.system_sql("UPDATE __sys_outbox_lanes SET retry_at_ms=?,attempts=0,blocked_reason=NULL WHERE lane IN (SELECT lane FROM __sys_outbox_seals WHERE state!='accepted')",&[crate::asyncrt::wall_ms().into()])?;
            tx.system_sql("UPDATE __sys_outbox_seals SET state='pending' WHERE state!='accepted'",&[])?;
            let seals=tx.system_sql("SELECT 1 AS pending FROM __sys_outbox_seals WHERE state!='accepted' LIMIT 1",&[])?;
            if !rows.is_empty() || !seals.is_empty() { tx.schedule_work(cellar_logic::work::Reason::Outbox,crate::asyncrt::wall_ms())?; }
            Ok(())
        })
    }

    pub fn emit<E: Event>(&self, stream: &Stream<E>, event: E) -> Result<()> {
        let scope = self.write_scope()?;
        self.authorize_outbox(stream, true)?;
        if self.1.is_none() && self.resources().transaction.lock().unwrap().is_none() {
            return Err(DeliveryError::new(
                ErrorCode::TransactionRequired,
                "event emission requires an active transaction",
            )
            .into());
        }
        anyhow::ensure!(
            crate::storage::facet_opening(scope).is_none(),
            "facets cannot emit durable events"
        );
        let limits = crate::application::limits::current().events;
        let key = event.key();
        if key.len() > limits.max_key_bytes.min(limits.event_bytes()) {
            return Err(
                DeliveryError::new(ErrorCode::PayloadTooLarge, "event key is too large").into(),
            );
        }
        if E::FAMILY != stream.resource().family {
            return Err(DeliveryError::new(
                ErrorCode::EventFamilyMismatch,
                "event family does not match stream binding",
            )
            .into());
        }
        if E::FAMILY.len() > limits.max_family_bytes {
            return Err(DeliveryError::new(
                ErrorCode::PayloadTooLarge,
                "event family is too large",
            )
            .into());
        }
        if E::VERSION == 0 {
            return Err(DeliveryError::new(
                ErrorCode::InvalidRequest,
                "event version must be positive",
            )
            .into());
        }
        let key = key.to_owned();
        let payload =
            crate::application::event_stream::encoding::payload(&event, limits.event_bytes())?;
        if self.is_raft() {
            let history = crate::storage::capture_raft_dispatch()?
                .ok_or(consensus::CommandFailure::UnsupportedCapability)?
                .source_history()?;
            crate::storage::outbox::emit_raft(
                &stream.binding,
                E::VERSION,
                key,
                payload,
                limits,
                &history,
            )
        } else {
            crate::storage::outbox::emit(scope, &stream.binding, E::VERSION, key, payload, limits)
        }
    }
}

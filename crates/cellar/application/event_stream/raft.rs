// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared-group source identity, provisioning and bounded durable event work.

use super::*;
use crate::application::consensus::CellHistory;
use crate::worker_config::WorkerConfig;

/// Only immutable event declarations enter the storage capability graph.
/// Provider/runtime handles stay with the native execution adapter.
pub(crate) struct Configuration {
    pub script_name: String,
    pub stream_bindings: Vec<BindingConfig>,
    pub event_readiness: Arc<super::readiness::Readiness>,
}

impl From<&WorkerConfig> for Configuration {
    fn from(config: &WorkerConfig) -> Self {
        Self {
            script_name: config.script_name.clone(),
            stream_bindings: config.stream_bindings.clone(),
            event_readiness: config.event_readiness.clone(),
        }
    }
}
use sha2::{Digest, Sha256};

#[derive(Clone)]
pub(crate) struct Source {
    pub config: Arc<Configuration>,
    pub scope: String,
    pub incarnation: String,
}

impl Source {
    pub fn new(history: &CellHistory, config: Arc<Configuration>) -> Self {
        let mut hash = Sha256::new();
        hash.update(b"cellar-event-source-history-1");
        hash.update(history.cluster.bytes());
        hash.update(history.namespace.bytes());
        hash.update((history.cell.as_str().len() as u64).to_be_bytes());
        hash.update(history.cell.as_str().as_bytes());
        hash.update(history.incarnation.bytes());
        let digest = hash.finalize();
        Self {
            config,
            scope: history.cell.as_str().into(),
            incarnation: hex::encode(&digest[..16]),
        }
    }
}

pub(crate) struct Emission<'a> {
    pub source: &'a Source,
    pub input_limit: usize,
}

pub(crate) fn append_limit(binding: &Binding) -> Result<usize> {
    crate::storage::capture_raft_dispatch()?
        .ok_or(crate::application::consensus::CommandFailure::UnsupportedCapability)?
        .event_wire_limit(&cell_scope(&binding.resource)?, "__streamAppend")
}

/// Match the exact owned wire used by the child dispatcher, including host
/// provenance, resource/grant metadata, base64 and structured-value framing.
pub(crate) fn append_fits(
    binding: &Binding,
    source: &str,
    lane: &str,
    records: &[String],
    input_limit: usize,
) -> Result<bool> {
    let authority = Authority::producer(binding, source)?;
    let args = crate::application::encode_rpc(&cellar_native::clone::StructuredValue::from_json(
        serde_json::json!([{"protocol":1,"binding":binding,"lane":lane,"records":records}]),
    )?)?;
    match crate::application::consensus::native_rpc::encode_event_request(
        Some(&cell_name(&binding.resource)?),
        &args,
        &authority,
        None,
        input_limit,
    ) {
        Ok(_) => Ok(true),
        Err(error)
            if error.downcast_ref::<crate::application::consensus::CommandFailure>()
                == Some(&crate::application::consensus::CommandFailure::Overloaded) =>
        {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

pub(crate) fn operation(definition: &Definition) -> Result<String> {
    let bytes = serde_json::to_vec(definition)?;
    Ok(hex::encode(&Sha256::digest(bytes)[..16]))
}

pub(crate) fn provision(
    context: &Context,
    definitions: &[Definition],
    catalog: bool,
    principal: &str,
) -> Result<()> {
    let scope = &context
        .identity()
        .ok_or_else(|| anyhow::anyhow!("missing event identity"))?
        .scope;
    for definition in definitions {
        let expected = if catalog {
            catalog_scope(&definition.resource)?
        } else {
            cell_scope(&definition.resource)?
        };
        if expected != *scope {
            continue;
        }
        let mut authority = context.clone();
        authority.event_authority = Some(Authority::deployment(principal, definition)?);
        if catalog {
            super::catalog::provision(&authority, definition, &operation(definition)?)?;
        } else if context
            .storage()?
            .system_sql("SELECT singleton FROM __sys_stream LIMIT 1", &[])?
            .is_empty()
        {
            use base64::Engine;
            store::initialize(
                &authority,
                &serde_json::json!({"operation":operation(definition)?,"definition":base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(definition)?)}),
            )?;
        }
    }
    Ok(())
}

pub(crate) async fn maintain(context: Context) -> Result<()> {
    let Some(installation) = crate::storage::work::raft_due(crate::asyncrt::wall_ms())? else {
        // An earlier unclaimed discovery hint can survive a client moving the
        // embedded deadline forward. Claiming it must preserve that future work.
        return crate::storage::work::raft_project();
    };
    use cellar_logic::work::Reason;
    match installation.reason {
        Reason::Outbox => {
            crate::application::outbox::dispatch(context, installation).await?;
        }
        Reason::StreamMaintenance => {
            super::broker::maintain(context, installation).await?;
        }
        Reason::ResourceReconciliation => {
            super::catalog::maintain(context, installation).await?;
        }
        _ => anyhow::bail!("unsupported shared event work"),
    }
    // A callback can replace another deadline. Project the surviving rows before
    // consuming the claimed native-work generation.
    crate::storage::work::raft_project()
}

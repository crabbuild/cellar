// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared-group bounded event transport and trusted resource provisioning.

use super::*;
use sha2::{Digest, Sha256};

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

// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared-group bounded event transport and trusted resource provisioning.

use super::*;
use sha2::{Digest, Sha256};

pub(crate) fn append_limit(binding: &Binding) -> Result<usize> {
    crate::storage::capture_raft_dispatch()?
        .ok_or(crate::application::consensus::CommandFailure::UnsupportedCapability)?
        .event_wire_limit(binding)
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
    match crate::application::consensus::native_event_rpc::encode(
        &cell_scope(&binding.resource)?,
        Some(&cell_name(&binding.resource)?),
        "__streamAppend",
        authority.into(),
        &args,
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

pub(crate) fn operation(principal: &str, definition: &Definition) -> Result<String> {
    let bytes = serde_json::to_vec(definition)?;
    let mut identity = Sha256::new();
    identity.update(b"cellar-event-deployment-operation-v1\0");
    identity.update(u32::try_from(principal.len())?.to_be_bytes());
    identity.update(principal.as_bytes());
    identity.update(bytes);
    Ok(hex::encode(&identity.finalize()[..16]))
}

/// A missing broker can be born for a validated deployment receipt, never for
/// an ordinary metadata reader or a malformed/conflicting operation identity.
pub(crate) fn validate_broker_genesis(
    scope: &str,
    caller: &Authority,
    body: &serde_json::Value,
) -> Result<()> {
    caller.require_provisioning()?;
    store::validate_authority(scope, caller, authority::Action::Configure)?;
    let encoded = body["definition"].as_str().ok_or_else(|| {
        DeliveryError::new(ErrorCode::InvalidRequest, "missing broker definition")
    })?;
    let (definition, _) = store::decode_definition(encoded)?;
    let supplied = body["operation"]
        .as_str()
        .ok_or_else(|| DeliveryError::new(ErrorCode::InvalidRequest, "missing broker operation"))?;
    store::validate_id(supplied)?;
    anyhow::ensure!(
        definition.resource.same_identity(&caller.resource)
            && cell_scope(&definition.resource)? == scope
            && operation(&caller.principal, &definition)? == supplied
            && Authority::deployment(&caller.principal, &definition)?.grant == caller.grant,
        DeliveryError::new(
            ErrorCode::ConfigConflict,
            "broker genesis differs from its deployment receipt"
        )
    );
    Ok(())
}

/// Install compiled resource state in its private creation transaction.
/// Existing broker rows, including retired incarnations, remain untouched.
pub(in crate::application) fn provision_storage(
    db: &crate::application::Storage,
    scope: &str,
    definitions: &[Definition],
    catalog: bool,
    principal: &str,
    now_ms: i64,
) -> Result<()> {
    use base64::Engine as _;
    if catalog {
        super::catalog::open(db)?;
    }
    for definition in definitions {
        let expected = if catalog {
            catalog_scope(&definition.resource)?
        } else {
            cell_scope(&definition.resource)?
        };
        if expected != scope {
            continue;
        }
        let caller = Authority::deployment(principal, definition)?;
        if catalog {
            super::catalog::provision_storage(
                db,
                &caller,
                definition,
                &operation(principal, definition)?,
                now_ms,
            )?;
        } else {
            super::schema::open(db)?;
            if db
                .system_sql("SELECT singleton FROM __sys_stream LIMIT 1", &[])?
                .is_empty()
            {
                let body = serde_json::json!({
                    "operation":operation(principal, definition)?,
                    "definition":base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(definition)?),
                });
                store::initialize_storage(db, scope, &caller, &body, || now_ms)?;
            }
        }
    }
    Ok(())
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
            super::catalog::provision(&authority, definition, &operation(principal, definition)?)?;
        } else if context
            .storage()?
            .system_sql("SELECT singleton FROM __sys_stream LIMIT 1", &[])?
            .is_empty()
        {
            use base64::Engine;
            store::initialize(
                &authority,
                &serde_json::json!({"operation":operation(principal, definition)?,"definition":base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(definition)?)}),
            )?;
        }
    }
    Ok(())
}

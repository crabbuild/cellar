// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::consensus::{CommandAdmission, RaftClass};

fn definition() -> event_stream::Definition {
    event_stream::Definition {
        resource: event_stream::Resource {
            tenant: "tenant".into(),
            application: "shop".into(),
            id: "00000000000000000000000000000001".into(),
            name: "orders".into(),
            family: "orders".into(),
        },
        revision: 1,
        retention: event_stream::Retention::Bounded,
        max_bytes: 4096,
        max_records: 10,
        max_age_ms: 1000,
        grants: vec![event_stream::Grant {
            id: "owner".into(),
            principal: "shop".into(),
            publish: true,
            read: true,
            subscriptions: vec![],
            administer: true,
        }],
        subscriptions: vec![],
    }
}

#[test]
fn private_event_request_retains_authority_and_global_name() -> Result<()> {
    let definition = definition();
    let authority = Authority::Event(event_stream::Authority::deployment("shop", &definition)?);
    let scope = event_stream::cell_scope(&definition.resource)?;
    let name = event_stream::cell_name(&definition.resource)?;
    let input = encode(
        &scope,
        Some(&name),
        "__streamConfigure",
        authority.clone(),
        &RpcData::Json("[{}]".into()),
        4096,
    )?;
    let decoded = decode(&scope, &input)?;
    assert_eq!(decoded.method, "__streamConfigure");
    assert_eq!(decoded.rpc.name.as_deref(), Some(name.as_str()));
    assert!(matches!(decoded.authority, Authority::Event(_)));
    assert!(native_rpc::decode_request(&input).is_err());
    let public = native_rpc::encode_request(Some(&name), &RpcData::Json("[{}]".into()), 4096)?;
    assert!(decode(&scope, &public).is_err());
    native_rpc::validate_identity(
        "a different producer",
        crate::deploy::EVENT_STREAM_CLASS,
        &scope,
        Some(&name),
    )?;
    assert!(native_rpc::validate_identity(
        "shop",
        crate::deploy::EVENT_CATALOG_CLASS,
        &scope,
        Some(&name)
    )
    .is_err());
    assert!(encode(
        &scope,
        None,
        "__streamConfigure",
        authority.clone(),
        &RpcData::Json("[{}]".into()),
        4096
    )
    .is_err());
    assert!(encode(
        &scope,
        Some("foreign"),
        "__streamConfigure",
        authority.clone(),
        &RpcData::Json("[{}]".into()),
        4096
    )
    .is_err());
    assert!(encode(
        &scope,
        Some(&name),
        "__streamAppend",
        authority.clone(),
        &RpcData::Json("[{}]".into()),
        4096
    )
    .is_err());
    assert!(encode(
        &scope,
        Some(&name),
        "__streamConfigure",
        authority,
        &RpcData::Json("[{}]".into()),
        input.len() - 1
    )
    .is_err());
    for end in 0..input.len() {
        assert!(decode(&scope, &input[..end]).is_err());
    }
    let mut trailing = input.clone();
    trailing.push(0);
    assert!(decode(&scope, &trailing).is_err());
    let mut foreign = definition.resource;
    foreign.id = "00000000000000000000000000000002".into();
    assert!(decode(&event_stream::cell_scope(&foreign)?, &input).is_err());
    Ok(())
}

#[test]
fn changed_event_provenance_changes_the_retained_fingerprint() -> Result<()> {
    let mut definition = definition();
    let scope = event_stream::catalog_scope(&definition.resource)?;
    let name = event_stream::catalog_name(&definition.resource)?;
    let first = encode(
        &scope,
        Some(&name),
        "__catalogConfigure",
        Authority::Event(event_stream::Authority::deployment("shop", &definition)?),
        &RpcData::Json("[{}]".into()),
        4096,
    )?;
    definition.grants[0].id = "new-owner".into();
    let second = encode(
        &scope,
        Some(&name),
        "__catalogConfigure",
        Authority::Event(event_stream::Authority::deployment("shop", &definition)?),
        &RpcData::Json("[{}]".into()),
        4096,
    )?;
    assert_ne!(
        super::super::command_request_digest(&first),
        super::super::command_request_digest(&second)
    );
    let mut oversized = first;
    oversized[5..9].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(decode(&scope, &oversized).is_err());
    Ok(())
}

#[test]
fn event_contracts_are_not_public_or_maintenance_capabilities() -> Result<()> {
    for catalog in [false, true] {
        let class = RaftClass::event_service(catalog)?;
        let contract = class.event_contract().unwrap();
        assert!(class.contract(OPERATION).is_none());
        assert!(class.system_contract(OPERATION).is_none());
        assert!(class
            .validate_command_admission(&contract, CommandAdmission::Public)
            .is_err());
        assert!(class
            .validate_command_admission(&contract, CommandAdmission::System)
            .is_err());
        class.validate_command_admission(&contract, CommandAdmission::Event)?;
        let mut changed = contract;
        changed.digest[0] ^= 1;
        assert!(class
            .validate_command_admission(&changed, CommandAdmission::Event)
            .is_err());
    }
    Ok(())
}

#[test]
fn event_service_factories_use_one_global_contract_and_keep_callbacks_private() -> Result<()> {
    for catalog in [false, true] {
        let class = super::super::native_services::event_service(catalog)?;
        let repeated = super::super::native_services::event_service(catalog)?;
        assert_eq!(class.schema_digest(), repeated.schema_digest());
        assert!(class.event_contract().is_some());
        assert!(class.has_native_callbacks());
        assert!(class.has_binding_maintenance());
        assert!(class.contract(OPERATION).is_none());
        assert!(class.system_contract(OPERATION).is_none());
        class.validate_shared_host(&Default::default())?;
    }
    Ok(())
}

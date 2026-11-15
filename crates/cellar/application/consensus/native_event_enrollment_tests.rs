// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::consensus::{AdmittedCellConfiguration, NativeCellConfiguration};

fn definition() -> events::Definition {
    events::Definition {
        resource: events::Resource {
            tenant: "tenant".into(),
            application: "shop".into(),
            id: "00000000000000000000000000000001".into(),
            name: "orders".into(),
            family: "orders".into(),
        },
        revision: 1,
        retention: events::Retention::Bounded,
        max_bytes: 4096,
        max_records: 10,
        max_age_ms: 1000,
        grants: vec![events::Grant {
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

fn request(
    definition: &events::Definition,
    principal: &str,
    operation: &str,
    nonce: u8,
) -> Result<Vec<u8>> {
    let args = crate::application::encode_rpc(&cellar_native::clone::StructuredValue::from_json(
        json!([{
            "operation":operation,
            "definition":base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(definition)?),
        }]),
    )?)?;
    native_event_rpc::encode_ingress(
        &events::catalog_scope(&definition.resource)?,
        Some(&events::catalog_name(&definition.resource)?),
        "__catalogConfigure",
        events::Authority::deployment(principal, definition)?.into(),
        &args,
        usize::MAX,
        [nonce; 16],
    )
}

#[test]
fn initial_intent_ignores_only_the_ingress_nonce() -> Result<()> {
    let definition = definition();
    let intent = CatalogEnrollment::new("shop", &definition)?;
    let operation = events::raft::operation("shop", &definition)?;
    let first = request(&definition, "shop", &operation, 2)?;
    let second = request(&definition, "shop", &operation, 3)?;
    assert_ne!(first, second);
    let a = fingerprint(&intent.scope, &first)?;
    assert_eq!(a, fingerprint(&intent.scope, &second)?);
    assert_eq!(a.0, definition.resource.id);
    let pending = intent.pending(a.1).expect("exact compiled initial intent");
    assert_eq!(pending["state"], "creating");
    assert_eq!(pending["resource"], definition.resource.id);
    assert_eq!(pending["operation"], operation);
    assert!(pending["accepted_revision"].is_null());
    assert!(pending["accepted_digest"].is_null());
    assert!(pending["last_error"].is_null());
    Ok(())
}

#[test]
fn changed_operation_definition_and_caller_cannot_join_initial_intent() -> Result<()> {
    let original = definition();
    let intent = CatalogEnrollment::new("shop", &original)?;
    let operation = events::raft::operation("shop", &original)?;
    let mut variants = Vec::new();
    variants.push(request(&original, "shop", &"a".repeat(32), 2)?);
    let mut changed = original.clone();
    changed.revision += 1;
    variants.push(request(&changed, "shop", &operation, 2)?);
    changed = original.clone();
    changed.grants[0].id = "different-grant".into();
    variants.push(request(&changed, "shop", &operation, 2)?);
    changed = original.clone();
    changed.grants[0].principal = "other".into();
    variants.push(request(&changed, "other", &operation, 2)?);
    changed = original.clone();
    changed.resource.id = "00000000000000000000000000000002".into();
    variants.push(request(&changed, "shop", &operation, 2)?);
    for wire in variants {
        assert!(intent
            .pending(fingerprint(&intent.scope, &wire)?.1)
            .is_none());
    }
    Ok(())
}

#[test]
fn malformed_or_foreign_catalog_wire_cannot_join() -> Result<()> {
    let definition = definition();
    let intent = CatalogEnrollment::new("shop", &definition)?;
    let input = request(
        &definition,
        "shop",
        &events::raft::operation("shop", &definition)?,
        2,
    )?;
    for end in 0..input.len() {
        if let Ok((_, digest)) = fingerprint(&intent.scope, &input[..end]) {
            assert!(intent.pending(digest).is_none());
        }
    }
    let mut trailing = input.clone();
    trailing.push(0);
    assert!(intent
        .pending(fingerprint(&intent.scope, &trailing)?.1)
        .is_none());
    let mut foreign = definition.resource;
    foreign.tenant = "other".into();
    assert!(fingerprint(&events::catalog_scope(&foreign)?, &input).is_err());
    Ok(())
}

#[test]
fn one_catalog_can_hold_multiple_enrollment_intents_with_bounded_registration() -> Result<()> {
    let first = definition();
    let mut second = first.clone();
    second.resource.id = "00000000000000000000000000000002".into();
    second.resource.name = "returns".into();
    let options = NativeCellConfiguration::default().options("shop");
    let configuration = AdmittedCellConfiguration::from_options(&options)?;
    let definitions = [first, second];
    let class = super::super::native_services::event_service(&configuration, &definitions, true)?;
    let intents = class
        .event_enrollments
        .as_ref()
        .expect("catalog descriptors");
    assert_eq!(intents.len(), 2);
    let mut charged = 0;
    for definition in &definitions {
        let wire = request(
            definition,
            "shop",
            &events::raft::operation("shop", definition)?,
            2,
        )?;
        let scope = events::catalog_scope(&definition.resource)?;
        let (id, digest) = fingerprint(&scope, &wire)?;
        let intent = intents
            .get(&(scope, id))
            .expect("same catalog, independent resource");
        assert!(intent.pending(digest).is_some());
        charged += intent.retained_bytes().unwrap();
    }
    assert_eq!(class.event_enrollment_bytes(), charged);
    let configured = class.configured_cell(&configuration);
    assert!(std::sync::Arc::ptr_eq(
        intents,
        configured.event_enrollments.as_ref().unwrap()
    ));
    assert_eq!(configured.event_enrollment_bytes(), charged);
    let broker = super::super::native_services::event_service(&configuration, &definitions, false)?;
    assert!(broker.event_enrollments.is_none());
    assert_eq!(broker.event_enrollment_bytes(), 0);
    assert!(super::super::native_services::event_service(
        &configuration,
        &[definitions[0].clone(), definitions[0].clone()],
        true
    )
    .is_err());
    Ok(())
}

use super::*;
use crate::engine_api::RpcData;
use cellar_native::clone::{self, Object};
use prost::Message;
use prost_types::{
    field_descriptor_proto::{Label, Type},
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    MethodDescriptorProto, ServiceDescriptorProto,
};
use std::sync::Arc;

fn descriptor() -> FileDescriptorSet {
    let field =
        |name: &str, number, kind: Type, repeated, reference: Option<&str>| FieldDescriptorProto {
            name: Some(name.into()),
            number: Some(number),
            r#type: Some(kind as i32),
            label: Some(if repeated {
                Label::Repeated
            } else {
                Label::Optional
            } as i32),
            type_name: reference.map(str::to_string),
            ..Default::default()
        };
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("counter.proto".into()),
            package: Some("test".into()),
            syntax: Some("proto3".into()),
            message_type: vec![DescriptorProto {
                name: Some("Value".into()),
                field: vec![
                    field("count", 1, Type::Int64, false, None),
                    field("values", 2, Type::Uint32, true, None),
                    field("child", 3, Type::Message, false, Some(".test.Value")),
                    field("text", 4, Type::String, false, None),
                    field("bytes", 5, Type::Bytes, false, None),
                ],
                ..Default::default()
            }],
            service: vec![ServiceDescriptorProto {
                name: Some("Counter".into()),
                method: ["Increment", "GetCount"]
                    .into_iter()
                    .map(|name| MethodDescriptorProto {
                        name: Some(name.into()),
                        input_type: Some(".test.Value".into()),
                        output_type: Some(".test.Value".into()),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn contract(limits: Limits) -> Contract {
    Contract::from_descriptor(&descriptor().encode_to_vec(), limits).unwrap()
}

#[test]
fn incoming_resource_exhaustion_is_distinct_from_malformed_protobuf() {
    for (limits, bytes) in [
        (
            Limits {
                max_message_bytes: 128,
                ..Default::default()
            },
            vec![0; 129],
        ),
        (
            Limits {
                max_depth: 1,
                ..Default::default()
            },
            vec![26, 2, 26, 0],
        ),
        (
            Limits {
                max_elements: 1,
                ..Default::default()
            },
            vec![8, 1, 8, 1],
        ),
        (
            Limits {
                max_decoded_slots: 5,
                ..Default::default()
            },
            vec![],
        ),
    ] {
        let failure = contract(limits)
            .validate_message("test.Value", &bytes)
            .unwrap_err();
        assert_eq!(failure.code(), Code::ResourceExhausted);
        assert_eq!(failure.outcome(), EffectOutcome::NotCommitted);
    }
    let failure = contract(Limits::default())
        .validate_message("test.Value", &[26, 2, 0])
        .unwrap_err();
    assert_eq!(failure.code(), Code::InvalidArgument);
    assert_eq!(failure.outcome(), EffectOutcome::NotCommitted);
}

#[test]
fn descriptor_digest_normalizes_source_locations() {
    let a = contract(Limits::default());
    let mut set = descriptor();
    set.file[0].source_code_info = Some(Default::default());
    let b = Contract::from_descriptor(&set.encode_to_vec(), Limits::default()).unwrap();
    assert_eq!(a.digest(), b.digest());
    assert_eq!(a.descriptor(), b.descriptor());
    assert_eq!(a.methods().count(), 2);
    assert_eq!(a.inventory()["digest"], a.digest());
}

#[test]
fn descriptor_rejects_unsupported_and_unresolved_features() {
    let mut cases = Vec::new();
    let mut set = descriptor();
    set.file[0].service[0].method[0].server_streaming = Some(true);
    cases.push(set);
    let mut set = descriptor();
    set.file[0].message_type[0].field[2].type_name = Some(".missing.Value".into());
    cases.push(set);
    let mut set = descriptor();
    set.file[0].syntax = Some("proto2".into());
    cases.push(set);
    let mut set = descriptor();
    set.file[0].dependency.push("missing.proto".into());
    cases.push(set);
    let mut set = descriptor();
    set.file[0].message_type[0].field[0].r#type = Some(Type::Group as i32);
    cases.push(set);
    let mut set = descriptor();
    let duplicate = set.file[0].service[0].method[0].clone();
    set.file[0].service[0].method.push(duplicate);
    cases.push(set);
    for set in cases {
        assert!(Contract::from_descriptor(&set.encode_to_vec(), Limits::default()).is_err());
    }
}

#[test]
fn bounded_wire_validation_preserves_empty_negative_and_unknown_fields() {
    let contract = contract(Limits::default());
    for bytes in [
        vec![],
        vec![8, 1],
        vec![8, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1],
        vec![0x7a, 2, 0xff, 0xff],
        vec![0x2a, 0],
    ] {
        contract.validate_message("test.Value", &bytes).unwrap();
    }
}

#[test]
fn bounded_wire_validation_refuses_truncation_invalid_utf8_and_overflow() {
    let contract = contract(Limits::default());
    for bytes in [
        vec![8],
        vec![0],
        vec![0x22, 1, 0xff],
        vec![0x2a, 3, 1],
        vec![0x0b],
        vec![8, 255, 255, 255, 255, 255, 255, 255, 255, 255, 2],
        vec![16, 128, 128, 128, 128, 16],
    ] {
        assert!(
            contract.validate_message("test.Value", &bytes).is_err(),
            "accepted {bytes:?}"
        );
    }
}

#[test]
fn duplicate_and_packed_occurrences_consume_element_budget() {
    let contract = contract(Limits {
        max_elements: 3,
        ..Default::default()
    });
    contract
        .validate_message("test.Value", &[8, 1, 8, 2, 8, 3])
        .unwrap();
    assert!(contract
        .validate_message("test.Value", &[8, 1, 8, 2, 8, 3, 8, 4])
        .is_err());
    contract
        .validate_message("test.Value", &[18, 2, 1, 2])
        .unwrap();
    assert!(contract
        .validate_message("test.Value", &[18, 3, 1, 2, 3])
        .is_err());
}

#[test]
fn nested_depth_and_message_byte_limits_are_enforced() {
    let contract = contract(Limits {
        max_depth: 1,
        max_message_bytes: 128,
        ..Default::default()
    });
    contract.validate_message("test.Value", &[26, 0]).unwrap();
    assert!(contract
        .validate_message("test.Value", &[26, 2, 26, 0])
        .is_err());
    assert!(contract.validate_message("test.Value", &[8; 129]).is_err());
}

#[test]
fn exact_binary_envelope_round_trips_empty_and_non_utf8_bytes() {
    let digest = "f".repeat(64);
    for bytes in [vec![], vec![0, 255, 128]] {
        let encoded = encode_envelope(&digest, bytes.clone(), 128).unwrap();
        assert_eq!(
            decode_envelope(&encoded, 128).unwrap(),
            (digest.clone(), bytes)
        );
    }
    assert!(encode_envelope("not-a-digest", vec![], 128).is_err());
    assert!(decode_envelope(&RpcData::Json("[]".into()), 128).is_err());
}

#[test]
fn clone_alias_properties_and_extra_nodes_are_refused() {
    let encoded = encode_envelope(&"a".repeat(64), vec![], 128).unwrap();
    let RpcData::Structured(bytes) = encoded else {
        unreachable!()
    };
    let graph = clone::decode(&bytes).unwrap();
    let mut extra = graph.clone();
    extra.objects.push(Object::Object(vec![]));
    // Unreachable nodes are omitted by the clone encoder. Make the extra node
    // reachable so this fixture actually changes the encoded argument graph.
    let Object::DenseArray { elements, .. } = &mut extra.objects[0] else {
        unreachable!()
    };
    elements.push(clone::Value::Reference(2));
    let mut property = graph.clone();
    let Object::DenseArray { properties, .. } = &mut property.objects[0] else {
        unreachable!()
    };
    properties.push(("extra".into(), "value".into()));
    let mut resizable = graph;
    let Object::ArrayBuffer { max_length, .. } = &mut resizable.objects[1] else {
        unreachable!()
    };
    *max_length = Some(128);
    for graph in [extra, property, resizable] {
        let data = RpcData::Structured(clone::encode(&graph).unwrap().into());
        assert!(decode_envelope(&data, 128).is_err());
    }
}

#[test]
fn prepared_rejects_mismatched_contract_before_execution() {
    let contract = Arc::new(contract(Limits::default()));
    let result = Prepared::new(contract, "test.Counter/Increment", "b".repeat(64), vec![]);
    let failure = result.err().unwrap();
    assert_eq!(failure.code(), Code::ContractMismatch);
    assert_eq!(failure.outcome(), EffectOutcome::NotCommitted);
}

#[test]
fn handler_failures_cannot_claim_nested_refusal_as_rollback() {
    let contract = Arc::new(contract(Limits::default()));
    let prepared = Prepared::new(
        contract.clone(),
        "test.Counter/Increment",
        contract.digest().into(),
        vec![],
    )
    .unwrap();
    let result = prepared
        .finish(Err(Failure::refused(Code::ResourceExhausted)))
        .unwrap();
    let failure = envelope::response(contract.digest(), &result, contract.limits()).unwrap_err();
    assert_eq!(failure.code(), Code::ResourceExhausted);
    assert_eq!(failure.outcome(), EffectOutcome::Unknown);
}

#[test]
fn empty_response_is_distinct_from_unset_result_and_invalid_response_is_unknown() {
    let contract = Arc::new(contract(Limits::default()));
    let prepared = Prepared::new(
        contract.clone(),
        "test.Counter/GetCount",
        contract.digest().into(),
        vec![],
    )
    .unwrap();
    let result = prepared.finish(Ok(vec![])).unwrap();
    assert_eq!(
        envelope::response(contract.digest(), &result, contract.limits()).unwrap(),
        Vec::<u8>::new()
    );
    let unset = encode_envelope(
        contract.digest(),
        vec![],
        contract.limits().max_message_bytes,
    )
    .unwrap();
    assert_eq!(
        envelope::response(contract.digest(), &unset, contract.limits())
            .unwrap_err()
            .code(),
        Code::InvalidResponse
    );
    let prepared = Prepared::new(
        contract.clone(),
        "test.Counter/GetCount",
        contract.digest().into(),
        vec![],
    )
    .unwrap();
    let result = prepared.finish(Ok(vec![8])).unwrap();
    let failure = envelope::response(contract.digest(), &result, contract.limits()).unwrap_err();
    assert_eq!(failure.code(), Code::InvalidResponse);
    assert_eq!(failure.outcome(), EffectOutcome::Unknown);
}

#[test]
fn native_service_registration_requires_complete_unique_methods() {
    let contract = Arc::new(contract(Limits::default()));
    let handler = |_, bytes| Box::pin(async move { Ok(bytes) }) as HandlerFuture<Vec<u8>>;
    let router = Router::new()
        .method(contract.clone(), "test.Counter/Increment", handler)
        .unwrap();
    assert!(router.validate().is_err());
    let router = router
        .method(contract.clone(), "test.Counter/GetCount", handler)
        .unwrap();
    router.validate().unwrap();
    assert!(router
        .method(contract, "test.Counter/Increment", handler)
        .is_err());
}

#[test]
fn arbitrary_short_wire_inputs_never_panic() {
    let contract = contract(Limits::default());
    for a in 0..=255_u8 {
        for b in [0, 1, 127, 128, 255] {
            let _ = contract.validate_message("test.Value", &[a, b]);
        }
    }
}

#[test]
fn framework_reply_occurrences_are_bounded_before_codec_allocation() {
    let limits = Limits {
        max_elements: 2,
        ..Default::default()
    };
    let bytes = [10, 0, 10, 0, 10, 0];
    let encoded = encode_envelope(&"a".repeat(64), bytes.to_vec(), limits.reply_limit()).unwrap();
    let failure = envelope::response(&"a".repeat(64), &encoded, limits).unwrap_err();
    assert_eq!(failure.code(), Code::InvalidResponse);
    assert_eq!(failure.outcome(), EffectOutcome::Unknown);
}

#[test]
fn framework_refusals_have_a_fixed_budget_independent_of_tiny_business_limits() {
    let limits = Limits {
        max_elements: 1,
        max_depth: 1,
        max_decoded_slots: 1,
        ..Default::default()
    };
    let digest = "a".repeat(64);
    let reply = Prepared::refusal(&digest, Failure::refused(Code::MethodNotFound), limits).unwrap();
    let failure = decode_reply(&digest, &reply, limits).unwrap_err();
    assert_eq!(failure.code(), Code::MethodNotFound);
    assert_eq!(failure.outcome(), EffectOutcome::NotCommitted);
    for bytes in [vec![10, 0, 16, 1], vec![18, 4, 8, 1, 8, 2]] {
        let reply = encode_envelope(&digest, bytes, limits.reply_limit()).unwrap();
        assert_eq!(
            decode_reply(&digest, &reply, limits).unwrap_err().code(),
            Code::InvalidResponse
        );
    }
}

#[test]
fn tiny_repeated_empty_messages_are_refused_before_decoder_default_layout_amplifies() {
    let mut set = descriptor();
    let wide = DescriptorProto {
        name: Some("Wide".into()),
        field: (1..=512)
            .map(|number| FieldDescriptorProto {
                name: Some(format!("slot_{number}")),
                number: Some(number),
                r#type: Some(Type::String as i32),
                label: Some(Label::Repeated as i32),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let batch = DescriptorProto {
        name: Some("Batch".into()),
        field: vec![FieldDescriptorProto {
            name: Some("items".into()),
            number: Some(1),
            r#type: Some(Type::Message as i32),
            label: Some(Label::Repeated as i32),
            type_name: Some(".test.Wide".into()),
            ..Default::default()
        }],
        ..Default::default()
    };
    set.file[0].message_type.extend([wide, batch]);
    // Only 4 KiB of valid bytes / 2,000 wire occurrences. A decoder would
    // initialize more than a million declared fields and empty containers.
    let bytes = [10, 0].repeat(2000);
    let checked = Contract::from_descriptor(&set.encode_to_vec(), Limits::default()).unwrap();
    let failure = checked.validate_message("test.Batch", &bytes).unwrap_err();
    assert_eq!(failure.code(), Code::ResourceExhausted);
    assert_eq!(failure.outcome(), EffectOutcome::NotCommitted);
    let exact = 2 + 2000 * 514;
    let allowed = Contract::from_descriptor(
        &set.encode_to_vec(),
        Limits {
            max_decoded_slots: exact,
            ..Default::default()
        },
    )
    .unwrap();
    allowed.validate_message("test.Batch", &bytes).unwrap();
}

#[test]
fn effect_outcomes_match_existing_guest_sdk_spelling() {
    assert_eq!(
        serde_json::to_value(EffectOutcome::Unknown).unwrap(),
        "unknown"
    );
    assert_eq!(
        serde_json::to_value(EffectOutcome::NotCommitted).unwrap(),
        "not_committed"
    );
}

#[test]
fn catalog_refuses_unknown_methods_and_digests_before_factories() {
    let contract = Arc::new(contract(Limits::default()));
    let mut catalog = Catalog::new();
    catalog
        .register("Counter", "test.Counter", contract.clone())
        .unwrap();
    assert!(catalog
        .register("Counter", "test.Counter", contract.clone())
        .is_err());
    let args = encode_envelope(contract.digest(), vec![], 1024).unwrap();
    assert!(catalog
        .refusal("Counter", "test.Counter/GetCount", &args)
        .unwrap()
        .is_none());
    let response = catalog
        .refusal("Counter", "test.Counter/private", &args)
        .unwrap()
        .unwrap();
    let failure = decode_reply(contract.digest(), &response, contract.limits()).unwrap_err();
    assert_eq!(failure.code(), Code::MethodNotFound);
    assert_eq!(failure.outcome(), EffectOutcome::NotCommitted);
    assert!(catalog
        .refusal("Untyped", "anything", &args)
        .unwrap()
        .is_none());
    assert_eq!(
        catalog.inventory()["Counter"]["test.Counter/GetCount"]["input"],
        "test.Value"
    );
    assert!(catalog.validate_bridge(128).is_err());
}

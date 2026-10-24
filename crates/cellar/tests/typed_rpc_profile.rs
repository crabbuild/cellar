// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Compile generated portable-profile services using only Cellar's SDK exports.
use cellar::sdk::{rpc, Context};
use rpc::prost::Message;
use std::sync::Arc;

#[allow(dead_code)]
#[path = "../../../tools/rpc/fixtures/profile-generated/rust/cellar.rpc.conformance.v1.rs"]
mod generated;
use generated::{all_types, AllTypes, ProfileHandler, ProfileRegistration, RuntimeMappings};

struct Echo;

impl ProfileHandler for Echo {
    fn echo_mappings(
        self: Arc<Self>,
        _: Context,
        request: RuntimeMappings,
    ) -> rpc::HandlerFuture<RuntimeMappings> {
        Box::pin(async move { Ok(request) })
    }

    fn echo(self: Arc<Self>, _: Context, request: AllTypes) -> rpc::HandlerFuture<AllTypes> {
        Box::pin(async move { Ok(request) })
    }

    fn ping(self: Arc<Self>, _: Context, _: ()) -> rpc::HandlerFuture<()> {
        Box::pin(async move { Ok(()) })
    }

    fn echo_child(
        self: Arc<Self>,
        _: Context,
        request: all_types::Child,
    ) -> rpc::HandlerFuture<all_types::Child> {
        Box::pin(async move { Ok(request) })
    }

    fn echo_timestamp(
        self: Arc<Self>,
        _: Context,
        request: rpc::prost_types::Timestamp,
    ) -> rpc::HandlerFuture<rpc::prost_types::Timestamp> {
        Box::pin(async move { Ok(request) })
    }

    fn echo_duration(
        self: Arc<Self>,
        _: Context,
        request: rpc::prost_types::Duration,
    ) -> rpc::HandlerFuture<rpc::prost_types::Duration> {
        Box::pin(async move { Ok(request) })
    }
}

#[test]
fn generated_profile_registers_empty_and_recursive_well_known_messages() {
    ProfileRegistration::register(rpc::Router::new(), Arc::new(Echo)).unwrap();
    <() as Message>::decode(&b""[..]).unwrap();
    assert!(().encode_to_vec().is_empty());
}

#[test]
fn native_profile_preserves_scalar_boundaries_maps_presence_and_oneof() {
    let request = AllTypes {
        signed32: i32::MIN,
        unsigned32: u32::MAX,
        zigzag32: i32::MIN,
        fixed_unsigned32: u32::MAX,
        fixed_signed32: i32::MIN,
        signed64: i64::MIN,
        unsigned64: u64::MAX,
        zigzag64: i64::MIN,
        fixed_unsigned64: u64::MAX,
        fixed_signed64: i64::MIN,
        float32: f32::INFINITY,
        float64: f64::NEG_INFINITY,
        enabled: true,
        text: "\u{feff}😀\0".into(),
        data: vec![0, 255, 128],
        state: 777, // Unknown enum numbers are valid proto3 values.
        present: Some(0),
        choice: Some(all_types::Choice::SelectedText(String::new())),
        packed: vec![i32::MIN, -1, 0, i32::MAX],
        unpacked: vec![i32::MIN, -1, 0, i32::MAX],
        by_id: [(i64::MIN, true), (i64::MAX, false)].into(),
        timestamp: Some(rpc::prost_types::Timestamp {
            seconds: -1,
            nanos: 1,
        }),
        duration: Some(rpc::prost_types::Duration {
            seconds: -1,
            nanos: -1,
        }),
        ..Default::default()
    };
    let bytes = request.encode_to_vec();
    let contract = rpc::Contract::from_descriptor(
        include_bytes!("../../../tools/rpc/fixtures/profile.binpb"),
        rpc::Limits::default(),
    )
    .unwrap();
    contract
        .validate_message("cellar.rpc.conformance.v1.AllTypes", &bytes)
        .unwrap();
    let response = AllTypes::decode(bytes.as_slice()).unwrap();
    assert_eq!(response, request);
    let nan = AllTypes {
        float32: f32::NAN,
        float64: f64::NAN,
        ..Default::default()
    };
    let bytes = nan.encode_to_vec();
    contract
        .validate_message("cellar.rpc.conformance.v1.AllTypes", &bytes)
        .unwrap();
    let response = AllTypes::decode(bytes.as_slice()).unwrap();
    assert!(response.float32.is_nan() && response.float64.is_nan());
}

#[test]
fn runtime_mapping_vectors_preserve_wire_values_and_native_preflight_budgets() {
    let vectors: Vec<serde_json::Value> = serde_json::from_str(include_str!(
        "../../../tools/rpc/fixtures/message-vectors.json"
    ))
    .unwrap();
    assert_eq!(vectors.len(), 3);
    for vector in vectors {
        let bytes: Vec<u8> = serde_json::from_value(vector["bytes"].clone()).unwrap();
        let limits = rpc::Limits {
            max_depth: serde_json::from_value(vector["max_depth"].clone()).unwrap(),
            max_elements: serde_json::from_value(vector["max_elements"].clone()).unwrap(),
            max_decoded_slots: serde_json::from_value(vector["max_decoded_slots"].clone()).unwrap(),
            ..Default::default()
        };
        let validate = |limits| {
            rpc::Contract::from_descriptor(
                include_bytes!("../../../tools/rpc/fixtures/profile.binpb"),
                limits,
            )
            .unwrap()
            .validate_message("cellar.rpc.conformance.v1.RuntimeMappings", &bytes)
        };
        validate(limits).unwrap();
        assert!(validate(rpc::Limits {
            max_decoded_slots: limits.max_decoded_slots - 1,
            ..limits
        })
        .is_err());
        if limits.max_depth > 1 {
            assert!(validate(rpc::Limits {
                max_depth: limits.max_depth - 1,
                ..limits
            })
            .is_err());
        }
        if limits.max_elements > 1 {
            assert!(validate(rpc::Limits {
                max_elements: limits.max_elements - 1,
                ..limits
            })
            .is_err());
        }
        let decoded = RuntimeMappings::decode(bytes.as_slice()).unwrap();
        assert_eq!(rpc::validate_graph(&decoded, limits).unwrap(), bytes.len());
        match vector["name"].as_str().unwrap() {
            "present-wrapper-zero" => assert_eq!(decoded.wrapped64, Some(0)),
            "wrapper-map" => assert_eq!(decoded.wrapper_map.get(&true), Some(&1)),
            "struct-number" => assert_eq!(
                decoded.object.as_ref().unwrap().fields["x"].kind,
                Some(rpc::prost_types::value::Kind::NumberValue(7.0))
            ),
            name => panic!("unknown runtime mapping vector: {name}"),
        }
        assert_eq!(decoded.encode_to_vec(), bytes);
    }
}

#[test]
fn generated_rust_guards_exact_wire_budgets_before_allocating_encoders() {
    let limits = rpc::Limits {
        max_message_bytes: 128,
        max_elements: 4,
        ..Default::default()
    };
    let packed = AllTypes {
        packed: vec![-1, 0, 1],
        ..Default::default()
    };
    assert_eq!(
        rpc::validate_graph(&packed, limits).unwrap(),
        packed.encoded_len()
    );
    assert!(rpc::validate_graph(
        &packed,
        rpc::Limits {
            max_elements: 3,
            ..limits
        }
    )
    .is_err());
    let unpacked = AllTypes {
        unpacked: vec![-1, 0, 1],
        ..Default::default()
    };
    rpc::validate_graph(
        &unpacked,
        rpc::Limits {
            max_elements: 3,
            ..limits
        },
    )
    .unwrap();
    for request in [
        AllTypes {
            data: vec![0; 128],
            ..Default::default()
        },
        AllTypes {
            unpacked: vec![-1; 13],
            ..Default::default()
        },
        AllTypes {
            by_name: [("k".repeat(125), all_types::Child::default())].into(),
            ..Default::default()
        },
    ] {
        assert_eq!(
            rpc::encode_message(
                request,
                rpc::Limits {
                    max_elements: 100,
                    ..limits
                }
            )
            .unwrap_err()
            .code(),
            rpc::Code::ResourceExhausted
        );
    }
}

#[test]
fn very_deep_generated_and_well_known_values_are_refused_and_dropped_iteratively() {
    std::thread::Builder::new()
        .stack_size(256 * 1024)
        .spawn(|| {
            let mut request = AllTypes::default();
            for _ in 0..20_000 {
                request = AllTypes {
                    children: vec![all_types::Child {
                        name: String::new(),
                        recursive: Some(Box::new(request)),
                    }],
                    ..Default::default()
                };
            }
            assert_eq!(
                rpc::encode_message(request, rpc::Limits::default())
                    .unwrap_err()
                    .code(),
                rpc::Code::ResourceExhausted
            );
            let mut value = rpc::prost_types::Value::default();
            for _ in 0..20_000 {
                value = rpc::prost_types::Value {
                    kind: Some(rpc::prost_types::value::Kind::ListValue(
                        rpc::prost_types::ListValue {
                            values: vec![value],
                        },
                    )),
                };
            }
            assert_eq!(
                rpc::encode_message(value, rpc::Limits::default())
                    .unwrap_err()
                    .code(),
                rpc::Code::ResourceExhausted
            );
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn default_map_entries_charge_only_the_fields_prost_actually_writes() {
    let message = RuntimeMappings {
        wrapper_map: [(false, 0)].into(),
        ..Default::default()
    };
    let limits = rpc::Limits {
        max_depth: 1,
        max_elements: 1,
        max_decoded_slots: 25,
        ..Default::default()
    };
    assert_eq!(rpc::encode_message(message, limits).unwrap(), vec![42, 0]);
    let contract = rpc::Contract::from_descriptor(
        include_bytes!("../../../tools/rpc/fixtures/profile.binpb"),
        limits,
    )
    .unwrap();
    contract
        .validate_message("cellar.rpc.conformance.v1.RuntimeMappings", &[42, 0])
        .unwrap();
    let contract = rpc::Contract::from_descriptor(
        include_bytes!("../../../tools/rpc/fixtures/profile.binpb"),
        rpc::Limits {
            max_decoded_slots: 24,
            ..limits
        },
    )
    .unwrap();
    assert_eq!(
        contract
            .validate_message("cellar.rpc.conformance.v1.RuntimeMappings", &[42, 0])
            .unwrap_err()
            .code(),
        rpc::Code::ResourceExhausted
    );
    let message = AllTypes {
        present: Some(0),
        choice: Some(all_types::Choice::SelectedText(String::new())),
        ..Default::default()
    };
    rpc::validate_graph(
        &message,
        rpc::Limits {
            max_elements: 2,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(rpc::validate_graph(
        &message,
        rpc::Limits {
            max_elements: 1,
            ..Default::default()
        }
    )
    .is_err());
}

#[test]
fn scalar_only_oneofs_encode_selected_defaults_without_recursive_children() {
    for (result, expected) in [
        (generated::scalar_choice::Result::Number(0), vec![8, 0]),
        (generated::scalar_choice::Result::Flag(false), vec![16, 0]),
    ] {
        assert_eq!(
            rpc::encode_message(
                generated::ScalarChoice {
                    result: Some(result)
                },
                rpc::Limits {
                    max_elements: 1,
                    ..Default::default()
                }
            )
            .unwrap(),
            expected
        );
    }
}

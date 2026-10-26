// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use crate::application::rpc::{Contract, Limits};
use prost::Message;
use prost_types::{
    field_descriptor_proto::{Label, Type},
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto, FileDescriptorSet, MethodDescriptorProto, OneofDescriptorProto,
    ServiceDescriptorProto, UninterpretedOption,
};

fn fixture() -> FileDescriptorSet {
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("service.proto".into()),
            package: Some("test".into()),
            syntax: Some("proto3".into()),
            message_type: vec![DescriptorProto {
                name: Some("Value".into()),
                field: vec![FieldDescriptorProto {
                    name: Some("text".into()),
                    number: Some(1),
                    r#type: Some(Type::String as i32),
                    label: Some(Label::Optional as i32),
                    oneof_index: Some(0),
                    ..Default::default()
                }],
                oneof_decl: vec![OneofDescriptorProto {
                    name: Some("choice".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            enum_type: vec![EnumDescriptorProto {
                name: Some("State".into()),
                value: vec![EnumValueDescriptorProto {
                    name: Some("ZERO".into()),
                    number: Some(0),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            service: vec![ServiceDescriptorProto {
                name: Some("Counter".into()),
                method: vec![MethodDescriptorProto {
                    name: Some("Read".into()),
                    input_type: Some(".test.Value".into()),
                    output_type: Some(".test.Value".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn reject(bytes: &[u8], expected: &str) {
    let error = Contract::from_descriptor(bytes, Limits::default()).unwrap_err();
    assert!(
        error.to_string().contains(expected),
        "unexpected refusal: {error:#}"
    );
}

fn nested(tag: u32, body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    prost::encoding::encode_key(tag, prost::encoding::WireType::LengthDelimited, &mut bytes);
    prost::encoding::encode_varint(body.len() as u64, &mut bytes);
    bytes.extend_from_slice(body);
    bytes
}

#[test]
fn file_count_is_bounded_before_decoding_a_later_malformed_file() {
    let mut bytes = [0x0a, 0].repeat(257);
    bytes.extend(nested(1, &[0]));
    reject(&bytes, "RPC descriptor file count");
}

#[test]
fn field_count_is_bounded_before_decoding_a_later_malformed_member() {
    let mut message = [0x12, 0].repeat(65_537);
    message.push(0);
    reject(&nested(1, &nested(4, &message)), "too many RPC fields");
}

#[test]
fn metadata_nodes_are_bounded_before_allocating_source_locations() {
    let mut locations = [0x0a, 0].repeat(200_001);
    locations.push(0);
    reject(
        &nested(1, &nested(9, &locations)),
        "RPC descriptor node limit",
    );
}

#[test]
fn scalar_occurrences_have_a_work_budget_before_descriptor_decoding() {
    let mut file = [0x0a, 0].repeat(1_000_001);
    file.push(0);
    reject(&nested(1, &file), "RPC descriptor wire-field limit");
}

#[test]
fn message_declaration_depth_matches_the_pinned_compiler() {
    for levels in [31, 32] {
        let mut set = fixture();
        let mut value = &mut set.file[0].message_type[0];
        for _ in 1..levels {
            value.nested_type.push(DescriptorProto {
                name: Some("Child".into()),
                ..Default::default()
            });
            value = &mut value.nested_type[0];
        }
        let bytes = set.encode_to_vec();
        if levels == 31 {
            Contract::from_descriptor(&bytes, Limits::default()).unwrap();
        } else {
            reject(&bytes, "RPC descriptor nesting exceeds limit");
        }
    }
}

#[test]
fn unknown_descriptor_attributes_are_not_silently_normalized_away() {
    let set = fixture();
    let mut file = set.file[0].encode_to_vec();
    // FileDescriptorProto.edition is absent from the pinned descriptor model.
    prost::encoding::encode_key(14, prost::encoding::WireType::Varint, &mut file);
    prost::encoding::encode_varint(1000, &mut file);
    reject(&nested(1, &file), "unsupported RPC descriptor field");

    // A future option can affect parsing or generated APIs. Refuse it instead
    // of hashing a descriptor from which prost has discarded that option.
    let mut file = set.file[0].encode_to_vec();
    file.extend(nested(8, &nested(50, &[])));
    reject(&nested(1, &file), "unsupported RPC descriptor field");

    let mut set = fixture();
    let value = &mut set.file[0].message_type[0];
    let mut field = value.field.remove(0).encode_to_vec();
    // FieldOptions.unverified_lazy is not represented by prost-types 0.14.
    field.extend(nested(8, &[0x78, 1]));
    let mut value = value.encode_to_vec();
    value.extend(nested(2, &field));
    set.file[0].message_type.clear();
    let mut file = set.file[0].encode_to_vec();
    file.extend(nested(4, &value));
    reject(&nested(1, &file), "unsupported RPC descriptor field 15");
}

#[test]
fn malformed_descriptor_wire_is_refused_without_panics() {
    for bytes in [
        vec![0],
        vec![0x0a, 0x80],
        vec![0x0a, 2, 0x0a],
        vec![8, 1],
        vec![0x0d, 0, 0, 0, 0],
        vec![0x09, 0, 0, 0, 0, 0, 0, 0, 0],
        vec![0x0b, 0x0c],
        vec![0xff; 10],
    ] {
        assert!(
            Contract::from_descriptor(&bytes, Limits::default()).is_err(),
            "admitted malformed descriptor: {bytes:?}"
        );
    }
}

#[test]
fn supported_source_locations_are_validated_then_removed_from_identity() {
    let mut set = fixture();
    let expected = set.encode_to_vec();
    set.file[0].source_code_info = Some(prost_types::SourceCodeInfo {
        location: vec![prost_types::source_code_info::Location {
            path: vec![4, 0, 2, 0],
            span: vec![0, 0, 2],
            leading_comments: Some("Unicode source comment: ☃".into()),
            trailing_comments: Some("trailing comment".into()),
            leading_detached_comments: vec!["detached comment".into()],
        }],
    });
    assert_eq!(
        Contract::from_descriptor(&set.encode_to_vec(), Limits::default())
            .unwrap()
            .descriptor(),
        expected
    );
}

#[test]
fn unresolved_options_are_refused_at_every_descriptor_scope() {
    for scope in 0..8 {
        let mut set = fixture();
        let file = &mut set.file[0];
        let option = UninterpretedOption {
            name: vec![prost_types::uninterpreted_option::NamePart {
                name_part: "deprecated".into(),
                is_extension: false,
            }],
            identifier_value: Some("true".into()),
            ..Default::default()
        };
        match scope {
            0 => file
                .options
                .get_or_insert_default()
                .uninterpreted_option
                .push(option),
            1 => file.message_type[0]
                .options
                .get_or_insert_default()
                .uninterpreted_option
                .push(option),
            2 => file.message_type[0].field[0]
                .options
                .get_or_insert_default()
                .uninterpreted_option
                .push(option),
            3 => file.message_type[0].oneof_decl[0]
                .options
                .get_or_insert_default()
                .uninterpreted_option
                .push(option),
            4 => file.enum_type[0]
                .options
                .get_or_insert_default()
                .uninterpreted_option
                .push(option),
            5 => file.enum_type[0].value[0]
                .options
                .get_or_insert_default()
                .uninterpreted_option
                .push(option),
            6 => file.service[0]
                .options
                .get_or_insert_default()
                .uninterpreted_option
                .push(option),
            7 => file.service[0].method[0]
                .options
                .get_or_insert_default()
                .uninterpreted_option
                .push(option),
            _ => unreachable!(),
        }
        reject(&set.encode_to_vec(), "unresolved RPC descriptor options");
    }
}

#[test]
fn proto3_message_set_and_scalar_lazy_options_are_refused() {
    let mut set = fixture();
    set.file[0].message_type[0]
        .options
        .get_or_insert_default()
        .message_set_wire_format = Some(true);
    reject(&set.encode_to_vec(), "RPC MessageSet");

    let mut set = fixture();
    set.file[0].message_type[0].field[0]
        .options
        .get_or_insert_default()
        .lazy = Some(true);
    reject(&set.encode_to_vec(), "lazy RPC option");
}

#[test]
fn supported_metadata_and_message_lazy_hints_keep_their_identity() {
    let mut set = fixture();
    let file = &mut set.file[0];
    file.options.get_or_insert_default().go_package = Some("example.test/rpc".into());
    file.message_type[0]
        .options
        .get_or_insert_default()
        .message_set_wire_format = Some(false);
    let field = &mut file.message_type[0].field[0];
    field.options.get_or_insert_default().lazy = Some(false);
    field.options.get_or_insert_default().jstype = Some(0);
    field.options.get_or_insert_default().weak = Some(true);
    let original = set.encode_to_vec();
    assert_eq!(
        Contract::from_descriptor(&original, Limits::default())
            .unwrap()
            .descriptor(),
        original
    );

    for label in [Label::Optional, Label::Repeated] {
        let mut set = fixture();
        let value = &mut set.file[0].message_type[0];
        value.oneof_decl.clear();
        let field = &mut value.field[0];
        field.oneof_index = None;
        field.label = Some(label as i32);
        field.r#type = Some(Type::Message as i32);
        field.type_name = Some(".test.Value".into());
        field.options.get_or_insert_default().lazy = Some(true);
        let original = set.encode_to_vec();
        let contract = Contract::from_descriptor(&original, Limits::default()).unwrap();
        assert_eq!(contract.descriptor(), original);
        assert!(
            contract
                .validate_message("test.Value", &[0x0a, 1, 0])
                .is_err(),
            "lazy hint bypassed nested wire validation"
        );
    }
}

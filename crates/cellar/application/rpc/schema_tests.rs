// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::rpc::{Contract, Limits};
use prost::Message;
use prost_types::{
    descriptor_proto::ReservedRange, enum_descriptor_proto::EnumReservedRange, EnumOptions,
    EnumValueDescriptorProto, FileDescriptorSet, OneofDescriptorProto,
};

fn fixture() -> FileDescriptorSet {
    FileDescriptorSet::decode(&include_bytes!("../../../../tools/rpc/fixtures/profile.binpb")[..])
        .unwrap()
}

fn message(set: &mut FileDescriptorSet) -> &mut DescriptorProto {
    &mut set
        .file
        .iter_mut()
        .find(|file| file.package.as_deref() == Some("cellar.rpc.conformance.v1"))
        .unwrap()
        .message_type[0]
}

fn rejected(mutate: impl FnOnce(&mut FileDescriptorSet)) {
    let mut set = fixture();
    mutate(&mut set);
    assert!(Contract::from_descriptor(&set.encode_to_vec(), Limits::default()).is_err());
}

#[test]
fn protoc_profile_preserves_maps_optional_oneofs_and_well_known_messages() {
    let contract =
        Contract::from_descriptor(&fixture().encode_to_vec(), Limits::default()).unwrap();
    assert!(contract
        .method("cellar.rpc.conformance.v1.Profile/Echo")
        .is_some());
    // A map entry has a key and a nested value, both descriptor-validated.
    contract
        .validate_message(
            "cellar.rpc.conformance.v1.AllTypes",
            &[0xba, 1, 8, 0x0a, 1, b'x', 0x12, 3, 0x0a, 1, b'y'],
        )
        .unwrap();
    // Presence of zero survives wire admission; it is not an absent optional.
    contract
        .validate_message("cellar.rpc.conformance.v1.AllTypes", &[0x88, 1, 0])
        .unwrap();
}

#[test]
fn optional_oneofs_refuse_multiple_members_bad_indexes_and_wrong_order() {
    rejected(|set| {
        message(set).field[0].proto3_optional = Some(true);
    });
    rejected(|set| {
        message(set).field[0].oneof_index = Some(-1);
    });
    rejected(|set| {
        message(set).field[0].oneof_index = Some(99);
    });
    rejected(|set| {
        message(set).field[0].oneof_index = Some(1);
    });
    rejected(|set| {
        message(set).field[16].label = Some(Label::Repeated as i32);
    });
    rejected(|set| {
        message(set).oneof_decl.push(OneofDescriptorProto {
            name: Some("empty".into()),
            ..Default::default()
        });
    });
    rejected(|set| {
        let message = message(set);
        message.oneof_decl.swap(0, 1);
        for field in &mut message.field {
            field.oneof_index = field.oneof_index.map(|index| 1 - index);
        }
    });
}

#[test]
fn maps_refuse_illegal_keys_entry_shapes_and_cross_owner_references() {
    for key in [
        Type::Bytes,
        Type::Double,
        Type::Float,
        Type::Enum,
        Type::Message,
    ] {
        rejected(|set| {
            message(set).nested_type[1].field[0].r#type = Some(key as i32);
        });
    }
    rejected(|set| {
        message(set).field[22].label = Some(Label::Optional as i32);
    });
    rejected(|set| {
        message(set).nested_type[1].field[0].name = Some("wrong".into());
    });
    rejected(|set| {
        message(set).nested_type[1].field[1].number = Some(3);
    });
    rejected(|set| {
        message(set).nested_type[1].field[1].proto3_optional = Some(true);
    });
    rejected(|set| {
        message(set).nested_type[1].field.swap(0, 1);
    });
    rejected(|set| {
        message(set).nested_type[1].field.pop();
    });
    rejected(|set| {
        message(set).nested_type[1]
            .nested_type
            .push(DescriptorProto {
                name: Some("Invalid".into()),
                ..Default::default()
            });
    });
    rejected(|set| {
        message(set).field[22].name = Some("renamed".into());
    });
    rejected(|set| {
        message(set).field[22].type_name =
            Some(".cellar.rpc.conformance.v1.AllTypes.By_IDEntry".into());
    });
    rejected(|set| {
        let field = message(set).field.remove(22);
        message(set).nested_type[0].field.push(field);
    });
    rejected(|set| {
        message(set).field.remove(22);
    });
    rejected(|set| {
        let file = set
            .file
            .iter_mut()
            .find(|file| !file.service.is_empty())
            .unwrap();
        file.service[0].method[0].input_type =
            Some(".cellar.rpc.conformance.v1.AllTypes.ByNameEntry".into());
    });
}

#[test]
fn enum_definitions_require_valid_zero_and_explicit_aliases() {
    rejected(|set| {
        message(set).enum_type[0].value.clear();
    });
    rejected(|set| {
        message(set).enum_type[0].value[0].number = Some(1);
    });
    rejected(|set| {
        message(set).enum_type[0].value[1].number = Some(0);
    });
    rejected(|set| {
        message(set).enum_type[0].value[1].name = Some("STATE_UNSPECIFIED".into());
    });
    rejected(|set| {
        message(set).enum_type[0].value[1].name = Some("not.valid".into());
    });
    let mut set = fixture();
    let definition = &mut message(&mut set).enum_type[0];
    definition.options = Some(EnumOptions {
        allow_alias: Some(true),
        ..Default::default()
    });
    definition.value.push(EnumValueDescriptorProto {
        name: Some("STATE_ALIAS".into()),
        number: Some(1),
        ..Default::default()
    });
    Contract::from_descriptor(&set.encode_to_vec(), Limits::default()).unwrap();
}

#[test]
fn reserved_names_ranges_and_packing_cannot_change_message_meaning() {
    for jstype in [1, 2] {
        rejected(|set| {
            message(set).field[5].options.get_or_insert_default().jstype = Some(jstype);
        });
    }
    rejected(|set| {
        message(set).reserved_name.push("text".into());
    });
    rejected(|set| {
        message(set).reserved_range.push(ReservedRange {
            start: Some(1),
            end: Some(2),
        });
    });
    rejected(|set| {
        message(set).reserved_range.extend([
            ReservedRange {
                start: Some(100),
                end: Some(103),
            },
            ReservedRange {
                start: Some(102),
                end: Some(105),
            },
        ]);
    });
    rejected(|set| {
        message(set).enum_type[0]
            .reserved_range
            .push(EnumReservedRange {
                start: Some(1),
                end: Some(1),
            });
    });
    rejected(|set| {
        message(set).enum_type[0]
            .reserved_name
            .push("STATE_READY".into());
    });
    rejected(|set| {
        message(set).field[0].type_name = Some(".not.a.message".into());
    });
    rejected(|set| {
        message(set).field[13]
            .options
            .get_or_insert_default()
            .packed = Some(true);
    });
}

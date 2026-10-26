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

fn json_message(fields: &[(&str, Option<&str>)]) -> DescriptorProto {
    DescriptorProto {
        name: Some("JsonNames".into()),
        field: fields
            .iter()
            .enumerate()
            .map(|(index, (name, json_name))| FieldDescriptorProto {
                name: Some((*name).into()),
                number: Some(i32::try_from(index + 1).unwrap()),
                label: Some(Label::Optional as i32),
                r#type: Some(Type::String as i32),
                json_name: json_name.map(str::to_owned),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn reject_json_names(fields: &[(&str, Option<&str>)], expected: &str) {
    let mut set = fixture();
    // Nested declarations are checked even if no method references them.
    message(&mut set).nested_type.push(json_message(fields));
    let result = Contract::from_descriptor(&set.encode_to_vec(), Limits::default());
    assert!(result.is_err(), "admitted JSON name conflict: {fields:?}");
    let error = format!("{:#}", result.unwrap_err());
    assert!(error.contains(expected), "unexpected refusal: {error}");
}

#[test]
fn json_field_names_require_unique_defaults_even_with_custom_overrides() {
    for fields in [
        [("foo_bar", None), ("fooBar", None)],
        [("foo_bar", None), ("foo__bar", None)],
        [("_", None), ("__", None)],
        [("foo_bar", Some("first")), ("fooBar", Some("second"))],
    ] {
        reject_json_names(&fields, "duplicate default RPC JSON field name");
    }
}

#[test]
fn json_field_names_require_unique_effective_custom_names() {
    for fields in [
        [("first", Some("same")), ("second", Some("same"))],
        [("first", Some("second")), ("second", None)],
        [("first", None), ("second", Some("first"))],
        [("first", Some("")), ("second", Some(""))],
    ] {
        reject_json_names(&fields, "duplicate effective RPC JSON field name");
    }
}

#[test]
fn json_field_names_refuse_embedded_nulls_and_extension_spellings() {
    for name in ["\0", "a\0b", "[test.field]", "[]", "[[field]]"] {
        reject_json_names(&[("first", Some(name))], "invalid RPC JSON field name");
    }
}

#[test]
fn json_field_names_preserve_legal_spelling_identity_and_message_scopes() {
    for fields in [
        vec![("first", Some(""))],
        vec![("first", Some("a b")), ("second", Some("☃"))],
        vec![("first", Some("[open")), ("second", Some("close]"))],
        vec![("first", Some("Case")), ("second", Some("case"))],
        vec![("_leading", None), ("leading", None)],
        vec![("FooBar", None), ("foo_bar", None)],
        vec![("foo_1bar", None), ("foo_1_bar", None)],
        vec![("URL_value", None), ("u_r_l_value", None)],
    ] {
        let mut set = fixture();
        // Artifact identity uses canonical file order, unlike protoc's raw
        // dependency-first descriptor output used by this fixture.
        set.file.sort_by(|left, right| left.name.cmp(&right.name));
        let parent = message(&mut set);
        let child = json_message(&fields);
        // The same field spellings in different messages are independent.
        let mut sibling = child.clone();
        sibling.name = Some("SiblingJsonNames".into());
        parent.nested_type.extend([child, sibling]);
        let original = set.encode_to_vec();
        let contract = Contract::from_descriptor(&original, Limits::default()).unwrap();
        assert_eq!(contract.descriptor(), original);
        contract
            .validate_message("cellar.rpc.conformance.v1.AllTypes.JsonNames", &[0x0a, 0])
            .unwrap();
    }
}

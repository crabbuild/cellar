// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::super::{Contract, Limits};
use prost::Message;
use prost_types::{
    field_descriptor_proto::{Label, Type},
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto, FileDescriptorSet, MethodDescriptorProto, OneofDescriptorProto,
    ServiceDescriptorProto,
};

fn message(name: &str) -> DescriptorProto {
    DescriptorProto {
        name: Some(name.into()),
        ..Default::default()
    }
}

fn field(name: &str, kind: Type, reference: Option<&str>) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(1),
        label: Some(Label::Optional as i32),
        r#type: Some(kind as i32),
        type_name: reference.map(str::to_owned),
        ..Default::default()
    }
}

fn enumeration(name: &str, value: &str) -> EnumDescriptorProto {
    EnumDescriptorProto {
        name: Some(name.into()),
        value: vec![EnumValueDescriptorProto {
            name: Some(value.into()),
            number: Some(0),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn file(name: &str, package: &str) -> FileDescriptorProto {
    FileDescriptorProto {
        name: Some(name.into()),
        package: Some(package.into()),
        syntax: Some("proto3".into()),
        ..Default::default()
    }
}

fn fixture() -> FileDescriptorSet {
    let mut file = file("service.proto", "test");
    file.message_type.push(message("Value"));
    file.service.push(ServiceDescriptorProto {
        name: Some("Counter".into()),
        method: vec![MethodDescriptorProto {
            name: Some("Read".into()),
            input_type: Some(".test.Value".into()),
            output_type: Some(".test.Value".into()),
            ..Default::default()
        }],
        ..Default::default()
    });
    FileDescriptorSet { file: vec![file] }
}

fn load(set: &FileDescriptorSet) -> anyhow::Result<Contract> {
    Contract::from_descriptor(&set.encode_to_vec(), Limits::default())
}

fn refused(mutate: impl FnOnce(&mut FileDescriptorSet)) {
    let mut set = fixture();
    mutate(&mut set);
    assert!(load(&set).is_err(), "malformed descriptor was admitted");
}

#[test]
fn enum_values_share_their_enclosing_namespace() {
    for collision in ["SAME", "Value", "Counter"] {
        refused(|set| {
            set.file[0].enum_type = vec![
                enumeration("First", "SAME"),
                enumeration("Second", collision),
            ];
        });
    }
    refused(|set| {
        let value = &mut set.file[0].message_type[0];
        value.enum_type.push(enumeration("State", "Child"));
        value.nested_type.push(message("Child"));
    });
}

#[test]
fn fields_oneofs_and_nested_types_cannot_reuse_a_symbol() {
    refused(|set| {
        let value = &mut set.file[0].message_type[0];
        value.field.push(field("Child", Type::Int32, None));
        value.nested_type.push(message("Child"));
    });
    refused(|set| {
        let value = &mut set.file[0].message_type[0];
        value.nested_type.push(message("Choice"));
        value.oneof_decl.push(OneofDescriptorProto {
            name: Some("Choice".into()),
            ..Default::default()
        });
        let mut member = field("member", Type::Int32, None);
        member.oneof_index = Some(0);
        value.field.push(member);
    });
    refused(|set| {
        let value = &mut set.file[0].message_type[0];
        value.field.push(field("STATE_ZERO", Type::Int32, None));
        value.enum_type.push(enumeration("State", "STATE_ZERO"));
    });
}

#[test]
fn packages_cannot_reopen_message_or_service_namespaces() {
    for package in ["test.Value", "test.Value.child", "test.Counter"] {
        refused(|set| set.file.push(file("other.proto", package)));
    }
    // File order cannot decide whether the conflicting package is accepted.
    refused(|set| set.file.push(file("aaa.proto", "test.Value")));
}

#[test]
fn declarations_use_simple_identifiers() {
    refused(|set| set.file[0].enum_type.push(enumeration("Bad.Name", "ZERO")));
    refused(|set| set.file[0].service[0].name = Some("Bad.Name".into()));
}

fn imported(reference: Type) -> FileDescriptorSet {
    let mut set = fixture();
    let mut foreign = file("foreign.proto", "foreign");
    foreign.message_type.push(message("Other"));
    foreign.enum_type.push(enumeration("State", "STATE_ZERO"));
    let type_name = match reference {
        Type::Enum => ".foreign.State",
        _ => ".foreign.Other",
    };
    set.file[0].message_type[0]
        .field
        .push(field("child", reference, Some(type_name)));
    set.file.push(foreign);
    set
}

#[test]
fn field_references_require_declared_imports_even_in_the_same_package() {
    for kind in [Type::Message, Type::Enum] {
        let mut set = imported(kind);
        assert!(load(&set).is_err(), "undeclared import was admitted");
        set.file[0].dependency.push("foreign.proto".into());
        load(&set).unwrap();
    }
    let mut set = fixture();
    let mut sibling = file("sibling.proto", "test");
    sibling.message_type.push(message("Other"));
    set.file.push(sibling);
    set.file[0].message_type[0]
        .field
        .push(field("child", Type::Message, Some(".test.Other")));
    assert!(load(&set).is_err());
    set.file[0].dependency.push("sibling.proto".into());
    load(&set).unwrap();
}

#[test]
fn method_input_and_output_require_declared_imports() {
    for input in [true, false] {
        let mut set = imported(Type::Message);
        set.file[0].message_type[0].field.clear();
        let method = &mut set.file[0].service[0].method[0];
        if input {
            method.input_type = Some(".foreign.Other".into());
        } else {
            method.output_type = Some(".foreign.Other".into());
        }
        assert!(load(&set).is_err());
        set.file[0].dependency.push("foreign.proto".into());
        load(&set).unwrap();
    }
}

#[test]
fn only_public_edges_reexport_transitive_imports() {
    let mut set = imported(Type::Message);
    set.file[0].dependency.push("forward.proto".into());
    let mut forward = file("forward.proto", "forward");
    forward.dependency.push("foreign.proto".into());
    set.file.push(forward);
    assert!(
        load(&set).is_err(),
        "private transitive import was admitted"
    );
    set.file[2].public_dependency.push(0);
    load(&set).unwrap();
    // Repeated public reachability through a diamond is valid, not a cycle.
    let mut second = file("second.proto", "forward");
    second.dependency.push("foreign.proto".into());
    second.public_dependency.push(0);
    set.file.push(second);
    set.file[0].dependency.push("second.proto".into());
    load(&set).unwrap();
}

#[test]
fn import_graph_refuses_duplicate_self_and_cyclic_edges() {
    refused(|set| set.file[0].dependency.push("service.proto".into()));
    refused(|set| {
        set.file.push(file("empty.proto", "test"));
        set.file[0].dependency = vec!["empty.proto".into(), "empty.proto".into()];
    });
    refused(|set| {
        let mut other = file("other.proto", "test");
        other.dependency.push("service.proto".into());
        set.file[0].dependency.push("other.proto".into());
        set.file.push(other);
    });
}

#[test]
fn import_modifiers_require_unique_valid_dependency_indexes() {
    for index in [-1, 1, i32::MAX] {
        for public in [true, false] {
            refused(|set| {
                set.file.push(file("empty.proto", "test"));
                set.file[0].dependency.push("empty.proto".into());
                if public {
                    set.file[0].public_dependency.push(index);
                } else {
                    set.file[0].weak_dependency.push(index);
                }
            });
        }
    }
    for public in [true, false] {
        refused(|set| {
            set.file.push(file("empty.proto", "test"));
            set.file[0].dependency.push("empty.proto".into());
            if public {
                set.file[0].public_dependency = vec![0, 0];
            } else {
                set.file[0].weak_dependency = vec![0, 0];
            }
        });
    }
    refused(|set| {
        set.file.push(file("empty.proto", "test"));
        set.file[0].dependency.push("empty.proto".into());
        set.file[0].public_dependency.push(0);
        set.file[0].weak_dependency.push(0);
    });
}

#[test]
fn present_weak_imports_are_directly_visible_but_not_public() {
    let mut set = imported(Type::Message);
    set.file[0].dependency.push("foreign.proto".into());
    set.file[0].weak_dependency.push(0);
    load(&set).unwrap();
    let mut forward = file("forward.proto", "forward");
    forward.dependency.push("foreign.proto".into());
    forward.weak_dependency.push(0);
    set.file.push(forward);
    set.file[0].dependency[0] = "forward.proto".into();
    set.file[0].weak_dependency.clear();
    assert!(load(&set).is_err());
}

#[test]
fn symbols_are_bounded_after_qualification() {
    let prefix_len = "test.Value.".len();
    let mut set = fixture();
    set.file[0].message_type[0].field.push(field(
        &"x".repeat(1024 - prefix_len),
        Type::Int32,
        None,
    ));
    load(&set).unwrap();
    set.file[0].message_type[0].field[0]
        .name
        .as_mut()
        .unwrap()
        .push('x');
    assert!(
        load(&set).is_err(),
        "expanded field name exceeded the symbol budget"
    );
    refused(|set| {
        let mut other = file("long.proto", &"p".repeat(1020));
        other.message_type.push(message("Value"));
        set.file.push(other);
    });
}

#[test]
fn expanded_inventory_and_symbol_count_have_independent_budgets() {
    let mut set = fixture();
    let mut large = file("large.proto", &"p".repeat(900));
    let mut value = message("Value");
    for index in 0..20_000 {
        let mut member = field(&format!("field_{index}"), Type::Int32, None);
        member.number = Some(index + 1);
        value.field.push(member);
    }
    large.message_type.push(value);
    set.file.push(large);
    assert!(set.encoded_len() < 4 * 1024 * 1024);
    assert!(load(&set)
        .unwrap_err()
        .to_string()
        .contains("expanded symbol inventory"));

    let mut set = fixture();
    let mut values = enumeration("Large", "ZERO");
    for number in 1..100_000 {
        values.value.push(EnumValueDescriptorProto {
            name: Some(format!("VALUE_{number}")),
            number: Some(number),
            ..Default::default()
        });
    }
    set.file[0].enum_type.push(values);
    assert!(set.encoded_len() < 4 * 1024 * 1024);
    assert!(load(&set)
        .unwrap_err()
        .to_string()
        .contains("too many RPC descriptor symbols"));
}

#[test]
fn nested_references_obey_their_own_files_imports() {
    let mut set = imported(Type::Message);
    let value = &mut set.file[0].message_type[0];
    let mut nested = message("Nested");
    nested.field = std::mem::take(&mut value.field);
    value.nested_type.push(nested);
    assert!(load(&set).is_err());
    set.file[0].dependency.push("foreign.proto".into());
    load(&set).unwrap();
}

#[test]
fn checked_in_protoc_artifacts_keep_their_exact_identity() {
    for (source, normalized, inventory) in [
        (
            &include_bytes!("../../../../../tools/rpc/fixtures/counter.binpb")[..],
            &include_bytes!("../../../../../tools/rpc/fixtures/generated/contracts.binpb")[..],
            &include_bytes!("../../../../../tools/rpc/fixtures/generated/rpc.json")[..],
        ),
        (
            &include_bytes!("../../../../../tools/rpc/fixtures/profile.binpb")[..],
            &include_bytes!("../../../../../tools/rpc/fixtures/profile-generated/contracts.binpb")
                [..],
            &include_bytes!("../../../../../tools/rpc/fixtures/profile-generated/rpc.json")[..],
        ),
    ] {
        let contract = Contract::from_descriptor(source, Limits::default()).unwrap();
        assert_eq!(contract.descriptor(), normalized);
        assert_eq!(
            contract.inventory(),
            serde_json::from_slice::<serde_json::Value>(inventory).unwrap()
        );
        let mut reordered = FileDescriptorSet::decode(source).unwrap();
        reordered.file.reverse();
        assert_eq!(load(&reordered).unwrap().descriptor(), normalized);
    }
}

#[test]
fn separate_nested_scopes_and_shared_packages_remain_valid() {
    let mut set = fixture();
    for name in ["First", "Second"] {
        let mut nested = message(name);
        nested.enum_type.push(enumeration("State", "ZERO"));
        nested.nested_type.push(message("Child"));
        set.file[0].message_type[0].nested_type.push(nested);
    }
    set.file.push(file("empty.proto", "test"));
    load(&set).unwrap();
}

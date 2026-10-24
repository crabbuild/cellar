//! Structural codec identity, separate from the exact artifact digest. Maps
//! project their logical key/value types; synthetic optional oneof names and
//! source locations are not part of the language-neutral message shape.
use prost_types::{
    field_descriptor_proto::{Label, Type},
    DescriptorProto, EnumDescriptorProto, FileDescriptorSet,
};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

pub(super) fn project(set: &FileDescriptorSet) -> Value {
    fn enumeration(
        prefix: &str,
        definition: &EnumDescriptorProto,
        output: &mut Map<String, Value>,
    ) {
        let name = format!("{prefix}.{}", definition.name());
        output.insert(
            name,
            json!([
                "enum",
                definition
                    .value
                    .iter()
                    .map(|value| json!([value.name(), value.number()]))
                    .collect::<Vec<_>>()
            ]),
        );
    }

    fn message(prefix: &str, definition: &DescriptorProto, output: &mut Map<String, Value>) {
        let name = format!("{prefix}.{}", definition.name());
        if definition
            .options
            .as_ref()
            .is_some_and(|options| options.map_entry())
        {
            return;
        }
        let entries = definition
            .nested_type
            .iter()
            .filter(|child| {
                child
                    .options
                    .as_ref()
                    .is_some_and(|options| options.map_entry())
            })
            .map(|child| (format!(".{name}.{}", child.name()), child))
            .collect::<BTreeMap<_, _>>();
        let mut fields = definition.field.iter().collect::<Vec<_>>();
        fields.sort_by_key(|field| field.number());
        let fields = fields
            .into_iter()
            .map(|field| {
                let entry = entries.get(field.type_name());
                let repeated = field.label() == Label::Repeated;
                let cardinality = if entry.is_some() {
                    2
                } else {
                    u8::from(repeated)
                };
                let value = entry.map(|entry| &entry.field[1]).unwrap_or(field);
                let kind = value.r#type();
                let reference = if matches!(kind, Type::Message | Type::Enum) {
                    value.type_name().trim_start_matches('.')
                } else {
                    ""
                };
                let presence = !repeated
                    && (kind == Type::Message
                        || field.proto3_optional()
                        || field.oneof_index.is_some());
                let group = if field.proto3_optional() {
                    ""
                } else {
                    field
                        .oneof_index
                        .map(|index| definition.oneof_decl[index as usize].name())
                        .unwrap_or("")
                };
                let packed = cardinality == 1
                    && !matches!(kind, Type::Message | Type::String | Type::Bytes)
                    && field
                        .options
                        .as_ref()
                        .and_then(|options| options.packed)
                        .unwrap_or(true);
                let key = entry
                    .map(|entry| entry.field[0].r#type() as i32)
                    .unwrap_or(0);
                json!([
                    field.number(),
                    field.name(),
                    kind as i32,
                    cardinality,
                    presence,
                    group,
                    packed,
                    reference,
                    key
                ])
            })
            .collect::<Vec<_>>();
        output.insert(name.clone(), json!(["message", fields]));
        for child in &definition.nested_type {
            message(&name, child, output);
        }
        for child in &definition.enum_type {
            enumeration(&name, child, output);
        }
    }

    let mut output = Map::new();
    for file in &set.file {
        for definition in &file.message_type {
            message(file.package(), definition, &mut output);
        }
        for definition in &file.enum_type {
            enumeration(file.package(), definition, &mut output);
        }
    }
    output.into()
}

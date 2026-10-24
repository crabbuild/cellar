//! Reject identifiers that become ambiguous after language name conversion.
use anyhow::{ensure, Result};
use heck::{ToLowerCamelCase, ToSnakeCase, ToUpperCamelCase};
use prost_types::FileDescriptorSet;
use std::collections::{BTreeMap, BTreeSet};

pub fn validate(descriptor: &FileDescriptorSet) -> Result<()> {
    let mut packages: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
    for file in &descriptor.file {
        let names = packages
            .entry(file.package.as_deref().unwrap_or_default())
            .or_default();
        for name in file
            .message_type
            .iter()
            .filter_map(|m| m.name.as_deref())
            .chain(file.enum_type.iter().filter_map(|e| e.name.as_deref()))
        {
            ensure!(
                names.insert(name.to_upper_camel_case()),
                "generated message identifier collision: {name}"
            );
        }
    }
    for file in &descriptor.file {
        let names = packages
            .entry(file.package.as_deref().unwrap_or_default())
            .or_default();
        for service in &file.service {
            let name = service.name.as_deref().unwrap_or_default();
            for suffix in ["Client", "Handler", "Registration"] {
                let generated = format!("{}{suffix}", name.to_upper_camel_case());
                ensure!(
                    names.insert(generated.clone()),
                    "generated service identifier collision: {generated}"
                );
            }
            let helper = format!("__cellar_{}_contract", name.to_lowercase());
            ensure!(
                names.insert(helper),
                "generated service helper collision: {name}"
            );
            let mut rust = BTreeSet::from(["bind".to_string(), "clone".to_string()]);
            let mut js = BTreeSet::from([
                "bind".to_string(),
                "constructor".to_string(),
                "then".to_string(),
            ]);
            let mut python = BTreeSet::from(["bind".to_string()]);
            for method in &service.method {
                let method = method.name.as_deref().unwrap_or_default();
                let snake = method.to_snake_case();
                let camel = method.to_lower_camel_case();
                ensure!(
                    rust.insert(snake.clone()) && rust.insert(format!("{snake}_with_options")),
                    "generated Rust method collision: {method}"
                );
                ensure!(
                    python.insert(snake.clone()) && python.insert(format!("{snake}_with_options")),
                    "generated Python method collision: {method}"
                );
                ensure!(
                    js.insert(camel.clone()) && js.insert(format!("{camel}WithOptions")),
                    "generated JavaScript method collision: {method}"
                );
                ensure!(
                    !["self", "super", "crate", "Self"].contains(&snake.as_str()),
                    "reserved RPC method name: {method}"
                );
                ensure!(
                    ![
                        "False", "None", "True", "and", "as", "assert", "async", "await", "break",
                        "class", "continue", "def", "del", "elif", "else", "except", "finally",
                        "for", "from", "global", "if", "import", "in", "is", "lambda", "nonlocal",
                        "not", "or", "pass", "raise", "return", "try", "while", "with", "yield"
                    ]
                    .contains(&snake.as_str()),
                    "reserved Python RPC method name: {method}"
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_types::{
        DescriptorProto, FileDescriptorProto, MethodDescriptorProto, ServiceDescriptorProto,
    };

    fn schema(methods: &[&str], messages: &[&str]) -> FileDescriptorSet {
        FileDescriptorSet {
            file: vec![FileDescriptorProto {
                package: Some("test".into()),
                message_type: messages
                    .iter()
                    .map(|name| DescriptorProto {
                        name: Some((*name).into()),
                        ..Default::default()
                    })
                    .collect(),
                service: vec![ServiceDescriptorProto {
                    name: Some("Counter".into()),
                    method: methods
                        .iter()
                        .map(|name| MethodDescriptorProto {
                            name: Some((*name).into()),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    #[test]
    fn generated_names_reject_cross_language_and_helper_collisions() {
        validate(&schema(&["Increment", "GetCount"], &["Count"])).unwrap();
        for methods in [
            vec!["GetCount", "get_count"],
            vec!["Increment", "IncrementWithOptions"],
            vec!["Bind"],
            vec!["Then"],
            vec!["Async"],
        ] {
            assert!(
                validate(&schema(&methods, &[])).is_err(),
                "accepted {methods:?}"
            );
        }
        assert!(validate(&schema(&["Increment"], &["CounterClient"])).is_err());
    }
}

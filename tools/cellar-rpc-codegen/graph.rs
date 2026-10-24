//! Generate visitors from Prost's actual Rust declarations and field attributes.
//! This avoids duplicating its Rust type/path/boxing/keyword conversion rules.
use anyhow::{bail, ensure, Context, Result};
use std::fmt::Write;
use syn::{punctuated::Punctuated, Attribute, Expr, Item, Lit, Meta, Token};

const SDK: &str = "::cellar::sdk::rpc";

struct Field {
    kind: String,
    map: Option<(String, String)>,
    repeated: bool,
    optional: bool,
    packed: bool,
    layout_fields: usize,
}

impl Field {
    fn parse(attrs: &[Attribute]) -> Result<Self> {
        let mut output = Self {
            kind: String::new(),
            map: None,
            repeated: false,
            optional: false,
            packed: true,
            layout_fields: 1,
        };
        for attr in attrs.iter().filter(|attr| attr.path().is_ident("prost")) {
            for meta in attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)? {
                let name = meta
                    .path()
                    .get_ident()
                    .context("unsupported Prost attribute path")?
                    .to_string();
                let value = if let Meta::NameValue(value) = &meta {
                    match &value.value {
                        Expr::Lit(value) => match &value.lit {
                            Lit::Str(value) => Some(value.value()),
                            _ => None,
                        },
                        _ => None,
                    }
                } else {
                    None
                };
                match name.as_str() {
                    "repeated" => output.repeated = true,
                    "optional" => output.optional = true,
                    "packed" => output.packed = value.as_deref() != Some("false"),
                    "map" | "btree_map" => {
                        let value = value.context("missing Prost map kinds")?;
                        let (key, value) =
                            value.split_once(',').context("invalid Prost map kinds")?;
                        output.map = Some((key.trim().into(), value.trim().into()));
                    }
                    "tags" => {
                        output.layout_fields =
                            value.context("missing oneof tags")?.split(',').count()
                    }
                    "tag" | "boxed" | "required" => {}
                    "default" => bail!("default overrides are outside the RPC profile"),
                    _ => {
                        ensure!(output.kind.is_empty(), "multiple Prost field kinds");
                        output.kind = name;
                    }
                }
            }
        }
        ensure!(
            output.map.is_some() || !output.kind.is_empty(),
            "missing Prost field kind"
        );
        Ok(output)
    }

    fn child(&self) -> bool {
        self.kind == "message" || self.kind == "oneof"
    }

    fn visit(&self, field: &str, selected: bool) -> Result<String> {
        let mut code = String::new();
        if let Some((key, value)) = &self.map {
            writeln!(code, "budget.elements({field}.len())?; for (key, value) in {field} {{ budget.message(depth + 1, 3)?;").unwrap();
            writeln!(
                code,
                "if !{SDK}::GraphBudget::is_default(key) {{ budget.elements(1)?; {} }}",
                payload(key, "key")?
            )
            .unwrap();
            let body = if value == "message" {
                format!("{SDK}::MessageGraph::visit(value, budget, depth + 2)?;")
            } else {
                payload(value, "value")?
            };
            let missing = if value == "message" {
                format!("else {{ budget.layout({SDK}::MessageGraph::layout_slots(value))?; }}")
            } else {
                String::new()
            };
            writeln!(
                code,
                "if !{SDK}::GraphBudget::is_default(value) {{ budget.elements(1)?; {body} }} {missing} }}"
            )
            .unwrap();
        } else if self.repeated {
            writeln!(code, "budget.elements({field}.len())?;").unwrap();
            if self.kind == "message" {
                writeln!(code, "for value in {field} {{ {SDK}::MessageGraph::visit(value, budget, depth + 1)?; }}").unwrap();
            } else {
                if self.packed && !["string", "bytes"].contains(&self.kind.as_str()) {
                    writeln!(code, "if !{field}.is_empty() {{ budget.elements(1)?; }}").unwrap();
                }
                let payload = payload(&self.kind, "value")?;
                if !payload.is_empty() {
                    writeln!(code, "for value in {field} {{ {payload} }}").unwrap();
                }
            }
        } else if self.optional || self.kind == "oneof" {
            let binding = if self.child() || ["string", "bytes"].contains(&self.kind.as_str()) {
                "value"
            } else {
                "_value"
            };
            writeln!(
                code,
                "if let Some({binding}) = {field} {{ {} }}",
                self.singular(binding)?
            )
            .unwrap();
        } else if self.kind == "message" || selected {
            code = self.singular(field)?;
        } else {
            code = format!(
                "if {} {{ {} }}",
                present(&self.kind, field)?,
                self.singular(field)?
            );
        }
        Ok(code)
    }

    fn singular(&self, value: &str) -> Result<String> {
        if self.kind == "oneof" {
            return Ok(format!(
                "{SDK}::MessageGraph::visit({value}, budget, depth)?;"
            ));
        }
        let body = if self.kind == "message" {
            format!("{SDK}::MessageGraph::visit({value}, budget, depth + 1)?;")
        } else {
            payload(&self.kind, value)?
        };
        Ok(format!("budget.elements(1)?; {body}"))
    }

    fn discard(&self, field: &str) -> String {
        if self
            .map
            .as_ref()
            .is_some_and(|(_, value)| value == "message")
        {
            format!("pending.children(std::mem::take(&mut {field}).into_values());")
        } else if self.child() {
            if self.optional || self.repeated || self.kind == "oneof" {
                format!("pending.children(std::mem::take(&mut {field}));")
            } else {
                format!("pending.children([std::mem::take(&mut {field})]);")
            }
        } else {
            String::new()
        }
    }
}

fn payload(kind: &str, value: &str) -> Result<String> {
    if ["string", "bytes"].contains(&kind) {
        return Ok(format!("budget.bytes({value}.len())?;"));
    }
    ensure!(
        kind.starts_with("enumeration")
            || [
                "double", "float", "int32", "int64", "uint32", "uint64", "sint32", "sint64",
                "fixed32", "fixed64", "sfixed32", "sfixed64", "bool"
            ]
            .contains(&kind),
        "unsupported Prost scalar kind: {kind}"
    );
    Ok(String::new())
}

fn present(kind: &str, value: &str) -> Result<String> {
    payload(kind, value)?;
    Ok(match kind {
        "string" | "bytes" => format!("!{value}.is_empty()"),
        "bool" => format!("*{value}"),
        "double" | "float" => format!("*{value} != 0.0"),
        _ => format!("*{value} != 0"),
    })
}

fn derives(attrs: &[Attribute], name: &str) -> bool {
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("derive"))
        .any(|attr| {
            attr.parse_args_with(Punctuated::<syn::Path, Token![,]>::parse_terminated)
                .is_ok_and(|paths| {
                    paths
                        .iter()
                        .any(|path| path.segments.last().is_some_and(|part| part.ident == name))
                })
        })
}

pub fn implementations(source: &str) -> Result<String> {
    fn append_items(
        entries: &[Item],
        prefix: &str,
        existing: &std::collections::BTreeSet<String>,
        out: &mut String,
    ) -> Result<()> {
        for item in entries {
            let (name, visit, discard, slots) = match item {
                Item::Mod(module) => {
                    if let Some((_, children)) = &module.content {
                        append_items(
                            children,
                            &format!("{prefix}{}::", module.ident),
                            existing,
                            out,
                        )?;
                    }
                    continue;
                }
                Item::Struct(message) if derives(&message.attrs, "Message") => {
                    let mut visit = String::new();
                    let mut discard = String::new();
                    let mut slots = 1;
                    for field in &message.fields {
                        let name = field
                            .ident
                            .as_ref()
                            .context("RPC messages require named Rust fields")?;
                        let spec = Field::parse(&field.attrs)?;
                        slots += spec.layout_fields;
                        write!(
                            visit,
                            "{{ let field = &self.{name}; {} }}",
                            spec.visit("field", false)?
                        )
                        .unwrap();
                        discard.push_str(&spec.discard(&format!("self.{name}")));
                    }
                    (message.ident.to_string(), visit, discard, slots)
                }
                Item::Enum(oneof) if derives(&oneof.attrs, "Oneof") => {
                    let mut visit = String::from("match self {");
                    let mut discard = String::from("match *self {");
                    for variant in &oneof.variants {
                        ensure!(
                            variant.fields.len() == 1,
                            "RPC oneof cases require one value"
                        );
                        let spec = Field::parse(&variant.attrs)?;
                        let binding =
                            if spec.child() || ["string", "bytes"].contains(&spec.kind.as_str()) {
                                "value"
                            } else {
                                "_value"
                            };
                        writeln!(
                            visit,
                            "Self::{}({binding}) => {{ {} }},",
                            variant.ident,
                            spec.visit(binding, true)?
                        )
                        .unwrap();
                        if spec.kind == "message" {
                            writeln!(
                                discard,
                                "Self::{}(value) => pending.children([value]),",
                                variant.ident
                            )
                            .unwrap();
                        } else {
                            writeln!(discard, "Self::{}(_) => {{}},", variant.ident).unwrap();
                        }
                    }
                    visit.push('}');
                    discard.push('}');
                    (oneof.ident.to_string(), visit, discard, 0)
                }
                _ => continue,
            };
            // Prost finalizes each file but accumulates files from one package
            // in the same buffer. Do not duplicate already generated visitors.
            if existing.contains(&format!("{prefix}{name}")) {
                continue;
            }
            let receiver = if !discard.is_empty() && matches!(item, Item::Struct(_)) {
                "mut self"
            } else {
                "self"
            };
            let pending = if !discard.contains("pending.children(") {
                "_pending"
            } else {
                "pending"
            };
            let layout = if slots == 0 {
                "budget.depth(depth)?;".to_string()
            } else {
                format!("budget.message(depth, {slots})?;")
            };
            writeln!(out, "impl {SDK}::MessageGraph for {prefix}{name} {{
                fn layout_slots(&self) -> usize {{ {slots} }}
                fn visit(&self, budget: &mut {SDK}::GraphBudget, depth: usize) -> Result<(), {SDK}::Failure> {{ {layout} {visit} Ok(()) }}
                fn discard({receiver}: Box<Self>, {pending}: &mut {SDK}::GraphDiscard) {{ {discard} }}
            }}").unwrap();
        }
        Ok(())
    }
    let parsed = syn::parse_file(source)?;
    let existing = parsed
        .items
        .iter()
        .filter_map(|item| {
            let Item::Impl(item) = item else { return None };
            let (_, path, _) = item.trait_.as_ref()?;
            if path.segments.last()?.ident != "MessageGraph" {
                return None;
            }
            let syn::Type::Path(path) = &*item.self_ty else {
                return None;
            };
            Some(
                path.path
                    .segments
                    .iter()
                    .map(|part| part.ident.to_string())
                    .collect::<Vec<_>>()
                    .join("::"),
            )
        })
        .collect();
    let mut output = String::new();
    append_items(&parsed.items, "", &existing, &mut output)?;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn visitors_follow_rust_identifiers_and_do_not_duplicate_package_fragments() {
        let source = r#"
            #[derive(Clone, ::cellar::sdk::rpc::prost::Message)]
            pub struct Type { #[prost(string, tag="1")] pub r#type: String }
            pub mod nested {
                #[derive(Clone, ::cellar::sdk::rpc::prost::Oneof)]
                pub enum Choice { #[prost(int64,tag="2")] Value(i64) }
            }
        "#;
        let generated = implementations(source).unwrap();
        assert!(generated.contains("for Type"));
        assert!(generated.contains("self.r#type"));
        assert!(generated.contains("for nested::Choice"));
        let mut combined = format!("{source}\n{generated}");
        assert!(implementations(&combined).unwrap().is_empty());
        combined.push_str("#[derive(::cellar::sdk::rpc::prost::Message)] pub struct Next {}");
        let next = implementations(&combined).unwrap();
        assert!(next.contains("for Next"));
        assert!(!next.contains("for Type"));
    }
}

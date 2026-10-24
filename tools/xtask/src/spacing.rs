// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{anyhow, Result};
use proc_macro2::{Delimiter, Span, TokenStream, TokenTree};
use std::collections::BTreeSet;
use syn::{spanned::Spanned, visit::Visit, Attribute, Item};

#[derive(Default)]
struct Declarations {
    starts: Vec<Span>,
    previous: Option<(u8, usize)>,
}

fn skipped(attributes: &[Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        let path = attribute.path();
        path.segments.len() == 2
            && path.segments[0].ident == "rustfmt"
            && path.segments[1].ident == "skip"
    })
}

impl<'ast> Visit<'ast> for Declarations {
    fn visit_item(&mut self, item: &'ast Item) {
        let (kind, attributes) = match item {
            Item::Enum(item) => (0, &item.attrs),
            Item::Fn(item) => (0, &item.attrs),
            Item::Impl(item) => (0, &item.attrs),
            Item::Struct(item) => (0, &item.attrs),
            Item::Trait(item) => (0, &item.attrs),
            Item::Union(item) => (0, &item.attrs),
            Item::Type(item) => (1, &item.attrs),
            Item::Const(item) => (2, &item.attrs),
            Item::Static(item) => (2, &item.attrs),
            Item::Mod(item) if skipped(&item.attrs) => return,
            _ => {
                self.previous = None;
                syn::visit::visit_item(self, item);
                return;
            }
        };
        if skipped(attributes) {
            self.previous = None;
            return;
        }
        // Keep adjacent aliases and constant declarations grouped, as imports
        // and module declarations are. Existing blank lines remain untouched.
        let grouped = kind != 0
            && matches!(self.previous, Some((previous, end))
                if previous == kind && end + 1 == item.span().start().line);
        if !grouped {
            self.starts.push(item.span());
        }
        syn::visit::visit_item(self, item);
        self.previous = Some((kind, item.span().end().line));
    }

    fn visit_impl_item_fn(&mut self, method: &'ast syn::ImplItemFn) {
        if !skipped(&method.attrs) {
            self.starts.push(method.span());
            syn::visit::visit_impl_item_fn(self, method);
        }
    }

    fn visit_trait_item_fn(&mut self, method: &'ast syn::TraitItemFn) {
        if !skipped(&method.attrs) {
            self.starts.push(method.span());
            syn::visit::visit_trait_item_fn(self, method);
        }
    }

    fn visit_foreign_item_fn(&mut self, function: &'ast syn::ForeignItemFn) {
        if !skipped(&function.attrs) {
            self.starts.push(function.span());
        }
    }
}

struct CodeToken {
    span: Span,
    opens_block: bool,
}

fn code_tokens(stream: TokenStream, tokens: &mut Vec<CodeToken>) {
    for token in stream {
        if let TokenTree::Group(group) = token {
            tokens.push(CodeToken {
                span: group.span_open(),
                opens_block: group.delimiter() == Delimiter::Brace,
            });
            code_tokens(group.stream(), tokens);
            tokens.push(CodeToken {
                span: group.span_close(),
                opens_block: false,
            });
        } else {
            tokens.push(CodeToken {
                span: token.span(),
                opens_block: false,
            });
        }
    }
}

pub fn separate_items(source: &str) -> Result<String> {
    let syntax = syn::parse_file(source)?;
    if skipped(&syntax.attrs) {
        return Ok(source.to_owned());
    }
    // Preserve byte positions for files with a Unix interpreter header.
    let token_source = if let Some(header) = &syntax.shebang {
        format!("{}{}", " ".repeat(header.len()), &source[header.len()..])
    } else {
        source.to_owned()
    };
    let mut tokens = Vec::new();
    code_tokens(
        token_source.parse().map_err(|error| anyhow!("{error}"))?,
        &mut tokens,
    );
    tokens.sort_by_key(|token| token.span.byte_range().end);
    let mut declarations = Declarations::default();
    declarations.visit_file(&syntax);
    let lines: Vec<_> = source.split_inclusive('\n').collect();
    let mut separators = BTreeSet::new();
    for declaration in declarations.starts {
        let start = declaration.byte_range().start;
        let position = tokens.partition_point(|token| token.span.byte_range().end <= start);
        let Some(previous) = position.checked_sub(1).map(|index| &tokens[index]) else {
            continue;
        };
        if previous.opens_block {
            continue;
        }
        // Everything between these tokens is whitespace or a comment. Insert
        // before leading comments, never inside a raw string or after a doc
        // comment/attribute. A trailing comment stays beside its own token.
        let after_previous = previous.span.end().line;
        let header = declaration.start().line - 1;
        if after_previous <= header
            && !lines[after_previous..header]
                .iter()
                .any(|line| line.trim().is_empty())
        {
            separators.insert(after_previous);
        }
    }
    let newline = if source.contains("\r\n") {
        "\r\n"
    } else {
        "\n"
    };
    let mut result = String::with_capacity(source.len() + separators.len() * newline.len());
    for (index, line) in lines.iter().enumerate() {
        if separators.contains(&index) {
            result.push_str(newline);
        }
        result.push_str(line);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attributes_docs_and_comments_stay_attached() -> Result<()> {
        let source = "use std::sync::Arc;\n// A durable cell.\n/// Cell documentation.\n#[derive(Default)]\nstruct Counter;\nimpl Counter {\n    /// First method.\n    fn first() {} // trailing comment\n    /* Next method. */\n    #[cfg(unix)]\n    fn second() {}\n}\n";
        let formatted = separate_items(source)?;
        assert!(formatted.contains("Arc;\n\n// A durable cell.\n/// Cell documentation.\n#[derive(Default)]\nstruct Counter;\n\nimpl Counter"));
        assert!(formatted.contains("impl Counter {\n    /// First method."));
        assert!(formatted.contains(
            "{} // trailing comment\n\n    /* Next method. */\n    #[cfg(unix)]\n    fn second"
        ));
        assert_eq!(separate_items(&formatted)?, formatted);
        Ok(())
    }

    #[test]
    fn strings_and_macro_bodies_remain_verbatim() -> Result<()> {
        let source = "const SOURCE: &str = r###\"\nfn fake() {}\nstruct Fake;\n// fake comment\n\"###;\nmacro_rules! sample { () => { fn one() {} fn two() {} }; }\nfn real() {}\n";
        let formatted = separate_items(source)?;
        assert!(formatted.contains("r###\"\nfn fake() {}\nstruct Fake;\n// fake comment\n\"###;"));
        assert!(formatted.contains("macro_rules! sample { () => { fn one() {} fn two() {} }; }"));
        assert_eq!(
            source.parse::<TokenStream>().unwrap().to_string(),
            formatted.parse::<TokenStream>().unwrap().to_string()
        );
        Ok(())
    }

    #[test]
    fn groups_skips_nested_items_and_trait_methods_are_respected() -> Result<()> {
        let source = "use std::fmt;\nuse std::sync;\nconst A: u8 = 1;\nconst B: u8 = 2;\ntype AType = u8;\ntype BType = u8;\ntrait Example {\n    fn one();\n    fn two();\n}\n#[rustfmt::skip]\nimpl Example for AType {\n    fn one() {}\n    fn two() {}\n}\nmod nested {\n    struct First;\n    struct Second;\n}\n";
        let formatted = separate_items(source)?;
        assert!(formatted.contains("use std::fmt;\nuse std::sync;\n\nconst A: u8 = 1;\nconst B: u8 = 2;\n\ntype AType = u8;\ntype BType = u8;"));
        assert!(formatted.contains("fn one();\n\n    fn two();"));
        assert!(formatted.contains(
            "#[rustfmt::skip]\nimpl Example for AType {\n    fn one() {}\n    fn two() {}\n}"
        ));
        assert!(formatted.contains("mod nested {\n    struct First;\n\n    struct Second;"));
        Ok(())
    }

    #[test]
    fn crlf_is_preserved_and_invalid_rust_is_rejected() -> Result<()> {
        assert_eq!(
            separate_items("fn a() {}\r\nfn b() {}\r\n")?,
            "fn a() {}\r\n\r\nfn b() {}\r\n"
        );
        assert!(separate_items("fn broken( {").is_err());
        Ok(())
    }
}

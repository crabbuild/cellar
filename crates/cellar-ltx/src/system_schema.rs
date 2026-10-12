// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! All Cellar-owned SQLite tables, indexes and triggers use this namespace.

pub const PREFIX: &str = "__sys_";

pub fn is_system_name(name: &str) -> bool {
    name.get(..PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(PREFIX))
}

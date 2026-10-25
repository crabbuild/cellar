// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use std::{collections::BTreeSet, path::Path};

/// A module with Rust children has one directory entry point. Leaf modules
/// may keep substantial private tests in a sibling `<name>_tests.rs` file.
pub(super) fn conflicting_modules(paths: &[String]) -> Vec<&str> {
    let directories: BTreeSet<_> = paths
        .iter()
        .flat_map(|path| Path::new(path).ancestors().skip(1))
        .collect();
    paths
        .iter()
        .filter(|path| directories.contains(Path::new(path).with_extension("").as_path()))
        .map(String::as_str)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conflicts(paths: &[&str]) -> Vec<String> {
        conflicting_modules(
            &paths
                .iter()
                .map(|path| (*path).to_owned())
                .collect::<Vec<_>>(),
        )
        .into_iter()
        .map(str::to_owned)
        .collect()
    }

    #[test]
    fn leaf_with_test_only_directory_is_rejected() {
        assert_eq!(
            conflicts(&[
                "crates/cellar/node/driver.rs",
                "crates/cellar/node/driver/tests.rs"
            ]),
            ["crates/cellar/node/driver.rs"]
        );
    }

    #[test]
    fn production_children_also_require_directory_entry_point() {
        assert_eq!(
            conflicts(&[
                "src/queue.rs",
                "src/queue/wire/encode.rs",
                "src/queue/mod.rs"
            ]),
            ["src/queue.rs"]
        );
    }

    #[test]
    fn leaf_tests_and_component_tests_are_valid() {
        assert!(conflicts(&[
            "src/driver.rs",
            "src/driver_tests.rs",
            "src/queue/mod.rs",
            "src/queue/wire.rs",
            "src/queue/tests.rs",
            "tests/support/mod.rs",
        ])
        .is_empty());
    }

    #[test]
    fn similar_names_and_other_components_do_not_conflict() {
        assert!(conflicts(&[
            "src/queue.rs",
            "src/queue_client/mod.rs",
            "src/queue_client/tests.rs",
            "other/queue/tests.rs",
        ])
        .is_empty());
        assert!(conflicts(&[]).is_empty());
    }
}

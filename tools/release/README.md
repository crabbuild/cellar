# Publishing Cellar crates

The crates.io package `cellar-main` provides the `cellar` Rust library and CLI.
The support packages are `cellar-logic`, `cellar-native`, `cellar-ltx`, and
`cellar-axum`. Keep their release versions and workspace dependency requirements
in sync. `cellar-admin` is outside this release set.

1. Start from updated `origin/main`, set the release versions, and update
   package metadata and installation examples.
2. Run `python3 tools/release/crates.py` to generate registry READMEs. These
   copies use absolute links pinned to `v<version>` so links and illustrations
   work on crates.io. Edit the source `README.md` files, then regenerate.
3. Run the repository checks in [AGENTS.md](../../AGENTS.md) and verify the
   packaged sources with the SDK CI toolchain:

   ```sh
   cargo publish --dry-run --locked \
     -p cellar-logic -p cellar-native -p cellar-ltx -p cellar-main -p cellar-axum
   ```

   Cargo stages unpublished workspace dependencies locally and builds the
   archives. Inspect the archives before publishing. The main package contains
   the library and `cellar` CLI; repository-only example and performance binaries
   have paths outside the crate and Cargo omits them.
4. Commit the verified source, open a PR, and push the matching `v<version>` tag
   before publishing so the registry README links resolve.
5. With crates.io credentials configured through `cargo login`, publish the same
   package set without `--dry-run`. Cargo publishes in dependency order and waits
   for registry availability. Verify every version on crates.io and check a fresh
   consumer using the published packages.

Published package versions are immutable. If a partial publication fails,
inspect crates.io and resume with only the remaining packages. The GitHub
binary and container release workflow is separate from crates.io publication.

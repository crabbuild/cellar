# Repository Rust quality tools

From the repository root or a member crate directory:

```sh
cargo format          # Apply Rust layout and Cellar declaration spacing
cargo format --check  # Check without writing source files; exit 1 on differences
cargo check-layout    # Check component and test file placement without writing
```

These Cargo aliases run `cargo xtask fmt`, `cargo xtask fmt --check`, and
`cargo xtask layout`.
They need Rust, the rustfmt component (`rustup component add rustfmt`) and Git.
Use the checkout-specific `CARGO_TARGET_DIR` required by [AGENTS.md](../../AGENTS.md)
on the development machine. No nightly toolchain is needed.

The formatter lists tracked and unignored new `.rs` files with Git, including
example modules. It skips deleted files and Git-ignored files. Each file passes
through rustfmt in memory, then a syntax-aware pass inserts blank lines between
imports and declarations, types, implementations, functions and methods. It keeps
adjacent imports, module declarations, constants and type aliases grouped. The
first declaration within a block stays next to its opening brace.

Documentation, attributes and leading comments stay attached to their declaration.
Strings and macro bodies retain their contents during the spacing pass, and
`#[rustfmt::skip]` declarations retain their layout. Macro-specific payload layouts
still follow rustfmt's support for that macro. The tool computes all results before
writing source files, so parse failures and check mode do not partially apply edits.
A successful command prints `Formatted N Rust files` or `Checked N Rust files`.

Stable `cargo fmt --all` handles normal Rust layout, but rustfmt's
[`blank_lines_lower_bound`](https://github.com/rust-lang/rustfmt/blob/master/Configurations.md#blank_lines_lower_bound)
is currently an unstable option. The extra pass enforces Cellar's spacing without
changing the project's stable toolchain. Plain `cargo fmt --all` remains available.

Verify formatter changes with:

```sh
cargo test -p cellar-xtask --locked
cargo format --check
cargo check-layout
```

The tests cover read-only check mode, apply/check agreement, idempotency,
untracked/deleted/ignored sources, comment and attribute attachment, raw strings,
macro bodies, grouped declarations, skipped items, nested methods and CRLF input.

The layout check uses the same Git source inventory and rejects `feature.rs`
beside a `feature/` directory containing Rust sources, including test-only
directories. Components with production children use `feature/mod.rs`; a leaf's
substantial private tests use a sibling `feature_tests.rs` declared with
`#[cfg(test)] #[path = "feature_tests.rs"] mod tests;`. Small tests stay inline.
Grouping directories without a corresponding `.rs` file remain valid. The check
does not infer module semantics or replace compiler checks. See
[AGENTS.md](../../AGENTS.md#rust-quality-and-module-layout) for the full conventions.

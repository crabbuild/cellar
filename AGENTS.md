<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# AGENTS.md

Contributor guidance for Cellar. Read any more specific `AGENTS.md` before
editing a subtree. `CLAUDE.md` links to this file.

Keep this guide focused on stable conventions. Implementation details belong
in component documentation; design plans and validation history belong in
`docs/design/`. Link to those sources rather than copying them here.

## Project and repository map

Cellar is a native Rust framework for durable business cells. Each cell owns
SQLite state; a fleet coordinates ownership, routing, durability and recovery.
Applications implement `cellar::sdk` contracts and compile into native executables.

| Location | Responsibility |
| --- | --- |
| `crates/cellar-logic/` | Pure decision core and state transitions |
| `crates/cellar/` | Production host, effect adapters, SDK and CLI |
| `crates/cellar-native/` | Owned values, codecs and web primitives |
| `crates/cellar-ltx/` | SQLite WAL capture, LTX and replication mechanics |
| `crates/cellar-axum/` | Axum integration and external ingress |
| `crates/cellar-admin/`, `packages/console/` | Management API and browser console |
| `examples/` | Reference applications and runnable examples |
| `tools/`, `.github/workflows/` | Development tooling and CI |

Within `crates/cellar/`, place code beside its owning component. `lib.rs` maps
physical files to existing public Rust paths; preserve those paths during moves.
Start with the [documentation index](docs/README.md) for architecture and APIs.

## Architectural boundaries

- Keep `cellar-logic` free of dependencies, async work and I/O. The actor owns
  its state; adapters execute effects and return completion events.
- Keep application building blocks in `cellar/application`, beside their owning
  adapters. Use focused private modules for values, validation and codecs;
  expose application APIs through `cellar::sdk`.
- Use execution-domain services for tasks, clocks, randomness and node storage.
  Follow `clippy.toml`; keep configuration explicit and environment parsing at
  CLI boundaries. Cancellation and shutdown must release resources and join work.
- Preserve ownership, epoch and activation checks. Acknowledge writes only after
  the required durability proof; failed recovery must refuse admission.
- Preserve typed failures and operation-specific retry rules. A lost response
  does not establish whether a mutation committed.
- The Rust core and native-only applications remain free of JavaScript engines.
  The optional `cellar-javascript` adapter may embed V8 when explicitly selected;
  keep engine dependencies out of the host and pure decision core. During
  internal development, Cellar-owned format markers stay at 1; use hard cutovers
  rather than legacy readers or state migrations.

## Rust quality and module layout

Organize code by responsibility, with private implementation modules and a small,
intentional public API. Split modules when responsibilities separate, rather
than to meet arbitrary file-size limits. Avoid unrelated catch-all helpers.

| Module | Layout |
| --- | --- |
| Leaf | `feature.rs`, with small unit tests inline |
| Leaf with substantial tests | `feature.rs` and sibling `feature_tests.rs` |
| Component with production submodules | `feature/mod.rs` and named child modules |

Never combine `feature.rs` with a `feature/` directory containing Rust files,
including a directory containing only tests. Sibling test files remain private
`#[cfg(test)]` children, declared with `#[path]`. Public API integration tests
belong in the crate's `tests/` directory. `cargo check-layout` enforces the layout.

- Prefer domain types, clear ownership and the narrowest useful visibility.
- Return recoverable errors with `Result` and useful context. Validate untrusted
  inputs; avoid input-triggered panics and silent success defaults.
- Keep synchronous lock scopes short and guards out of `.await` points. Run
  blocking I/O through the owning execution domain.
- Add abstractions for real contracts or reuse. Document public behavior and
  non-obvious invariants; explain new unsafe blocks with `SAFETY:` comments.
- Use `cargo format` for Rust formatting and declaration spacing. Preserve
  public APIs, conditional compilation, embedded-file paths and source links
  when reorganizing code.

## Build and validation

Use the manifests and [SDK CI workflow](.github/workflows/native-sdk.yml) as the
source of truth for toolchains and feature modes. Keep dependencies centralized
in the workspace manifest and Cargo builds locked.

On the development machine, use a writable target directory on the workspace
volume, unique to this checkout. Never reuse or clean another checkout's artifacts.

```sh
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-<checkout-name>"
cargo format --check
cargo check-layout
python3 tools/check_native_dependencies.py
python3 tools/docs/check.py
```

Run tests and Clippy for the affected crates and feature modes. Use existing
SDK and fleet harnesses for lifecycle, durability and recovery changes. Follow
[testing and qualification](docs/19-testing-qualification.md) for broader checks;
[tooling instructions](tools/xtask/README.md) cover formatter and layout changes.
Do not enable `cellar_internal_tests` without its external harness.

## Change discipline

Read the affected code, callers and tests before editing. Keep changes focused,
preserve unrelated work, and update documentation when contracts change. Test
behavior at its owning boundary; never weaken assertions or suppress failures.
Report completed checks and their limits without overstating qualification.

Keep credentials and live fleet data out of source and output. Preserve license
notices. Reuse an active PR for follow-ups; start from updated `main` after merge.

For specialized work, consult the [Axum guide](crates/cellar-axum/README.md),
[LTX guide](crates/cellar-ltx/README.md), or
[event-delivery status](docs/design/outbox-implementation-status.md) as appropriate.

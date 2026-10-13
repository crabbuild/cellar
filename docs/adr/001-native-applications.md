# ADR 001: Compile application factories into native executables

Status: observed baseline. Source: `c24feb2`. Recorded: 2026-10-12.

## Context and decision

Cellar applications implement the Rust SDK and compile their factories into
native executables. The workspace excludes JavaScript engines, including
all-feature builds. The generic CLI builds/manages packages but cannot serve
uncompiled factories. Source JavaScript and previous Python/Wasm wrappers are
unsupported executable formats.

## Consequences

Teams need Rust build/package pipelines, compatible native targets, locked
dependencies and verified artifacts. Browser JavaScript in documentation is
independent of the Cellar application execution format. Containers can host
other languages through their own process/storage boundary.

Future proposals for untrusted plugins or source-code execution require a
separate architecture and isolation decision rather than extending loader
quotas into an implied sandbox. Verify dependency graphs and package format in
CI. See [packaging](../12-packaging-deployment.md),
[SDK](../../crates/cellar/sdk.rs), [dependency guard](../../tools/check_native_dependencies.py).

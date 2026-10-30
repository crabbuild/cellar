<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# ADR 001: Compile application factories into native executables

Status: observed baseline. Source: `c24feb2`. Recorded: 2026-10-12.

## Context and decision

Cellar applications implement the Rust SDK and compile their factories into
native executables. Native-only applications exclude JavaScript engines, including
all-feature builds of the native host. Optional language adapters are described
in the amendments below. The generic CLI builds/manages packages but cannot serve
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

## Optional CPython adapter (2026-10-19)

The [embedded Python adapter](../../crates/cellar-python/README.md) implements the
same compiled Rust application contracts using PyO3. It preserves this native
executable format and the JavaScript engine exclusion. It introduces trusted,
GIL-enabled CPython 3.14 execution above the SDK boundary, with invocation-scoped
bridges rather than direct access to Rust contexts. Source and SDK facade are
embedded in the executable; verified native packages can distribute and restore
the runtime/package closure. External OS libraries remain platform prerequisites.
The integration decision is recorded in [ADR 004](004-embedded-python.md).
This is not an isolation mechanism for untrusted code. See the
[design and outstanding qualification](../design/007-embedded-python-pyo3.md).

## Optional JavaScript/TypeScript adapter (2026-10-22)

[ADR 005](005-embedded-javascript.md) permits the explicit `cellar-javascript`
adapter to embed V8. The native host and decision core remain engine-free.
TypeScript and compatible npm packages are compiled into the executable; the
native executable format and digest verification remain authoritative. The
JavaScript build opt-in does not permit other engines or imply Node compatibility.

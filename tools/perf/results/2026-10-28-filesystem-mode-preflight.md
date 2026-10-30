<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Filesystem mode-option precedence, 2026-10-28

The O13/O25 audit reproduced an API contract defect on baseline
`47de03ec511ab028cd8b8a1b10d5e068d14d12e1`: unsupported `chmod` mode bits
returned EROFS under a read-only class policy. The regression expected
EOPNOTSUPP. Validation happened in the SQLite mutation callback, which the
read-only policy rejected before evaluating its options.

`chmod` and `fchmod` now validate supported mode bits after authority admission
and before the read-only mutation gate. The backend retains defensive validation;
`mkdir` shares the same private validator. Public signatures, supported bits
(`0o777`), handler budgets and transaction limits are unchanged.

The [structured record](2026-10-28-filesystem-mode-preflight.json) pins source,
executable and log hashes. Raw evidence and frozen native, Node/V8 and Python
executables are retained in the checkout-specific workspace target under
`qualification/mode-preflight/`. This is macOS arm64, Rust 1.98.1, Node 24.13.1
and CPython 3.14.5 GIL/free-threaded, using unoptimized builds without debug
information. Independent runtime fixtures ran serially.

## Observed behavior

| Profile | Result and assertions |
| --- | --- |
| Before fix | Native read-only regression failed at `filesystem.chmod`: EROFS, NotApplied, zero completed bytes |
| Native SDK | All three integration tests pass; supported read-only mutations retain EROFS, unsupported modes produce EOPNOTSUPP without changing bytes, metadata, usage, offsets or cookies |
| Native authority/transaction | Retained views, mismatched nested transactions and expired descriptors retain their authority errors despite unsupported modes; both metadata refusals roll back preceding business writes |
| Native units | All 21 tests matching `filesystem` pass, including storage quota, GC, format, inspection and local disk-full cases |
| Node and embedded V8 | All six filesystem integration cases pass, including reopened read-only state, normal POSIX/transaction/recovery behavior and bounded result admission |
| CPython GIL | Fresh ABI-linked executable passes read-only and normal SDK/reopen profiles |
| CPython free-threaded | Separate frozen ABI-linked executable passes the same two profiles; runtime reports `free_threaded=true`, `gil_enabled=false` |

Native unsupported-mode cases cover sticky, setgid, setuid, file-type bits and
`u32::MAX`, under writable and read-only policies. Guest read-only cases cover
setuid and the maximum unsigned mode. Each requires the public operation,
EOPNOTSUPP, NotApplied and zero confirmed bytes. The fixture reopens the writer's
persisted cell with a read-only class, invokes the assertions twice and verifies
joined shutdown. Python executables are frozen before changing the build ABI;
linkage and runtime profiles are recorded separately.

The initial library filter used the physical source path
`storage::sqlite::filesystem` and matched zero tests. That probe is retained as
non-qualification. The corrected public-module filter `filesystem` runs the
21 named tests in the record.

## Repository checks

Strict all-target Clippy passes for `cellar`, `cellar-javascript` (Node and
embedded V8) and `cellar-python` (embed), with warnings denied for those targets.
Formatting, module layout, native dependency checks, the documentation checker
and diff whitespace checks pass. All 106 local file references in the four
affected filesystem documents resolve.

## Scope

This checkpoint resolves the identified mode-option defect and verifies affected
SDK behavior. It does not establish a complete per-operation error matrix or a
production capacity envelope. The installed-package, S3, dense-capacity and
broader proof reports retain their previously recorded sources and artifacts;
they were not recaptured for this metadata validation change. Optimized Linux
artifacts, physical faults, sustained load and deployment objectives remain in
the [release ledger](../../../docs/design/per-cell-filesystem-implementation-status.md).

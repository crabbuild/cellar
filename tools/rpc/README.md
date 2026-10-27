<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Named RPC codec and qualification tools

Implementation of [plan 009](../../plans/009-named-typed-rpc.md) is in progress.
The [status ledger](../../plans/named-typed-rpc/implementation-status.md) records
the delivered boundaries and remaining language/fleet tests.

## SDK source and receiving peer saturation

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite saturation --report .cellar/rpc/saturation.json
python3 tools/rpc/check_saturation.py .cellar/rpc/saturation.json
```

This runs 112 cases: all 16 Rust/Python/V8/Node generated caller/provider pairs,
source quotas over local/authenticated peer routing, receiving quotas over
authenticated peer routing, and post-commit reply-byte refusal over peer routes.
Source and receiving request admission have independent count and byte cases.
V8 uses the generated TypeScript binding; Node uses plain JavaScript.
The byte case retains the default 128-call quota and reduces only the byte
allowance to 128 KiB. The count case retains the default byte allowance.

```text
generated Rust holder ----> reserve SDK quota ----> native driver barrier
                                  |                         |
generated client ---------> quota exhausted                 | release
                            ResourceExhausted                v
                            not_committed           original provider + proof
                                  |                         |
                            no business entry         reservation retires
                                  |
                     explicit receipt retry --> one mutation + saved result
```

Receiving overload uses `ResourceExhausted/unknown`. Its paired host transport
status/header carries a fixed category without rollback evidence or automatic
replay. Source refusal before submission remains `not_committed` for that child
call. Event calls preserve ambiguous outcomes for receiving overload.

The barrier observes the real owner, scope, epoch and closed transaction before
calling the original provider. While it holds capacity, a second generated
client must be refused without constructing a provider or entering business
code. After release, the holder finishes, reservations drain, and explicit
same-ID retries apply the refused command once; a changed command conflicts.
Every node joins, including after a failed assertion. Receiving requests,
reserved input bytes, tunnels and peer entries retire to zero. The separate auditor
checks coverage, authority, counts, quotas and zero retained reservations and
rejects seventeen corrupted reports. The suite also runs in the combined qualifier
and Linux lifecycle CI.

The sixteen reply cases hold a generated call from each actual caller runtime.
After release the original provider durably commits a count and receipt, then
returns a valid 64-KiB business-error message. The combined receiving quota
refuses that payload with `ResourceExhausted/unknown`. Host observations verify
codec-valid length, stored count, closed transaction and write position. No
automatic retry enters the provider. An explicit same-ID retry returns the
saved count, while the separately refused command and its receipt/conflict
checks retain their original observations. Final request/reply/tunnel
reservations must be zero.

Receiving bodies use a dense buffer whose capacity and possible native/JSON
copy are reserved before growth. Partial uploads count against admission;
uploads have an independent host deadline. Per-host/per-peer tunnel limits and
an idle timeout bound pooled connections. Fragments do not accumulate list
nodes, and empty chunks yield to timers/replies. Socket and dense-buffer tests
also exercise quota recovery, idle retirement and native ticket retention.

The upgrade carries the receiving idle interval as canonical positive
milliseconds. Sources limit cache reuse to its first half, starting at dial;
clean reply EOF cannot renew an aging socket. This leaves time for the next
submission to reach the receiver before its idle timer expires. Expiry never
cancels an active exchange or grants permission to replay an uncertain one.
The original source domain's clock controls reuse, including after shutdown;
missing or malformed interval metadata refuses a mixed or invalid peer before
producing a business-call sender. Socket regressions reproduce the held-request
race, late reply retirement, metadata refusal and stopped-owner cleanup.
Receivers also require one version-1 reuse capability before admission; neither
side silently falls back to the previous pooling contract.

Reply admission follows each yielded transport buffer and its slices/clones,
without copying the payload. Once a handler result is ready, the original
caller deadline and peer transport interval cap delivery. A stalled socket
closes within that budget; it cannot turn a lost reply into rollback evidence.
Refunds use the receiving domain's clock, including late buffer drops after
shutdown. Published payload lengths now share receiving input/copy/control
capacity. Known body bounds reserve before headers; actual cumulative frame
lengths are checked as well. The single merged permit follows every frame alias
until its final owner drops. New frames refuse a stopped owner or an expired
delivery deadline without requiring an ambient execution domain.

Before headers a reply-capacity refusal returns `ResourceExhausted/unknown`;
a later frame refusal breaks the body. A handler may already have committed,
so neither path authorizes automatic replay or establishes rollback. Length
reservations do not measure shared backing allocations or exact allocator/RSS
usage. Native allocator overhead, response copies, decoded graphs, guest heaps
and broader flood/disconnect/cancellation matrices need further qualification. See the host settings in
[configuration](../../docs/11-configuration.md#library-defaults).

## Guest call metadata refusals

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite guest-control --report .cellar/rpc/guest-control.json
python3 tools/rpc/check_guest_control.py .cellar/rpc/guest-control.json
```

The 48 cases cover Python, V8 TypeScript and Node JavaScript callers × all four
providers × local/signed-peer routes × oversized or unknown metadata. Oversized
targets use the generated client; the unknown-field probe uses the same native
transport with an extra graph. Rust callers do not use this guest JSON boundary.

```text
guest call metadata + binary request
             |
      raw UTF-8 <= 16 KiB?
             |
      reserve decode allowance
             |
      flat, known, unique fields?
             |
      native routing -> provider -> proof

invalid control -> InvalidArgument / not_committed
                  zero provider visits or factories
fresh generated read -> original owner, same zero count
```

Both adapters share a bounded decoder before native dispatch. It rejects
oversized wire input before parsing or reserving, and unknown fields before
reading their values. Decode storage is reserved before parsing and held with
the native work; malformed input releases it. The original encoded payload is
charged separately. Python uses UTF-8 JSON for this flat control, so Unicode
target names spend the same wire allowance as JavaScript.

The independent auditor checks exact coverage, fixed refusal categories,
provider isolation, fresh-call authority and zero retained native reservations
after node join. This suite does not measure allocator overhead, engine heaps
or process RSS; both qualification flags remain false.

## Verify the loaded Python mode

Build the qualifier with the intended CPython 3.14 interpreter and its pinned
Protobuf environment. Use a checkout-specific Cargo target as described in
[AGENTS.md](../../AGENTS.md); keep GIL and free-threaded artifacts separate.

```sh
export PYO3_PYTHON=/path/to/python3.14t
export CELLAR_RPC_PYTHON_SITE=/path/to/venv/site-packages
cargo build --locked -p cellar-rpc-qualification --features full

"$CARGO_TARGET_DIR/debug/cellar-rpc-qualification" \
  --suite all --python-mode free-threaded --report .cellar/rpc/all-ft.json
python3 tools/rpc/check_python_mode.py --expected free-threaded .cellar/rpc/all-ft.json

# The portable profile needs a separate process, as Python initializes once.
"$CARGO_TARGET_DIR/debug/cellar-rpc-qualification" \
  --suite profile --python-mode free-threaded --report .cellar/rpc/profile-ft.json
python3 tools/rpc/check_profile.py .cellar/rpc/profile-ft.json
python3 tools/rpc/check_python_mode.py --expected free-threaded .cellar/rpc/profile-ft.json
```

Use `--python-mode gil` with the GIL build. Without this option the qualifier
expects its compiled interpreter mode. CI always passes the expected mode
explicitly, and runs both `3.14` and `3.14t`, including the full Counter/failure
suite and separate portable profile. The original handler/operation budgets
remain in force.

```text
Python imports application + generated codecs
                  |
       observe actual loaded CPython
       version / SOABI / GIL / Protobuf
                  |
       run generated RPC/failure matrix
                  |
       observe again: identity unchanged?
                  |
       join Python, V8 and Node; census bytes
                  |
       publish report -> independent auditor
```

The observations attach to the interpreter owned by `PythonRuntime`; they do
not inspect another Python executable. The report's `python_runtime` contains
`expected_mode`, `startup` and `after_matrix`. A free-threaded ABI with the GIL
enabled is refused before business work. Completed reports also require joined
language runtimes and zero retained bridge reservations. Private crash and
supervision children only record startup; that cannot pass a completed-matrix
mode audit.

The mode auditor rejects missing, changed or mismatched identity and cleanup
evidence, including integer values masquerading as booleans. It checks mode
and cleanup separately from case coverage: each suite's assertions and case
auditors must also pass. A successful mode check does not establish full
resource, packaging or release qualification.

## Native storage and lease progress

```sh
cargo test --locked -p cellar --lib local_store:: -- --test-threads=1
cargo test --locked -p cellar --test native_local_store -- --test-threads=1
```

The checkpoint regression pauses a real main-database sync during an unrelated
put or delete. A following lease CAS must finish within its original 333 ms
attempt window and be readable before checkpoint I/O is released. The fixture
leaves FULL WAL syncs enabled, retains the held put's native I/O reservation
until cleanup, and joins entered work before retiring its private VFS. The
public bucket race requires exactly one winner among 32 same-token updates,
then checks the stored body/token and stale-token refusal after reopen.

```text
fair writer admission -> SQLite write transaction -> release admission
                                                       |
                                         FULL WAL commit -> checkpoint sync
                                                       |        (held)
next lease CAS -> SQLite write transaction -> FULL commit + visible readback
```

SQLite serializes each transaction through COMMIT. Cellar's admission mutex
must not serialize the later checkpoint I/O as well. This follows SQLite's
[WAL concurrency and checkpoint behavior](https://sqlite.org/wal.html).
Neither a timeout nor a lost reply establishes rollback or permits replay.
These controlled regressions address a specific storage bottleneck; fresh
hosted and original-budget adapter regressions remain required.

## Generate the shared contract

Run from the repository root with the repository Rust environment, protoc 34.0
and Node 24+ on PATH:

```sh
npm ci --prefix tools/rpc --ignore-scripts
protoc -I plans/named-typed-rpc --include_imports \
  --descriptor_set_out=tools/rpc/fixtures/counter.binpb counter.proto
cargo run --locked -p cellar-rpc-codegen -- \
  --descriptor tools/rpc/fixtures/counter.binpb \
  --out tools/rpc/fixtures/generated \
  --languages rust,python,typescript,javascript
```

`fixtures/generated/` contains one descriptor/digest, native inventory, Rust
services, Python messages/stubs/services, TS messages/services and plain JS
messages/services with declarations. The [generator guide](../cellar-rpc-codegen/README.md)
explains tool/runtime pins and the `--javascript-tools` option.

Check the generated artifact before compiling a provider:

```sh
cargo run --locked -p cellar --bin cellar -- rpc check \
  --descriptor tools/rpc/fixtures/generated/contracts.binpb \
  --inventory tools/rpc/fixtures/generated/rpc.json
```

This reads local artifacts and needs no compiler or live node. Add `--catalog`
to validate a native registration snapshot, as shown in the
[CLI reference](../../docs/23-cli-reference.md#offline-typed-rpc-checks).

## Build and run real named methods

```text
Counter schema + immutable descriptor
                 |
           generated clients
                 |
       Rust / TypeScript / plain JS
                 |
           native cell routing
                 |
       Rust / V8 / Node providers
                 |
      atomic SQLite count + receipt
                 |
        output durability gate
```

Build the fixtures, then run the real-host test:

```sh
npm ci --prefix tools/javascript
npm run build --prefix tools/rpc
cargo test --locked -p cellar-javascript --features embed,node \
  --lib --test typed_rpc --test node --test node_runtime
```

[app.ts](app.ts) implements the full Counter with exact i64 arithmetic and
checked atomic receipts. [v8.ts](v8.ts) uses generated TypeScript services;
[node.mjs](node.mjs) uses the independently generated plain-JS services.
The build emits an enclosed V8 bundle and Node modules into ignored `dist/`.
It strictly checks application code and both generated API forms before emit.

The [real-host test](../../crates/cellar-javascript/tests/typed_rpc.rs) runs all
nine Rust/V8/Node caller-provider pairs through named business methods. It checks
large i64 values, business rejections, duplicate/conflicting IDs, distinct Unicode
and leading-BOM IDs, refused private helpers, expired clients and a committed
write with an invalid reply. A second native node startup reads the same SQLite
files and replays receipts without duplicate effects. Guest runtimes remain
supervised across those two node starts; this is not a process-crash test.

The same test injects slow verification, encoding and decoding for each
V8/Node caller against Rust, V8 and Node providers across both starts: 36 probes.
Verification/encoding expiry must leave count zero. Decode expiry must return
`DeadlineExceeded/unknown` after one saved increment; explicit same-ID retries
recover the original receipt, including after restart. Incoming parent caps,
foreign-budget refusal, bounded control reuse and admission with a held budget
are checked with `max_pending_operations = 1`.

```text
generated call
     |
original native budget -- host/parent cap
     |
verify -> encode -> binary bridge -> native route/proof -> decode
     |                       |                            |
same deadline          opaque native handle          same deadline
```

V8 uses a native clock callback. Node's private control charges its IPC round
trip and one native-clock tick conservatively; no absolute timestamp crosses
clock domains. Control capacity is separate from SDK mutation/operation slots,
and release/retirement reclaim it. Nine SDK/control regression tests cover
pre-codec expiry, remaining-time subtraction, decode ambiguity, options captured
before the Node wait, foreign scope and cleanup. The signed two-node deadline
suite below adds explicit guest remote/nested and codec-expiry evidence.

## Codec and generated API checks

Native startup and generation share the generator's
[descriptor admission rules and budgets](../cellar-rpc-codegen/README.md#descriptor-admission).
Global symbols and import visibility are validated before output files or
language compilers; valid fixture descriptor bytes and inventories remain stable.

```sh
npm test --prefix tools/rpc
npm run typecheck --prefix tools/rpc
```

The tests cover message presence, empty oneofs, exact integers, Unicode/BOMs,
malformed wire data, graph budgets, invocation-bound clients, options and explicit
registration. Generator tests exercise the plugin and generated JS code. Strict
typechecks require actual errors for numeric i64 inputs and undeclared methods.

The SDK codec receives the generated package's pinned Protobuf runtime. It
preserves leading BOMs and refuses malformed Unicode before encoding. Embedded
V8 installs native text-encoding globals before descriptor initialization;
Node uses its normal globals. No JSON projection is used for business payloads.

```text
application initializer
       |
descriptor fields + exact scalar types + own data properties
       |
depth / wire occurrences / decoded layout slots / complete encoded size
       |
owned container snapshot -> create() -> binary encoding
       |
native wire preflight -> existing Cellar routing
```

Wrong JS types, unknown field names, malformed oneofs, getters and sparse/cyclic
containers fail before the message constructor or binary writer runs. Size
checks include negative integer varints, tag/length overhead, packed elements
and synthetic map entries. Tests also cover wrapper scalars and the expanded
`Struct` wire graph used by Protobuf-ES's field mappings. A shared set of
[wire vectors](fixtures/message-vectors.json) checks bytes, decoded values and
exact graph-budget boundaries in JavaScript, native Rust and Python.

The JS codec accepts but discards unknown wire fields, and never serializes
caller-built `$unknown` metadata. It is not a lossless forwarding API. Native
wire validation remains a prerequisite to decoding; runtime heap/process limits
and retained bridge-copy accounting remain separate resource boundaries.
Incoming limits apply to the bytes received. A decoder must not reuse its own
encoder's occurrence/depth costs: Prost can omit default map keys/values that
Protobuf-ES writes explicitly. A regression test decodes that valid Rust wire
shape at its exact native budget, while outgoing encoding still checks its own
complete cost.

Wire preflight also charges estimated decoded slots: one object plus all
declared fields for each message, each wire occurrence/packed element, and the
flat layout of any omitted message-valued map default. This catches default
allocation amplification from repeated empty messages before decoding. Outgoing
Rust/JS/Python graph checks use the same estimate before allocating encoders.
The default is 1,000,000 slots; `max_decoded_slots` in the native catalog becomes
`maxDecodedSlots` in JS and `max_decoded_slots` in Python. Shared vectors assert
exact boundaries. This estimate does not establish a process RSS bound.

The native worker snapshots the application catalog once. Generated native
clients and router response encoders use matching declarations instead of their
embedded defaults; consumer-only Rust apps can use `Registry::rpc_contract`.
Framework refusal envelopes have a fixed control budget so small business limits
cannot turn a valid refusal into an invalid response.

Native framework reply conversion validates the borrowed envelope, digest and
control wire before allocating a business buffer. A successful result copies
only its business body; wrong digests, invalid controls and oversized bodies
are refused without copying that body. Failure text has a fixed pre-decode
allowance. The allocation regression measures new allocator layouts separately
from the already-owned input:

```sh
cargo test --locked -p cellar --test native_rpc_reply_allocation
```

This guard covers framework reply conversion. Business-message decoders, queued
responses, shared backing allocations and process RSS require separate evidence.

Ordinary peer value replies also use dense collection and yield after 64 frames
or 64 KiB, including empty/trailer floods. They retain the generic forwarding
ceiling; event controls keep their stricter 64 MiB ceiling. Oversize/allocation
refusal after a successful ordinary reply head is `ResourceExhausted/unknown`,
without a network-retry tag. Real transport failures keep their original tag.
Typed callers additionally carry their declared response ceiling through their
native reservation. Peer collection refuses an oversized declaration before
polling the body, or stops streamed growth at that ceiling. The allowance is
the business byte limit plus 1 KiB for each of the Protobuf and clone controls.
Malformed or oversized typed replies remain `InvalidResponse/unknown`; typed
methods also refuse resource-lease replies. Untyped values keep their allowance.
Aggregate response/codec pools and native decoder layout bounds remain open.

The generated inventory now records structural codec shapes. Provider startup
compares live JS/Python descriptors with the native artifact before readiness;
clients verify their codecs before encoding. Tests alter real descriptor fields,
maps, packing, enum values, optional presence and oneofs while retaining the
copied digest and type names. They must fail verification. The JS build also
produces deliberately stale V8/Node entries; real runtime startup must refuse
them before admitting the host callback. Python's cold runner check refuses a
stale provider without invoking its constructor, and a stale client never
reaches serialization or submission. Static binding validation remains a
separate gate. See the [projection contract](../../plans/named-typed-rpc/registration-and-dispatch.md#structural-projection).

## Python

Run Python codec/client checks with the pinned dependency in a local environment:

```sh
python3.14 -m venv .cellar/rpc/python
.cellar/rpc/python/bin/python -m pip install protobuf==7.34.2
.cellar/rpc/python/bin/python -B tools/rpc/test_python_rpc.py
```

For static qualification, install the locked tool environment and check the
generated services and application-facing type assertions:

```sh
.cellar/rpc/python/bin/python -m pip install -r tools/rpc/requirements.lock
.cellar/rpc/python/bin/python -m mypy --config-file tools/rpc/mypy.ini
```

Generated services use static imports, concrete async result types and structural
handler protocols. The negative assertions must produce type errors for wrong
requests, return types, handler signatures and undeclared methods. The adapter's
[cellar_rpc.pyi](../../crates/cellar-python/python/cellar_rpc.pyi) describes the
module installed by the native host; `mypy.ini` adds its directory and generated
messages to the type search path. It does not add a second runtime SDK package.

The real-host test `cargo test --locked -p cellar-python --features embed
--test typed_rpc` uses that environment's site packages by default. Set
`CELLAR_RPC_PYTHON_SITE` to an absolute site-packages directory for another
supported Python 3.14 environment. It tests all four local Rust/Python pairs,
exact 64-bit values, atomic receipts, node restart and conservative failure
outcomes. It does not establish free-threaded or remote-fleet qualification.

The Python client mints a native-owned budget before validation/encoding and
passes that same opaque deadline with its bytes. Twenty-two Python unit tests
cover codec/client behavior, including requested, default-host and parent caps.

The real-host test injects slow verification, encoding and decoding against both
Rust and Python providers across two node starts. Verification/encoding expiry
submits no business call; decode expiry stays unknown and an explicit same-ID
retry recovers the saved receipt. It also checks a real incoming parent cap,
cross-invocation budget refusal and the captured clock from a native thread
without an installed execution domain. The V8/Node budget evidence is described
above; the signed two-node deadline suite below also exercises Python callers
and nested providers alongside V8 and Node.

The decode probe first activates its provider through a separate read-only
invocation, so cold startup cannot consume its 100 ms child budget before the
decoder runs. The test also runs in a fresh process with 120 ms Python
constructors. Both profiles retain the 250 ms host handler budget, exact
receipt checks and original expiry outcomes.

## Portable Protobuf profile

The [profile fixture](fixtures/profile.proto) covers all scalar wire types,
unknown enum numbers, packed/unpacked fields, recursive/nested messages, maps,
optional presence, real oneofs and the supported well-known messages. Reproduce
its checked-in artifacts with the same toolchain used for Counter:

```sh
protoc -I tools/rpc/fixtures --include_imports \
  --descriptor_set_out=tools/rpc/fixtures/profile.binpb profile.proto
cargo run --locked -p cellar-rpc-codegen -- \
  --descriptor tools/rpc/fixtures/profile.binpb \
  --out tools/rpc/fixtures/profile-generated \
  --languages rust,python,typescript,javascript
cargo test --locked -p cellar --test typed_rpc_profile
```

The separate live profile suite submits 18 shared vectors through all sixteen
Rust/Python/V8/Node generated caller/provider pairs, over both local and signed
peer routes (576 cases).

```text
18 vectors × 4 generated callers × 4 typed providers × 2 routes = 576

caller encode -> native preflight -> registered handler -> validated reply
      ^                                                       |
      +------------- caller decode and comparison ------------+
```

V8 uses TypeScript bindings and Node uses independently
generated JavaScript bindings. Native observations check the actual request and
returned message semantics, owner, scope, epoch and method. Each caller also
checks its decoded result; replies are never accepted from JSON summaries alone.

```sh
node tools/rpc/profile/vectors.mjs --check
node --test tools/rpc/profile-vectors.test.mjs
.cellar/rpc/python/bin/python tools/rpc/profile/check_vectors.py
.cellar/rpc/python/bin/python tools/rpc/test_profile_vectors.py
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite profile --report .cellar/rpc/profile.json
python3 tools/rpc/check_profile.py .cellar/rpc/profile.json
```

Run this suite in a fresh process: the embedded Python runtime permits one
initialization per process. `--suite all` retains the existing Counter suites;
profile qualification is an additional command and CI gate. The reference
[corpus](profile/cases.json) uses Protobuf JSON; regeneration produces canonical
JSON plus reference wire bytes for native authoring. An independent protoc
Python check validates both representations. Update intentional vectors with
`node tools/rpc/profile/vectors.mjs --write` and review both generated outputs.
The auditor checks exact coverage and identity, then rejects 22 corrupt reports.
Only a successfully completed live report establishes these 576 cases; corpus
checks alone do not. Parser abuse, interpreter-mode, durability/recovery,
resource/RSS and package qualification keep their separate gates.

The native test compiles the generated service using only SDK re-exports,
including the unit representation of `Empty` and the `prost_types` well-known
types. Native, JS and Python codec checks preserve integer boundaries, non-finite
floats, Unicode/BOMs, map keys, selected empty oneofs and explicit zero presence.
The generated TS, JS declarations and Python services pass static checks.

Rust graph checks run before recursive `encoded_len` or allocating writers.
Tests cover packed/unpacked budgets, optional/selected defaults, scalar-only
oneofs, omitted map defaults and complete byte overhead. Generated messages and
well-known `Value`/`ListValue` chains 20,000 levels deep are refused and cleaned
up on a 256 KiB stack. A codec spy panics if sizing/writing is reached after graph
refusal. The real native-node test also refuses an oversized request before cell
construction and an oversized response after durable count/receipt writes. The
response failure stays unknown; explicit same-ID retry recovers the saved result
across restart. These are additional encode/recovery boundaries, not completion
of all resource or fleet gates.

Native descriptor tests mutate the valid image to reject malformed map entries,
illegal keys and references, orphaned entries, invalid synthetic oneofs, enum
aliases/reservations and invalid field reservations/packing. These are schema
and codec checks; fleet, crash and resource qualification remain separate gates.
Non-default `jstype` options are refused before generation or host startup so
JavaScript 64-bit values always use `bigint`.

## All 16 local backend pairs

After the JS build and Python environment setup above:

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --report .cellar/rpc/local-matrix.json
```

This opt-in [executable](host/main.rs) owns Python on the process main thread,
starts supervised V8 and Node runtimes, then composes four providers into a native
application. Every Rust/Python/V8/Node caller reaches every provider through
generated named methods. It checks exact integers, atomic receipts, distinct
Unicode/BOM operation IDs, overflow, private-method refusals, expired clients and
invalid replies after writes. Two native node starts recover the same state and
receipts. Guest runtimes stay alive across those node starts.

The JSON report is published only after native shutdown proofs and all runtime
joins succeed. It records the exact contract digest and explicitly leaves remote
and abrupt-crash qualification false. A failed rerun removes its previous report.
The `full` feature selects both optional adapters here; native-only applications
retain no JavaScript engine dependency.

## Authenticated remote calls, handoff and concurrency

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite remote --report .cellar/rpc/remote-matrix.json
# Run every delivered suite, including Unix process-crash qualification:
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite all --report .cellar/rpc/full-matrix.json
```

The remote suite uses the production `NodeFleet`, signed peer transport, node
leases, owner log and replication services with a local development bucket.
Fleet durability is enabled. Three node starts form two successive two-node
fleets; the language runtimes stay supervised in the same process:

```text
phase 1   ingress B -- authenticated RPC --> owner A
               all 16 caller/provider pairs
                              |
                    A drains with Handoff
                              v
phase 2   ingress C -- authenticated RPC --> owner B
               all 16 pairs + recovered receipts
                              |
                   concurrent commands and races
```

Every pair runs the same business/failure corpus as the local suite. A native
observer records actual ingress and cell execution domains, registered method
names, scope and epoch. It verifies all 16 pairs execute remotely, then verifies
scope identity is unchanged and epochs advance after handoff. Two adversarial
guard tests reject incomplete/local/cross-scope/cross-epoch observations; a URL
or a requested target alone cannot certify remote execution.

After handoff, all four callers race eight distinct commands against each
provider. The exact total must equal the sum of their deltas, and replaying each
ID must return that call's saved result. Eight calls then race one shared ID;
all receive one saved result and only one mutation is applied. A changed delta
must conflict. The observer also checks each concurrent operation stays on the
same owner, scope and epoch.

Reports are published only after every native node drains and every language
runtime joins. A non-async dispatcher constructs one heap-owned suite future at
a time, so the combined driver does not retain every suite’s debug temporaries
on the Python host stack. The host stack, executors and timeout budgets remain
unchanged. `--suite local` is the default; `--suite all` writes separate
`local`, `remote`, `peer_faults`, `deadline`, `cancellation`, `cycles`, `provenance`, `proof_wait`, `recovery`, `crash`
and `proof` evidence objects. `remote_matrix_verified` and
`remote_matrix_after_handoff_verified` record these measured boundaries. Full
fleet qualification remains false: further interpreter modes, resource limits
and the full lifecycle matrix are separate requirements. The dedicated proof suite below
tests withheld/failed RPC output proof. This run uses normal GIL CPython; it is
not free-threaded evidence.

## Generated clients under peer faults

After building the same locked fixtures and Python environment:

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite peer-faults --report .cellar/rpc/peer-faults.json
python3 tools/rpc/check_peer_faults.py .cellar/rpc/peer-faults.json
```

The **112-case suite** uses generated clients: Rust, Python, embedded V8 and Node callers
against each of the four providers, at each fault below. The owner keeps real
SQLite state, signed peer authentication and normal fleet durability proof.

```text
generated client -> native source -> relay -> signed owner -> typed handler
                                       ^                       |
                                       |                  count + receipt
                                 observe real reply <---- output proof
                                       |
                         forward / drop / trusted refusal
```

| Fault | Required result | Independent observations |
| --- | --- | --- |
| Close a warm RPC tunnel | Fresh signed upgrade; successful generated call | Exact method, request ID, remaining budget, scope and owner epoch; one handler entry |
| Drop a complete successful owner reply | `Unavailable/unknown`; no automatic replay | Closed mutation transaction and exact stored count; one handler entry before explicit retries |
| Replace a proven reply with an oversized declared body; withhold its bytes | `InvalidResponse/unknown` before the original deadline | Real owner commit/receipt observed first; no replay, successful explicit receipt retries |
| Stream beyond the caller's reply ceiling; withhold the terminating chunk | `InvalidResponse/unknown` without waiting for the suffix | Same committed owner evidence; source enforces its limit while the body grows |
| Change the inner method to an unregistered name | `MethodNotFound/not_committed` from the real owner | No additional factory or business-handler entry; unchanged count before explicit retry |
| Add valid unknown fields beyond the receiving element cap | `ResourceExhausted/not_committed` | Caller validation still passes; five occurrences exceed the owner's four-element limit, within its byte limit; no handler entry |
| Add one valid unknown byte field beyond the receiving byte cap | `ResourceExhausted/not_committed` | Caller validation still passes; three occurrences stay below the element cap, but exceed the owner's 128-byte limit; no handler entry |

The owner snapshots a stricter native catalog (128 message bytes and four
Protobuf occurrences) than the source's default limits. Its real Rust, Python,
V8 and Node providers retain their generated implementations. The relay changes
one admitted wire request or one observed, proven reply. It validates the request against the source contract and
preserves its method, operation ID, request ID, scope and remaining deadline.
The independent report auditor checks the actual byte/element geometry and
rejects twelve corrupted-coverage, admission, response-limit, provenance and cleanup reports.

Host admission borrows the encoded body instead of decoding a clone graph and
copying its buffer merely to inspect it. It bounds envelope metadata to 1 KiB,
checks the receiving message limits, then allows an admitted handler to own its
request. Size refusal keeps its exact-call provenance through the normal output
gate. A refusal after factory entry remains `unknown`.

Each case then explicitly retries the original operation ID twice, requires the
saved result and exactly one total mutation, and refuses a changed delta with
that same ID. Native audits verify execution stays on the original remote owner,
scope and epoch. The relay checks the actual descriptor digest and Protobuf
request/reply; a requested target or a guest error string cannot certify a case.

The relay bounds observed inner headers to 16 KiB, ordinary bodies to 64 KiB and
connection tasks to 128. The streamed overflow uses a fixed 4 KiB scratch buffer
and sends only the declared caller ceiling plus one byte. Its selected response
may use chunked framing; other observed traffic must use full-body framing.
Ordinary outer HTTP requests ask
the real server for `Connection: close`: an unrelated keepalive must not later
hide an RPC upgrade inside an unobserved byte stream. Signed upgrade headers,
request signatures, payloads and proof responses retain their production paths.
These are fixture bounds, not evidence of production-wide memory limits.

Successful reports require both native nodes to drain and every relay task and
language runtime to join. The independent auditor verifies all 112 unique tuples
and rejects twelve corrupted reports covering missing/duplicate pairs, forged
rollback, business execution after refusal, invalid budgets and retained sockets.
This suite runs in `all` and Linux lifecycle CI, which retains its raw report/log.

The described reconnect, reply-loss and receiving-refusal boundaries have both
GIL and free-threaded evidence; the ledger records the source and pending
qualification of the response-limit additions. The deliberate warm-tunnel close does not
force every provably-unsent Hyper race; the native transport regression separately
checks that retry preserves identity and decreases its original budget. Full
admission/saturation, parser/RSS, remaining profile/interpreter modes and
packaging qualification remain open.

## Failures after a parent commits

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite provenance --python-mode gil --report .cellar/rpc/provenance-matrix.json
python3 tools/rpc/check_provenance.py .cellar/rpc/provenance-matrix.json
cargo test --locked -p cellar --test native_rpc_failure_provenance -- --test-threads=1
```

The T06 matrix executes **112 cases**. Its original 96 cover all 16 Rust/Python/V8/Node
caller/provider pairs, local and signed-peer ingress, and three failures after
the provider syncs its atomic count and receipt. Sixteen additional cases use
every generated caller against a custom native cell that returns a raw envelope.

| Failure inside the parent | Parent's caller receives |
| --- | --- |
| Generated child call expires before its own submission | `DeadlineExceeded/unknown` |
| Exception claims `MethodNotFound` and `not_committed` | `MethodNotFound/unknown` |
| Exception has an unrecognized private code | `Internal/unknown` |
| Native cell fabricates a raw `not_committed` reply | `MethodNotFound/unknown` |
| Native cell forwards an honest host refusal after its own write | `MethodNotFound/unknown` |

```text
generated Increment -> parent commits exact i64 count + operation receipt
                              |
                    fail after that durable write
                      /                   \
    real child DeadlineExceeded       forged exception metadata
        / not_committed                    |
                      \                   /
                    parent effects: unknown
                              |
       two explicit same-ID retries -> saved result, unchanged count
```

The child uses a zero timeout: it never enters a business factory or handler.
Its refusal is call-local evidence. The original parent must still read its
own committed state before throwing, proving child expiry did not revoke the
parent. Forged outcome accessors must never run. Fixed categories cross the
guest completion channel; private text, unknown codes and outcome fields do not.
The native host also checks raw typed outputs from custom cells. Only its own
catalog refusal before business construction may retain `not_committed`.

```text
typed request -> host catalog
                   |
           +-------+----------------+
           |                        |
     reject before factory     construct/invoke application
           |                        |
     not_committed           generated or raw failure reply
                                    |
                              host: unknown effects
                                    |
                              normal output proof
```

Raw failure normalization borrows the existing response buffer. Only a valid
framework failure within the fixed 1 KiB control allowance is decoded and
rewritten; successful business buffers retain their ownership. Non-minimal
varints cannot bypass this rule. Malformed controls still fail as invalid
responses, and untyped calls retain their original value behavior.

Counts exceed JavaScript's exact-number range and are checked as canonical
64-bit values. Native observations verify the actual caller node, provider
owner, scope, epoch, six warmed factories and exactly three Increment entries
(one original plus two explicit retries). No refused child is constructed and
no mutation is automatically replayed. Reports publish after native/guest drains.
Both raw-test objects are warmed before the peer joins, so caller location
cannot silently change which route the test exercises. The standalone native
regression also reopens the node and verifies durable receipts and safe retries.

The independent auditor checks all pairs and exact counts and rejects missing
coverage, false rollback, altered child proof, duplicate mutations, extra
factories, changed authority and incomplete cleanup. The same check accepts a
full `--suite all` report. Use `--python-mode free-threaded` with the corresponding
interpreter build. Broader peer/admission provenance, resource/RSS, descriptor
and package qualification retain their separate gates.

## Deadlines and cancellation during native construction

```sh
cargo test --locked -p cellar --test native_typed_rpc cold_factory:: -- --test-threads=1
```

The [cold-factory tests](../../crates/cellar/tests/support/rpc_cold_factory.rs)
hold a real synchronous native factory until its caller has observed either
`DeadlineExceeded/unknown` or `Cancelled/unknown`:

```text
generated caller ------> cold native factory (held)
       |                           |
 deadline / signal                 |
       |                           |
 caller receives unknown           |
       |                     release factory
       |                           |
       |              new/cached SDK operations refused
       |              generated business handler not entered
       |                           |
 fresh call -----------> read count 0; no late writes
 explicit retry -------> count 1 + receipt
 same-ID retry --------> count 1
```

The factory verifies storage works before the hold, then checks new storage,
new child bindings, a cached write and a synchronous transaction after release.
Both tests join the node and require exactly two explicit increment-handler
entries. Construction has already entered, so the caller's failure cannot
promise rollback. The deadline case uses a 200 ms child budget under a separate
three-second parent policy; existing language-adapter tests keep their 250 ms
host budget.

These two public-node tests qualify the native construction boundary. They are
separate from the cross-language matrices below and do not establish the cause
of the intermittent streaming cold-start timeout.

## Peer deadlines and nested calls

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite deadline --report .cellar/rpc/deadline-matrix.json
```

The [peer budget adapter](../../crates/cellar/node/rpc_deadline.rs) sends a
remaining duration immediately before transmission. Tunnel establishment and
provably unsent retries consume the original budget. The receiver derives its
own deadline before collecting the body and caps it by its host policy:

```text
Node A: local deadline 12,300 ms
    |
    +-- open/reuse tunnel; current local time 12,080 ms
    |
    +-- x-cells-rpc-budget-ms: 220 -------------------------+
    |                                                     |
    |                          Node B: local clock 900 ms  |
    |                          host policy permits 100 ms <+
    |                          receiver deadline = 1,000 ms
    |                                  |
    |                          body -> route -> handler -> proof
    |                                  |
    |                          nested call uses remaining budget
    |
original caller timer stays authoritative
```

No domain timestamp crosses the peer boundary. Independent execution-domain
timers cover receiver upload, routing, handler and output-proof waits. Expiry
uses a fixed host status/header and returns `DeadlineExceeded/unknown`; it
does not prove rollback or authorize replay. Existing event-stream RPC retains
its separate long-poll policy.

The deadline fixture uses real signed tunnels between nodes whose execution
domains start at different times. Its report contains 55 cases:

| Group | Cases | Boundary |
| --- | --- | --- |
| Native callers | 3 | Direct, nested and receiver-capped deadlines |
| Python, V8/TS and Node/JS callers | 12 | Local, remote, receiver-capped and nested guest-provider calls |
| Remote guest codecs | 36 | Three guest callers × four providers × verification/encoding/decoding |
| Node control acknowledgements | 4 | An options getter blocks the event loop while the private control reply is queued |

The [native guest audit](host/deadline/guests.rs) drives
[JS/TS fault controls](deadline.ts) and [Python fault controls](deadline.py).

The three native cases retain these checks:

| Case | Caller budget | Deliberate overrun | Required evidence |
| --- | --- | --- | --- |
| Direct remote call | 400 ms | 800 ms application poll | Late client binding, storage access, cached write and commit are refused |
| Nested call requesting 60 seconds | 400 ms parent | 800 ms child poll | The child inherits the parent deadline |
| Receiver host cap | 5,000 ms | 2,200 ms application poll | Receiver caps the budget at 1,500 ms and returns the fixed timeout code |

Each case checks the actual owner node, scope and epoch. A fresh call then
confirms no late write persisted, the expired transaction rolled back and
storage is still usable. The fixture drains both nodes even on audit failure.
The guest caller cases use the same blocked native targets and late-operation
checks. Nested cases enter a real guest provider, spend 120 ms in codec
verification, then request a 60-second child timeout:

```text
source: guest client, 400 ms
          |
      signed peer
          v
owner: guest provider, incoming cap <= 400 ms
          |
      verify for 120 ms
          |
      child requests 60,000 ms
          v
owner: native child records its actual remaining budget
          |
      block for 800 ms -> late write and commit refused
```

Native observations at child entry must show at least 80 ms consumed relative
to the parent's captured native budget. This proves budget inheritance even if
caller cancellation would otherwise mask a reset child timer. Retained and new
guest RPC clients and storage writes are refused after parent expiry. Fresh
invocations confirm no late write persisted and admission is usable. The
observer checks the original owner, scope and epoch throughout.

Remote codec expiry before submission must produce zero increment-handler
entries and leave the counter unchanged. Decoder expiry follows a saved write
and returns `DeadlineExceeded/unknown`; explicit same-ID retries recover the
receipt. The Node control probe must expire before codec verification starts.
These are real-clock and signed-peer tests with GIL CPython. They do not qualify
every interpreter, forced peer retry, proof/lifecycle wait or full T14/T17 boundary.

Native transport tests also exercise a stalled upload with no caller timer,
malformed/zero budgets, ordinary unbudgeted calls and a real Hyper unsent retry
whose identity/body stay unchanged while its duration shrinks.

## Explicit cancellation authority

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite cancellation --report .cellar/rpc/cancellation-matrix.json
```

Native `CallOptions.signal` now reaches host dispatch directly. Cancelling a
call can revoke its SDK authority while either application worker is occupied:

```text
caller AbortSignal / dropped call
              |
         source host wait
          /           \
   local lifetime     signed peer abort (original request ID)
          |                    |
          +-----------> callee lifetime
                               |
                  permanent revocation flag
                    /                    \
       Context + cached storage      pending nested calls
       refuse writes and commits     local flags / peer aborts
```

The [host cancellation authority](../../crates/cellar/runtime/cancellation.rs)
is an opaque Rust capability, separate from the driver's consumable wakeup.
Taking or clearing a wake cannot revive a cancelled context. Descendant
revocation is iterative; weak registrations do not retain completed calls.
Cancelling a child leaves its parent and siblings live. No guest JSON field
can manufacture or clear this authority.

When a callee fixes its reply, its link to the caller detaches. A late caller
abort cannot cancel valid callee `waitUntil`/native I/O; that work remains
subject to its own deadline, activation and driver lifetime. Driver retirement
permanently revokes its authority. HTTP request-abort delivery and event-stream
long-poll budgets retain their existing contracts.

| Caller case | Required evidence |
| --- | --- |
| Local signal while a poll blocks | Host cancellation works without waiting for a free caller or target application turn |
| Remote signal | Signed cancellation reaches the original remote invocation |
| Dropped remote call future | Owned RPC/peer guards revoke and retire that call |
| Nested call | A blocked child loses SDK authority with its cancelled parent |
| Synced write before cancellation | The synced state survives; the later transaction rolls back and late writes are refused |
| Guest cancellation: Python tasks, V8/TS signals, Node/JS native signals | Each caller covers local, remote, nested and after-sync cases; foreign SDK access is refused and a fresh RPC is admitted after cleanup |

Every case waits until the target transaction has actually entered before
cancelling. The 800 ms poll stays below the 5,000 ms call timeout, so deadline
expiry cannot explain the refusal. Actual node, scope and epoch observations
are checked; fresh calls verify persisted state and usable storage. Both nodes
drain even after an audit failure. The suite contains 17 cases: five Rust
caller cases and four each for GIL CPython, embedded V8/TypeScript and real
Node/JavaScript callers. Guest cases keep their parent invocation alive until
the target has attempted late SDK work; parent retirement cannot explain the
refusal. They reject attempts to acquire SDK authority under another invocation
and require a new RPC to be admitted after cleanup. JS controllers fire from a
different HTTP invocation using only the original call's private cancellation
capability. V8 bundles `abort-controller`; Node uses its native implementation.
Earlier synced state survives. These guest cases call Rust providers; they do
not qualify every provider, pre-codec budget, interpreter or proof-wait boundary.

```text
JS AbortSignal                   Python Task.cancel()
      |                                  |
private per-call capability       original bridge + operation ID
      |                                  |
      +------------ native AbortSignal --+
                         |
               existing local/peer path
                         |
          await native acknowledgement
                         |
         release admission and signal listener
```

The [runtime control tests](../../crates/cellar-javascript/runtime/cancellation_tests.rs)
set both JS operation limits to one, cancel from another continuation, reject
foreign raw RPC admission, retain the source byte reservation until native
acknowledgement, and verify full reclamation and subscription cleanup on return
and retirement. Node's protected listener survives `stopImmediatePropagation`;
synthetic abort events cannot consume the real subscription. The SDK also
checks cancellation before encoding/submission and around response decoding.
The separate codec-budget probes above check whole-call expiry; these 17
cancellation cases do not qualify every provider or proof-wait boundary.

```text
asyncio.Task.cancel()
         |
original bridge scope check -> native per-operation AbortSignal
         |                                  |
         |                         ordinary native/peer cancellation
         v
await native acknowledgement -> release mutation/byte permits -> CancelledError
```

The Python cancellation control uses no operation queue slot. Its host-owned
registration follows the original native future and disappears on completion or
drop, including invocation retirement. Repeated cancellation waits for that same
acknowledgement. Unit tests cover cancellation/ready-reply/retirement races and
require no unobserved asyncio failures:

```sh
python3 tools/rpc/test_python_cancellation.py
```

The native concurrency test also holds an unrelated input gate closed, cancels
a queued RPC and observes the actual payload owner's drop before opening that
gate. A returned caller error alone cannot establish admission cleanup.

## Cancellation and deadlines during proof waits

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite proof-wait --report .cellar/rpc/proof-wait-matrix.json
```

This T14 matrix contains 128 cases:

| Axis | Values |
| --- | --- |
| Caller and provider | Rust, GIL Python, V8/TS, Node/JS: all 16 pairs |
| Route | Local or authenticated peer |
| Wait | Handler returned into native output proof, or original handler awaiting sync |
| Trigger | Explicit cancellation or the original call deadline |

A five-case diagnostic provides faster feedback on caller liveness and captured
native revocation:

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite proof-wait-smoke --report .cellar/rpc/proof-wait-smoke.json
```

Its report is explicitly `qualified: false`; it cannot replace the full matrix.

For the observed V8/local/sync/deadline warmup failure, repeat that exact case
up to 16 times without changing lease or watchdog policy:

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite proof-wait-v8-repro --report .cellar/rpc/proof-wait-v8-repro.json
```

The later Node-to-Rust/peer/sync/deadline failure has its own exact diagnostic:

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite proof-wait-node-repro --report .cellar/rpc/proof-wait-node-repro.json
```

Both diagnostics report `qualified: false`. If warmup fails, bounded fresh
health/read probes and joined native shutdown outcomes distinguish a closed
HTTP connection from node fencing. The failed mutation is never replayed.
Every proof-wait error retains the most recent 64 in-memory native lease observations:
attempt start/completion, remaining validity and the proof-wait setup phases.
Those observations distinguish delayed dispatch from a slow storage attempt.

```text
native SQLite count + receipt committed
                  |
      peer and bucket proof held
           /               \
     handler returned     handler awaiting sync
           \               /
           caller signal / deadline
                  |
      native call settles; fresh RPC admitted
                  |
      original caller remains live
      old provider SDK operations refused
                  |
        open proof -> explicit receipt retries
```

Both proof paths stay held while the caller joins its native operation and
admits a fresh RPC. JS callers have one pending SDK slot. Sync cases resume the
original provider continuation and require its retained storage and RPC clients
to refuse late work. The caller performs another fresh RPC afterwards, proving
its own authority survived the callee's revocation. Scope, epoch, execution node,
local count and receipt come from native observations.

Rust/JS signals and deadlines return fixed failures with `unknown` effects.
Python task cancellation raises `CancelledError`, which makes no rollback claim;
Python deadline failures retain `DeadlineExceeded/unknown`. Proof is opened only
after cleanup checks. Two explicit same-ID retries must recover the saved result
without another increment. Cancellation does not undo the already committed
SQLite write. All native nodes and guest runtimes drain before report publication.
T15 cycles have a separate matrix below. Broader lifecycle failures, interpreter
modes and resource saturation remain separate gates.

Node watchdog failures include heartbeat age, the Rust observer's loop gap,
non-consuming input readiness and active invocation/object counts. These bounded
diagnostics help distinguish worker silence from delayed host observation while
preserving the configured watchdog deadline.

## Named dependency cycles

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite cycles --report .cellar/rpc/cycle-matrix.json
```

The T15 matrix contains **80 cases**. All calls use generated Counter methods,
the production native RPC path and real SQLite cells. No mock transport or
JavaScript timer supplies the cancellation result.

| Axis | Values |
| --- | --- |
| Cell dependency | Self-call in each of four providers; A → B → A for all 16 provider pairs |
| Placement | One native node; or signed peers with A and B on different owners |
| Trigger | Signal after every frame enters; or the original one-second deadline |

```text
source invocation remains live throughout cleanup
      |
      +--> A.outer --+--> A.inner ----------> native gate (held)
                    |    same cell
                    |
                    +--> B.middle --> A.inner --> native gate (held)
                         other cell   original A owner/scope/epoch

each business frame saves an atomic count + receipt, then awaits its child
      |
signal / deadline -> native frames retire and lose authority
      |
resume original guest continuations -> late storage/read/RPC refused
      |
same-cell fresh reads + mutation admitted; committed counts preserved
      |
source performs another fresh RPC -> nodes and runtimes join -> publish
```

Native observations establish actual execution nodes, scopes, epochs and
captured authority. A control callback failing a foreign-context check alone
cannot establish revocation. Python deliberately retains cancelled frames until
the supervisor releases them; V8 and Node retain their original continuations.
JS still has one pending SDK operation slot. Each case checks the exact entered
frame inventory, no automatic mutation replay, and fresh admission to the same
named cells. Counts written before cancellation remain visible; failures are
`Cancelled/unknown` or `DeadlineExceeded/unknown`.

This qualifies the described Counter dependency waits with GIL CPython. It does
not establish arbitrary CPU interruption, all Python modes, resource saturation
or every T14/T17 lifecycle boundary. Full-qualification flags remain false.

## Owned Node worker loss and active-call shutdown

These suites keep the caller runtimes separate from the disposable Node
provider. They use the fleet harness, signed peers and generated named methods:

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite worker-failure --report .cellar/rpc/worker-failure.json
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite worker-shutdown --report .cellar/rpc/worker-shutdown.json
```

For one Rust/local/post-sync diagnostic, use `worker-failure-smoke` or
`worker-shutdown-smoke`. The shutdown smoke exercises the deadline cut.

```text
surviving Rust / Python / V8 / Node caller
                 |
          local or signed peer
                 |
       disposable Node Counter
       before write OR after synced count + receipt
                 |
       typed Rust child held in its native turn
                 |
      SIGKILL worker / preserve owner shutdown
                 |
      native frames retire + budget sources expire
      invocation / byte reservations return to zero
                 |
      fresh worker + owner -> read -> two explicit retries
```

| Trigger | Cases | Required evidence |
| --- | ---: | --- |
| Worker `SIGKILL` | 16 | `Unavailable/unknown`, native owner still serves a fresh method, worker group disappears after join. |
| Graceful owner shutdown | 16 | Health and new-root admission close before factory entry; accepted work remains live and completes after gate release; the normal drain proof succeeds. |
| Forced owner deadline cut | 16 | The held call never receives a test release; the report retains timeout/unclean flags, followed by physical cleanup and recovery. A local lost HTTP reply remains unknown; a surviving peer caller receives a typed failure. |

Each group covers four caller backends, both routes and both write checkpoints.
Native storage observations run only inside the provider's original executing
turn, with a closed transaction and matching exact-i64 receipt. They freeze
before termination and cannot complete or cancel the business future. Weak
budget sources prove distinct, initially live authority and later refusal;
the fixture retains no native `Context` for those probes.

`BridgeUsage` must show exactly one held provider invocation and positive bytes
before injection, then zero native invocation and byte reservations. The native
child must physically retire. A fresh runtime and owner read the prior state
before two explicit same-ID retries, preserving `9_007_199_254_740_993` exactly.
No mutation is automatically replayed. The aggregate `all` run includes both
suites and publishes only after all runtime joins. V8 lifecycle has separate
suites below; Python cancellation suppression, saturation/RSS and broader T17
qualification remain open.

## Owned V8 termination and active-call shutdown

The V8 provider owns a different isolate from every caller, including V8
callers. It uses the same native storage checkpoints, signed routes, weak
authority probes and fresh-owner receipt recovery as the Node suites.

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite v8-termination --report .cellar/rpc/v8-termination.json
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite v8-shutdown --report .cellar/rpc/v8-shutdown.json
```

For a Rust/local/post-sync diagnostic, use `v8-termination-smoke` or
`v8-shutdown-smoke`. The shutdown smoke keeps native work held until the owner
cuts its drain.

The Node/V8 forced-owner fixtures use a 250 ms no-progress window while keeping
the full eight-second shutdown budget for durability cleanup. Both original
native call budgets must have more than 1,250 ms remaining at injection. Their
physical frames must retire before their original RPC deadlines; a natural
deadline response cannot qualify as a forced cutoff. RPC/handler budgets, lease
TTLs, the execution watchdog and the normal graceful-drain policy are unchanged.

```text
original provider + child still live (3,500 ms remaining)
                    |
      forced drain stalls for 250 ms
                    |
      unclean cutoff proof, then native retirement
                    |
   retire before original RPC expiry -> joined engine -> receipt recovery
```

The Node-caller/V8-provider/peer/before-sync case deliberately waits until that
near-deadline window in the normal matrix. With the old five-second stall
window, the call could expire first and legitimately produce a clean drain.
Bounded `shutdown_observation` fields retain original/current budget headroom,
frame retirement ages, shutdown duration and guest reservation gauges.

Run the same peer topology 16 times, or just its near-deadline regression:

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite v8-forced-peer-repro --report .cellar/rpc/v8-forced-peer.json
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite v8-forced-peer-deadline-repro --report .cellar/rpc/v8-forced-peer-deadline.json
```

These diagnostic reports keep `qualified: false` and do not replace the matrix.

```text
surviving Rust / Python / V8 / Node caller
                    |
         disposable V8 Counter
         before write OR after synced receipt
                    |
         typed native child stays held
                    |
separate trigger -> typed native read -> CPU loop
                                        |
                              production V8 watchdog
                                        |
                 isolate terminates; native frames retire
                 weak budgets expire; reservations reach zero
                                        |
                 engine + watchdog join -> fresh isolate + owner
                                        |
                      read state -> two explicit receipt retries
```

| Trigger | Cases | Required evidence |
| --- | ---: | --- |
| Execution watchdog | 16 | Trigger reaches a distinct real native child before its CPU loop. The engine reports its execution-budget failure; the original call returns `Unavailable/unknown`; native owner and caller still serve fresh methods. |
| Graceful owner shutdown | 16 | Fresh-root admission closes before a factory runs; accepted work finishes after gate release and the normal drain proof succeeds. |
| Forced owner cutoff | 16 | The held business child gets no test release before cleanup is proven. The shutdown proof remains unclean; a surviving peer receives a typed unknown failure. |

Each trigger covers four caller backends, local/signed-peer routing and both
write checkpoints. The watchdog uses the existing two-second pump budget, which
includes microtasks. It is not an RPC I/O deadline. The CPU loop runs in a
separate invocation while the original typed provider still awaits native work;
no test API closes the engine. Native observation freezes before injection.

Termination requires retirement and exact `ContextExpired` refusal for all
three captured native frames: provider, held child and trigger child. Shutdown
cases capture the first two. Bootstrap return establishes joined engine and
watchdog threads; the retained application must report zero native invocation
and byte reservations. V8 has no worker process-group exit claim. A fresh
isolate and native owner verify the exact prior state and receipt-safe explicit
retries. `all` includes these suites and publishes after runtime joins.

These checks do not qualify process-fatal heap exhaustion, Python cancellation
suppression, saturation/RSS, packaging or all T17 requirements.

Use the focused `lifecycle` suite to run all four Node/V8 suites together:

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite lifecycle --report .cellar/rpc/lifecycle.json
```

Its 96 cases reuse the same four caller runtimes across provider backends and
publish only after those runtimes join. This focused report does not replace
the earlier deadline, proof, recovery, cycle and provenance suites in `all`.
The [lifecycle CI job](../../.github/workflows/typed-rpc.yml) builds the locked
fixtures, checks the Rust qualifier and runs this matrix on Linux with Node 24
and GIL CPython 3.14. It retains the report and execution log as artifacts.

## Python cancellation suppression

These suites keep an actual typed Python provider task alive after its native
invocation has retired. The caller's cancellation is acknowledged before late
SDK attempts; a Python caller shares the interpreter with the stale provider.

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite python-cancellation --report .cellar/rpc/python-cancellation.json
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite python-supervision --report .cellar/rpc/python-supervision.json
```

The matching `-smoke` variants run one Rust/local/post-sync diagnostic.

```text
typed Python increment -> native Rust child held
          |
  caller cancellation OR forced owner cutoff
          |
  native frames retire; original weak authority expires
          |
  Python catches CancelledError and stays alive
          |
  late storage / typed RPC / raw bridge attempts refused
          |
  native admissions + byte reservations = 0; guest invocation = 1
          |
  fresh native owner -> receipt retries -> repeat stale attempts
          |
    +-----+------------------------------------+
    |                                          |
 explicit task release                  persistent suppression
 guest joins; complete drain            typed Incomplete; exit 1
                                               |
                                    observed process-group exit
                                               |
                                    fresh-process receipt recovery
```

| Boundary | Cases | Required evidence |
| --- | ---: | --- |
| Caller cancellation | 16 | Original parent remains live through the first late probe and serves a fresh followup; the target's native frames and budgets have already expired. |
| Forced owner shutdown | 16 | The report keeps its unclean cut and unknown effect. Native reservations release while the Python handler still suppresses cancellation. |
| Persistent suppression and supervision | 16 | The runtime returns typed `Incomplete` with exactly one known pending guest invocation and zero native admissions/bytes/operations. Exit code 1 alone cannot qualify it. Both owned process groups must disappear before independent-process recovery. |

Every group covers all four caller languages, local/signed-peer routing and
before/after-sync native storage checkpoints. A before-write case recovers zero;
an after-sync case recovers the exact committed count and receipt. Two explicit
retries apply at most one effect. A second stale probe after reactivation must
still fail. Fresh native reads independently inspect the late-write key and
the late RPC target, so guest error claims cannot establish absence of writes.

`bridge_report().native_invocations` counts native admission permits, while
`interpreter_snapshot().work.active_invocations` counts managed Python handler
registrations. During suppression the former reaches zero while the latter
stays one. A Python caller's still-live parent is accounted for separately
until that parent finishes. Snapshot fields are independently sampled; these
are not heap or RSS measurements.

Serialized shutdown reports require the bridge section and its native-admission
observation. Missing observations are rejected instead of implying a clean drain.

The persistent fixture configures a 200 ms guest shutdown bound. Its supervisor
requires the structured incomplete report, the expected nonzero exit, joined
owner/Node worker groups and cold receipts. A supervisor timeout is a failed
case, followed by owned-process cleanup. The 48 Python cases are included in
`all` and in the focused Linux lifecycle CI job. Broader interpreter, schema,
resource and packaging qualification remains open.

## Abrupt owner process loss

On Unix, after building the fixtures and Python environment above:

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite crash --report .cellar/rpc/crash-matrix.json
```

The coordinator starts a fresh owner process for each caller/provider pair and
crash point: **48 owner processes** for all 16 pairs at three boundaries. Each
owner starts its own Rust application, CPython, embedded V8 and Node worker.
The surviving caller runs in a separate process with a distinct node directory
and the same development bucket. Peer calls use the production signed transport
with fleet durability enabled.

```text
before_handler
  admit RPC -> hold before business method -> SIGKILL owner
       -> recover count 0 -> explicit same-ID retry -> one mutation + receipt

after_synced_handler
  atomic count + receipt -> await sync -> validate encoded typed reply -> hold
       -> SIGKILL owner -> recover count + receipt -> explicit same-ID retry
       -> return saved result; count stays unchanged

after_count_before_receipt (all 16 caller/provider pairs)
  open transaction -> count write finishes -> receipt write has not started
       -> SIGKILL owner -> recover count 0 -> explicit same-ID retry
       -> one mutation + receipt
```

Crash cleanup requires both the owner's and Node worker's process groups to
report `ESRCH`. A permission denial remains unproven until the same five-second
bound; it never counts as exit. Real-child regression tests hold a live group,
then an exited/unreaped group, and require reaping before cleanup succeeds.

The native barrier records the actual class, scope, owner epoch, observed count
and receipt before the kill. The second boundary also validates the held typed
business response against that count. The coordinator requires the original call
to fail with unknown effects and verifies that no admitted mutation is silently
replayed on the survivor. Bounded polling retries read-only routing refusals while
the lease expires and recovery establishes admission. Mutation retries remain
explicit and use the original operation ID.

The midpoint observes the real storage transaction in every provider:

| Provider | Observation inside the original native invocation |
| --- | --- |
| Rust | A callback in the ordinary `transaction_sync` closure reads count and the absent receipt, then checks the connection's open transaction and epoch. |
| Python, V8, Node | A qualification-only SQLite trace observes the receipt read returning zero rows, then the exact count-write statement finishing with one changed row while autocommit is off. |

The guest observer verifies the native connection epoch before the handler
starts. Its state follows each poll of that original invocation, including
worker migration, and is removed between polls. It matches the actual KV SQL
and bound scope/key/value; unexpected statement order or connection changes
fail the run. The completed statement is observed through SQLite's
[PROFILE trace event](https://sqlite.org/c3ref/c_trace.html), without running SQL
from the callback. Only the disposable crash owner installs tracing; production
storage and language adapters gain no fault API.

An atomically renamed evidence file lets the separate coordinator observe the
checkpoint while the native poll is stopped. If it is not killed within the
finite budget, the Rust callback rolls back and the guest observer exits the
disposable process before the receipt write or commit. Recovery must expose the
pre-transaction count before any explicit mutation retry.

Every case verifies exact i64 values above 2^53, stable scope identity, advanced
epochs, duplicate receipt replay, changed-delta conflicts and the final exact
count. The owner must exit with `SIGKILL`. Its Node worker has a separate process
group: the fixture reports the worker's actual PID, and both owned groups must
disappear after host IPC closes. Failure cleanup targets only those fixture
groups. Surviving nodes drain normally; the parent language runtimes join before
the report is published.

The evidence validator requires all three points for every pair. It rejects duplicate
cases, non-SIGKILL termination, rollback claims, missing held replies, stale epochs
and inconsistent counts/receipts. `crash_matrix_verified` records this boundary.
The report gives per-point pair counts and records
`all_provider_transaction_checkpoints_verified` only after the four native and
12 guest midpoint cases pass. Guest evidence must include the finished
statement, zero receipt rows, one changed row and its exact change-counter
advance. The earlier 36-case matrix cannot satisfy this validator. Broader
recovery refusal and free-threaded Python remain open
qualification gates, so the broader
`abrupt_crash_qualified` flag remains false.

## Recovery admission before provider construction

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite recovery --report .cellar/rpc/recovery-matrix.json
```

The 32 cases cover all 16 language pairs with two real metadata faults:

| Fault | What changes after the owner is killed |
| --- | --- |
| `missing_root` | Remove the original owner's published proof root. |
| `replaced_root` | Replace it with a fresh generation/lineage that cannot authorize the original proof. |

```text
owner A                  witness W                  successor C
count + receipt -------> real signed log ack               |
bucket writes held       retains actual batches             |
reply acknowledged                                         |
SIGKILL                                                     |
remove/replace A's root -------------------------------> RPC refused
                                                     factory calls: 0
                                                     handler calls: 0
restore original root -------------------------------> recover same scope
                                                     advance owner epoch
explicit retry --------------------------------------> one mutation
same ID again ---------------------------------------> saved receipt
```

The data cut first drains already-admitted ordinary and multipart uploads, then
takes a bounded byte inventory. Node-lease writes remain independent. The next
large mutation must acknowledge through an observed signed follower append,
while bucket publication is held and its inventory remains unchanged. The
witness retains real follower service and proof reads; only its conditional
claims to this cell and the old owner root are excluded so C is the successor.

Two deliberately issued mutations must fail with unknown effects while C stays
live. Native observations require zero provider factory and handler entries.
The successor must read the actual missing/replaced metadata, and both bucket
and follower proof must remain byte-for-byte unchanged. Restoring the original
root must let that same process recover the acknowledged count and receipt at
an advanced epoch. No failed mutation may replay automatically. Explicit retries
then verify one mutation, saved receipts and changed-delta conflicts.

The observer preserves real object-store bytes, CAS tokens, ranges and
attributes. Reports require positive counts for signed append acknowledgements,
held publications, retained follower batches and observed metadata faults. They
publish only after native drains and supervised runtime joins. These cases use
GIL CPython and do not qualify every recovery failure or interpreter mode.

For a single Rust→Python missing-root diagnostic, use `--suite recovery-probe`.
Its report explicitly sets `qualified: false`; it cannot replace the matrix.

## Withheld proof and owner fencing

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite proof --report .cellar/rpc/proof-matrix.json
```

The suite runs all 16 caller/provider pairs at four boundaries across three
authenticated nodes, reusing the native SDK's real log-transport and object-store fault gates.
Distinct acknowledged warmup commands establish both replica members before
any fault is armed. Lease renewal and reads remain independent of data proof.

| Point | Original handler at the barrier | Coordinator action |
| --- | --- | --- |
| `release` | Returned a typed result without explicit sync | Release both held proof paths |
| `fail` | Returned a typed result without explicit sync | Fail both held proof paths |
| `fence_output` | Returned a typed result without explicit sync | Fence the actual owner before output delivery |
| `fence_sync` | Still awaiting `storage.sync()` after its atomic batch | Fence the actual owner, then resume expired cleanup |

```text
caller B -> owner A: atomic count + receipt
                         |
                 handler returns without sync()
                         |
                 valid typed result matches local state
                         |
                 native output gate holds response
                    /            |             \
               release         failure      owner fence
                  |              |             |
         original typed result   +----- unknown effects
                  |                            |
          explicit same-ID retries + receipt/conflict audit
```

Only tool-owned `proof-no-sync-*` commands omit the handler's explicit sync.
Native observations require a returned typed result matching SQLite, a closed
transaction, an actual write position, and both proof paths entering their hold.
The original HTTP future must remain pending before release, failure or fence. A
released proof must deliver that original typed result; a failed proof must
remain unknown and must not replay the mutation automatically. Recovery may
retain or discard an unacknowledged atomic mutation. Explicit same-ID retries
must yield one exact result and preserve changed-delta conflicts in either case.
The validator refuses incomplete/duplicate pairs, missing proof observations,
early delivery, forged rollback and inconsistent counts. Reports are published
only after native nodes and supervised language runtimes drain. Each sequential
case starts a fresh bounded audit after the preceding case's nodes have joined;
completed evidence is retained in the report, rather than in live audit cursors.

For fencing, the coordinator changes the published owner lease's address while
preserving its generation and log-session provenance. It requires the real
`NodeLeaseMismatch` outcome and joined execution domain before testing refusal of
a new request. Survivor reads must observe the same scope at an advanced epoch,
preserve an already acknowledged receipt, and retain exact i64 state.

```text
original handler                 native owner                coordinator
atomic count + receipt                 |                         |
await storage.sync() -------- held proof paths ------------------+
                                       |                  replace owner lease
                                       X <-----------------------+
catch cancellation <---------- revoke + join                      |
await local release promise -------------------------------------+
old storage read/write: refused                           release cleanup
old bound typed client: refused                                  |
drop retained probe -------------------------------------- audit + clear
```

The live-handler probe retains the original context and bound typed client.
V8/Node resume the original JavaScript continuation; Python deliberately
suppresses cancellation until released and also checks its raw native sender.
Rust observes cancellation through the dropped original handler future, then
tests its retained context and client. All late storage reads/writes and typed
calls must be refused. A native admission audit rejects additional business
invocations. The probe is cleared before the next case to release retained
contexts. No mutation is automatically retried.

Run just the 32 fencing cases while diagnosing a lifecycle failure:

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite fence --report .cellar/rpc/fence-matrix.json
```

This focused matrix cannot substitute for the 64-case proof suite or the full
`--suite all` run. Neither report claims broader fleet qualification.

The suite also verifies late follower recruitment: it first observes a signed
append acknowledgement from the ingress node, then starts the second replica
and requires both actual member names before arming faults. The fast log driver
keeps a healthy one-follower ensemble on its normal recruitment cadence until
it reaches the two-follower target. The focused regression command is:

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite proof-recruitment --report .cellar/rpc/recruitment.json
```

This single-case diagnostic explicitly reports `qualified: false`. It supports
debugging the real background driver; it cannot replace the full proof matrix.

Remaining qualification includes further recovery faults/interpreter modes,
full descriptor semantics, resource/deadline/provenance gates, verified
packaging and measured performance.

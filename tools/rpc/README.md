<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Named RPC codec and qualification tools

Implementation of [plan 009](../../plans/009-named-typed-rpc.md) is in progress.
The [status ledger](../../plans/named-typed-rpc/implementation-status.md) records
the delivered boundaries and remaining language/fleet tests.

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
runtime joins. `--suite local` is the default; `--suite all` writes separate
`local`, `remote`, `deadline`, `cancellation`, `cycles`, `provenance`, `proof_wait`, `recovery`, `crash`
and `proof` evidence objects. `remote_matrix_verified` and
`remote_matrix_after_handoff_verified` record these measured boundaries. Full
fleet qualification remains false: further interpreter modes, resource limits
and the full lifecycle matrix are separate requirements. The dedicated proof suite below
tests withheld/failed RPC output proof. This run uses normal GIL CPython; it is
not free-threaded evidence.

## Failures after a parent commits

```sh
cargo run --locked -p cellar-rpc-qualification --features full -- \
  --suite provenance --report .cellar/rpc/provenance-matrix.json
```

The T06 matrix executes **96 cases**: all 16 Rust/Python/V8/Node
caller/provider pairs, local and signed-peer ingress, and three failures after
the provider syncs its atomic count and receipt.

| Failure inside the parent | Parent's caller receives |
| --- | --- |
| Generated child call expires before its own submission | `DeadlineExceeded/unknown` |
| Exception claims `MethodNotFound` and `not_committed` | `MethodNotFound/unknown` |
| Exception has an unrecognized private code | `Internal/unknown` |

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
The native dispatcher always assigns unknown effects to entered-handler errors.

Counts exceed JavaScript's exact-number range and are checked as canonical
64-bit values. Native observations verify the actual caller node, provider
owner, scope, epoch, four warmed factories and exactly three Increment entries
(one original plus two explicit retries). No refused child is constructed and
no mutation is automatically replayed. Reports publish after native/guest drains.

This covers a locally proven child refusal and forged exception metadata through
real local/peer parent calls with GIL CPython. Broader peer/admission provenance,
interpreter modes and resource/lifecycle qualification remain open.

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

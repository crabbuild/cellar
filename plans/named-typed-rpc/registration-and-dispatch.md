<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Named RPC: registration and dispatch contract

This is the implementation detail for [plan 009](../009-named-typed-rpc.md).
It specifies the target contract; it does not claim every API below is delivered.
The [implementation ledger](implementation-status.md) records verified behavior
and remaining work. Examples marked as sketches describe the intended API.

## 1. Keep identity and interface separate

A call selects a durable entity and then an operation on that entity:

```text
CounterClient.bind(ctx, "COUNTERS", "alice").increment(request)
                         |           |          |
                  namespace binding  |    generated business method
                                 cell name
                         |
               trusted deployment mapping
                         |
                  scope + class + ID
                         |
             current owner and activation
                         |
            example.counter.v1.Counter/Increment
```

| Name | Example | Owner / meaning |
| --- | --- | --- |
| Namespace binding | `COUNTERS` | Deployment maps a caller-visible name to an allowed namespace |
| Cell class | `Counter` | Native application registration selects a factory/backend |
| Cell name | `alice` | Existing identity rules select durable cell state |
| Service | `example.counter.v1.Counter` | Protobuf defines an interface that a class implements |
| Method | `example.counter.v1.Counter/Increment` | Exact wire identity, independent of a language's spelling |
| Contract digest | SHA-256 of the descriptor artifact | Identifies the exact shared schema bundle |

`Counter` and `COUNTERS` need not share a name. A cell class can implement
multiple services and retain HTTP/alarm handlers. Registering a service does
not create a second cell, database, actor or listener.

Generated names are `increment` / `getCount` in JS/TS and `increment` /
`get_count` in Rust/Python. Wire identities stay unchanged. Reject collisions
with helper names such as `bind`, as well as names that collide after language
conversion. Never use reflective dispatch on a caller-supplied property name.

## 2. Build artifacts and application registration

There are three inputs to startup validation:

```text
Schema bundle                   Application                  Deployment
counter.proto + imports         class/factory exports         namespace bindings
          |                     generated registrations      consumer requirements
     build once                         |                            |
          v                             |                            |
contracts.binpb + rpc.json --------------+----------------------------+
                                        |
                              native startup validation
                                        |
                        immutable class/service/method catalog
                                        |
                              admit application traffic
```

| Artifact or registration | Required data |
| --- | --- |
| `contracts.binpb` | Normalized descriptor image, including imports; exact bytes shared by every generator |
| Generated `rpc.json` | Digest, full method names, input/output full names, supported profile and codec limits |
| Generated language registration | Exact wire-to-language method mapping and actual request/response codec descriptors |
| Native provider registration | Cell class, implemented service, descriptor artifact and effective limits |
| Consumer requirement | Calling application/entrypoint, namespace binding, required service and digest |
| Verified package inventory | Generated files and runtime dependencies, generator/runtime versions and package integrity metadata |

Schema inventory is independent of deployment. For example, one generated
Counter contract can be implemented by `RustCounter` and `PythonCounter` with
separate namespace bindings. There is no per-cell schema registry lookup.

For static validation, resolve each consumer requirement through its configured
binding to the selected provider class and compare service, digest and methods.
Bindings resolved by an external deployment still receive exact checks at the
target. Static validation does not replace receiving-side validation.

### Registration API sketches

The Rust class factory owns its business implementation and dispatcher. Declare
the service separately so the host can validate calls before the factory runs:

```rust,ignore
use cellar::sdk::{rpc, Registry};
use std::sync::Arc;

let contract = Arc::new(rpc::Contract::from_descriptor(
    include_bytes!("generated/contracts.binpb"),
    rpc::Limits::default(),
)?);

let app = Registry::new(ingress)
    .cell("Counter", |_, _| {
        let router = CounterRegistration::register(
            rpc::Router::new(),
            Arc::new(Counter),
        )?;
        Ok(Arc::new(router))
    })?
    .rpc_service("Counter", "example.counter.v1.Counter", contract)?;
```

`Counter` implements the generated `CounterHandler`. An application that wraps
the router to compose HTTP or lifecycle handlers forwards both the service
catalog and ordinary invocation handling. A wrapper that does not expose
pre-factory validation cannot establish a pre-construction refusal proof.

The native worker snapshots the catalog once and shares it with invocation
contexts. Generated clients resolve a matching artifact from that catalog before
encoding; generated router handlers receive its effective limits before response
encoding. A provider declaration therefore governs both receiving preflight and
outgoing response construction. For a native consumer with no local provider,
use `Registry::rpc_contract(contract)` to declare its limits. Existing custom
native applications without a matching catalog entry retain the explicit
client/router contract's limits; static binding qualification must audit that
fallback. Router dispatch refuses a declared/generated descriptor mismatch
before entering its registered handler, with conservative unknown effects if a
factory has already run.

JS/TS uses the same registration export for embedded V8 and Node. The intended
module shape is:

```ts
// API sketch: Counter contains the business method implementations.
import { Counter } from "./counter.js";
import { CounterRegistration } from "./generated/counter_cellar.js";

export const cells = { Counter };
export const rpc = { Counter: CounterRegistration };
```

The corresponding native `Source` declares the class/service with `cell_rpc`.
It does not call the existing `.cell("Counter")` shortcut, which requires
`fetch`. Add `cell_events` only for actual HTTP/alarm handlers.

Python's equivalent uses the existing named class export and an explicit RPC
registration export:

```python
# API sketch: Counter is the application's business class.
from counter import Counter
from generated.counter_cellar import CounterRegistration

__cellar_rpc__ = {"Counter": CounterRegistration}
```

The native Python `Source` also declares `cell_rpc("Counter", service, contract)`.
A consumer-only source registers the required contract without creating a cell
class. Generated clients refer to that registered artifact rather than accepting
a guest-supplied descriptor. Neither guest export may add methods missing from
the native catalog.

### Startup validation order

1. Bound and validate the descriptor image before invoking any code generator
   or runtime decoder. Validate imports, field numbers/types, map entries,
   oneofs, proto3 optional synthetic oneofs and language-name collisions.
2. Build the native provider catalog. Reject duplicate full methods, unknown
   services/classes, reserved framework classes and conflicting limits.
3. Load the verified guest package and inspect its explicit exports. Do not
   instantiate business cells to discover methods. Reject missing, extra or
   non-callable handlers; Python inspection must not execute custom descriptors.
4. Check actual message codec shapes against the artifact: full name, field
   number/kind/cardinality, referenced types, presence, oneof membership,
   enum values, packing and map structure. Matching type names and a copied
   digest string alone do not detect stale generated message code.
5. Check consumer requirements and binding targets. Verify that method limits
   plus envelope overhead fit every selected runtime and host budget.
6. Freeze the catalogs and dispatch tables for the application generation.
   A reload builds and validates a new generation before admitting its work.

Compare codec shapes using a documented structural projection with bounded,
memoized traversal of recursive types. Do not reserialize runtime-specific
descriptors and assume they hash to the common artifact. The artifact digest
remains a hash of exact build output bytes. Protobuf serialization is
[not canonical](https://protobuf.dev/programming-guides/serialization-not-canonical/).

### Structural projection

The native inventory includes a `types` table, once per contract bundle. Keys
are full Protobuf names without a leading dot. Values are:

```text
message: ["message", [field, ...]]        fields sorted by wire number
enum:    ["enum", [[name, number], ...]]  declaration order, including aliases

field = [number, proto_name, value_type, cardinality, explicit_presence,
         real_oneof_name, packed, referenced_type, map_key_type]
```

Type numbers are the standard `FieldDescriptorProto.Type` numbers. Cardinality
is `0` for singular, `1` for repeated and `2` for maps. A map projects its value
type and key scalar type directly; its synthetic entry message is omitted from
the type table. Referenced message/enum names are empty for scalar values, and
the map-key type is zero for other fields. Presence is false for collections.
Proto3 optional fields retain explicit presence but omit their synthetic oneof
name. Real oneof names and membership remain significant. Packing is significant
only for repeated packable fields.

JS/Python verification compares the live codec's reachable message and enum
descriptors against this native table. It follows references with memoization,
including recursive types, maps, wrappers and `Struct`/`Value`. Traversal is
bounded by 4,096 types and 65,536 fields. JS also checks that reflection members
and fields describe the same records, then freezes the reflection records used
by constructors and binary codecs. Python memoizes immutable runtime descriptor
objects. A matching projection does not replace the exact artifact digest.

Providers must pass verification before readiness. Generated clients repeat the
check against their originating adapter's native catalog before their first
encode; missing catalogs and stale codecs fail locally as
`ContractMismatch/not_committed`. Client verification is lazy and does not
establish the separate static binding/deployment gate. Native code still
validates every request and response wire body independently.

The generated package records its codec runtime explicitly. Do not substitute
an older runtime merely because imports succeed; generated-code/runtime
compatibility has its own
[requirements](https://protobuf.dev/support/cross-version-runtime-guarantee/).
For Prost and Protobuf-ES, qualify the pinned generator/runtime pair according
to that project's policy. Native-only applications do not acquire an engine
dependency through registration or code generation.

## 3. Per-call dispatch state machine

The state machine is common to Rust, V8, Node and Python:

```text
BOUND CLIENT
     |
     v
CHECK CONTEXT + INPUT ---- local failure ----> not_committed
     |
     v
SUBMIT / ROUTE ------------------------------> unknown on lost result
     |
     v
CHECK OWNER + CATALOG + WIRE
     |                    |
     |                    +-- proven refusal -> not_committed
     v
DECODE + CONSTRUCT + ENTER HANDLER
     |
     |   exception / bad reply / timeout ------> unknown
     v
ENCODE + VALIDATE RESPONSE
     |
     v
OUTPUT DURABILITY GATE --- proof failure -----> unknown
     |
     v
DELIVER + VALIDATE + DECODE
     |
     v
TYPED BUSINESS RESULT
```

Every target response, including a structured failure, uses the existing native
output path. The diagram's outcome branches do not bypass its proof gates.
Caller cancellation can win before delivery even if the target committed.

| Stage | Required behavior |
| --- | --- |
| Bind | Capture the current invocation, configured namespace and cell handle; do not grant cross-invocation authority |
| Encode | Check types, numeric ranges, Unicode, graph depth/elements and predicted encoded bytes before allocating the message buffer |
| Submit | Use the original native context, source egress gate and operation reservation; start no detached SDK tasks |
| Receive | Preserve routing/admission/ownership checks; resolve method from the trusted class catalog, not payload-selected class metadata |
| Pre-factory validate | Check method, digest, envelope and wire bounds before business constructors; record call-local trusted proof for refusal |
| Enter | Decode the declared input, lazily instantiate if needed, then invoke the captured registered function with a fresh context |
| Complete | Validate the response type, graph and wire bytes; native code constructs the framework reply envelope |
| Release | Wait for required write/observed-position proof, then deliver; recheck caller deadline/context after reply decoding |

Class constructors and handlers are business code. If a custom wrapper runs
them before validation, subsequent refusals are conservatively `unknown`.
Failures escaping an entered handler also become `unknown`, including a nested
child's proven refusal. That child cannot prove that its parent made no writes.
Only private native framework evidence can establish `not_committed`.

## 4. Language adapter contract

Each adapter implements the same small boundary. Method lookup, codec selection
and resource limits come from immutable registration, never from guest requests.

| Direction | Control metadata | Owned attachment |
| --- | --- | --- |
| Native starts handler | Invocation ID, class, exact method, contract, effective limits | Request bytes; required even when empty |
| Guest calls another cell | Operation ID, binding, cell name, exact method, contract, timeout | Request bytes |
| Native replies to guest operation | Operation ID, success or fixed SDK failure | Response bytes on success |
| Guest finishes handler | Invocation ID, success or bounded failure indication | Response bytes on success |

A missing attachment is distinct from a zero-length message. Bytes never pass
through JSON, base64, JS number arrays or language object serialization. The
shared native adapter unwraps framework failures for outbound calls; guest
business code receives its language's fixed SDK error or decoded response.

V8 copies exact typed-array slices into native-owned storage and holds no V8
handles across native awaits. Its UTF-8 APIs must exist before generated message
descriptors execute. Node preserves byte attachments in bounded binary frames
and retains stdout for application logs. Python uses its owned byte attachment
path and must support its declared interpreter modes. [Plan 009's bridge
section](../009-named-typed-rpc.md#5-binary-language-bridge-changes) defines framing,
limits, reservations and cancellation ownership.

New language adapters can reuse this boundary and the same Protobuf artifact.
They still need generated clients, handler registration, bounded codecs,
invocation authority and the complete interoperability/recovery tests. Sharing
the wire format alone is insufficient to advertise a supported backend.

## 5. Execution and deployment semantics

Named methods inherit Cellar's existing cell invocation scheduling. A method
called `getCount` receives no automatic read-only or concurrent-read privilege.
Do not add a new mutex or promise that a handler is one uninterrupted transaction.
Handlers can yield; use observed snapshots and checked storage batches for
atomic business invariants. Object fields are activation-local caches.

RPC response types describe business outcomes. For example, the Counter returns
`Count`, `OutOfRange` or `OperationConflict` in its response `oneof`. Transport
success does not mean the business command was accepted. Missing required
business fields or an unset response `oneof` need explicit application validation;
Protobuf decoding does not enforce those rules. Use
[explicit scalar presence](https://protobuf.dev/programming-guides/field_presence/)
where omitted and default values differ.

A mutation retry reuses its application operation ID and semantic arguments.
The reference implementation atomically stores the count and receipt, waits for
proof and returns the saved result on a duplicate. No bridge automatically
replays an admitted mutation. The full algorithm and retention policy are in
[the Counter design](../009-named-typed-rpc.md#7-durability-and-the-reference-counter).

Exact bundle digests require a coordinated cutover or parallel, explicitly
named contracts/bindings. Additive schema evolution, a package's `v1` suffix
and a compatible codec runtime are three different concerns; none bypasses
the exact contract check. A source-language change alone can retain the same
contract and durable storage layout, but any backend switch must separately
qualify storage compatibility and activation handoff.

## 6. Review and delivery checkpoints

Implement a thin Rust-to-Rust slice first, then connect shared binary operations,
Python and both JS backends. Follow the ordered RPC-01 through RPC-07 work
packages in plan 009. For each package, record the changed owner, checks run,
observed behavior and open failures in the implementation ledger.

Before claiming end-to-end support, require all 16 caller/callee backend pairs
both locally and across fleet nodes. Include static typing checks, codec edge
cases, cold-constructor refusal, ambiguous parent/child outcomes, proof-gate
failure, abrupt crash, receipt replay, ownership transfer, cancellation cleanup
and existing HTTP/lifecycle regressions. Passing a named method call alone is
not the completion criterion; use [T01-T19 and the release
gates](../009-named-typed-rpc.md#10-qualification-and-release-gates).

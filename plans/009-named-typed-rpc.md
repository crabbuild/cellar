<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# 009: Named, typed cell RPC across languages

Status: **Implementation in progress; cross-language qualification remains open.**
Track delivered behavior and remaining gates in the
[implementation ledger](named-typed-rpc/implementation-status.md).
Prepared: 2026-10-23.
Reviewed baseline: `5f92e2c72c774d14a0e26d453b48d25437de54ce` (`origin/main`,
including the merged adapter documentation in PR #105).

## Decision and outcome

Use Protobuf services and messages to define business methods once. Generate
typed clients and server dispatchers for Rust, JavaScript/TypeScript, and Python.
Execute calls through Cellar's existing native cell RPC path. Rust retains
ownership of routing, activation, invocation authority, SQLite, durability proof,
cancellation and recovery.

An application can implement `Counter` in Python and call `increment` from
TypeScript or Rust. The caller chooses the namespace and cell name; the contract
chooses the method and message types. A service definition does not create a
network listener or a separate cell identity.

```text
                           counter.proto
                                 |
                    messages + Cellar code generation
                                 |
           +---------------------+---------------------+
           |                     |                     |
       Rust client          JS/TS client           Python client
           |                     |                     |
           +---------------------+---------------------+
                                 |
                  Rust host: namespace + name + method
                                 |
                    ownership / epoch / admission
                                 |
                registered Rust, V8, Node or Python cell
                                 |
                       SQLite + durability proof
```

This document specifies the first implementation, its source changes and its
release gates. The implementation ledger identifies delivered surfaces; API
sketches and commands below remain design contracts until their gates pass.

### Reading map

| Question | Start here |
| --- | --- |
| What does an application call? | [Contract](#2-contract-definition) and [generated APIs](#3-code-generation-and-developer-workflow) |
| How do classes, services and bindings connect? | [Registration and dispatch contract](named-typed-rpc/registration-and-dispatch.md) |
| What changes in the Rust host and language bridges? | [Wire contract](#4-wire-contract-over-existing-cell-rpc) and [binary bridges](#5-binary-language-bridge-changes) |
| What happens after a timeout or lost reply? | [Failure semantics](#6-invocation-authority-failures-and-cancellation) and [durable receipts](#7-durability-and-the-reference-counter) |
| How is implementation divided and qualified? | [Work packages](#9-ordered-implementation-work-packages), [release gates](#10-qualification-and-release-gates) and [evidence ledger](named-typed-rpc/implementation-status.md) |

The application experience follows the named asynchronous calls documented for
[Cloudflare Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/create-durable-object-stubs-and-send-requests/):
select a named entity and call a business method. Cellar uses an explicit,
language-neutral contract and generated registration so the same call can reach
a Rust, JavaScript/TypeScript or Python implementation.

### Scope

| Included in the first release | Deferred |
| --- | --- |
| Named cells and unary, named business methods | Client/server/bidirectional RPC streaming |
| Rust, embedded V8, actual Node.js, CPython | Additional language runtimes |
| Generated messages, typed clients and handler interfaces | Arbitrary remote objects, callbacks and capability returns |
| Explicit method registration and runtime validation | Automatic exposure of public class methods |
| Local and fleet routing through the existing host | Public gRPC/Connect listeners and service RPC generation |
| Bounded binary payloads and typed infrastructure failures | Automatic transaction wrapping or automatic mutation retry |
| Counter qualification with receipts and restart recovery | Transparent upgrades of persisted application messages |

Existing `fetch`, alarm, queue, scheduled and untyped native RPC APIs keep their
contracts. Typed RPC is an additional cell surface. This design applies to
ordinary fleet cells; it does not add language handlers to restricted multi-Raft
state-machine definitions.

## 1. Existing implementation and the missing pieces

This table describes the reviewed baseline above. In-progress code can already
implement some entries; use the implementation ledger for current evidence.

| Existing boundary | Evidence | Design consequence |
| --- | --- | --- |
| `Invocation::CellRpc`, `Output::Rpc`, RPC codecs | [application/mod.rs](../crates/cellar/application/mod.rs) | Extend the language adapters; keep the host invocation model |
| Namespace stubs with `rpc`, `call`, cancellation variants | [application/cells.rs](../crates/cellar/application/cells.rs) | Generated clients wrap a checked native stub |
| Submission, cancellation ownership and reply waits | [invocation/rpc.rs](../crates/cellar/runtime/invocation/rpc.rs) | Reuse this path; ordinary calls need additional typed submission evidence |
| Cell RPC responses include write/observed positions and pass output gates | [native worker](../crates/cellar/runtime/workers/native/mod.rs) | Return through `Output::Rpc`; never reply directly from a language thread |
| Structured values support byte buffers without an engine | [clone/mod.rs](../crates/cellar-native/clone/mod.rs) | Carry Protobuf bytes inside an existing structured RPC argument |
| Remote error envelopes and capability refusal | [native/rpc.rs](../crates/cellar-native/rpc.rs) | Preserve outer failures; do not treat arbitrary remote errors as typed business results |
| Shared guest operations include `cell.fetch`, but no RPC | [guest operations](../crates/cellar-guest/operations.rs) | Add one bounded `cell.call` operation |
| JS and Python invocation adapters reject `CellRpc` | [JS application](../crates/cellar-javascript/application.rs), [Python application](../crates/cellar-python/application.rs) | Add inbound RPC and RPC completion conversion |
| JS messages and Node IPC are JSON-only | [JS runtime](../crates/cellar-javascript/runtime/mod.rs), [Node transport](../crates/cellar-javascript/runtime/node/transport.rs) | Add binary attachments throughout the call lifecycle |
| Python already separates HTTP bytes from metadata | [Python application](../crates/cellar-python/application.rs) | Reuse its attachment mechanism with RPC-specific validation |
| Guest failures distinguish proven refusal from unknown effects | [guest failure](../crates/cellar-guest/failure.rs) | Retain conservative outcomes, especially for nested calls |

The workspace already pins `protobuf = 2.28.0` for its Raft dependencies in
[Cargo.toml](../Cargo.toml). This design does not replace that codec. A separately
pinned `prost` dependency serves application messages; generator dependencies
stay outside the pure decision core and native-only applications remain
independent of JavaScript engines.

## 2. Contract definition

The compilable [counter.proto](named-typed-rpc/counter.proto) is the design's
reference contract. Its core is:

```proto
syntax = "proto3";
package example.counter.v1;

service Counter {
  rpc Increment(IncrementRequest) returns (IncrementResponse);
  rpc GetCount(GetCountRequest) returns (GetCountResponse);
}

message IncrementRequest {
  optional int64 delta = 1;
  string operation_id = 2;
}

message IncrementResponse {
  oneof result {
    Count count = 1;
    OutOfRange out_of_range = 2;
    OperationConflict operation_conflict = 3;
  }
}

message Count { int64 value = 1; }
message OutOfRange { string reason = 1; }
message OperationConflict {}
message GetCountRequest {}
message GetCountResponse { Count count = 1; }
```

The example uses distinct request/response messages per method. A successful
transport reply can contain a business rejection. Exceptions are reserved for
infrastructure or unexpected handler failure, rather than the expected
`OutOfRange` result.

| Contract rule | First-release behavior |
| --- | --- |
| Method identity | Exact `package.Service/Method`, e.g. `example.counter.v1.Counter/Increment`; case-sensitive, no leading slash |
| Cell identity | Resolved separately from the configured namespace binding and cell name |
| Multiple interfaces | A class can implement several services; fully qualified method names avoid collisions |
| Service-to-class mapping | Explicit in native registration; never infer a class from untrusted payload metadata |
| Method shape | Exactly one request message and one response message; reject streaming declarations during generation |
| Missing scalar vs default | Use proto3 `optional` where presence matters; absence is not automatically an error |
| Constraints | Handler or generated validation hook enforces business requirements; decoding alone is insufficient |
| Output `oneof` | Generated code exposes the union; the Counter validator requires a selected result |
| Empty messages | Zero-byte message bodies are valid and distinct from a missing attachment |

Protobuf separates the interface definition from transport. Its presence rules
make omitted fields observable when explicitly declared; these rules must be
preserved across generated languages. See the [proto3 guide](https://protobuf.dev/programming-guides/proto3/)
and [field-presence guide](https://protobuf.dev/programming-guides/field_presence/).

### Portable message profile

Support proto3 scalar fields, enums, messages, `optional`, `oneof`, repeated
fields and maps. Imported schemas are included in the descriptor closure.
Reject `Any`, groups, application extensions and streaming methods in the
initial generator. Support well-known `Empty`, `Timestamp` and `Duration` as
messages, without implicit conversion to language-specific date objects.
Scalar wrappers and `Struct`/`Value`/`ListValue` retain their Protobuf wire
shapes. Generated APIs follow their pinned codec's field representations:
Protobuf-ES unwraps singular scalar wrappers outside oneofs and uses JSON objects
for `Struct` fields outside `Value`. These API mappings do not introduce a JSON
transport. Reject non-default `jstype` options so 64-bit values retain the
portable `bigint` representation in JavaScript.

| Protobuf value | Rust | JS / TypeScript | Python |
| --- | --- | --- | --- |
| `int32`, `uint32` | `i32`, `u32` | checked `number` | range-checked `int` |
| `int64`, `uint64` | `i64`, `u64` | `bigint` | range-checked `int` |
| `bytes` | generated byte container | `Uint8Array` | `bytes` |
| `string` | `String` | `string` with valid Unicode | `str` with valid Unicode |
| Message | generated struct | generated message type | generated message class |
| Enum | generated representation retaining numeric access | generated enum/numeric value | generated enum/numeric value |

No JSON projection occurs in the business payload path. Validate integer bounds,
reject lone surrogates before encoding strings, and test enum values unknown to
the schema. Protobuf supports non-finite floating-point values; applications
that disallow them must validate them explicitly. Do not assume every runtime
preserves unknown fields after decoding and re-encoding; the first release does
not offer a message-forwarding compatibility guarantee.

## 3. Code generation and developer workflow

Use a pinned build toolchain: Buf for schema build/lint, standard message
generators, and Cellar-specific service generators. Generation runs at build
time. Runtime nodes never fetch schemas or invoke a compiler.

| Output | Generator responsibility |
| --- | --- |
| Rust messages | `prost-build` with workspace-pinned runtime/build dependencies |
| Rust service code | `prost-build::ServiceGenerator`: trait, client, registration table and dispatcher |
| JS/TS messages | Protobuf-ES; ESM JavaScript plus declarations or TypeScript build input |
| JS/TS service code | Cellar plugin: concrete client methods, handler interface, explicit registration table |
| Python messages | Official `protoc` Python output plus `.pyi` typing stubs |
| Python service code | Cellar plugin: async client, `Protocol`, registration and dispatcher |
| Contract artifact | Descriptor image, exact contract digest, service/method inventory and generator lock metadata |

[Prost's service generator](https://docs.rs/prost-build/latest/prost_build/trait.ServiceGenerator.html)
supports custom RPC interfaces. [Protobuf-ES](https://github.com/bufbuild/protobuf-es)
and the [Python generator](https://protobuf.dev/reference/python/python-generated/)
provide message code, not Cellar routing. [Buf generation](https://buf.build/docs/generate/)
orchestrates plugins; the proposed workflow uses pinned local plugins so ordinary
builds do not depend on a hosted registry.

Proposed developer commands:

```sh
buf lint
buf build -o build/contracts.binpb
buf generate
cellar rpc check --descriptor build/contracts.binpb --inventory build/rpc.json
```

`cellar rpc check` now validates descriptor/profile compliance, exact canonical
inventory and an optional native `Catalog::inventory()` registration snapshot
offline. Frozen language/package files and consumer binding validation remain
delivery work; see the [CLI contract](../docs/23-cli-reference.md#offline-typed-rpc-checks).
The generator must advertise proto3 optional support and emit
actionable errors for unsupported schemas and language identifier collisions.
Use standard language name conversion (`GetCount` to `getCount` or `get_count`)
while preserving the exact wire method. Reserve SDK/client helper names; reject
collisions rather than silently rename methods.

Generated outputs for shipped examples are checked in with their schema and
tool lock metadata. CI regenerates to a temporary directory and compares files.
Published npm/Python/Rust artifacts contain their needed message/runtime
dependencies and contract metadata; consumers do not need `protoc`. Pin exact
tool/runtime versions in the first implementation milestone, not floating
`latest` references. Qualify Python dependencies against both supported CPython
modes and JS dependencies inside both embedded V8 and Node.

### Proposed caller API

The following are API sketches. `bind` resolves an existing namespace and
captures the current invocation; it performs no network discovery. JS/Python
binding validation throws, while Rust returns `Result`.

```ts
import { CounterClient } from "./generated/counter_cellar.js";

const counter = CounterClient.bind(ctx, "COUNTERS", "alice");
const reply = await counter.increment({
  delta: 1n,
  operationId: "order-123",
});

if (reply.result.case === "count") {
  console.log(reply.result.value.value);
}
```

```python
from generated.counter_pb2 import IncrementRequest
from generated.counter_cellar import CounterClient

counter = CounterClient.bind(ctx, "COUNTERS", "alice")
reply = await counter.increment(
    IncrementRequest(delta=1, operation_id="order-123")
)

if reply.WhichOneof("result") == "count":
    print(reply.count.value)
```

```rust,ignore
let counter = CounterClient::bind(&ctx, "COUNTERS", "alice")?;
let reply = counter.increment(IncrementRequest {
    delta: Some(1),
    operation_id: "order-123".into(),
}).await?;
```

JS callers may supply a generated message initializer; the client normalizes
and validates it before encoding. Python uses generated messages. Rust uses
owned message values. None of these clients exposes arbitrary reflection or
method-name dispatch as its normal application API.

### Proposed server API

Business methods receive their decoded input and a fresh invocation context:

```ts
interface CounterHandler {
  increment(input: IncrementRequest, ctx: Context): Promise<IncrementResponse>;
  getCount(input: GetCountRequest, ctx: Context): Promise<GetCountResponse>;
}
```

```python
class CounterHandler(Protocol):
    async def increment(
        self, request: IncrementRequest, ctx: Context
    ) -> IncrementResponse: ...

    async def get_count(
        self, request: GetCountRequest, ctx: Context
    ) -> GetCountResponse: ...
```

```rust,ignore
pub type RpcHandlerFuture<T> = Pin<Box<
    dyn Future<Output = Result<T, RpcFailure>> + Send + 'static
>>;

pub trait CounterHandler: Send + Sync + 'static {
    fn increment(
        self: Arc<Self>,
        ctx: Context,
        request: IncrementRequest,
    ) -> RpcHandlerFuture<IncrementResponse>;

    fn get_count(
        self: Arc<Self>,
        ctx: Context,
        request: GetCountRequest,
    ) -> RpcHandlerFuture<GetCountResponse>;
}
```

`RpcHandlerFuture` is a proposed alias under `sdk::rpc`. Generated dispatchers
adapt it to the existing `HandlerFuture<Output>`, whose error channel remains
available for host failures. Handler `RpcFailure` values become sanitized
structured failures with unknown outcome. Expected Counter failures are
response variants instead.

Generated `CounterRegistration` connects these methods to the native class
registration and the language's class export. At startup, validate the contract
digest, full method set, input/output types, and existence of callable handlers.
Use an explicit generated dispatch table; never invoke `target[user_method]`,
prototype members or Python attributes selected directly from wire input.

The existing `.cell("Counter")` helper implies a fetch handler. Add a separate
registration builder for RPC-only classes and composable HTTP/alarm/RPC handlers
without requiring a dummy `fetch`. One native class registration merges its
declared event handlers and services; duplicate method registrations fail.

## 4. Wire contract over existing cell RPC

Avoid a new public transport or `RpcData` variant in the first release. Use
`RpcData::Structured` and the existing structured-clone codec. The outer RPC
method is the fully qualified service method. Its argument root is a dense
array with exactly these entries and no own properties:

```text
[
  "cellar.protobuf",       // discriminator
  1,                       // Cellar-owned format marker
  "<contract-sha256>",     // lowercase, 64 hexadecimal characters
  ArrayBuffer(request)     // exact Protobuf bytes; zero bytes is valid
]
```

The array and its byte buffer are the only object nodes. Reject aliases,
additional nodes/properties, resizable buffers, views and capability values.
Use a bounded structured decoder before checking the exact graph shape. The
format marker stays at 1 under current repository policy; unsupported formats
are refused without legacy readers or fallback decoding.

Successful native RPC output uses the same tagged array, with the final buffer
holding a small framework `RpcReply` Protobuf message. Its required logical
`oneof` selects either response-message bytes or a structured failure:

| `RpcReply` field | Meaning |
| --- | --- |
| `response` (`bytes`, oneof member) | Encoded application response; empty is a valid selected value |
| `failure` (`RpcFailure`, oneof member) | Fixed code, effect outcome and bounded safe message |
| Neither selected | Invalid result; caller treats effects as unknown |

Responses echo the request digest, including a `ContractMismatch` refusal, so
the caller can decode that failure without requiring the provider's descriptor.
Echoing a digest is correlation, not evidence that the provider supports it.
The provider compares the request against its own registration before business
dispatch. Invalid outer tags that cannot be safely correlated fail through the
existing native error channel.

`RpcFailure` has an enum code and an enum outcome whose zero value is `UNKNOWN`.
`NOT_COMMITTED` is emitted only from a framework-proven pre-handler rejection.
Unexpected enum values, malformed responses or an incorrect digest are protocol
failures with unknown outcome. Application handlers cannot claim
`NOT_COMMITTED` by constructing an exception with matching text or properties.

The tagged response travels through `Output::Rpc`, including its normal output
gates. Existing outer native failures and remote error envelopes remain outer
failures and are mapped conservatively. Never turn a failed output gate into a
successful `RpcReply`.

The compilable framework schema is [rpc.proto](named-typed-rpc/rpc.proto).
The framework, rather than business handlers, builds this envelope. Guest
completion carries either application response bytes or a failure indication;
the native adapter assigns the trusted outcome and constructs `RpcReply`.
Generated native dispatchers apply the same rule. No application-supplied
failure flag is accepted as pre-admission evidence.

```text
Generated client                  Host                      Target
      |                            |                           |
 validate + encode                 |                           |
      |---- bound cell.call ------>|                           |
      |                     egress gate + route                |
      |                            |---- CellRpc ------------->|
      |                            |                   registry / digest
      |                            |                   bounded decode
      |                            |                   business handler
      |                            |                   encode RpcReply
      |                            |<--- Output::Rpc ----------|
      |                            |                   output proof gate
      |<---- result via host ------|                           |
 validate + decode                 |                           |
```

The diagram shows logical stages; the output proof gate completes before the
result is released to the caller. Source egress gates also preserve the caller's
own write ordering before its nested RPC is sent.

### Contract identity and deployment

Build one immutable descriptor image per contract bundle, including imports.
Sort file entries by name, remove source-location information, and serialize
with the pinned descriptor toolchain. Compute SHA-256 over those exact artifact
bytes. Generate every language from that same artifact and embed the same digest.
Do not independently reserialize descriptors in each runtime to compute identity.

First release requires an **exact digest match**. At deployment validation,
check known binding/interface requirements against the selected target class's
registered inventory. Repeat the digest check at the receiving dispatcher to
cover stale clients, remote routing and deployments that cannot be checked
statically. Reject mismatches before business code runs.

This intentionally does not negotiate mixed schema revisions. A contract change
requires coordinated deployment or an explicitly separate service and binding.
Buf compatibility checks remain useful authoring checks, but they do not grant
runtime compatibility. A Protobuf package suffix such as `v1` names an
application API; it is separate from Cellar's internal format marker.

The generated schema inventory records the descriptor digest, method wire name,
request/response full names and codec limits. It cannot infer application class
names or namespace bindings from a `.proto` file. Application registration adds
the class-to-service mapping; consumer requirements add the service/digest
needed through each binding. Native startup joins these entries with the actual
guest export table before admitting work. The
[registration and dispatch contract](named-typed-rpc/registration-and-dispatch.md)
defines this join and the validation order. Manifests are inputs to validation,
never authority to load arbitrary code.

The digest covers a whole bundle, including imports. Changing an unrelated file
in that bundle therefore also changes its identity. Keep independently deployed
interfaces in separate bundles with their required import closure. A class can
register services from several bundles, but a fully qualified method can have
only one registration. Coordinated cutovers or separate service/binding names
must be explicit; additive Protobuf compatibility alone cannot satisfy an exact
digest check.

## 5. Binary language bridge changes

Add `cell.call` with JSON control metadata and one owned byte attachment:

```text
control: {binding, name, method, contract_digest}
body:    encoded request bytes

reply control: {ok: true} or fixed framework failure metadata
reply body:    encoded application response bytes when ok
```

The shared Rust adapter validates binding/method metadata, reserves capacity,
constructs the structured argument and calls the checked native stub using the
original `Context`. The native typed client terminates the framework `RpcReply`
envelope; child failures use the existing fixed SDK control-error envelope, and
successful application bytes pass unchanged to the guest message codec. This
avoids decoding infrastructure failures separately in each language. SDK futures
continue to be polled inside that invocation.
`cell.call` is mutation-class for guest operation admission; naming a method
`GetCount` does not grant a concurrent-read exemption.

| Boundary | Required work |
| --- | --- |
| Embedded V8 | Add owned byte attachments to `Start`, `Operation`, `Reply`, `Complete`; copy exact `Uint8Array` slices across V8, never hold local handles in native futures |
| JS runner | Normalize bytes without `JSON.stringify`; recognize RPC invocation/completion separately from HTTP; dispatch only generated registrations |
| Node worker | Carry attachments in both directions; preserve invocation/operation IDs and supervised lifecycle |
| Node transport | Replace internal JSON-only frames with the binary frame below; maintain authenticated startup and stdout separation |
| Python bridge | Reuse metadata-plus-bytes operations, add RPC start/completion, validate byte types and preserve cancellation reservations |
| Shared guest | Add bounded RPC encode/decode and typed failure conversion; retain original SDK authority |

Node's proposed frame is `u32be metadata_length`, `u32be body_length`, UTF-8 JSON
metadata, then raw bytes. Metadata includes `has_body` so an absent attachment
is distinguishable from an empty one. Reject contradictory length/presence
values. Validate both lengths and their checked sum before allocating; receive
one bounded frame at a time and account for partial writes. Control/heartbeat
frames use no attachment. Old/new workers are not mixed: the verified runtime
package is updated together with the host, using the existing internal format
policy rather than a permanent fallback parser.

All affected existing HTTP and lifecycle tests must pass after that framing
change. JSON storage values stay JSON; this change introduces an RPC binary
path without changing their public representation.

### Limits and accounting

These are proposed conservative defaults, to be measured before release:

| Limit | Initial default |
| --- | --- |
| Protobuf request or application response bytes | 1 MiB each |
| Control metadata per frame | 16 KiB |
| Fully qualified method name | 256 UTF-8 bytes |
| Protobuf nesting depth | 64 |
| Decoded field/element budget | 100,000 per message |
| Estimated decoded layout slots | 1,000,000 per message |
| Safe failure message | 256 UTF-8 bytes |

Use the minimum of method, runtime and host limits. Validate startup
configuration so a maximum permitted response plus envelope overhead fits the
existing value and queue budgets. Count the internal frame header and every
simultaneously retained owned copy against bridge reservations; do not reuse
the HTTP base64-equivalent accounting formula as an unexplained RPC limit.
Owned reservations survive cancellation until queued/native work drops them.

The native worker snapshots the application's RPC catalog at startup. Host
pre-factory admission, invocation-bound clients and generated native response
encoders use that same immutable inventory. A generated descriptor's default
limits do not override a matching provider or consumer declaration. Native
consumer-only applications declare `Registry::rpc_contract(contract)`, just as
guest sources declare their consumer artifact. A native router also verifies
that its generated descriptor matches the declared provider before dispatch.
Receiving owners enforce their own limits independently; differing peer limits
must never cause a caller to bypass receiving-side validation.

Binary size alone does not bound decoded object allocation. Generate a bounded
wire preflight walker from descriptors: check length-delimited fields, nesting,
packed elements and repeated field occurrences before language decoding. The
walker must follow Protobuf merge/duplicate-field semantics and reject malformed
varints or truncation without panics. Enforce the same profile in Rust clients
and both guest paths. Retain each runtime's heap/process limits; these budgets
are not a claim of a strict whole-process RSS cap.

Serialized occurrences alone miss default initialization. A few bytes can
describe thousands of empty messages, each with hundreds of declared fields.
Use a conservative, language-neutral decoded-slot estimate:

```text
slots(message) = 1 object + number of declared physical fields
              + each wire field occurrence / packed element
              + slots(each present nested message or synthetic map entry)
              + flat layout of an omitted message-valued map default

4 KiB wire: 2,000 empty Wide messages, 512 declared fields each
    Batch layout:                2
    repeated occurrences:    2,000
    Wide default layouts: 1,026,000
    total:                1,028,002 slots -> refuse at the 1,000,000 default
```

Count every physical oneof alternative in the layout, even when unset, because
language runtimes represent defaults differently. A missing message-valued map
value charges its flat layout without inventing a nested wire occurrence or
extra wire depth. Do not recursively initialize absent ordinary message fields.
Run the native slot check before decoding, and the corresponding outgoing graph
check before message construction, sizing or serialization. The allowed range
is 1–4,000,000 slots. This estimates work and allocation amplification; it does
not measure allocator bytes or replace a runtime heap limit.

Framework replies have a separate fixed control allowance: depth 2, 16 wire
elements and 32 layout slots, with the existing application-byte limit plus
1 KiB envelope allowance. A business budget of one element or one slot must
still carry a refusal. The private framework envelope refuses unknown fields,
duplicate control fields and multiple reply alternatives; application Protobuf
messages retain their supported unknown/duplicate-field behavior.

Client encode preflight validates the input graph, numeric ranges, depth,
element count and calculated wire size before allocating an encoded buffer.
Rust generation emits graph visitors from the actual Prost declarations,
including imported messages and oneofs. Generated clients and response handlers
consume messages through a checked encoder. It validates the graph before
recursive `encoded_len`, checks the complete size before the writer, then
releases owned child graphs with an iterative cleanup stack. Refusing a deep
message must not trigger recursive destruction. Borrowed graph inspection does
not take over the caller's cleanup responsibility.
JavaScript preflight checks actual descriptor fields and exact scalar types;
it must not rely on TypeScript annotations or a codec's coercions. Refuse
misspelled properties, malformed oneofs, getters, cycles, sparse arrays and
shared/resizable/detached byte storage. Snapshot accepted container properties
before the codec creates its message. Count tags, lengths, packed elements and
synthetic map entries, including the expanded wire shape of `Struct` values.
Concurrent mutation of a JS byte view cannot affect a submitted call: submission
copies its exact slice into owned storage. Response preflight occurs after the
handler, so failure there never proves rollback.

The JavaScript codec discards unknown wire fields while decoding and does not
serialize caller-built `$unknown` metadata. Other codecs may retain unknown
fields internally; message forwarding across runtimes is outside this release's
compatibility contract. Native preflight still validates every incoming wire
occurrence before any language decoder runs. Encode preflight cannot replace
runtime heap/process limits or retained bridge-copy accounting.
Incoming wire limits and outgoing encoding costs are distinct: default map
fields can be omitted by one codec and written explicitly by another. Decoders
must not reject a native-preflighted message by charging its prospective
re-encoding cost. Exact allocation/RSS and saturation qualification remain
separate boundaries from the decoded-slot estimate and encode buffers.

## 6. Invocation authority, failures and cancellation

```text
validate locally --> submit --> admit target --> execute --> proof --> deliver
      |                |             |             |           |
 proven local       reply lost / cancellation / encoding failure
 refusal                  => effects may be unknown
```

Generated clients capture the originating context and checked stub. They cannot
be reused across invocations, activation retirement or application generations.
Server methods receive a new context on every invocation. Cell object fields
remain caches; neither `this` nor a Python instance is durable storage.

| Failure | Outcome delivered to the caller | Retry rule |
| --- | --- | --- |
| Client type/size/profile validation before submission | `not_committed` for this target call | Fix input |
| Unknown registered method or digest mismatch before user code | `not_committed` for this target call | Fix registration/deployment |
| Valid business rejection in a response | Application-defined result | Follow that business contract |
| Overload with no trustworthy call-local admission evidence | `unknown` | Resolve or deduplicate before mutation retry |
| Timeout, cancellation, fencing, transport loss after submission | `unknown` | Resolve or deduplicate |
| Handler exception, invalid/oversize response after execution | `unknown` | Resolve or deduplicate |
| Durability proof failure | `unknown`; never success | Recover/resolve under host policy |

Standardize fixed codes such as `InvalidArgument`, `MethodNotFound`,
`ContractMismatch`, `ResourceExhausted`, `ContextExpired`, `Cancelled`,
`DeadlineExceeded`, `Unavailable`, `OwnershipLost`, `DurabilityUnproven`,
`InvalidResponse` and `Internal`. Code and effect outcome are separate fields.
Do not expose SQL, arguments, credentials, local paths or arbitrary exception
strings. A diagnostic identifier can correlate a failure with protected logs.

**Call provenance matters.** A parent handler can commit a write and then fail
because its child RPC was rejected. That child's `not_committed` must not become
the parent's `not_committed`. Generated dispatchers convert failures escaping
an entered handler to unknown unless a separate trusted host proof applies.
Keep submission/admission evidence in private Rust types, never a caller-provided
JSON flag. The current ordinary RPC path does not provide all this evidence;
adding it is an implementation task, not an existing guarantee.

Validation must precede business construction as well as method execution if
the framework reports a pre-handler refusal. Generated wrappers delay lazy
business-object construction until validation passes. Native factory work that
runs earlier must be side-effect-free, or failure is conservatively unknown.

### Deadline and cancellation policy

Add a bounded `RpcCallOptions` with an optional relative timeout and a
language-appropriate cancellation signal. Defaults come from explicit host
configuration; calls cannot extend the parent invocation lifetime. Start the
deadline before validation/encoding, check it before submission, and cover
queueing, execution, proof and reply decoding.

Reuse the native RPC cancellation registration and drop behavior. A dedicated
execution-domain timer races the full call; do not depend on a blocked JS/Python
event loop for enforcement. If the remaining budget crosses a peer boundary,
send a remaining duration and cap it locally, never transmit a machine-local
monotonic timestamp. The original caller's timer remains authoritative.

The existing ordinary RPC path needs this budget plumbing; event-stream RPC
already has separate special-case deadlines that must remain unchanged.
Reject nested attempts to extend a parent's deadline. Cancellation revokes SDK
authority and releases resources once work settles; it does not roll back
already committed writes or promise to interrupt arbitrary application CPU code.

No generated client silently retries mutations. Existing safe host routing
behavior remains, but adding typed methods must not replay admitted user code.
Nested call cycles still risk admission exhaustion; qualification includes
self-calls and A-to-B-to-A waits with bounded failure and cleanup.

### Authorization and observability

Namespace bindings, current invocation authority and existing peer authentication
remain the access boundary. A service name or digest is not an authorization
token. The host resolves namespaces rather than accepting arbitrary guest scope
strings, and reserved framework RPC capabilities remain inaccessible. Business
authorization belongs in an explicit handler policy using trusted caller context;
never infer a tenant or principal from request fields alone. This release does
not introduce a public RPC endpoint or a new end-user identity system.

Emit spans for registered service/method, backend, decode, queue wait, execution,
proof wait and response encoding. Metrics include call count, refusal/unknown
outcomes, request/response bytes, latency and retained bridge bytes. Labels must
come from bounded registration inventories; never label by cell name, operation
ID or unvalidated method text. Log digests only as deployment diagnostics and
keep arguments/results out of default logs. Preserve existing parent trace and
host request identity across calls without exposing SDK authority to guests.

## 7. Durability and the reference Counter

A method invocation is not automatically one SQLite transaction. Application
code can await between several independently committed operations. Reuse
checked storage batches/transactions and the host's existing output proof path.
RPC does not add an application-wide distributed transaction.

The Counter example has a deliberately narrow contract:

1. Require `delta` to be present and positive; bound `operation_id` length.
2. Read the counter and receipt for that operation ID.
3. If a receipt exists, compare its stored delta. Return the saved result when
   equal; return `OperationConflict` when the same ID has different semantics.
4. Check signed 64-bit overflow. Encode the result before committing it.
5. Atomically store the next count and a receipt containing delta and result,
   checking the observed counter and receipt snapshots in the same batch.
6. On a storage conflict, perform a bounded internal reread/recompute. This is
   local optimistic concurrency, not a retry of external side effects.
7. Await the configured durability proof and return through the normal host gate.

```text
Counter state + receipt in one transaction
                    |
                 commit
                    |
             durability proof
                    |
              reply is lost
                    |
       retry SAME operation ID + delta
                    |
           find durable receipt
                    |
       return saved result; no increment
```

For full `i64` interoperability, the JS example cannot store an unchecked
`bigint` in JSON storage. Store count/receipt numeric fields as canonical decimal
strings in a documented common storage layout; use exact `bigint` arithmetic in
JS and checked `i64`/bounded integer arithmetic in Rust/Python. Each backend
implements that same layout for the qualification example.

The example retains receipts for its entire qualification run. Production
applications must document receipt retention, a minimum supported retry window,
and behavior after expiry. Do not imply deduplication forever after deleting
receipts. External HTTP/model calls require their own idempotency or durable
workflow/outbox design.

Do not hash serialized Protobuf bytes as a universal semantic command identity:
Protobuf serialization is not canonical across implementations. The Counter
compares its typed semantic fields; more complex applications must define their
own canonical command representation. [Protobuf serialization guidance](https://protobuf.dev/programming-guides/serialization-not-canonical/)

## 8. Ownership of implementation changes

Paths marked **new** are proposed; links point to existing source owners.

| Owner / path | Concrete responsibility |
| --- | --- |
| **New** `crates/cellar/application/rpc/mod.rs` | Public `sdk::rpc` facade, contract registration, bounded envelope, call options and failures |
| Private `rpc/{contract,catalog,envelope,wire,client,router,failure}.rs` | Separate descriptor/profile validation, class registration, envelope/wire parsing, invocation-bound clients and dispatch responsibilities |
| **New** `tools/cellar-rpc-codegen/` | Descriptor/profile validation and language generators; build-only tooling |
| **New** `proto/cellar/rpc.proto` | Framework reply/failure schema; owned marker remains 1 |
| [cell stubs](../crates/cellar/application/cells.rs) | Typed wrapper integration while preserving untyped `call`/`rpc` |
| [host RPC](../crates/cellar/runtime/invocation/rpc.rs) and [native worker](../crates/cellar/runtime/workers/native/mod.rs) | Call-local failure evidence and deadlines; retain output-gate behavior |
| [node](../crates/cellar/node/mod.rs) | Preserve method, bytes, deadline budget and typed refusal evidence on peer forwarding |
| [guest adapter](../crates/cellar-guest/operations.rs) | Shared `cell.call`, binary envelope and conservative failure conversion |
| [JS application](../crates/cellar-javascript/application.rs) and [runtime](../crates/cellar-javascript/runtime/mod.rs) | Registration, RPC dispatch, binary messages and RPC results |
| [Node transport](../crates/cellar-javascript/runtime/node/transport.rs) and [worker](../crates/cellar-javascript/runtime/node/worker.mjs) | Bounded binary framing and byte-preserving process boundary |
| [JS SDK](../packages/cellar-sdk/index.d.ts) | Generated-client transport contract and caller-facing errors/options |
| [Python application](../crates/cellar-python/application.rs), [runner](../crates/cellar-python/python/runner.py), [facade](../crates/cellar-python/python/cellar.py) | RPC registration, typed dispatch and operation-bound Python client transport |
| [native registration](../crates/cellar/node/native.rs) | Include service/digest requirements in verified application inventory |
| **New** `examples/typed-rpc/` and `tools/rpc/` | Cross-language Counter, generated artifacts, conformance and failure harness |

Keep the schema/compiler in tools, language codecs in their optional adapters,
and the public runtime API under `cellar::sdk`. No new dependencies, I/O or
business schema logic belong in `cellar-logic`. Follow the repository's component
module layout; use sibling test files for leaf-module unit tests.

## 9. Ordered implementation work packages

Each package is independently reviewable. A partial package does not establish
cross-language or durability qualification.

| Package | Depends on | Deliverable | Exit evidence |
| --- | --- | --- | --- |
| RPC-01: Contract/toolchain | None | Pin tools; compile reference schema; descriptor profile, digests, generated messages and golden fixtures | Reproducible output; all languages agree on fixtures, integer boundaries and presence |
| RPC-02: Native typed path | 01 | Rust client/dispatcher, tagged envelopes, registration, limits, failures and deadlines | Rust-to-Rust local/remote calls; malformed envelopes and output-proof tests |
| RPC-03: Shared binary bridge | 02 | `cell.call`, reservations, V8 attachment path, Node framing, Python attachment extension | Fragmented/truncated/oversize frames; cancellation cleanup; existing HTTP behavior unchanged |
| RPC-04: JS/TS services | 01-03 | Generated clients/handlers and RPC dispatch for V8 and Node | JS and TS examples compile/bundle; both backends call Rust and each other |
| RPC-05: Python services | 01-03 | Generated async clients, typing stubs, registrations and RPC dispatch | Python calls Rust/JS and receives their calls; supported CPython modes pass |
| RPC-06: Fleet/durability qualification | 02-05 | All-backend Counter with atomic receipts; crash, handoff, nested-call and resource tests | Full matrix and failure ledger; acknowledged state recovered and no duplicate effects |
| RPC-07: Packaging and performance | 01-06 | Reproducible artifact workflow, docs, CI checks and benchmark report | Clean-machine examples; frozen builds; TPS/throughput/latency/RSS evidence |

The native binary envelope and JS attachment work are prerequisites for claiming
binary typed RPC. A JSON/base64 prototype may aid local development but does
not satisfy RPC-03 or the release gate.

## 10. Qualification and release gates

### Interoperability matrix

Run every row/column combination locally and across two fleet nodes. JS and TS
are authoring modes for each JS backend, not separate wire protocols.

| Caller / callee | Rust | V8 | Node | Python |
| --- | --- | --- | --- | --- |
| Rust | Required | Required | Required | Required |
| V8 | Required | Required | Required | Required |
| Node | Required | Required | Required | Required |
| Python | Required | Required | Required | Required |

Each pairing covers `GetCount`, `Increment`, a business rejection and a generic
codec fixture with bytes, optional fields, maps, repeated values and `oneof`.
Compile Rust clients, typecheck TS clients, execute plain JS clients and check
Python `.pyi`/`Protocol` usage. Test that wrong request types fail static checks
where available and fail runtime validation for dynamic callers.

### Required failure and correctness cases

| ID | Test boundary | Required observation |
| --- | --- | --- |
| T01 | Codec extremes | Signed/unsigned 64-bit bounds, empty bytes, Unicode, unknown enums and presence survive all pairs |
| T02 | Parser abuse | Truncation, invalid varints, deep/large messages, repeated-field amplification and malformed clone graphs fail boundedly |
| T03 | Registration | Missing method, duplicate service, identifier collision, unsupported schema and digest mismatch fail before business code |
| T04 | Isolation | Constructor/prototype/private helpers cannot be called; reserved host capability scopes remain inaccessible |
| T05 | Context lifetime | Retained clients/contexts fail after return, cancellation, retirement and generation replacement |
| T06 | Failure provenance | Parent writes then child refuses: parent result stays unknown; no forged failure can claim rollback |
| T07 | Reply conversion | Handler commits then returns invalid/oversize data: caller receives unknown; committed state remains visible |
| T08 | Output proof | Withhold/fail proof: no successful response escapes; release proof permits the appropriate result |
| T09 | Crash before commit | Same operation ID retry yields one eventual mutation |
| T10 | Crash after commit/before reply | Recovery plus same-ID retry returns saved result without incrementing twice |
| T11 | Ownership transfer | Stale owner/context cannot acknowledge new work; new owner recovers acknowledged count and receipts |
| T12 | Receipt conflict | Same ID/same delta returns stored result; same ID/different delta returns conflict |
| T13 | Concurrent callers | Distinct operations produce exact total; same-ID races produce one mutation |
| T14 | Timeout/cancel | Before/after submission and during proof wait: correct outcome, bounded cleanup, no detached SDK work |
| T15 | Dependency cycles | Self-call and A-to-B-to-A cannot leak admission slots indefinitely |
| T16 | Binary transport | Fragmented headers/bodies, empty bodies, partial writes, flood and disconnect cannot bypass limits or mix calls |
| T17 | Lifecycle | Node death, V8 termination, Python cancellation suppression and host shutdown release owned reservations and join work |
| T18 | Packaging | Missing runtime dependency, wrong descriptor digest and mixed host/Node package are refused |
| T19 | Regression | HTTP, alarms, queue, scheduled, untyped RPC and existing framework cells retain their contracts |

Use existing SDK/fleet harnesses at the owning boundary. Run standard format,
layout, dependency and documentation checks, affected locked Rust tests and
Clippy, TS typechecks and Python checks. Do not enable `cellar_internal_tests`
without its external harness. Record toolchains, exact revisions, test topology
and known failures; passing in-memory fixtures is not durability evidence.

### Performance report

Compare native untyped RPC, native typed RPC, and all guest backend paths on the
same host policy and revision. Separate echo overhead from durable work:

| Workload | Measurements |
| --- | --- |
| Encode/decode only | Messages/s, bytes/s, allocation/copy cost by language |
| Unary echo, local and remote | Completed calls/s, payload MiB/s, p50/p95/p99 latency |
| One hot durable Counter | Acknowledged mutation TPS, contention/conflicts, latency |
| Many independent Counters | Aggregate acknowledged TPS, saturation, admission failures |
| Repeated saturation/cancellation | RSS/heap, bridge reservations, queues, cleanup and recovery |

Use 0 B, 1 KiB, 64 KiB and maximum legal payloads; concurrency 1, 8, 32 and 64.
Report offered load, successes, refusals, unknown outcomes, durability mode,
CPU/RSS and encoded overhead separately. Throughput counts successful payload
bytes, not failed attempts. Durable TPS counts proven acknowledgements and is
followed by a recovery audit. Record a measured native reference before setting
regression budgets; this proposal makes no unmeasured performance promise.

### Completion definition

The feature is ready only when all work packages and required tests pass,
generated code is reproducible, every local/remote backend pairing works, and
restart/handoff tests recover all acknowledged mutations with receipt-based
deduplication. Streaming and public gRPC remain explicitly outside that claim.

## 11. Alternatives and implementation risks

| Decision | Reason and remaining risk |
| --- | --- |
| Protobuf over a new YAML IDL | Reuse mature message tooling; Cellar still owns service generation and runtime semantics |
| Existing RPC transport over a new internal gRPC stack | Preserve native identity, output gates and cancellation; public gRPC can be a separate ingress adapter later |
| Explicit registration over reflective public methods | Stable contracts and no accidental exposure; generated tables need startup validation |
| Structured byte envelope over a new `RpcData` variant | Smaller host transport change; measure clone-envelope copies and overhead |
| Exact digest over compatibility negotiation | Fits current hard-cutover policy; coordinated updates are required even for additive schema changes |
| Unary first | Avoid inventing stream resume/backpressure semantics as part of typed dispatch |

Toolchain spikes must verify Protobuf-ES under Cellar's restricted embedded V8
globals, Python Protobuf under supported interpreter modes, and bounded parser
behavior across codecs. If a selected codec cannot satisfy those gates, change
the codec choice with recorded evidence; do not silently narrow the advertised
backend matrix. Native-only dependency checks must remain engine-free.

## Design validation record

The initial design change added a design and reference schemas. Its source
map was inspected at the baseline above. Planning validation used `protoc 34.0`
to compile the reference contracts and generate Python messages/typing stubs;
the Counter schema embedded above produced the same descriptor as its reference
file. Generated Python files were syntax-checked, not imported or executed.
The repository documentation checker, changed-document local references,
balanced fences, ASCII diagram widths and `git diff --check` were also checked.
Those planning checks did not qualify a generated Cellar client, bridge,
cross-language execution, performance or durability. Subsequent implementation
evidence and remaining work belong in the linked implementation ledger.

The registration/dispatch refinement separates generated schema inventory from
application and deployment registration, defines actual codec-shape checks and
documents validation before business construction. Its planning validation
compiled both reference schemas with `protoc 34.0`; documentation checks covered
the new local links, anchors, balanced fences and ASCII diagram bounds. These
remain design checks, with no additional runtime qualification claim.

Related guides: [routing and RPC](../docs/07-routing-rpc.md),
[storage and transactions](../docs/06-storage-transactions.md),
[SDK lifetimes](../docs/05-sdk-and-lifetimes.md),
[testing and qualification](../docs/19-testing-qualification.md),
[Python design](007-embedded-python-pyo3.md), and
[JavaScript design](008-embedded-javascript-typescript.md).

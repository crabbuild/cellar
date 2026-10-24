<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cellar RPC code generator

Build-time generation for plan 009. The current generator emits Rust, JS/TS and Python
messages, typed cell clients, handler interfaces and explicit registrations. The
cross-language implementation is in progress; see the
[implementation ledger](../../plans/named-typed-rpc/implementation-status.md).

Compile a descriptor with imports, then generate its immutable artifact and
service code (requires `protoc` and Node 24+ on PATH for JS/TS generation):

```sh
npm ci --prefix tools/rpc --ignore-scripts
protoc -I plans/named-typed-rpc --include_imports \
  --descriptor_set_out=/tmp/counter.binpb counter.proto
cargo run --locked -p cellar-rpc-codegen -- \
  --descriptor /tmp/counter.binpb --out /tmp/counter-generated \
  --languages rust,python,typescript,javascript
```

The runtime does not invoke a compiler. Generation validates the supported
Protobuf profile, strips source locations, sorts descriptor files, and computes
one digest shared by the generated clients and server registrations. The output
directory contains `contracts.binpb`, `rpc.json`, and requested `rust/`,
`typescript/`, `javascript/` and `python/` sources. The default language is `rust`.
Generated source files include the Cellar copyright/Apache-2.0 notice while
retaining producer comments. Regeneration preserves this preamble; descriptor
and inventory artifacts remain binary/JSON without comment headers, following
[NOTICE](../../NOTICE).

## Descriptor admission

Generation and native startup use the same `Contract::from_descriptor` validator.
An invalid descriptor fails before the generator creates its output directory,
writes an artifact or invokes a language compiler.

```text
descriptor set (at most 4 MiB)
        |
canonical files + acyclic imports + bounded symbol namespace
        |
visible message/enum references + portable schema semantics
        |
normalized descriptor + digest + codec inventory
        |
language name checks -> output files -> message/service generators
```

Packages, messages, enums, services, fields, oneofs and methods share one symbol
namespace. Enum values occupy their enclosing package or message scope, so
`enum First { ZERO = 0; }` and `enum Second { ZERO = 0; }` collide in the same
scope. Distinct nested message scopes may each declare `ZERO`.

A file may reference its own types, direct imports, and the public exports of
those imports. Including another file in the descriptor set is insufficient:

```text
service.proto -> forward.proto --public--> types.proto   visible
                             --ordinary-> private.proto hidden
```

This follows Protobuf's [import visibility rules](https://protobuf.dev/programming-guides/proto3/#importing-definitions).
Present weak imports behave as direct imports and do not reexport their types;
every dependency must still be included. Cellar additionally rejects duplicate
imports, duplicate modifier indexes and indexes marked both public and weak.
Negative or out-of-range indexes, self imports and import cycles are refused.

| Descriptor budget | Maximum |
| --- | --- |
| Files | 256 |
| Qualified symbol or referenced type name | 1,024 ASCII bytes |
| All symbols, including package prefixes and enum values | 100,000 |
| Aggregate expanded symbol-name bytes | 16 MiB |
| Message and enum types | 4,096 |
| Fields | 65,536 |
| Methods | 4,096; each wire name at most 256 bytes |

Expanded-name limits are checked before allocating each index name or cloning
message subtrees. These limits bound descriptor inventory work; they are separate
from message codec budgets and do not establish an aggregate runtime RSS limit.
The Counter and portable-profile fixtures retain their exact artifact identity.

## Generated language APIs

JavaScript output includes `.js` and `.d.ts`; TypeScript output includes message
types, concrete named clients, handler interfaces and explicit registrations.
Both use the pinned Protobuf-ES plugins and the codec-neutral `@cellar/sdk/rpc`
transport. `--javascript-tools DIR` selects another installed copy of this
repository's `tools/rpc` package; the default resolves that directory here.

Python includes standard messages/`.pyi` files plus generated async clients,
`Protocol` handler types and
an immutable method registration. Its runtime support is installed as
`cellar_rpc` by the optional Python adapter; ordinary Python apps do not need
the Protobuf dependency. Imports are statically resolvable, including nested
message types and imported well-known types. Incompatible Python path spellings
and normalized module collisions are rejected. Protoc's bare message-initializer
`Mapping` annotations are parameterized with the built-in `str` and `object`
types so generated stubs pass strict application checks without generic-type
exceptions. A private built-ins import avoids collisions with legal message
fields such as `object`.

Rust services qualify message types through `prost::Message`, including `()` for
`google.protobuf.Empty`. Generated well-known types resolve through
`cellar::sdk::rpc::prost_types`; applications do not need a separate direct codec
dependency. The [portable-profile fixture](../rpc/fixtures/profile.proto) and
[qualification instructions](../rpc/README.md#portable-protobuf-profile) cover
the generated APIs beyond Counter.

Native message generation also emits `MessageGraph` visitors from Prost's actual
Rust declarations and field attributes. They preserve its identifier, boxing,
oneof and import choices, including packages that contain no service. Generated
clients and response dispatchers use `rpc::encode_message`:

```text
owned message -> graph/depth/element/layout-slot/raw-byte checks
                      |
                 encoded_len -> complete byte limit -> binary writer
                      |
                 iterative cleanup of owned children
```

Prost's recursive sizing runs only after graph admission. Cleanup uses heap
iterator frames so refusing a deeply boxed response cannot overflow the stack
while dropping it. `rpc::validate_graph` is a borrowed inspection helper;
`rpc::encode_message` also owns cleanup. Maps follow the fields Prost actually
writes, including omitted default keys/values. Layout slots count declared
physical fields (including all oneof alternatives) plus objects and serialized
occurrences; omitted map-message values still charge their default flat layout.
Generated handlers receive invocation-effective catalog limits from
`Router::method_with_limits`, rather than capturing generated defaults. These
checks bound codec work; actual allocation/RSS qualification and aggregate
bridge accounting remain separate.

The local fixture uses protoc 34.0 and Python Protobuf 7.34.2. The Rust fixture
uses workspace-pinned Prost 0.14.4. Generate every language from the same image;
runtime contract identity is an exact artifact digest, not a hash recalculated
by each language's codec. The npm tool lock includes Protobuf-ES/protoplugin
2.16.0 and TypeScript 7.0.2 for full checks. Declaration emission uses a separately
pinned TypeScript 5.4.5 compiler API, matching the plugin framework's compiler.
Always use the `npm run typecheck --prefix tools/rpc` script: the two compiler
packages both declare a `tsc` bin, so its generic `.bin/tsc` link is ambiguous.

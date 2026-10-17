# cellar-native

Rust values, codecs and web primitives shared by Cellar's runtime adapters.
This crate operates on caller-owned values without a JavaScript engine.
The host supplies invocation dispatch, cell authority and durability; application
code normally accesses these primitives through [`cellar::sdk`](https://github.com/crabbuild/cellar/blob/v0.1.0/crates/cellar/README.md).

![Caller-owned Rust values pass through native primitives to owned results; the host separately supplies runtime authority.](https://raw.githubusercontent.com/crabbuild/cellar/v0.1.0/docs/diagrams/native-primitives.svg)

These primitives transform values. Use the SDK when an operation also needs
cell authority, tracked work or durability proof.

## Find a primitive

| Module | Responsibility |
| --- | --- |
| [`cell_id`](https://github.com/crabbuild/cellar/blob/v0.1.0/crates/cellar-native/cell_id.rs) | Namespace-derived cell IDs and identity validation |
| [`clone`](https://github.com/crabbuild/cellar/blob/v0.1.0/crates/cellar-native/clone/mod.rs) | Structured values, graph encoding and decoding |
| [`rpc`](https://github.com/crabbuild/cellar/blob/v0.1.0/crates/cellar-native/rpc.rs) | Structured response and remote-error envelopes |
| [`json`](https://github.com/crabbuild/cellar/blob/v0.1.0/crates/cellar-native/json.rs) | JSON conversion for structured values |
| [`sql`](https://github.com/crabbuild/cellar/blob/v0.1.0/crates/cellar-native/sql/mod.rs) | SQL values and parameter binding |
| [`streams`](https://github.com/crabbuild/cellar/blob/v0.1.0/crates/cellar-native/streams.rs) | Owned byte streams |
| [`web`](https://github.com/crabbuild/cellar/blob/v0.1.0/crates/cellar-native/web/mod.rs) | HTTP-related values, blobs, forms and URL patterns |
| [`encoding`](https://github.com/crabbuild/cellar/blob/v0.1.0/crates/cellar-native/encoding.rs) | Text encoding and decoding |
| [`compression`](https://github.com/crabbuild/cellar/blob/v0.1.0/crates/cellar-native/compression.rs) | Whole-buffer and streaming compression |
| [`crypto`](https://github.com/crabbuild/cellar/blob/v0.1.0/crates/cellar-native/crypto.rs) | Hashing, keys, signatures and cryptographic operations |
| [`call_order`](https://github.com/crabbuild/cellar/blob/v0.1.0/crates/cellar-native/call_order.rs) / [`event_context`](https://github.com/crabbuild/cellar/blob/v0.1.0/crates/cellar-native/event_context.rs) | Call ordering and event-context primitives |

Resource state belongs to the caller and is released on drop. Primitive values
do not grant permission to access a cell or extend an invocation's lifetime.
Use SDK contexts and handles for operations that require runtime authority.

## Run the example

The [primitives example](https://github.com/crabbuild/cellar/blob/v0.1.0/crates/cellar-native/examples/primitives.rs) compresses text, computes a
SHA-256 digest, matches an upload route and round-trips a multipart form.
Run it from the repository root after configuring the checkout-specific target
directory in [build guidance](https://github.com/crabbuild/cellar/blob/v0.1.0/AGENTS.md#build-and-validation):

```sh
cargo run -p cellar-native --example primitives --locked
```

The program verifies the decoded content and prints the upload size and digest.
It needs no node, bucket or running Cellar application.

## Use codecs at their owning boundary

Validate untrusted input and handle decoding failures. Structured values have
encoding and graph limits; a host may impose lower application budgets.
Preserve the distinction between an encoded remote failure and a malformed
response. For application-facing calls, use the SDK's `encode_rpc`, `decode_rpc`
and `decode_rpc_result` helpers rather than inventing another envelope.

See [routing and RPC](https://github.com/crabbuild/cellar/blob/v0.1.0/docs/07-routing-rpc.md),
[integrations](https://github.com/crabbuild/cellar/blob/v0.1.0/docs/09-integrations.md) and the
[SDK API map](https://github.com/crabbuild/cellar/blob/v0.1.0/docs/22-api-reference.md) for the host contracts.

## Validate changes

Run the crate's unit and documentation tests from the repository root:

```sh
cargo test -p cellar-native --locked
```

Changes to value encoding, identity or error envelopes also need the relevant
SDK integration tests. Follow the [SDK CI workflow](https://github.com/crabbuild/cellar/blob/v0.1.0/.github/workflows/native-sdk.yml)
for full workspace validation.

# Raft counter

This development executable runs three embedded nodes with two independent
three-voter groups, `Counter:one` and `Counter:two`. Each node serves native HTTP
on ports 9901–9903 and an application-owned Axum router on ports 9911–9913.
Both use the same registered synchronous commands, queries and node-owned
dispatcher. Followers apply LTX and serve committed-image reads without
executing the increment callback.

From the repository root, use the toolchain and checkout-specific target
directory from [build guidance](../../AGENTS.md#build-and-validation):

```sh
mkdir -p examples/.cellar/raft-counter
openssl rand -hex 32 > examples/.cellar/raft-counter/http-token
cargo run -p cellar --bin cellar-raft-counter --locked -- \
  examples/.cellar/raft-counter \
  crates/cellar/transport/tcp/fixtures/ca.der \
  crates/cellar/transport/tcp/fixtures/server.der \
  crates/cellar/transport/tcp/fixtures/key.der \
  examples/.cellar/raft-counter/http-token
```

The supplied TLS files are public test fixtures for this loopback-only example.
All replicas share one machine; the example does not provide zone resilience.
The development object-store adapter persists catalog roots and bootstrap claims
in `catalog.sqlite`. Cell data and Raft logs persist in each node's directory.
Keep all of these files for a normal restart. Losing a replica's disk requires
membership repair; this example cannot reinitialize its old voting identity.

Use a bearer header containing the token file's contents. `POST
/counters/one?delta=7` requires these headers:

| Header | Value |
| --- | --- |
| `Authorization` | `Bearer TOKEN` |
| `x-command-id` | Nonzero 16-byte client identity in hex, followed by `:1` |
| `x-retry-until-ms` | Future Unix time in milliseconds, within the configured receipt retention window |

The next new command from that client uses sequence 2. Persist a pending command's
ID, delta and retry deadline before sending it. After a lost response, resend
the same values. A conflicting delta returns 409. The optional
`x-contract-digest` header must match the registered operation when supplied;
a mismatch returns 422 before business execution or duplicate receipt lookup.
The command's normalized input is the signed 64-bit delta in big-endian bytes.
HTTP request bodies do not contribute to this endpoint's command input.

Before the Raft bindings are installed, authenticated requests return 503.
Send a command to the current leader's port. A follower returns 503 with a
`NotLeader` hint. A response containing `Ambiguous` retains the original command
ID; the gateway does not retry or generate a new one. Read using an authenticated
`GET /counters/one` on any replica after its first committed image is published.
The JSON response contains the value and the actual group/incarnation/index/term.
These fields are observations, not authenticated portable version tokens.

The current example uses Quorum durability and stale committed-image queries.
S3 command durability, minimum-version and linearizable queries are implemented
by subsequent work packages; unsupported SDK requests are refused. Ctrl-C closes
the HTTP servers and joins the services and nodes. Retain replica disks when
shutdown reports unfinished work.

The `native_axum_command_parity` integration case exercises real native and Axum
listeners, direct SDK commands, duplicate receipts, conflicting inputs,
authentication and contract mismatch. `public_multi_group_replication_and_leader_failover`
exercises two groups across three nodes, follower image publication and leader
replacement. These fixtures establish behavior within their test scope, not a
production capacity or complete disaster-recovery qualification.

Run the process restart drill against an already built binary with a new work
directory (the script retains replica disks and logs and never prints its token):

```sh
python3 examples/raft-counter/smoke.py \
  "$CARGO_TARGET_DIR/debug/cellar-raft-counter" /tmp/cellar-counter-drill
```

The drill first occupies the final Axum listener to force a late startup failure,
then resumes the same disks. It checks both ingress paths on all three nodes,
joined shutdown, retained receipts and new writes after restart.
A recovered receipt can name a later verified covering image; its stored result
and history must still match the original command.

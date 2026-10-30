<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Committed producer/receiver recovery

This finite development example runs three physical Node instances on one
machine, with separate Producer and Receiver Raft groups. A producer command
updates SQL and appends a signed outbox intent atomically. The owned scheduler
commits an attempt, sends it through `RaftNativeEffectSink`, verifies the durable
receiver receipt and commits source settlement. The receiver's SQL mutation and
deduplication receipt share a normal S3-gated command transaction.

The drill acknowledges one producer command with S3 durability, stops its
primary, retries the same command on the replacement and checks replicated
delivery progress and one receiver mutation. Running it again with the same
directory resumes retained replica disks and returns the same durable receipts.
The application uses explicit development keys and local TLS certificates.
Do not use these identities or keys for production.

```sh
cargo build --locked -p cellar --bin cellar-raft-effects
python3 examples/raft-effects/smoke.py \
  "$CARGO_TARGET_DIR/debug/cellar-raft-effects" /tmp/cellar-effects-demo \
  crates/cellar/transport/tcp/fixtures/ca.der \
  crates/cellar/transport/tcp/fixtures/server.der \
  crates/cellar/transport/tcp/fixtures/key.der
```

Use a new directory for a new drill; keep the same directory between its two
runs. The runner checks the executable digest before and after each run, retains
its logs and verifies `result.json`. The binary accepts those same four data/TLS
arguments for an individual run. Every successfully started service and Node is
joined on success or startup failure. A stopped primary can report ambiguous
internal work, which the replacement reconciles from committed state; the report
records its count. Final shutdown waits for settled S3 coverage. Retain disks and
the catalog: this example does not grant permission to discard unarchived data.

Lanes retain one immutable request until source settlement, and receivers retain
the latest receipt for each authorized lane. Generic command receipt limits still
apply; this component does not add automatic pruning or promise indefinite
operation through an outage. Its default payload bound is 32 KiB. There is no
generic HTTP or arbitrary external exactly-once guarantee: a custom sink must
honor its memory/cancellation bounds and return a valid configured receiver
receipt. Native application code is trusted, rather than OS-sandboxed.

These local processes demonstrate the stated recovery behavior. Physical zone
loss, large-cluster capacity, the release soak and federation have separate gates
in the [multi-Raft implementation plan](../../docs/design/multi-raft/implementation-plan.md).

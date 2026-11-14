<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Constrained three-node primitive comparison

`primitive_compare.py` measures the public SQL, KV and Queue bindings against
shared local RustFS. Each case launches three independent node containers, each
limited to **1 vCPU, 2 GiB RAM and no additional swap**. RustFS and the load client
have separate containers, each also limited to 1 vCPU and 2 GiB. CPU sets isolate
the three nodes (0–2), store (3) and client (4). The Colima VM needs at least five
CPUs and 11 GiB available memory; the measured VM capacity is recorded.

The application, requests and actor policy are shared across these modes:

| Mode | Write acknowledgement |
| --- | --- |
| `raft-quorum` | Three-voter Raft group quorum |
| `raft-s3` | Raft quorum plus shared-store publication |
| `fleet` | Existing native Fleet peer replication **or bucket proof** |
| `bucket` | Existing native bucket output gate |

These acknowledgement guarantees differ. Public reads retain their respective
production binding implementation; this benchmark does not establish equivalent
read algorithms or failure guarantees.

SQL and KV each use 24 service identities in three Raft groups of eight cells.
Queue uses 24 queues and their 24 declared dead-letter cells in three groups of
16. Each group has the same three voters. Readiness checks the complete enrolled
population and physical group inventory on every node, requires coherent terms
and one ready serving leader per group, and balances those leaders across the
three processes before load. Healthy followers report `ready=false`; the flag
grants serving leadership, not general replica health.
The fixed static cohort includes the inspection profile required by the local
physical inventory API; it does not qualify managed fleet capacity admission.
Each Raft node explicitly grants namespace query and command permission to the
other two declared frontend processes, pinned to their node, disk, process and
endpoint identities. Raft membership does not create command permission. Public
requests rotate across all three frontends in every durability mode.
Bucket/Fleet receive the same binding population and requests. Each case gets a
fresh RustFS process, data volume and bucket. The runner waits for
[storage/IAM readiness](https://docs.rustfs.com/en/operations/status-check) at
`/health/ready` before issuing S3 mutations. Failed resources remain retained.

The explicit comparison policy uses one host executor thread and one core
executor thread per node, a five-second actor operation deadline and a
five-second frontend lease. Raft SQLite work uses one worker. Raft image storage
uses extents, a 128-MiB image bank, a 16-MiB per-cell image ceiling (eight
coexisting ownership copies), and the existing production performance harness's
32,768 physical manifest-page budget per group page store. The older single-process fixture
has a separate 1,024-page budget and an independently reproduced exhaustion
failure; this new profile does not repair or qualify that fixture.

## Run

Build using Rust 1.98.1 and locked dependencies, in a checkout-owned Docker cache:

```sh
docker --context colima-cellar-multi-raft-6aff build \
  -f tools/perf/primitive_compare.Dockerfile \
  --build-arg TARGET_CACHE_ID=cellar-<checkout>-primitive-compare-target \
  --build-arg SOURCE_REVISION="$(git rev-parse HEAD)" \
  -t cellar-primitive-compare:<revision> .
```

Pass the resulting immutable image ID. The runner checks its complete compiler
input manifest against the checkout before launch and after measurement. Do not
edit compiler inputs while a run is active.

```sh
python3 tools/perf/primitive_compare.py \
  --image sha256:<image-id> \
  --output /absolute/checkout-owned/qualification/primitive-comparison
```

The default matrix covers all three APIs and all four acknowledgement modes:
concurrency 1, 4 and 16; two repetitions with reversed concurrency order; five
seconds warmup and 30 seconds per measured operation. SQL/KV each have separate
write and read windows. Payload values contain exactly 256 ASCII bytes. SQL uses
a prepared upsert/read, KV uses public text put/get, and Queue sends eight text
messages per batch through a real consumer. Application message identities and
payload hashes verify callback observations; Queue metrics additionally require
zero source backlog and zero dead-letter messages after settlement. Duplicate
callbacks are counted separately. The callback observer is bounded to one
million identities and is in memory; no process-crash durability claim is made
for that observer.

Latency is client-observed closed-loop HTTP completion latency. Raw successful
and attempted request samples are saved separately. Queue latency measures batch
enqueue acknowledgement, with message throughput reported both during enqueue
and including verified drain. Drain time includes verification overhead; it is
not a per-message delivery latency measurement. Failed or ambiguous mutations
stop the worker and retain their original identities; they are never replaced
with fresh-ID retries. Empty, failed, short or incomplete windows cannot qualify.

A shorter smoke matrix can be selected with `--seconds`, `--warmup`,
`--repetitions`, `--concurrency`, `--families` and `--modes`. Such a run is labelled
separately and cannot establish the full default comparison. All cases record
actual Docker and cgroup limits, independent node process identities, resource
samples, raw latency samples, source/compiler/binary hashes, exit codes, OOM
state, node logs and verification failures. This local topology provides one
failure domain and does not qualify cloud S3, crash recovery, partitions or soak.
If the successful load process exits between resource inspection and sampling,
the runner retains its verified terminal state without inventing a final cgroup
sample. Failed loads, changed ownership or limits, and stopped nodes or stores
still fail the case.

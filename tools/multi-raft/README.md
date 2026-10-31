<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Local multi-Raft qualification

The requested execution environment is a separate Colima profile, RustFS and
up to five Cellar containers. Each Cellar container must capture a hard limit
of one CPU and 3 GiB memory, with no additional swap. These local runs cannot
establish independent host/zone resilience or large-fleet capacity.

`local.py` currently prepares and probes the object-store environment. It does
not launch Cellar nodes or qualify the complete roadmap. Its public
`environment.json` records `completed: false`; credentials remain in a private
`rustfs.env` file outside the repository. Do not publish that credential file or
an unrestricted Docker inspection containing container environment variables.

Create the dedicated profile without changing the active Docker context:

```sh
colima start cellar-multi-raft --activate=false --cpus 8 --memory 16 --disk 60
python3 tools/multi-raft/local.py preflight --nodes 3
python3 tools/multi-raft/local.py prepare --nodes 3 --output /tmp/cellar-raft-local-001
python3 tools/multi-raft/local.py probe --output /tmp/cellar-raft-local-001
```

The helper uses `docker --context colima-cellar-multi-raft` explicitly. It never
stops another Colima profile or changes the user's global Docker context.
Preparation requires a fresh run directory and digest-pinned RustFS image. A
randomly named private network, object volume, credentials and loopback-only
S3 port belong to that run. The helper chooses a free host port explicitly:
Docker's guest-selected ephemeral port can collide with another Colima profile's
host forward and reach the wrong service. The pinned image uses the same
20-character access-key profile as SDK CI. RustFS has one CPU and 2 GiB memory. The readiness
probe checks its actual captured cgroup settings. The S3 probe checks seven real
HTTP operations: bucket creation, conditional creation, matching read, refused
overwrite, ETag compare-and-swap, refused stale ETag and read after replacement.
These are backend protocol checks; they do not prove Cellar archive recovery.
Container setup follows the [official RustFS Docker guide](https://docs.rustfs.com/en/installation/container/docker).

Three nodes require six guest CPUs and at least 11.5 GiB available guest memory,
including the store and observation headroom. A five-node run requires seven
CPUs and at least 17.5 GiB available guest memory; configure eight CPUs and
18 GiB or more before starting that run. The helper refuses insufficient
resources before creating a run directory or Docker resources. The declared
container limits are ceilings, not reserved or independent physical machines.
Record concurrent host activity and VM resources with performance results.

To stop just the recorded backend while keeping its object data for inspection:

```sh
python3 tools/multi-raft/local.py stop --output /tmp/cellar-raft-local-001
```

This checks the container's ownership label first. No command here deletes
volumes, replica directories, credentials or another run's resources. Failed
preparations retain their environment record for diagnosis. A separate cleanup
step can remove the exact recorded resources after their data is no longer
needed; never use a global Docker prune or delete a Colima profile as cleanup.

## Shared cell-group archive checks

Commit the test and harness sources, then run the native three-voter TLS fixture
against the prepared RustFS backend. Use an absolute Cargo target directory unique
to this checkout and a fresh output directory:

```sh
python3 tools/multi-raft/cell_groups.py \
  --backend /tmp/cellar-raft-local-001 \
  --output /tmp/cellar-cell-groups-rustfs-001 \
  --target-directory "$HOME/Workspace/crabbuild-target/cellar-my-checkout"
```

The runner verifies the backend's ownership label, image, running state, endpoint
and actual resource limits, then makes seven signed S3 probes in a new bucket.
It runs six exact ignored RustFS cases, each in materialized and extent modes:
stable publication with continuing quorum, uncertain-publication reconciliation
with joined close, refusal of unproved S3 responses, positive local command S3
settlement, deadline/cancellation isolation, and refusal when a sibling dependency
is missing. Catalog setup, archive
objects, conditional registry updates and independent recovery use the actual
S3 client. Fault injection surrounds that client; lost publication responses are
injected after the real conditional write commits. No memory fallback exists.

The refusal case verifies that an unbound dispatcher cannot claim S3 durability,
including a namespace minimum. The positive command cases use an explicit
controller binding, exact receipts from later archived images, retained proof
admission and fresh routing/leader checks. Receipt lookup, enrollment and remote
S3 responses remain unqualified. The
runner does not qualify local log file purge, fenced recovery or performance.
The voters run as native host tasks, so this is not the container resource gate.

`record.json` retains source and executable hashes, exact selections, output
hashes, S3 observations and captured backend identity without credentials.
Failed runs retain their record and logs. Every run owns a new remote prefix;
remote objects and the backend remain available for inspection until explicitly
stopped using the command above. No cleanup deletes another run's data.

## Native node runs

The repository's `tools/perf/raft.py` harness now provides three native SDK node
containers with the required one-CPU/3-GiB limits, its own RustFS instance, exact
replica/version checks and final archive coverage. See
[the harness guide](../perf/raft-README.md). Use this separate profile and
`--port-base 19900` to avoid another profile's host ports. Stop the preparation
backend first; the node harness allocates two CPUs/4 GiB for its own store and
two CPUs/1 GiB for its load container, requiring an eight-CPU/16-GiB VM. Its
current topology has three nodes. Five-node placement remains additional work.

Build in that context with a target cache unique to this checkout, then use a
fresh private run directory. Replace `my-checkout` before running:

```sh
python3 tools/multi-raft/local.py stop --output /tmp/cellar-raft-local-001
docker --context colima-cellar-multi-raft build -f tools/perf/raft.Dockerfile \
  --build-arg TARGET_CACHE_ID=cellar-my-checkout-raft-target \
  --build-arg SOURCE_REVISION="$(git rev-parse HEAD)" \
  -t cellar-multi-raft:mr13-local .
python3 tools/perf/raft.py --context colima-cellar-multi-raft --port-base 19900 \
  --image cellar-multi-raft:mr13-local --cells-per-primary 2 \
  --seconds 2 --repeats 1 --concurrency 1 --rates 2 \
  --output /tmp/cellar-raft-nodes-001
python3 tools/perf/analyze_raft.py /tmp/cellar-raft-nodes-001
```

This harness uses embedded SDK startup. Production `NativeDaemon::raft_cells`
has a separate process qualification; physical CLI/RustFS recovery drills remain
part of the operating qualification.

## Remaining qualification

An additional vertical slice must launch a compiled native application through
`NativeDaemon::raft_cells`, using real RustFS clients, authenticated TLS links,
unique persistent node disks and fresh process identities. Each group needs
three voters; on five nodes, rotate voter placement between groups and use the
remaining nodes as learners so all five can serve follower reads. Ordinary
retained-disk restart must resume the existing engine; disk loss requires
membership repair, never reuse of a lost voter identity.

Record executable/source/image hashes and inspect each actual node's CPU,
memory and swap settings before traffic. Require exact command IDs, receipts,
values and version tokens through writes, follower reads, process kill,
retained-disk restart, learner repair, quorum loss, object-store outage and
joined shutdown. Keep ambiguous writes in the history for reconciliation;
neither generate fresh IDs nor count only successful responses as the oracle.

Fenced new-history disaster restore needs its own verified deployment fence.
Stopping containers alone does not prevent an old quorum from returning.
Release/durability qualification, performance measurement and federation remain
separate gates in the [roadmap](../../docs/design/multi-raft/implementation-plan.md).

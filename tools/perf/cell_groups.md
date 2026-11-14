<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Shared-group workload campaigns

`cell_groups.py` runs serial, source-pinned occupancy cohorts through the existing
`raft_run.py` owner. It uses actual RF3 voters, separate SQLite cell histories,
Raft Engine, TLS and RustFS. The independent verifier replays the operation ledger,
all assigned replicas, complete physical group/cell inventory, archive coverage
and kernel/engine observations. It does not accept a reported pass boolean.

This is CG-14 infrastructure. It does not close the release campaign or the
unresolved occupancy-128 matrix failure recorded in the design progress log.

The separate managed-daemon runner, `tools/multi-raft/daemon.py`, accepts
`--node-memory-gib 2` for three 1-vCPU/2-GiB voters. Prepare its backend with
`tools/multi-raft/local.py prepare --nodes 3 --node-memory-gib 2` on an explicit
dedicated profile. RustFS retains its 1-vCPU/2-GiB limit. Preflight reserves five
VM CPUs and at least 8.5 GiB for this topology. Both Docker configuration and
effective cgroup CPU, memory and zero-swap limits are checked for every voter;
reported capacity uses the selected limit. The default remains 3 GiB per voter.
This profile does not change the workload-campaign limits below.

## Private configuration

Build the current source with `tools/perf/raft.Dockerfile` before running. Provide
the immutable image ID and actual Engine ID, never a tag or current-context
fallback. Use a dedicated `colima-cellar-multi-raft[-NAME]` context. Do not resize
another experiment's VM or change its active Docker context. Each voter requires
one CPU/3 GiB without swap; RustFS requires two CPUs/4 GiB. The existing observer
reserve makes the five-node preflight require eight CPUs/20 GiB. Missing resource
enforcement or insufficient resources refuses before workload provisioning.

The JSON configuration contains exactly these required fields:

| Field | Value |
| --- | --- |
| `format` | `1` |
| `run_id` | A fresh canonical UUID |
| `source_revision` | Exact checkout Git revision |
| `context`, `engine_id` | Explicit context and its `docker info` ID |
| `image` | Immutable Cellar image digest built from this source |
| `rustfs_image` | The pinned `RUSTFS` value in `raft_run.py` |
| `data_root` | Existing dedicated external directory, with symlinks resolved |
| `port_base` | Explicit loopback port allocation for RustFS and every voter |
| `network_cidr` | An unused RFC1918 pool, /16 through /24, with one /24 per cohort |
| `profiles_file`, `profiles_sha256` | Absolute frozen profile catalog and its file SHA-256 |
| `workload` | Complete workload template accepted by `raft_run.validate_profile` |
| `storage` | Explicit Colima home/profile/backing disk and per-voter/object-store byte bounds |

`storage` has exactly `colima_home`, `profile`, `backing_disk`, `node_bytes` and
`object_store_bytes`. The profile must identify the configured Engine context,
and its data disk must resolve to `colima_home/_lima/_disks/CONTEXT/datadisk`.
Keep the data disk on the writable workspace volume. Bounds are MiB-aligned
integers from 128 MiB to 64 GiB. Preflight reserves the complete matrix's retained
backing plus 4 GiB on both host and guest; it refuses an undersized campaign
before provisioning. Every `/data` is a separate prewritten ext4 loop device.
The root is read-only; scratch and Docker logs have explicit finite limits.
Disk-full errors remain failed attempts. The harness never expands a disk or
raises a request/resource budget to obtain a pass.

Start the workload template from `raft_profiles.json`'s `local-five` entry. The
campaign takes cell population, occupancies, image modes, repetitions, offered
rate and measurement duration from the selected frozen design profile. It fixes
uniform traffic, 80% commands/20% AtLeast reads and the requested Quorum or S3
durability. Payloads, setup/warmup, original request/retry/startup budgets,
concurrency and tick settings remain explicit in the template. Each repetition
uses a stable seed shared by every occupancy and both image modes. Every cohort
has fresh state and a separate immutable offered-operation ledger.

Optional boolean `observe_log_reclamation` enables periodic physical engine-file
censuses and the existing fixed quiet window. Engine/API log-write counters are
always collected. File censuses require running voters, so combining this option
with a follower-pause schedule refuses before provisioning; it cannot silently
lose physical log observations during the fault.

Optional `fault_schedule` contains exactly `scenario` and `config`. Supported
scenarios are `healthy` with null config, `slow-follower`, `s3-outage` and
`s3-recovery`, using the existing checked fault configuration formats. The entire
schedule is frozen in the campaign receipt and replayed identically across
cohorts. Pause support is qualified before provisioning. Missing capabilities
refuse; leader/process crashes, network partitions and disk erasure are not
silently substituted with pause experiments.

Runtime credentials are generated under each private `workload/config/`, with
restricted permissions. They are excluded from public artifact manifests. Do not
publish that directory or unrestricted Docker inspection output.

## Commands

```sh
python3 tools/perf/cell_groups.py preflight --config /private/config.json --profile mixed_quorum
python3 tools/perf/cell_groups.py run --profile mixed_quorum --config /private/config.json --output /dedicated/root/RUN_UUID
python3 tools/perf/cell_groups.py verify --run /dedicated/root/RUN_UUID
python3 tools/perf/cell_groups.py stop --run /dedicated/root/RUN_UUID
```

The output must be exactly the fresh `data_root/run_id` directory. Campaign
networks use the explicit CIDR pool; preflight refuses overlaps with retained
Engine networks. This also works when Docker's default pools are exhausted.
Supported campaigns are `idle_density`, `mixed_quorum` and `mixed_s3`. Every configured
occupancy, repetition and image mode runs serially. Stop joins each cohort before
the next one starts. It validates the exact Engine, run UUID, directory, container
name, label and immutable ID before signalling any process. Lost creation replies
retain their original intents. Stop never deletes containers, networks, volumes,
logs or voter data; it cannot erase a disk. A wrong/missing owner refuses cleanup.

After physical stop, each native voter must supply its actual `RaftCells.close`
record: full group counts, omitted/failed groups and local producer quiescence.
Joined process logs and immutable IDs are retained separately. Exit zero alone
cannot establish `unjoined_owners=0`. Missing reports, SIGKILL and unobserved
producer outcomes fail the join gate.

`archive-observations.jsonl` joins every successful command to a subsequent
observed archive frontier in the same group incarnation. Lag is an upper bound
from command send to observed coverage, including seed and warmup. Failed polls
retain outstanding debt; final archive coverage alone cannot establish zero lag.
The observer retains only one pending frontier per group and preserves the
highest index when replies arrive out of order.

`campaign.json` retains cohort plans, failures and cleanup outcomes. `receipt.json`
binds every public artifact to its file digest. Verify checks the recorded Git revision and executable-input manifest, so a later documentation-only commit can still verify the same runtime. It refuses changed source,
binaries, profile catalog, topology, operations, replica results or missing raw
logs. An explicit later stop updates only ownership-artifact hashes; it cannot
recertify a changed operation ledger. Failed attempts remain separate runs.

## Claims and remaining qualification

Verification distinguishes replayed workload correctness from release profile
acceptance. `qualified` requires every numeric gate, the canonical frozen release
catalog and its historical control contract. Missing measurements remain
`unmeasured`. Booleans, NaN, negative counters, zero ratio denominators and
unmatched controls cannot pass. A reduced private smoke catalog can exercise the
driver, but cannot become release evidence. Verify/run return a nonzero status
when release gates remain unmeasured or fail, while retaining the complete report.

The driver retains control socket bytes below TLS, engine/API write and sync
operations, file-size/allocated-block censuses, S3 request/body observations,
archive-lag upper bounds, CPU intervals, cgroup lifetime memory peak, process RSS
and descriptors. The serial independent model and all assigned replica reads
establish lost-effect, duplicate-mutation and stale-read checks within that
serialized workload scope. They do not establish concurrent double-writer safety,
physical device write attribution or independent failure domains. A
`double_writers` gate without a separate authority witness remains unmeasured.

Optional `historical_controls` is a list of `checkout`, `run`, `repetition`,
`image_mode`, and `artifacts_sha256` objects. The digest is `input_digest(artifacts(run))`;
repetition is zero-based. Import reads a clean checkout at the frozen historical
pin, verifies its executable-input and runner hashes, matches the complete
workload, and executes that revision's independent oracle against its retained
raw run. It never parses old per-cell topology as shared-group topology. Keep the
historical checkout and evidence available for re-verification. The pinned
standalone historical runner measures materialized images; an extents label
refuses. Its managed extent mode still needs a matched control adapter. The
frozen release matrix requires both modes, so its historical control gate
remains open for CG-15 to resolve. Missing controls and missing
historical safety measurements cannot qualify a release profile.

Hot-cell adaptation, movement, hibernation, managed disaster recovery,
independent-host loss and soak profiles refuse until their lifecycle/deployment
adapters exist. This allows CG-14 infrastructure development alongside CG-11/12/13
without claiming their unfinished features pass. Full profile execution and
release acceptance remain separate gates.

The five named CG-14 cases live in `native_cell_groups_harness`. The physical count
case reuses the actual RF3/SQLite fixture in both image modes. The other cases run
the independent Python ownership, offered-load, evidence and numeric-profile
oracles, including deliberately corrupted evidence. Run them individually with
`--exact --test-threads=1`; zero selected cases do not establish acceptance.

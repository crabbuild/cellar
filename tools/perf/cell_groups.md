<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Shared-group workload campaigns

`cell_groups.py` runs serial, source-pinned occupancy cohorts through the existing
`raft_run.py` owner. It uses actual RF3 voters, separate SQLite cell histories,
Raft Engine, TLS and RustFS. The independent verifier replays the operation ledger,
all assigned replicas, complete physical group/cell inventory, archive coverage
and kernel/engine observations. It does not accept a reported pass boolean.

This is CG-14 infrastructure. It does not close the release campaign or the
unresolved occupancy-128 matrix failure recorded in the design progress log.

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
| `profiles_file`, `profiles_sha256` | Absolute frozen profile catalog and its file SHA-256 |
| `workload` | Complete workload template accepted by `raft_run.validate_profile` |

Start the workload template from `raft_profiles.json`'s `local-five` entry. The
campaign takes cell population, occupancies, image modes, repetitions, offered
rate and measurement duration from the selected frozen design profile. It fixes
uniform traffic, 80% commands/20% AtLeast reads and the requested Quorum or S3
durability. Payloads, setup/warmup, original request/retry/startup budgets,
concurrency and tick settings remain explicit in the template. Each repetition
uses a stable seed shared by every occupancy and both image modes. Every cohort
has fresh state and a separate immutable offered-operation ledger.

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

The output must be exactly the fresh `data_root/run_id` directory. Supported
campaigns are `idle_density`, `mixed_quorum` and `mixed_s3`. Every configured
occupancy, repetition and image mode runs serially. Stop joins each cohort before
the next one starts. It validates the exact Engine, run UUID, directory, container
name, label and immutable ID before signalling any process. Lost creation replies
retain their original intents. Stop never deletes containers, networks, volumes,
logs or voter data; it cannot erase a disk. A wrong/missing owner refuses cleanup.

`campaign.json` retains cohort plans, failures and cleanup outcomes. `receipt.json`
binds every public artifact to its file digest. Verify refuses changed source,
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

The current driver retains control socket bytes below TLS, engine/API write and
sync operations, S3 request/body observations, CPU intervals, cgroup lifetime
memory peak, process RSS and descriptors. These do not imply physical device
write attribution or independently enforced disk quotas. Docker's ordinary data
volumes and one shared VM do not establish independent hosts/failure domains.
Application owner-join proofs, double-writer observation, continuous archive-lag
gates, historical executable cohorts and bounded disk provisioning remain open.
Those metrics are deliberately not inferred from successful SQL replies.

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

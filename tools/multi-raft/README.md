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
colima start cellar-multi-raft --activate=false --cpus 6 --memory 12 --disk 60
python3 tools/multi-raft/local.py preflight --nodes 3
python3 tools/multi-raft/local.py prepare --nodes 3 --output /tmp/cellar-raft-local-001
python3 tools/multi-raft/local.py probe --output /tmp/cellar-raft-local-001
```

The helper uses `docker --context colima-cellar-multi-raft` explicitly. It never
stops another Colima profile or changes the user's global Docker context.
Preparation requires a fresh run directory and digest-pinned RustFS image. A
randomly named private network, object volume, credentials and loopback-only
S3 port belong to that run. RustFS has one CPU and 2 GiB memory. The readiness
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

## Remaining node harness work

The next vertical slice must launch a compiled native application through
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
separate gates in the [roadmap](../../plans/multi-raft/implementation-plan.md).

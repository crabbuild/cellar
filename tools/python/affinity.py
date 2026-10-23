"""Optional Linux CPU placement for measurement children, before any threads start."""
import json
import os
from pathlib import Path
import sys


def cpu_list(value):
    parts = value.split(",")
    if not all(part.isascii() and part.isdecimal() for part in parts):
        raise ValueError("CPU lists must contain comma-separated nonnegative integers")
    cpus = [int(part) for part in parts]
    if len(set(cpus)) != len(cpus):
        raise ValueError("CPU lists must not repeat a CPU")
    return sorted(cpus)


def available():
    if not sys.platform.startswith("linux") or not hasattr(os, "sched_getaffinity"):
        raise ValueError("explicit CPU affinity requires Linux")
    return sorted(os.sched_getaffinity(0))


def placement(host, client):
    if host is None and client is None:
        return None
    if host is None or client is None:
        raise ValueError("provide both --host-cpus and --client-cpus")
    host, client, allowed = cpu_list(host), cpu_list(client), available()
    if set(host) & set(client):
        raise ValueError("host and client CPU sets must be disjoint")
    if not (set(host) | set(client)) <= set(allowed):
        raise ValueError("requested CPUs are outside the calling process affinity")
    return dict(host=host, client=client, available=allowed)


def prefix(cpus, evidence):
    if cpus is None:
        return []
    return [sys.executable, "-I", "-B", str(Path(__file__).resolve()),
            ",".join(map(str, cpus)), str(evidence)]


def verify(evidence, cpus, pid=None):
    proof = json.loads(evidence.read_text())
    if (proof["requested"] != cpus or proof["observed"] != cpus
            or not set(cpus) <= set(proof["available_before"])
            or not isinstance(proof["pid"], int) or proof["pid"] <= 0
            or (pid is not None and proof["pid"] != pid)):
        raise RuntimeError("measurement child CPU affinity differs from requested placement")
    return proof


def main():
    cpus, evidence, executable, *arguments = sys.argv[1:]
    requested, before = cpu_list(cpus), available()
    if not set(requested) <= set(before):
        raise ValueError("requested CPUs are outside the child process affinity")
    # This is a fresh, single-threaded child. Do not use preexec_fn in the
    # harness: it may have threads. exec preserves PID and affinity, and the
    # application's subsequently created threads inherit this mask.
    os.sched_setaffinity(0, requested)
    observed = available()
    if observed != requested:
        raise RuntimeError("kernel did not apply the requested CPU affinity")
    Path(evidence).write_text(json.dumps(dict(requested=requested, observed=observed,
                                           available_before=before, pid=os.getpid())) + "\n")
    os.execv(executable, [executable, *arguments])


if __name__ == "__main__":
    main()

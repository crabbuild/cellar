#!/usr/bin/env python3
"""Read-only delivery-plan validation. Never executes manifest commands."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import shlex
import sys
from urllib.parse import unquote, urlsplit

PACKAGE = Path(__file__).resolve().parent
ROOT = PACKAGE.parents[3]
STATUSES = {"TODO", "IN_PROGRESS", "BLOCKED", "DONE"}
GATES = {
    "history_safety", "store_cas", "single_failure", "two_failure_refusal",
    "scale_out", "scale_in", "maintenance", "steady_overhead", "skew_gain",
    "resource_bounds", "soak_24h",
    "vertical_resize", "lifecycle_fencing", "low_load_distribution", "receipt_finalization",
}


class PlanError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise PlanError(message)


def read_json(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"Duplicate JSON key: {key}")
            result[key] = value
        return result
    return json.loads(path.read_text(), object_pairs_hook=unique)


def within(root, name):
    require(isinstance(name, str) and name and not Path(name).is_absolute(),
            f"Expected repository-relative path: {name}")
    path = (root / name).resolve()
    require(path.is_relative_to(root.resolve()), f"Path escapes repository: {name}")
    return path


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def ordered(tasks):
    by_id = {task["id"]: task for task in tasks}
    require(len(by_id) == len(tasks), "Duplicate task ID")
    visiting, visited, result = set(), set(), []

    def visit(tid):
        require(tid in by_id, f"Unknown dependency: {tid}")
        require(tid not in visiting, f"Dependency cycle: {tid}")
        if tid in visited:
            return
        visiting.add(tid)
        for dependency in by_id[tid]["depends_on"]:
            visit(dependency)
        visiting.remove(tid)
        visited.add(tid)
        result.append(tid)

    for tid in by_id:
        visit(tid)
    return result


def verified_file(root, name, digest):
    path = within(root, name)
    require(path.is_file() and path.stat().st_size > 0, f"Missing/empty artifact: {name}")
    require(re.fullmatch(r"[0-9a-f]{64}", digest or "") is not None,
            f"Invalid SHA-256: {name}")
    require(sha256(path) == digest, f"Artifact hash mismatch: {name}")


def validate_evidence(task, root):
    require(task["evidence"], f"{task['id']}: DONE needs retained evidence")
    passed, commands = set(), []
    gates = set()
    for name in task["evidence"]:
        record = read_json(within(root, name))
        require(record.get("format") == 1 and record.get("task_id") == task["id"],
                f"Wrong evidence identity: {name}")
        require(re.fullmatch(r"[0-9a-f]{40}", record.get("source_revision", "")) is not None,
                "Evidence needs a full source revision")
        require(re.fullmatch(r"[0-9a-f]{64}", record.get("executable_digest", "")) is not None,
                "Evidence needs executable SHA-256")
        for field in ("environment", "limitations"):
            require(isinstance(record.get(field), str) and record[field].strip(),
                    f"Evidence missing {field}")
        for case in record.get("tests", []):
            require(case.get("result") == "passed", "DONE evidence contains an unpassed case")
            passed.add(case["name"])
        for command in record.get("commands", []):
            require(type(command.get("exit_code")) is int and command["exit_code"] == 0,
                    "DONE evidence contains a failed command")
            verified_file(root, command.get("output"), command.get("sha256"))
            commands.append(command.get("argv"))
        if task["id"] == "FO-10":
            verified_file(root, record.get("qualification_report"),
                          record.get("qualification_sha256"))
            for gate, result in record.get("gates", {}).items():
                require(gate in GATES and result == "passed", f"Unpassed/unknown gate: {gate}")
                gates.add(gate)
    expected = {case["name"] for case in task["test_cases"]}
    require(expected <= passed, f"Missing passed cases: {sorted(expected - passed)}")
    for command in task["verification"]:
        require(command in commands, f"Missing command evidence: {command}")
    if task["id"] == "FO-10":
        require(gates == GATES, f"Missing qualification gates: {sorted(GATES - gates)}")


def validate(manifest, baseline, root=ROOT):
    require(manifest.get("format") == baseline.get("format") == 1, "Unsupported format")
    require(manifest["baseline_revision"] == baseline["revision"], "Baseline mismatch")
    require(re.fullmatch(r"[0-9a-f]{40}", baseline["revision"]) is not None, "Invalid baseline SHA")
    source_paths = set()
    for source in baseline["files"]:
        require(source["path"] not in source_paths, "Duplicate source anchor")
        source_paths.add(source["path"])
        require(within(root, source["path"]).is_file(), f"Missing source: {source['path']}")
        require(re.fullmatch(r"[0-9a-f]{64}", source["sha256"]) is not None, "Invalid source hash")
    tasks = {task["id"]: task for task in manifest["tasks"]}
    order = ordered(manifest["tasks"])
    require(set(tasks) == {f"FO-{n:02d}" for n in range(1, 11)}, "Incomplete task inventory")
    invariants = set(manifest["invariants"])
    require(invariants == {f"F{n:02d}" for n in range(1, 13)}, "Incomplete invariant inventory")
    tests, coverage, producers, ancestors = set(), set(), {}, {}
    for tid in order:
        task = tasks[tid]
        require(task["status"] in STATUSES, f"Invalid status: {tid}")
        deps = task["depends_on"]
        require(len(deps) == len(set(deps)), f"Duplicate dependency: {tid}")
        ancestors[tid] = set(deps)
        for dep in deps:
            ancestors[tid].update(ancestors[dep])
        for field in ("title", "steps", "acceptance", "stop_conditions", "test_cases"):
            require(bool(task[field]), f"Empty {field}: {tid}")
        for path in task["modifies"]:
            within(root, path)
            require(path in source_paths or producers.get(path) in ancestors[tid],
                    f"Modification lacks source anchor/dependency: {path}")
        for path in task["creates"]:
            within(root, path)
            require(path not in source_paths and path not in producers, f"Duplicate output: {path}")
            producers[path] = tid
        target = task["test_target"]
        require(re.fullmatch(r"native_raft_fleet_[a-z_]+", target) is not None, "Invalid test target")
        require(f"crates/cellar/tests/{target}.rs" in task["creates"], "Missing test target deliverable")
        listing = ["cargo", "test", "-p", "cellar", "--test", target, "--locked", "--", "--list"]
        execution = listing[:-1] + ["--test-threads=1"]
        require(listing in task["verification"] and execution in task["verification"],
                f"Missing list/execution commands: {tid}")
        require(all(isinstance(argv, list) and argv and
                    all(isinstance(arg, str) and arg for arg in argv)
                    for argv in task["verification"]), "Invalid command argv")
        for case in task["test_cases"]:
            name = case["name"]
            require(re.fullmatch(r"[a-z][a-z0-9_]+", name) is not None and name not in tests,
                    f"Invalid/duplicate test: {name}")
            require(case["oracle"].strip(), f"Missing oracle: {name}")
            require(case["invariants"] and set(case["invariants"]) <= invariants,
                    f"Invalid test invariants: {name}")
            tests.add(name)
            coverage.update(case["invariants"])
        if task["status"] == "DONE":
            require(all(tasks[dep]["status"] == "DONE" for dep in deps), f"{tid}: dependency not DONE")
            validate_evidence(task, root)
    require(coverage == invariants, f"Untested invariants: {invariants - coverage}")
    return {"tasks": len(tasks), "required_cases": len(tests), "source_anchors": len(source_paths)}


def preflight(baseline, root=ROOT):
    drift = [source["path"] for source in baseline["files"]
             if sha256(within(root, source["path"])) != source["sha256"]]
    require(not drift, "Review source drift before implementation:\n" + "\n".join(drift))


def check_listing(task, text):
    actual = re.findall(r"^([\w:]+): test$", text, re.M)
    require(actual, "Cargo listing contains no tests")
    for case in task["test_cases"]:
        matches = [name for name in actual if name == case["name"] or name.endswith("::" + case["name"])]
        require(len(matches) == 1, f"Missing or ambiguous test: {case['name']}")


def check_links(package=PACKAGE, root=ROOT):
    count = 0
    for source in package.glob("*.md"):
        prose = re.sub(r"```.*?```|~~~.*?~~~", "", source.read_text(), flags=re.S)
        for raw in re.findall(r"!?\[[^\]]*\]\(([^)]+)\)", prose):
            url = urlsplit(raw.strip().strip("<>"))
            if url.scheme or url.netloc:
                continue
            target = (source.parent / unquote(url.path)).resolve() if url.path else source
            require(target.is_relative_to(root.resolve()) and target.is_file(), f"Invalid link: {raw}")
            if url.fragment and target.suffix == ".md":
                titles = re.findall(r"^#{1,6}\s+(.+)$", target.read_text(), re.M)
                anchors = {re.sub(r"[^\w\- ]", "", title.lower().replace("`", "")).replace(" ", "-")
                           for title in titles}
                require(unquote(url.fragment) in anchors, f"Missing anchor: {raw}")
            count += 1
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("validate", "preflight", "ready"):
        sub.add_parser(name)
    sub.add_parser("show").add_argument("task")
    listing = sub.add_parser("check-list")
    listing.add_argument("task")
    listing.add_argument("listing", type=Path)
    args = parser.parse_args()
    try:
        manifest = read_json(PACKAGE / "tasks.json")
        baseline = read_json(PACKAGE / "baseline.json")
        stats = validate(manifest, baseline)
        stats["document_links"] = check_links()
        tasks = {task["id"]: task for task in manifest["tasks"]}
        if args.command == "validate":
            print(json.dumps({"plan_validation": "passed", **stats}, indent=2))
        elif args.command == "preflight":
            preflight(baseline)
            print(f"Source anchors match {baseline['revision']}; runtime completion is tracked separately.")
        elif args.command == "ready":
            for tid in ordered(manifest["tasks"]):
                task = tasks[tid]
                if task["status"] == "TODO" and all(tasks[d]["status"] == "DONE" for d in task["depends_on"]):
                    print(f"{tid}: {task['title']}")
        else:
            require(args.task in tasks, f"Unknown task: {args.task}")
            task = tasks[args.task]
            if args.command == "check-list":
                check_listing(task, args.listing.read_text())
                print("Required cases are listed. Test execution and retained evidence are still required.")
            else:
                print(f"{task['id']}: {task['title']} [{task['status']}]")
                for field in ("depends_on", "modifies", "creates", "steps", "acceptance", "stop_conditions"):
                    print(field + ":\n  " + "\n  ".join(task[field]))
                print("Future verification commands (implement targets first):")
                for argv in task["verification"]:
                    print("  " + shlex.join(argv))
                for case in task["test_cases"]:
                    print(f"  {case['name']}: {case['oracle']}")
    except (PlanError, OSError, KeyError, TypeError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

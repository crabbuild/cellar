#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Read-only validation and work discovery. Never executes manifest commands."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shlex
import sys
from urllib.parse import unquote, urlsplit

PACKAGE = Path(__file__).resolve().parent
ROOT = PACKAGE.parents[1]
STATUSES = {"TODO", "IN_PROGRESS", "BLOCKED", "DONE"}


class PlanError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise PlanError(message)


def read_json(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"Duplicate JSON field: {key}")
            result[key] = value
        return result
    return json.loads(path.read_text(), object_pairs_hook=unique)


def within(base, relative):
    require(isinstance(relative, str) and relative, "Missing relative path")
    require(not Path(relative).is_absolute(), f"Absolute path not portable: {relative}")
    path = (base / relative).resolve()
    require(path.is_relative_to(base.resolve()), f"Path escapes its root: {relative}")
    return path


def dependency_order(manifest):
    tasks = manifest["tasks"]
    by_id = {t["id"]: t for t in tasks}
    require(len(by_id) == len(tasks), "Duplicate task ID")
    visiting, visited, order = set(), set(), []
    def visit(tid):
        require(tid in by_id, f"Unknown dependency: {tid}")
        require(tid not in visiting, f"Dependency cycle at {tid}")
        if tid in visited:
            return
        visiting.add(tid)
        for dep in by_id[tid]["depends_on"]:
            visit(dep)
        visiting.remove(tid)
        visited.add(tid)
        order.append(tid)
    for tid in by_id:
        visit(tid)
    return order


def validate_evidence(task, package):
    require(task["evidence"], f"{task['id']}: DONE has no evidence")
    passed, commands = set(), []
    for reference in task["evidence"]:
        path = within(package, reference)
        require(path.is_file(), f"Missing evidence: {reference}")
        record = read_json(path)
        require(record.get("task_id") == task["id"], "Wrong evidence task identity")
        require(re.fullmatch(r"[0-9a-f]{40}", record.get("source_revision", "")) is not None,
                "Evidence needs a full source revision")
        for field in ("toolchain", "executable_digest", "environment", "limitations"):
            require(isinstance(record.get(field), str) and record[field].strip(),
                    f"Evidence missing {field}")
        for case in record.get("tests", []):
            require(case.get("result") == "passed", f"Unpassed evidence case: {case}")
            passed.add(case["name"])
        for command in record.get("commands", []):
            require(command.get("exit_code") == 0, "Failed command in DONE evidence")
            log = within(package, command.get("output", ""))
            require(log.is_file() and log.stat().st_size > 0, "Evidence output is missing/empty")
            commands.append(command.get("argv"))
    expected = {c["name"] for c in task["test_cases"]}
    require(expected <= passed, f"{task['id']}: missing passed cases {sorted(expected - passed)}")
    for required in task["verification"]:
        require(required in commands, f"{task['id']}: missing command evidence {required}")


def validate_manifest(manifest, baseline, root=ROOT, package=PACKAGE):
    require(manifest.get("format") == 1 and baseline.get("format") == 1, "Unsupported package format")
    require(manifest["baseline_revision"] == baseline["revision"], "Baseline revision mismatch")
    require(re.fullmatch(r"[0-9a-f]{40}", baseline["revision"]) is not None, "Invalid baseline SHA")
    require(set(manifest["status_values"]) == STATUSES, "Invalid status vocabulary")
    order = dependency_order(manifest)
    by_id = {t["id"]: t for t in manifest["tasks"]}
    source_paths = set()
    for item in baseline["files"]:
        require(item["path"] not in source_paths, "Duplicate source anchor")
        source_paths.add(item["path"])
        require(within(root, item["path"]).is_file(), f"Missing baseline source: {item['path']}")
        require(re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) is not None, "Invalid source hash")
    producers, test_names, covered, ancestors = {}, set(), set(), {}
    invariants = set(manifest["invariants"])
    require(invariants == {f"I{i:02d}" for i in range(1, 17)}, "Incomplete invariant inventory")
    for tid in order:
        t = by_id[tid]
        require(re.fullmatch(r"MR-\d{2}", tid) is not None, f"Invalid task ID: {tid}")
        require(t["status"] in STATUSES, f"Invalid status: {tid}")
        require(t["phase"] in {"v1", "scale", "federation"}, f"Invalid phase: {tid}")
        require(len(set(t["depends_on"])) == len(t["depends_on"]), f"Duplicate dependency: {tid}")
        ancestors[tid] = set(t["depends_on"])
        for dep in t["depends_on"]:
            ancestors[tid].update(ancestors[dep])
        for field in ("title", "steps", "acceptance", "stop_conditions", "test_cases", "verification"):
            require(bool(t[field]), f"{tid}: empty {field}")
        for path in t["modifies"]:
            within(root, path)
            require(path in source_paths or producers.get(path) in ancestors[tid],
                    f"{tid}: modification has no baseline or dependency producer: {path}")
        for path in t["creates"]:
            within(root, path)
            require(path not in producers and path not in source_paths, f"Duplicate/new-source collision: {path}")
            producers[path] = tid
        target = t["test_target"]
        require(f"crates/cellar/tests/{target}.rs" in t["creates"], f"{tid}: missing test target output")
        expected_commands = [
            ["cargo", "test", "-p", "cellar", "--test", target, "--locked", "--", "--list"],
            ["cargo", "test", "-p", "cellar", "--test", target, "--locked", "--", "--test-threads=1"],
        ]
        require(t["verification"] == expected_commands, f"{tid}: verification command contract changed")
        for case in t["test_cases"]:
            name = case["name"]
            require(re.fullmatch(r"[a-z][a-z0-9_]*", name) is not None, f"Invalid case name: {name}")
            require(name not in test_names, f"Duplicate case name: {name}")
            require(bool(case["oracle"].strip()), f"Missing case oracle: {name}")
            require(bool(case["invariants"]) and set(case["invariants"]) <= invariants,
                    f"Invalid invariant mapping: {name}")
            test_names.add(name)
            covered.update(case["invariants"])
        if t["status"] == "DONE":
            require(all(by_id[d]["status"] == "DONE" for d in t["depends_on"]),
                    f"{tid}: DONE before its dependencies")
            validate_evidence(t, package)
    require(covered == invariants, f"Invariants without tests: {invariants - covered}")
    release = {t["id"] for t in by_id.values() if t["phase"] == "v1"}
    require(set(manifest["first_release_tasks"]) == release, "First-release task inventory mismatch")
    for tid in release:
        require(ancestors[tid] <= release, f"First release depends on later phase: {tid}")
    return {"tasks": len(by_id), "cases": len(test_names), "source_anchors": len(source_paths)}


def markdown_ids(text):
    return {re.sub(r"[^\w\- ]", "", title.lower().replace("`", "")).replace(" ", "-")
            for title in re.findall(r"^#{1,6}\s+(.+)$", text, re.M)}


def check_links(package=PACKAGE):
    checked = 0
    for source in package.rglob("*.md"):
        prose = re.sub(r"```.*?```|~~~.*?~~~", "", source.read_text(), flags=re.S)
        for raw in re.findall(r"!?\[[^\]]*\]\(([^)]+)\)", prose):
            url = urlsplit(raw.strip().strip("<>"))
            if url.scheme or url.netloc:
                continue
            target = (source.parent / unquote(url.path)).resolve() if url.path else source
            require(target.is_relative_to(package.resolve()), f"Nonportable package link: {raw}")
            require(target.exists(), f"{source.name}: missing link {raw}")
            if url.fragment and target.suffix == ".md":
                require(unquote(url.fragment) in markdown_ids(target.read_text()),
                        f"{source.name}: missing anchor {raw}")
            checked += 1
    return checked


def preflight(baseline, root=ROOT):
    drift = []
    for source in baseline["files"]:
        path = within(root, source["path"])
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != source["sha256"]:
            drift.append(source["path"])
    require(not drift, "Source drift requires review:\n" + "\n".join(drift))
    return len(baseline["files"])


def check_listing(task, listing):
    actual = re.findall(r"^([\w:]+): test$", listing, re.M)
    require(actual, "Cargo listing contains zero tests")
    for case in task["test_cases"]:
        name = case["name"]
        matches = [x for x in actual if x == name or x.endswith("::" + name)]
        require(len(matches) == 1, f"Missing or ambiguous listed case: {name}")
    return len(task["test_cases"])


def capacity(profile):
    for field in ("nodes", "groups", "voters"):
        require(type(profile[field]) is int and profile[field] > 0, f"Invalid {field}")
    require(type(profile["learners"]) is int and profile["learners"] >= 0, "Invalid learners")
    require(0 <= profile["active_fraction"] <= 1, "Invalid active fraction")
    require(profile["heartbeat_interval_seconds"] > 0, "Invalid heartbeat interval")
    n, g, r, learners = (profile[k] for k in ("nodes", "groups", "voters", "learners"))
    return {"name": profile["name"], "qualification": "illustrative arithmetic only",
            "replicas_per_node": g * (r + learners) / n, "leaders_per_node": g / n,
            "active_replicas_per_node": g * (r + learners) * profile["active_fraction"] / n,
            "logical_heartbeat_deliveries_per_second": g * (r - 1 + learners) / profile["heartbeat_interval_seconds"]}


def validate_package():
    manifest, baseline = read_json(PACKAGE / "tasks.json"), read_json(PACKAGE / "baseline.json")
    stats = validate_manifest(manifest, baseline)
    stats["local_links"] = check_links()
    document = (PACKAGE / "implementation-plan.md").read_text()
    design = (PACKAGE / "technical-design.md").read_text()
    for invariant in manifest["invariants"]:
        require(f"| {invariant} |" in design, f"Missing design invariant {invariant}")
    for task in manifest["tasks"]:
        require(f"### {task['id']} {task['title']}" in document, f"Missing task documentation {task['id']}")
        for case in task["test_cases"]:
            require(f"`{case['name']}`" in document, f"Missing documented test {case['name']}")
    profiles = read_json(PACKAGE / "capacity-profiles.json")["profiles"]
    require(len({p["name"] for p in profiles}) == len(profiles), "Duplicate capacity profile")
    for profile in profiles:
        capacity(profile)
    return manifest, baseline, stats


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("validate", "preflight", "ready"):
        sub.add_parser(name)
    sub.add_parser("show").add_argument("task")
    listing = sub.add_parser("check-list")
    listing.add_argument("task")
    listing.add_argument("listing", type=Path)
    sub.add_parser("capacity").add_argument("profile")
    args = parser.parse_args()
    try:
        manifest, baseline, stats = validate_package()
        by_id = {t["id"]: t for t in manifest["tasks"]}
        if args.command == "validate":
            print(json.dumps({"package_validation": "passed", **stats}, indent=2))
        elif args.command == "preflight":
            count = preflight(baseline)
            print(f"Source preflight passed: {count} anchors at baseline {baseline['revision']}.")
        elif args.command == "ready":
            for tid in dependency_order(manifest):
                task = by_id[tid]
                if task["status"] == "TODO" and all(by_id[d]["status"] == "DONE" for d in task["depends_on"]):
                    print(f"{tid}: {task['title']}")
        elif args.command == "capacity":
            profiles = read_json(PACKAGE / "capacity-profiles.json")["profiles"]
            matches = [p for p in profiles if p["name"] == args.profile]
            require(len(matches) == 1, f"Unknown profile: {args.profile}")
            print(json.dumps(capacity(matches[0]), indent=2))
        else:
            require(args.task in by_id, f"Unknown task: {args.task}")
            task = by_id[args.task]
            if args.command == "check-list":
                count = check_listing(task, args.listing.read_text())
                print(f"Listed test inventory passed: {count} required cases. Test execution is still required.")
            else:
                print(f"{task['id']}: {task['title']} [{task['status']}]")
                print("Dependencies: " + (", ".join(task["depends_on"]) or "none"))
                for i, step in enumerate(task["steps"], 1):
                    print(f"{i}. {step}")
                for field in ("modifies", "creates", "acceptance", "stop_conditions"):
                    print(field + ":\n  " + "\n  ".join(task[field]))
                print("Verification commands (targets must first be implemented):")
                for argv in task["verification"]:
                    print("  " + shlex.join(argv))
                print("Required cases: " + ", ".join(c["name"] for c in task["test_cases"]))
    except (PlanError, OSError, KeyError, TypeError, json.JSONDecodeError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Validate delivery contracts and print executor briefs; never execute their commands."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import shlex
import subprocess
import sys
from urllib.parse import unquote, urlsplit

PACKAGE = Path(__file__).resolve().parent
ROOT = PACKAGE.parents[3]
STATUSES = {"TODO", "IN_PROGRESS", "BLOCKED", "DONE"}
SHA256 = re.compile(r"[0-9a-f]{64}")
REVISION = re.compile(r"[0-9a-f]{40}")
CASE = re.compile(r"[a-z][a-z0-9_]*")
TASK_IDS = {f"CG-{number:02}" for number in range(1, 17)}
INVARIANTS = {f"I{number:02}" for number in range(1, 15)}
PROFILE_IDS = {"idle_density", "mixed_quorum", "mixed_s3", "hot_cold", "split_merge",
               "hibernate_wake", "rustfs_disaster", "independent_domain_loss", "soak_24h"}
# Include examples, vendored crates, build scripts and future inputs by default.
# Plan bookkeeping may advance without changing the qualified executable.
RUNTIME_PATHS = [".", ":(exclude)plans/**", ":(exclude)docs/design/**"]
SAFETY_METRICS = {"lost_acknowledged_effects", "duplicate_mutations", "stale_strong_reads",
                  "double_writers", "unexpected_errors", "unjoined_owners",
                  "old_token_acceptances", "old_host_restart_acceptances",
                  "restored_receipt_callbacks", "missed_due_effects", "writes_without_quorum"}


class PlanError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise PlanError(message)


def nonempty(value):
    return isinstance(value, str) and bool(value.strip())


def read_json(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"Duplicate JSON key: {key}")
            result[key] = value
        return result

    def constant(value):
        raise PlanError(f"Non-finite JSON value: {value}")

    return json.loads(path.read_text(), object_pairs_hook=unique, parse_constant=constant)


def within(root, name):
    require(nonempty(name) and not Path(name).is_absolute(), f"Not a relative path: {name}")
    path = (root / name).resolve()
    require(path.is_relative_to(root.resolve()), f"Path escapes repository: {name}")
    return path


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def artifact(root, item):
    require(isinstance(item, dict), "Artifact must be an object")
    path = within(root, item.get("path"))
    require(path.is_file() and path.stat().st_size > 0, f"Missing/empty artifact: {path}")
    require(isinstance(item.get("sha256"), str) and SHA256.fullmatch(item["sha256"]),
            f"Invalid artifact digest: {path}")
    require(digest(path) == item["sha256"], f"Artifact hash mismatch: {path}")
    return path


def git_bytes(root, revision, path):
    require(isinstance(revision, str) and REVISION.fullmatch(revision), "Invalid source revision")
    within(root, path)
    result = subprocess.run(["git", "show", f"{revision}:{path}"], cwd=root,
                            capture_output=True, check=False)
    require(result.returncode == 0, f"Source absent at {revision}: {path}")
    return result.stdout


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
        for dep in by_id[tid]["depends_on"]:
            visit(dep)
        visiting.remove(tid)
        visited.add(tid)
        result.append(tid)

    for tid in by_id:
        visit(tid)
    return result


def cargo(toolchain, *args):
    return ["cargo", f"+{toolchain}", *args]


def task_commands(task, toolchain):
    test = cargo(toolchain, "test", "-p", "cellar", "--test", task["test_target"], "--locked", "--")
    commands = [cargo(toolchain, "check", "-p", "cellar", "-p", "cellar-logic", "--all-targets", "--locked"),
                test + ["--list"]]
    commands.extend(test + [case["name"], "--exact", "--test-threads=1"] for case in task["test_cases"])
    commands.extend([cargo(toolchain, "format", "--check"), cargo(toolchain, "check-layout"),
                     cargo(toolchain, "clippy", "-p", "cellar", "-p", "cellar-logic", "--all-targets",
                           "--locked", "--", "-D", "warnings")])
    if task["id"] == "CG-01":
        commands.append(cargo(toolchain, "test", "-p", "cellar-logic", "--lib", "--locked"))
    if task["id"] == "CG-14":
        commands.append(["python3", "-m", "unittest", "discover", "-s", "tools/perf", "-p", "test_cell_groups.py"])
    return commands


def release_commands(toolchain):
    return [cargo(toolchain, "format", "--check"), cargo(toolchain, "check-layout"),
            ["python3", "tools/check_native_dependencies.py"], ["python3", "tools/docs/check.py"],
            cargo(toolchain, "check", "--workspace", "--all-targets", "--all-features", "--locked"),
            cargo(toolchain, "test", "--workspace", "--all-features", "--lib", "--tests", "--locked", "--", "--test-threads=1"),
            cargo(toolchain, "test", "--workspace", "--all-features", "--doc", "--locked"),
            cargo(toolchain, "clippy", "--workspace", "--all-targets", "--all-features", "--locked", "--", "-D", "warnings"),
            ["python3", "-m", "unittest", "discover", "-s", "tools/perf", "-p", "test_*.py"]]


def commands_for(task, manifest):
    tasks = manifest["tasks"] if task["id"] == "CG-16" else [task]
    commands = [command for item in tasks for command in task_commands(item, manifest["toolchain"])]
    if task["id"] == "CG-16":
        commands.extend(release_commands(manifest["toolchain"]))
    return [list(command) for command in dict.fromkeys(tuple(command) for command in commands)]


def validate_contract(manifest, baseline, profiles, root):
    require(manifest.get("format") == baseline.get("format") == profiles.get("format") == 1, "Wrong format")
    require(REVISION.fullmatch(manifest.get("planned_at", "")), "Missing planned-at revision")
    require(REVISION.fullmatch(baseline.get("source_revision", "")), "Missing reviewed source revision")
    require(nonempty(baseline.get("review_note")), "Missing baseline review note")
    require(manifest.get("toolchain") == "1.98.1", "Review CI/toolchain contract before changing it")
    require(set(manifest.get("invariants", [])) == INVARIANTS, "Incomplete invariant inventory")
    anchors = baseline.get("anchors", {})
    require(isinstance(anchors, dict) and anchors, "Missing source anchors")
    for name, anchor in anchors.items():
        require(within(root, anchor.get("path")).is_file(), f"Missing source anchor: {name}")
        require(SHA256.fullmatch(anchor.get("sha256", "")), f"Invalid anchor digest: {name}")
        require(type(anchor.get("line")) is int and anchor["line"] > 0 and nonempty(anchor.get("excerpt")),
                f"Missing source excerpt: {name}")
    tasks = manifest.get("tasks", [])
    require(isinstance(tasks, list) and tasks, "Missing tasks")
    require({task.get("id") for task in tasks} == TASK_IDS, "Incomplete task inventory")
    covered, names, producers = set(), set(), {}
    for task in tasks:
        tid = task["id"]
        require(task.get("status") in STATUSES, f"Unknown task status: {tid}")
        for field in ("title", "priority", "effort", "risk", "capability"):
            require(nonempty(task.get(field)), f"{tid}: missing {field}")
        for field in ("depends_on", "anchors", "scope", "creates", "steps", "test_cases", "invariants",
                      "evidence", "stop_conditions", "out_of_scope"):
            require(isinstance(task.get(field), list), f"{tid}: {field} must be a list")
        require(len(set(task["depends_on"])) == len(task["depends_on"]), f"{tid}: duplicate dependency")
        require(task["anchors"] and set(task["anchors"]) <= set(anchors), f"{tid}: unknown source anchor")
        for path in task["scope"]:
            require(within(root, path).exists(), f"{tid}: missing source scope: {path}")
        require(task["scope"] and task["stop_conditions"] and task["out_of_scope"], f"{tid}: incomplete scope")
        require(CASE.fullmatch(task.get("test_target", "")), f"{tid}: invalid test target")
        require(f'crates/cellar/tests/{task["test_target"]}.rs' in task["creates"], f"{tid}: missing target output")
        for path in task["creates"]:
            within(root, path)
            require(path not in producers, f"Duplicate output: {path}")
            require(path not in {a["path"] for a in anchors.values()}, f"Output collides with baseline: {path}")
            producers[path] = tid
        local_names, local_invariants = set(), set()
        for case in task["test_cases"]:
            require(CASE.fullmatch(case.get("name", "")), f"{tid}: invalid case")
            require(case["name"] not in names, f'Duplicate test: {case["name"]}')
            require(nonempty(case.get("oracle")), f"{tid}: empty oracle")
            require(case.get("invariants") and set(case["invariants"]) <= INVARIANTS, f"{tid}: invalid invariant")
            names.add(case["name"])
            local_names.add(case["name"])
            local_invariants.update(case["invariants"])
        require(local_names and local_invariants == set(task["invariants"]), f"{tid}: invariant coverage differs")
        step_cases = set()
        for step in task["steps"]:
            require(nonempty(step.get("action")) and step.get("tests"), f"{tid}: empty step")
            require(set(step["tests"]) <= local_names, f"{tid}: unknown step test")
            step_cases.update(step["tests"])
        require(step_cases == local_names, f"{tid}: cases missing from steps")
        covered.update(local_invariants)
    require(covered == INVARIANTS, "Missing invariant coverage")
    order = ordered(tasks)
    by_id = {task["id"]: task for task in tasks}
    for task in tasks:
        if task["status"] == "DONE":
            require(all(by_id[d]["status"] == "DONE" for d in task["depends_on"]), f'{task["id"]}: DONE before dependency')
            require(task["evidence"], f'{task["id"]}: DONE without evidence')
    profile_ids = set()
    require(profiles.get("profiles"), "Missing qualification profiles")
    for profile in profiles["profiles"]:
        pid = profile.get("id")
        require(isinstance(pid, str) and CASE.fullmatch(pid) and pid not in profile_ids, "Invalid/duplicate profile")
        profile_ids.add(pid)
        require(profile.get("required") is True, f"{pid}: required gate was disabled")
        require(profile.get("topology") in {"local_containers", "independent_hosts"}, f"{pid}: invalid topology")
        require(profile.get("checks"), f"{pid}: empty profile checks")
        for field in ("cells", "nodes", "voters", "duration_seconds", "repetitions"):
            require(type(profile.get(field)) is int and profile[field] > 0, f"{pid}: invalid {field}")
        require(profile.get("occupancies") and all(type(v) is int and v > 0 for v in profile["occupancies"]),
                f"{pid}: invalid occupancy matrix")
        require(set(profile.get("image_modes", [])) == {"materialized", "extents"}, f"{pid}: image mode missing")
        require(profile.get("gate_occupancies") and set(profile["gate_occupancies"]) <= set(profile["occupancies"]),
                f"{pid}: invalid gated cohorts")
        require(type(profile.get("historical_control")) is bool, f"{pid}: missing control contract")
        if profile["historical_control"]:
            require(REVISION.fullmatch(profile.get("historical_revision", "")), f"{pid}: missing historical pin")
        for name, ratio in profile.get("ratios", {}).items():
            require(name in profile["checks"] and nonempty(ratio.get("numerator")) and nonempty(ratio.get("denominator")),
                    f"{pid}: invalid ratio contract")
            require("control_occupancy" not in ratio or ratio["control_occupancy"] in profile["occupancies"],
                    f"{pid}: missing ratio control")
        for name, bounds in profile["checks"].items():
            require(bounds and set(bounds) <= {"eq", "min", "max"}, f"{pid}: invalid metric bound {name}")
            require(all(type(v) in (int, float) and math.isfinite(v) for v in bounds.values()), f"{pid}: nonfinite bound")
    require(profile_ids == PROFILE_IDS, "Required qualification profile inventory changed")
    require(any(p["required"] and p["topology"] == "independent_hosts" for p in profiles["profiles"]),
            "Release requires independent-domain qualification")
    return {"tasks": len(tasks), "required_cases": len(names), "invariants": len(covered),
            "profiles": len(profile_ids), "order": order}


def metric(value, name):
    require(type(value) in (int, float) and math.isfinite(value) and value >= 0, f"Missing/invalid metric: {name}")
    return value


def check_profile(report, profile, profile_digest, revision, root, executable):
    require(report.get("format") == 1 and report.get("profile_id") == profile["id"], "Wrong profile identity")
    require(report.get("source_revision") == revision, "Profile source differs from release source")
    require(report.get("profiles_sha256") == profile_digest, "Profile configuration changed")
    require(report.get("executable_sha256") == executable and SHA256.fullmatch(executable), "Profile executable differs")
    require(nonempty(report.get("environment")), "Missing actual profile environment")
    require(report.get("topology") == profile["topology"], "Profile topology differs")
    metrics = report.get("metrics", {})
    for name, bounds in profile["checks"].items():
        value = metrics.get(name)
        metric(value, name)
        for op, bound in bounds.items():
            passed = value == bound if op == "eq" else value >= bound if op == "min" else value <= bound
            require(passed, f'{profile["id"]}: {name}={value} violates {op} {bound}')
    artifacts = report.get("artifacts", {})
    for role in ("ledger", "measurements", "topology", "fault_schedule", "verification"):
        require(role in artifacts, f"Missing profile artifact: {role}")
        artifact(root, artifacts[role])
    measured = read_json(artifact(root, artifacts["measurements"]))
    require(measured.get("metrics") == metrics, "Summary differs from retained measurements")
    expected = {("replacement", o, mode, rep) for o in profile["occupancies"]
                for mode in profile["image_modes"] for rep in range(1, profile["repetitions"] + 1)}
    if profile["historical_control"]:
        expected.update(("historical", 1, mode, rep) for mode in profile["image_modes"]
                        for rep in range(1, profile["repetitions"] + 1))
    rows = {}
    for row in measured.get("cohorts", []):
        key = (row.get("variant"), row.get("occupancy"), row.get("image_mode"), row.get("repetition"))
        require(key in expected and key not in rows, "Unknown/duplicate measured cohort")
        source = revision if key[0] == "replacement" else profile["historical_revision"]
        require(row.get("source_revision") == source, "Cohort source mismatch")
        require(SHA256.fullmatch(row.get("executable_sha256", "")), "Missing cohort executable")
        if key[0] == "replacement":
            require(row["executable_sha256"] == executable, "Cohort executable differs")
        for name in ("cells", "nodes", "voters", "offered_requests_per_second"):
            require(type(row.get(name)) is int and row[name] == profile[name], f"Measured cohort changed {name}")
        require(metric(row.get("measured_seconds"), "measured_seconds") >= profile["duration_seconds"], "Cohort too short")
        require(row.get("ledger_sha256") == artifacts["ledger"]["sha256"], "Cohort ledger not bound")
        require(isinstance(row.get("metrics"), dict), "Missing cohort metrics")
        rows[key] = row
    require(set(rows) == expected, "Required cohort matrix incomplete")
    candidates = [(key, row) for key, row in rows.items() if key[0] == "replacement" and key[1] in profile["gate_occupancies"]]
    for key, row in candidates:
        values = row["metrics"]
        for name, ratio in profile.get("ratios", {}).items():
            control = rows[("replacement", ratio["control_occupancy"], key[2], key[3])] if "control_occupancy" in ratio else row
            numerator = metric(values.get(ratio["numerator"]), ratio["numerator"])
            denominator = metric(control["metrics"].get(ratio["denominator"]), ratio["denominator"])
            require(denominator > 0, "Ratio control denominator is zero")
            require(math.isclose(metric(values.get(name), name), numerator / denominator, rel_tol=1e-9), "Ratio is not derived from matched control")
    for name, bounds in profile["checks"].items():
        selected = list(rows.items()) if name in SAFETY_METRICS else candidates
        values = [metric(row["metrics"].get(name), name) for _, row in selected]
        for op, bound in bounds.items():
            aggregate = max(values) if op == "max" else min(values) if op == "min" else bound
            require(metrics[name] == aggregate, f"Metric does not retain worst cohort: {name}")
            if op == "eq":
                require(all(value == bound for value in values), f"Cohort equality gate failed: {name}")
    topology = read_json(artifact(root, artifacts["topology"]))
    require(topology.get("kind") == profile["topology"], "Actual topology mismatch")
    nodes = topology.get("nodes", [])
    require(len(nodes) == profile["nodes"] and len({n["id"] for n in nodes}) == len(nodes), "Incomplete topology inventory")
    if profile["topology"] == "independent_hosts":
        require(len({n["physical_host"] for n in nodes}) == len(nodes), "Containers do not prove independent hosts")
        require(len({n["failure_domain"] for n in nodes}) >= profile["voters"], "Independent failure domains missing")
    verified = read_json(artifact(root, artifacts["verification"]))
    require(verified.get("source_revision") == revision and verified.get("complete") is True, "History verification incomplete")
    require(verified.get("ledger_sha256") == artifacts["ledger"]["sha256"], "History verification used another ledger")
    require(verified.get("measurements_sha256") == artifacts["measurements"]["sha256"], "History verification used another measurement set")


def check_evidence(task, record, manifest, baseline, profiles, profile_digest, root):
    tid = task["id"]
    require(record.get("format") == 1 and record.get("task_id") == tid, "Wrong evidence task identity")
    revision = record.get("source_revision", "")
    require(REVISION.fullmatch(revision), "Missing evidence source revision")
    require(SHA256.fullmatch(record.get("executable_sha256", "")), "Missing evidence executable digest")
    require(nonempty(record.get("environment")) and nonempty(record.get("limitations")), "Missing evidence scope")
    cohort = manifest["tasks"] if tid == "CG-16" else [task]
    required_paths = {baseline["anchors"][a]["path"] for t in cohort for a in t["anchors"]}
    required_paths.update(f'crates/cellar/tests/{t["test_target"]}.rs' for t in cohort)
    inputs = record.get("source_inputs", [])
    require(isinstance(inputs, list) and inputs, "Missing source input inventory")
    paths = set()
    for item in inputs:
        path = item.get("path")
        require(path not in paths, "Duplicate source input")
        paths.add(path)
        content = git_bytes(root, revision, path)
        require(hashlib.sha256(content).hexdigest() == item.get("sha256"), f"Source hash mismatch: {path}")
    require(required_paths <= paths, f"Evidence omits source inputs: {sorted(required_paths - paths)}")
    # Evidence belongs to its tested revision. A later reviewed source baseline
    # must not make historical evidence depend on future edits, or stop requiring
    # earlier implementation inputs. Compare every record to the frozen plan base.
    changed = subprocess.check_output(["git", "diff", "--name-only", "--diff-filter=ACMRT", "-z",
                                       manifest["planned_at"], revision, "--", *RUNTIME_PATHS], cwd=root).decode().split("\0")
    require(set(changed) - {""} <= paths, "Evidence omits changed runtime inputs")
    outputs = {}
    for run in record.get("commands", []):
        require(type(run.get("exit_code")) is int and run["exit_code"] == 0, "Failed/unconfirmed evidence command")
        argv = run.get("argv")
        require(isinstance(argv, list) and all(nonempty(a) for a in argv), "Invalid recorded command")
        key = tuple(argv)
        require(key not in outputs, "Duplicate command evidence")
        path = artifact(root, {"path": run.get("output"), "sha256": run.get("sha256")})
        outputs[key] = path
    expected = commands_for(task, manifest)
    require(all(tuple(command) in outputs for command in expected), "Missing required command evidence")
    for item in cohort:
        prefix = cargo(manifest["toolchain"], "test", "-p", "cellar", "--test", item["test_target"], "--locked", "--")
        inventory = outputs[tuple(prefix + ["--list"])].read_text()
        for case in item["test_cases"]:
            name = case["name"]
            require(re.search(rf"^{re.escape(name)}: test$", inventory, re.M), f"Case absent from inventory: {name}")
            output = outputs[tuple(prefix + [name, "--exact", "--test-threads=1"])].read_text()
            require(re.search(rf"^test {re.escape(name)} \.\.\. ok$", output, re.M), f"Case did not pass: {name}")
            require("test result: ok. 1 passed; 0 failed; 0 ignored;" in output, f"Not exactly one passing case: {name}")
    required_profiles = {p["id"] for p in profiles["profiles"] if p["required"]} if tid in {"CG-15", "CG-16"} else set()
    if tid == "CG-13":
        required_profiles.add("rustfs_disaster")
    seen = set()
    by_id = {p["id"]: p for p in profiles["profiles"]}
    for item in record.get("profiles", []):
        report = read_json(artifact(root, item))
        pid = report.get("profile_id")
        require(pid in by_id and pid not in seen, "Unknown/duplicate profile evidence")
        qualified = record.get("qualified_executable_sha256", "")
        require(SHA256.fullmatch(qualified), "Missing qualified deployment executable digest")
        check_profile(report, by_id[pid], profile_digest, revision, root, qualified)
        seen.add(pid)
    require(required_profiles <= seen, f"Missing required profile evidence: {sorted(required_profiles - seen)}")
    return revision


def check_links(package, root):
    checked = 0
    for source in package.glob("*.md"):
        prose = re.sub(r"```.*?```", "", source.read_text(), flags=re.S)
        for raw in re.findall(r"!?\[[^\]]*\]\(([^)]+)\)", prose):
            url = urlsplit(raw.strip().strip("<>"))
            if url.scheme or url.netloc:
                continue
            target = (source.parent / unquote(url.path)).resolve() if url.path else source
            require(target.is_relative_to(root.resolve()) and target.exists(), f"Broken repository link: {source.name}: {raw}")
            if url.fragment and target.suffix == ".md":
                headings = re.findall(r"^#+\s+(.+)$", target.read_text(), re.M)
                ids = {re.sub(r"[^\w\- ]", "", h.lower().replace("`", "")).replace(" ", "-") for h in headings}
                require(unquote(url.fragment) in ids, f"Missing anchor: {raw}")
            checked += 1
    return checked


def index_text(manifest):
    lines = ["<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->", "",
             "# Implementation order", "", "Generated from `tasks.json`; print again with `plan.py index` after status changes.",
             "Every implementation task is pending until source-bound evidence passes.", "",
             "Use `python3 docs/design/multi-raft/cell-groups/plan.py show CG-01` for a complete",
             "executor brief. Replace the ID to select another task. Commands are displayed,",
             "never executed by the plan runner. Read the [design](design.md),",
             "[protocols](protocols.md) and [qualification](qualification.md) first.", "",
             "| Task | Deliverable | Dependencies | Status | Required cases |", "| --- | --- | --- | --- | ---: |"]
    by_id = {task["id"]: task for task in manifest["tasks"]}
    for tid in ordered(manifest["tasks"]):
        task = by_id[tid]
        lines.append(f'| {task["id"]} | {task["title"]} | {", ".join(task["depends_on"]) or "None"} | {task["status"]} | {len(task["test_cases"])} |')
    lines.extend(["", "CG-01 through CG-08 establish the shared runtime and public cutover. CG-09 through",
                  "CG-13 add movement, adaptive scheduling, hibernation and complete managed recovery.",
                  "CG-08 adapts effects before CG-07 public cutover. CG-14 can proceed after CG-07;",
                  "CG-13 requires its real-infrastructure harness.",
                  "CG-15 and CG-16 qualify the combined artifact. These milestones are not separate",
                  "production architectures. No full delivery claim precedes the final gate.", ""])
    return "\n".join(lines)


def brief(task, manifest, baseline, root):
    lines = [f'# {task["id"]}: {task["title"]}', "", f'Planned at `{manifest["planned_at"]}`; reviewed source `{baseline["source_revision"]}`.',
             f'Status {task["status"]}; priority {task["priority"]}; effort {task["effort"]}; risk {task["risk"]}.',
             f'Dependencies: {", ".join(task["depends_on"]) or "none"}.', "",
             "Implement TiKV-style groups containing whole independent SQLite cells. One physical group owns",
             "consensus, placement, contiguous apply, checkpoint/archive and recovery; each cell retains",
             "identity, transactions, receipt dedupe, LTX and query version. Normal transfers preserve cell",
             "history; disaster recovery fences old writers and creates fresh incarnations. No cross-cell",
             "transactions, old-format migration or quorum bypass is allowed.", "",
             "Read this repository's AGENTS.md and docs/design/multi-raft/cell-groups/{design,protocols,qualification}.md.",
             "Use Rust 1.98.1, locked Cargo and a private writable workspace-volume CARGO_TARGET_DIR.",
             "Keep cellar-logic pure; use execution-domain I/O/timers, typed failures and joined owners.",
             "Use leaf.rs with sibling leaf_tests.rs or component/mod.rs. Markers remain 1 with fresh domains.",
             "Use native_raft_snapshot.rs/support/raft_cluster.rs real-image fixtures, not stub markers.",
             "Do not enable cellar_internal_tests without its external harness. Preserve licenses and secrets.", "",
             "## Drift and source context", "", "Run `python3 docs/design/multi-raft/cell-groups/plan.py preflight --task " + task["id"] + "`.",
             "Expected exit 0. A changed input requires review and a recorded successor baseline, not blind rehashing.", ""]
    for name in task["anchors"]:
        anchor = baseline["anchors"][name]
        lines.extend([f'`{anchor["path"]}:{anchor["line"]}` at the reviewed revision:', "", "```", anchor["excerpt"], "```", ""])
    lines.extend(["## Scope", "", "Edit only these owner paths/subtrees and the listed new outputs:", ""])
    lines.extend(f'- `{path}`' for path in task["scope"] + task["creates"])
    lines.extend(["", "Changes to callers outside this scope require a recorded scope update and matching regression tests.",
                  "Keep unrelated work. Use codex/ branches and the active PR; no merge is implied.", "", "Out of scope:", ""])
    lines.extend(f'- {item}' for item in task["out_of_scope"])
    lines.extend(["", "## Steps and verification", ""])
    target = root / f'crates/cellar/tests/{task["test_target"]}.rs'
    lines.append("Test target: " + ("present; inspect its real assertions." if target.is_file() else "PLANNED OUTPUT, currently absent. Create it; absence is not a pass."))
    prefix = cargo(manifest["toolchain"], "test", "-p", "cellar", "--test", task["test_target"], "--locked", "--")
    for n, step in enumerate(task["steps"], 1):
        lines.extend(["", f'### Step {n}', "", step["action"], "", "After implementation, verify these exact cases (each must select and pass one test):", "", "```sh"])
        lines.extend(shlex.join(prefix + [name, "--exact", "--test-threads=1"]) for name in step["tests"])
        lines.append("```")
    lines.extend(["", "## Required behavior", ""])
    for case in task["test_cases"]:
        lines.append(f'- `{case["name"]}` ({", ".join(case["invariants"])}): {case["oracle"]}')
    lines.extend(["", "## Complete command/evidence gate", "", "First check inventory contains every named case; no zero-test success is accepted.", "", "```sh"])
    lines.extend(shlex.join(command) for command in commands_for(task, manifest))
    lines.extend(["```", "", "Every command must exit 0 with retained hashed output at the exact source revision.",
                  "Record environment, limitations, executable/source hashes and required deployment profiles",
                  "using qualification.md. Set DONE only after evidence validation and dependency completion.",
                  "Regenerate implementation-plan.md with plan.py index. A plan-only result is not DONE.", "", "## Stop and reconcile", ""])
    lines.extend(f'- {item}' for item in task["stop_conditions"])
    lines.extend(["", "Review protocol changes at their owning boundary and update source anchors, tests and docs together.",
                  "Do not weaken a failed safety assertion or discard failed load runs. CG-16 reruns every capability",
                  "at one release source; earlier isolated passes do not qualify a later combined artifact.", ""])
    return "\n".join(lines)


def load(package=PACKAGE, root=ROOT):
    manifest, baseline, profiles = (read_json(package / name) for name in ("tasks.json", "baseline.json", "profiles.json"))
    stats = validate_contract(manifest, baseline, profiles, root)
    return manifest, baseline, profiles, stats


def preflight(baseline, root, names=None):
    for name, anchor in baseline["anchors"].items():
        if names is not None and name not in names:
            continue
        content = git_bytes(root, baseline["source_revision"], anchor["path"])
        require(hashlib.sha256(content).hexdigest() == anchor["sha256"], f"Baseline is not pinned to source: {name}")
        require(anchor["excerpt"] in content.decode(), f"Excerpt differs at source: {name}")
        lines = content.decode().splitlines()
        start = anchor["line"] - 1
        excerpt_lines = anchor["excerpt"].split("\n")
        require(lines[start:start + len(excerpt_lines)] == excerpt_lines,
                f"Excerpt line differs at source: {name}")
        require(digest(within(root, anchor["path"])) == anchor["sha256"], f"Source drift requires review: {anchor['path']}")


def require_release_inputs(root, revision):
    require(REVISION.fullmatch(revision), "Invalid qualified source revision")
    compared = subprocess.run(["git", "diff", "--quiet", revision, "HEAD", "--", *RUNTIME_PATHS],
                              cwd=root, capture_output=True, check=False)
    require(compared.returncode == 0, "Current executable inputs differ from qualified source")
    changed = subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=all", "--", *RUNTIME_PATHS],
                                      cwd=root, text=True)
    require(not changed, "Release executable inputs are dirty")


def validate(package=PACKAGE, root=ROOT):
    manifest, baseline, profiles, stats = load(package, root)
    require((package / "implementation-plan.md").read_text() == index_text(manifest), "Implementation index is stale; regenerate with plan.py index")
    stats["local_links"] = check_links(package, root)
    profile_digest = digest(package / "profiles.json")
    for task in manifest["tasks"]:
        for path in task["evidence"]:
            check_evidence(task, read_json(within(root, path)), manifest, baseline, profiles, profile_digest, root)
    stats["done"] = sum(t["status"] == "DONE" for t in manifest["tasks"])
    return manifest, baseline, profiles, stats


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("validate", "ready", "index", "release"):
        sub.add_parser(name)
    sub.add_parser("show").add_argument("task")
    sub.add_parser("preflight").add_argument("--task")
    args = parser.parse_args()
    if args.command == "index":
        manifest, _, _, _ = load()
        print(index_text(manifest), end="")
        return 0
    manifest, baseline, profiles, stats = validate()
    by_id = {task["id"]: task for task in manifest["tasks"]}
    if args.command == "validate":
        print(json.dumps(stats, indent=2))
    elif args.command == "preflight":
        require(not args.task or args.task in by_id, "Unknown task")
        preflight(baseline, ROOT, by_id[args.task]["anchors"] if args.task else None)
        print("Reviewed source hashes and excerpts match. Runtime tasks remain pending.")
    elif args.command == "show":
        require(args.task in by_id, "Unknown task")
        print(brief(by_id[args.task], manifest, baseline, ROOT))
    elif args.command == "ready":
        ready = [t["id"] for t in manifest["tasks"] if t["status"] == "TODO"
                 and all(by_id[d]["status"] == "DONE" for d in t["depends_on"])]
        print(json.dumps({"dependency_ready": ready, "done": stats["done"], "note": "Ready means eligible to implement, not implemented."}, indent=2))
    elif args.command == "release":
        require(stats["done"] == len(manifest["tasks"]), "Release blocked: implementation tasks/evidence incomplete")
        revision = read_json(within(ROOT, by_id["CG-16"]["evidence"][-1]))["source_revision"]
        require_release_inputs(ROOT, revision)
        print(f"Release evidence verified for {revision}; only the recorded topology/load envelope is qualified.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (PlanError, OSError, KeyError, TypeError, json.JSONDecodeError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)

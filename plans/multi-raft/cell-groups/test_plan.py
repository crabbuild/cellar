# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Negative tests for the delivery gate, not tests of the proposed Raft runtime."""

import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("cell_group_plan", Path(__file__).with_name("plan.py"))
plan = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(plan)


class PlanTests(unittest.TestCase):
    def setUp(self):
        self.manifest = plan.read_json(plan.PACKAGE / "tasks.json")
        self.actual_manifest = copy.deepcopy(self.manifest)
        for task in self.manifest["tasks"]:
            task.update(status="TODO", evidence=[])
        self.baseline = plan.read_json(plan.PACKAGE / "baseline.json")
        self.profiles = plan.read_json(plan.PACKAGE / "profiles.json")
        self.temp = tempfile.TemporaryDirectory(prefix="cell-group-plan-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.revision = "a" * 40
        self.executable = "b" * 64
        self.profile_digest = "c" * 64

    def contract(self):
        return plan.validate_contract(self.manifest, self.baseline, self.profiles, plan.ROOT)

    def put(self, name, value):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value if isinstance(value, str) else json.dumps(value))
        return {"path": name, "sha256": plan.digest(path)}

    def evidence(self):
        task = self.manifest["tasks"][0]
        paths = {self.baseline["anchors"][a]["path"] for a in task["anchors"]}
        paths.add(f'crates/cellar/tests/{task["test_target"]}.rs')
        record = {"format": 1, "task_id": task["id"], "source_revision": self.revision,
                  "executable_sha256": self.executable, "environment": "Synthetic validator fixture only",
                  "limitations": "Not runtime evidence", "source_inputs": [], "commands": [], "profiles": []}
        for path in sorted(paths):
            record["source_inputs"].append({"path": path, "sha256": hashlib.sha256(path.encode()).hexdigest()})
        for index, command in enumerate(plan.commands_for(task, self.manifest)):
            output = "Command exited successfully.\n"
            if command[-1] == "--list":
                output = "\n".join(c["name"] + ": test" for c in task["test_cases"]) + "\n"
            elif "--exact" in command:
                name = command[command.index("--exact") - 1]
                output = f"running 1 test\ntest {name} ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 3 filtered out; finished in 0.01s\n"
            item = self.put(f"commands/{index}.log", output)
            record["commands"].append({"argv": command, "exit_code": 0, "output": item["path"], "sha256": item["sha256"]})
        return record

    def verify_evidence(self, record, changed=b""):
        with patch.object(plan, "git_bytes", side_effect=lambda root, rev, path: path.encode()), \
                patch.object(plan.subprocess, "check_output", return_value=changed):
            return plan.check_evidence(self.manifest["tasks"][0], record, self.manifest, self.baseline,
                                       self.profiles, self.profile_digest, self.root)

    def profile_fixture(self, pid="mixed_quorum"):
        profile = next(p for p in self.profiles["profiles"] if p["id"] == pid)
        metrics = {name: next(iter(bounds.values())) for name, bounds in profile["checks"].items()}
        for name in profile["ratios"]:
            metrics[name] = 1.0
        ledger = self.put("ledger.jsonl", '{"validator_fixture_only": true}\n')
        rows = []
        for variant, occupancies in [("replacement", profile["occupancies"]), ("historical", [1] if profile["historical_control"] else [])]:
            for occupancy in occupancies:
                for mode in profile["image_modes"]:
                    for rep in range(1, profile["repetitions"] + 1):
                        row = {"variant": variant, "occupancy": occupancy, "image_mode": mode, "repetition": rep,
                               "source_revision": self.revision if variant == "replacement" else profile["historical_revision"],
                               "executable_sha256": self.executable, "ledger_sha256": ledger["sha256"],
                               "measured_seconds": profile["duration_seconds"], "metrics": copy.deepcopy(metrics)}
                        row.update({k: profile[k] for k in ("cells", "nodes", "voters", "offered_requests_per_second")})
                        for ratio in profile["ratios"].values():
                            row["metrics"].setdefault(ratio["numerator"], 100)
                            row["metrics"].setdefault(ratio["denominator"], 100)
                        rows.append(row)
        measured = {"metrics": metrics, "cohorts": rows}
        measurements = self.put("measurements.json", measured)
        topology = {"kind": profile["topology"], "nodes": [
            {"id": str(n), "physical_host": f"host{n}", "failure_domain": f"domain{n % 3}"}
            for n in range(profile["nodes"])]}
        verification = {"source_revision": self.revision, "complete": True, "ledger_sha256": ledger["sha256"],
                        "measurements_sha256": measurements["sha256"]}
        report = {"format": 1, "profile_id": pid, "source_revision": self.revision,
                  "profiles_sha256": self.profile_digest, "executable_sha256": self.executable,
                  "environment": "Synthetic validator fixture only", "topology": profile["topology"],
                  "metrics": metrics, "artifacts": {"ledger": ledger, "measurements": measurements,
                      "topology": self.put("topology.json", topology),
                      "fault_schedule": self.put("faults.json", {"fixture": True}),
                      "verification": self.put("verification.json", verification)}}
        return profile, report

    def edit_artifact(self, report, role, change):
        item = report["artifacts"][role]
        content = plan.read_json(self.root / item["path"])
        change(content)
        report["artifacts"][role] = self.put(item["path"], content)
        if role == "measurements":
            self.edit_artifact(report, "verification", lambda obj: obj.update(measurements_sha256=report["artifacts"][role]["sha256"]))

    def verify_profile(self, profile, report):
        plan.check_profile(report, profile, self.profile_digest, self.revision, self.root, self.executable)

    def test_current_contract_and_index(self):
        stats = self.contract()
        self.assertEqual(stats["tasks"], 16)
        self.assertGreaterEqual(stats["required_cases"], 83)
        self.assertEqual(stats["invariants"], 14)
        self.assertEqual((plan.PACKAGE / "implementation-plan.md").read_text(), plan.index_text(self.actual_manifest))

    def test_evidence_coverage_keeps_frozen_base_after_baseline_advances(self):
        record = self.evidence()
        self.baseline["source_revision"] = "f" * 40
        with patch.object(plan, "git_bytes", side_effect=lambda root, rev, path: path.encode()), \
                patch.object(plan.subprocess, "check_output", return_value=b"") as diff:
            plan.check_evidence(self.manifest["tasks"][0], record, self.manifest,
                                self.baseline, self.profiles, self.profile_digest, self.root)
            argv = diff.call_args.args[0]
            self.assertEqual(argv[5:7], [self.manifest["planned_at"], self.revision])
        # Earlier implementation inputs stay required even when a later review
        # baseline already includes them. Advancing the baseline cannot waive them.
        with self.assertRaisesRegex(plan.PlanError, "omits changed runtime inputs"):
            self.verify_evidence(record, b"crates/cellar/earlier-input.rs\0")

    def test_dependency_order_covers_effects_and_harness(self):
        order = plan.ordered(self.manifest["tasks"])
        self.assertLess(order.index("CG-08"), order.index("CG-07"))
        self.assertLess(order.index("CG-14"), order.index("CG-13"))

    def test_cycle_refuses(self):
        self.manifest["tasks"][0]["depends_on"] = ["CG-16"]
        with self.assertRaisesRegex(plan.PlanError, "cycle"):
            self.contract()

    def test_unknown_dependency_refuses(self):
        self.manifest["tasks"][0]["depends_on"] = ["CG-99"]
        with self.assertRaisesRegex(plan.PlanError, "Unknown dependency"):
            self.contract()

    def test_duplicate_task_refuses(self):
        self.manifest["tasks"].append(copy.deepcopy(self.manifest["tasks"][0]))
        with self.assertRaises(plan.PlanError):
            self.contract()

    def test_done_without_evidence_refuses(self):
        self.manifest["tasks"][0]["status"] = "DONE"
        with self.assertRaisesRegex(plan.PlanError, "without evidence"):
            self.contract()

    def test_done_before_dependency_refuses(self):
        self.manifest["tasks"][1].update(status="DONE", evidence=["fake.json"])
        with self.assertRaisesRegex(plan.PlanError, "before dependency"):
            self.contract()

    def test_missing_case_oracle_refuses(self):
        self.manifest["tasks"][0]["test_cases"][0]["oracle"] = ""
        with self.assertRaisesRegex(plan.PlanError, "empty oracle"):
            self.contract()

    def test_uncovered_step_test_refuses(self):
        self.manifest["tasks"][0]["steps"][0]["tests"] = ["imaginary_test"]
        with self.assertRaisesRegex(plan.PlanError, "unknown step test"):
            self.contract()

    def test_profile_removal_or_disable_refuses(self):
        saved = copy.deepcopy(self.profiles)
        self.profiles["profiles"].pop()
        with self.assertRaisesRegex(plan.PlanError, "profile inventory"):
            self.contract()
        self.profiles = saved
        self.profiles["profiles"][0]["required"] = False
        with self.assertRaisesRegex(plan.PlanError, "disabled"):
            self.contract()

    def test_path_escape_and_symlink_refuse(self):
        with self.assertRaises(plan.PlanError):
            plan.within(self.root, "../outside")
        with tempfile.TemporaryDirectory() as outside:
            (self.root / "link").symlink_to(outside, target_is_directory=True)
            with self.assertRaises(plan.PlanError):
                plan.within(self.root, "link/secret")

    def test_duplicate_json_and_nan_refuse(self):
        for text in ('{"format":1,"format":2}', '{"metric":NaN}'):
            self.put("bad.json", text)
            with self.assertRaises(plan.PlanError):
                plan.read_json(self.root / "bad.json")

    def test_actual_reviewed_source_preflight(self):
        plan.preflight(self.baseline, plan.ROOT)

    def test_incorrect_excerpt_line_refuses(self):
        self.baseline["anchors"]["codec"]["line"] += 1
        with self.assertRaisesRegex(plan.PlanError, "Excerpt line differs"):
            plan.preflight(self.baseline, plan.ROOT)

    def test_source_drift_refuses(self):
        anchor = copy.deepcopy(self.baseline["anchors"]["identity"])
        content = (plan.ROOT / anchor["path"]).read_bytes()
        path = self.root / anchor["path"]
        path.parent.mkdir(parents=True)
        path.write_bytes(content + b"\n// changed\n")
        baseline = {"source_revision": self.revision, "anchors": {"identity": anchor}}
        with patch.object(plan, "git_bytes", return_value=content), self.assertRaisesRegex(plan.PlanError, "Source drift"):
            plan.preflight(baseline, self.root)

    def test_evidence_accepts_complete_exact_test_records(self):
        self.assertEqual(self.verify_evidence(self.evidence()), self.revision)

    def test_missing_command_evidence_refuses(self):
        record = self.evidence()
        record["commands"].pop()
        with self.assertRaisesRegex(plan.PlanError, "Missing required command"):
            self.verify_evidence(record)

    def test_zero_selected_or_ignored_test_refuses(self):
        for text in ("test result: ok. 0 passed; 0 failed; 0 ignored;", "test result: ok. 0 passed; 0 failed; 1 ignored;"):
            record = self.evidence()
            run = next(x for x in record["commands"] if "--exact" in x["argv"])
            item = self.put(run["output"], text)
            run["sha256"] = item["sha256"]
            with self.assertRaisesRegex(plan.PlanError, "Case did not pass"):
                self.verify_evidence(record)

    def test_false_inventory_refuses(self):
        record = self.evidence()
        run = next(x for x in record["commands"] if x["argv"][-1] == "--list")
        item = self.put(run["output"], "some_other_test: test\n")
        run["sha256"] = item["sha256"]
        with self.assertRaisesRegex(plan.PlanError, "absent from inventory"):
            self.verify_evidence(record)

    def test_tampered_log_or_failed_exit_refuses(self):
        record = self.evidence()
        (self.root / record["commands"][0]["output"]).write_text("tampered")
        with self.assertRaisesRegex(plan.PlanError, "hash mismatch"):
            self.verify_evidence(record)
        record = self.evidence()
        record["commands"][0]["exit_code"] = 1
        with self.assertRaisesRegex(plan.PlanError, "Failed/unconfirmed"):
            self.verify_evidence(record)

    def test_missing_or_wrong_source_input_refuses(self):
        record = self.evidence()
        record["source_inputs"].pop()
        with self.assertRaisesRegex(plan.PlanError, "omits source inputs"):
            self.verify_evidence(record)
        record = self.evidence()
        record["source_inputs"][0]["sha256"] = "0" * 64
        with self.assertRaisesRegex(plan.PlanError, "Source hash mismatch"):
            self.verify_evidence(record)

    def test_unrecorded_changed_runtime_source_refuses(self):
        with self.assertRaisesRegex(plan.PlanError, "changed runtime inputs"):
            self.verify_evidence(self.evidence(), b"crates/unrecorded.rs\0")

    def test_release_commands_repeat_every_exact_case(self):
        commands = plan.commands_for(self.manifest["tasks"][-1], self.manifest)
        selected = {x[x.index("--exact") - 1] for x in commands if "--exact" in x}
        expected = {c["name"] for task in self.manifest["tasks"] for c in task["test_cases"]}
        self.assertEqual(selected, expected)
        self.assertIn(["python3", "tools/docs/check.py"], commands)

    def test_complete_profile_matrix_passes(self):
        self.verify_profile(*self.profile_fixture())

    def test_profile_source_binary_or_config_drift_refuses(self):
        for field, value in (("source_revision", "d" * 40), ("executable_sha256", "d" * 64), ("profiles_sha256", "d" * 64)):
            profile, report = self.profile_fixture()
            report[field] = value
            with self.assertRaises(plan.PlanError):
                self.verify_profile(profile, report)

    def test_missing_or_duplicate_cohort_refuses(self):
        for mutate in (lambda x: x["cohorts"].pop(), lambda x: x["cohorts"].append(copy.deepcopy(x["cohorts"][0]))):
            profile, report = self.profile_fixture()
            self.edit_artifact(report, "measurements", mutate)
            with self.assertRaisesRegex(plan.PlanError, "cohort"):
                self.verify_profile(profile, report)

    def test_short_or_wrong_population_cohort_refuses(self):
        for field, value in (("measured_seconds", 1), ("cells", 1), ("nodes", 1), ("source_revision", "d" * 40), ("executable_sha256", "d" * 64)):
            profile, report = self.profile_fixture()
            self.edit_artifact(report, "measurements", lambda x: x["cohorts"][0].update({field: value}))
            with self.assertRaises(plan.PlanError):
                self.verify_profile(profile, report)

    def test_numeric_failure_negative_boolean_or_nan_refuses(self):
        for value in (1, -1, True, float("nan")):
            profile, report = self.profile_fixture()
            report["metrics"]["lost_acknowledged_effects"] = value
            with self.assertRaises(plan.PlanError):
                self.verify_profile(profile, report)

    def test_better_aggregate_cannot_hide_slow_cohort(self):
        profile, report = self.profile_fixture()
        def slow(measured):
            row = next(x for x in measured["cohorts"] if x["variant"] == "replacement" and x["occupancy"] == 64)
            row["metrics"]["p99_ms"] = 400
            row["metrics"]["p99_ratio_to_occupancy1"] = 2
        self.edit_artifact(report, "measurements", slow)
        with self.assertRaisesRegex(plan.PlanError, "worst cohort"):
            self.verify_profile(profile, report)

    def test_ratio_must_match_control_and_nonzero_denominator(self):
        profile, report = self.profile_fixture()
        self.edit_artifact(report, "measurements", lambda x: x["cohorts"][0]["metrics"].update(p99_ms=0))
        with self.assertRaisesRegex(plan.PlanError, "denominator"):
            self.verify_profile(profile, report)

    def test_safety_failure_in_control_or_historical_cohort_refuses(self):
        for variant in ("replacement", "historical"):
            profile, report = self.profile_fixture()
            def lose(measured):
                row = next(x for x in measured["cohorts"] if x["variant"] == variant and x["occupancy"] == 1)
                row["metrics"]["lost_acknowledged_effects"] = 1
            self.edit_artifact(report, "measurements", lose)
            with self.assertRaisesRegex(plan.PlanError, "worst cohort"):
                self.verify_profile(profile, report)

    def test_independent_hosts_cannot_be_faked_by_container_count(self):
        profile, report = self.profile_fixture("independent_domain_loss")
        self.verify_profile(profile, report)
        self.edit_artifact(report, "topology", lambda x: [n.update(physical_host="same-host") for n in x["nodes"]])
        with self.assertRaisesRegex(plan.PlanError, "independent hosts"):
            self.verify_profile(profile, report)

    def test_verification_must_bind_actual_ledger(self):
        profile, report = self.profile_fixture()
        self.edit_artifact(report, "verification", lambda x: x.update(ledger_sha256="d" * 64))
        with self.assertRaisesRegex(plan.PlanError, "another ledger"):
            self.verify_profile(profile, report)

    def test_verification_cannot_omit_artifact(self):
        profile, report = self.profile_fixture()
        del report["artifacts"]["fault_schedule"]
        with self.assertRaisesRegex(plan.PlanError, "Missing profile artifact"):
            self.verify_profile(profile, report)

    def test_brief_labels_unimplemented_target_and_exact_commands(self):
        text = plan.brief(self.manifest["tasks"][0], self.manifest, self.baseline, self.root)
        self.assertIn("PLANNED OUTPUT, currently absent", text)
        self.assertIn("--exact --test-threads=1", text)
        self.assertIn("A plan-only result is not DONE", text)

    def test_release_covers_all_executable_paths_and_plan_only_commits(self):
        def git(*args):
            return subprocess.check_output(["git", "-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false",
                                            "-c", "user.name=Plan Validator Test", "-c", "user.email=plan-test@example.invalid",
                                            *args], cwd=self.root, text=True, stderr=subprocess.DEVNULL)
        git("init", "--quiet")
        inputs = ["crates/lib.rs", "examples/main.rs", "vendor/lib.rs", ".cargo/config.toml", "build.rs",
                  "rust-toolchain.toml", "docs/api.md", "future-runtime-input.txt"]
        for name in inputs + ["plans/status.json"]:
            self.put(name, "original\n")
        git("add", ".")
        git("commit", "--quiet", "-m", "Synthetic validator fixture")
        revision = git("rev-parse", "HEAD").strip()
        plan.require_release_inputs(self.root, revision)
        self.put("plans/status.json", "updated plan\n")
        plan.require_release_inputs(self.root, revision)
        git("add", "plans")
        git("commit", "--quiet", "-m", "Plan-only status")
        plan.require_release_inputs(self.root, revision)
        for name in inputs:
            self.put(name, "changed\n")
            with self.assertRaisesRegex(plan.PlanError, "dirty"):
                plan.require_release_inputs(self.root, revision)
            self.put(name, "original\n")
        self.put("examples/main.rs", "changed\n")
        git("add", "examples")
        git("commit", "--quiet", "-m", "Changed executable")
        with self.assertRaisesRegex(plan.PlanError, "differ from qualified"):
            plan.require_release_inputs(self.root, revision)


if __name__ == "__main__":
    unittest.main()

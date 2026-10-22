"""Negative tests for package checks; these do not exercise a Raft runtime."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
import plan


class PackageTests(unittest.TestCase):
    def setUp(self):
        self.manifest = plan.read_json(plan.PACKAGE / "tasks.json")
        self.baseline = plan.read_json(plan.PACKAGE / "baseline.json")

    def validate(self):
        return plan.validate_manifest(self.manifest, self.baseline)

    def test_current_package(self):
        stats = self.validate()
        self.assertEqual(stats["tasks"], 17)
        self.assertEqual(stats["cases"], 146)

    def test_cycle_rejected(self):
        self.manifest["tasks"][0]["depends_on"] = ["MR-15"]
        with self.assertRaisesRegex(plan.PlanError, "cycle"):
            self.validate()

    def test_missing_dependency_rejected(self):
        self.manifest["tasks"][1]["depends_on"] = ["MR-99"]
        with self.assertRaisesRegex(plan.PlanError, "Unknown dependency"):
            self.validate()

    def test_duplicate_task_rejected(self):
        self.manifest["tasks"].append(copy.deepcopy(self.manifest["tasks"][0]))
        with self.assertRaisesRegex(plan.PlanError, "Duplicate task"):
            self.validate()

    def test_duplicate_case_rejected(self):
        self.manifest["tasks"][0]["test_cases"][1]["name"] = "mode_isolation"
        with self.assertRaisesRegex(plan.PlanError, "Duplicate case"):
            self.validate()

    def test_unknown_invariant_rejected(self):
        self.manifest["tasks"][0]["test_cases"][0]["invariants"] = ["I99"]
        with self.assertRaisesRegex(plan.PlanError, "invariant mapping"):
            self.validate()

    def test_future_source_needs_dependency(self):
        self.manifest["tasks"][2]["depends_on"] = ["MR-01"]
        with self.assertRaisesRegex(plan.PlanError, "dependency producer"):
            self.validate()

    def test_done_without_evidence_rejected(self):
        self.manifest["tasks"][0]["status"] = "DONE"
        self.manifest["tasks"][0]["evidence"] = []
        with self.assertRaisesRegex(plan.PlanError, "no evidence"):
            self.validate()

    def test_done_before_dependencies_rejected(self):
        self.manifest["tasks"][0]["status"] = "TODO"
        self.manifest["tasks"][1]["status"] = "DONE"
        with self.assertRaisesRegex(plan.PlanError, "before its dependencies"):
            self.validate()

    def test_shell_command_substitution_rejected(self):
        self.manifest["tasks"][0]["verification"] = [["sh", "-c", "true"]]
        with self.assertRaisesRegex(plan.PlanError, "command contract"):
            self.validate()

    def test_path_escape_rejected(self):
        self.manifest["tasks"][0]["creates"].append("../outside.rs")
        with self.assertRaisesRegex(plan.PlanError, "escapes"):
            self.validate()

    def test_empty_or_partial_test_inventory_rejected(self):
        task = self.manifest["tasks"][0]
        with self.assertRaisesRegex(plan.PlanError, "zero tests"):
            plan.check_listing(task, "0 tests, 0 benchmarks\n")
        with self.assertRaisesRegex(plan.PlanError, "Missing"):
            plan.check_listing(task, "mode_isolation: test\n")

    def test_full_test_inventory_accepted(self):
        task = self.manifest["tasks"][0]
        listing = "\n".join(f"tests::{c['name']}: test" for c in task["test_cases"])
        self.assertEqual(plan.check_listing(task, listing), len(task["test_cases"]))

    def test_source_drift_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.rs").write_text("changed")
            baseline = {"files": [{"path": "source.rs", "sha256": "0" * 64}]}
            with self.assertRaisesRegex(plan.PlanError, "Source drift"):
                plan.preflight(baseline, root)

    def test_broken_link_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory)
            (package / "README.md").write_text("[missing](missing.md)")
            with self.assertRaisesRegex(plan.PlanError, "missing link"):
                plan.check_links(package)

    def test_forged_evidence_missing_cases_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory)
            task = copy.deepcopy(self.manifest["tasks"][0])
            task["evidence"] = ["record.json"]
            record = {"task_id": task["id"], "source_revision": "a" * 40,
                      "toolchain": "fixture", "executable_digest": "fixture",
                      "environment": "fixture", "limitations": "checker fixture only",
                      "tests": [], "commands": []}
            (package / "record.json").write_text(json.dumps(record))
            with self.assertRaisesRegex(plan.PlanError, "missing passed cases"):
                plan.validate_evidence(task, package)

    def test_capacity_does_not_treat_idle_as_hibernating(self):
        profile = plan.read_json(plan.PACKAGE / "capacity-profiles.json")["profiles"][0]
        result = plan.capacity(profile)
        self.assertEqual(result["replicas_per_node"], 3000)
        self.assertEqual(result["leaders_per_node"], 1000)
        self.assertEqual(result["logical_heartbeat_deliveries_per_second"], 2000000)
        profile["nodes"] = 0
        with self.assertRaisesRegex(plan.PlanError, "nodes"):
            plan.capacity(profile)


if __name__ == "__main__":
    unittest.main()

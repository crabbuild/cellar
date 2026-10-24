"""Reject incomplete or misleading delivery evidence and malformed plans."""

import copy
import json
from pathlib import Path
import tempfile
import unittest

import plan


class PlanTests(unittest.TestCase):
    def setUp(self):
        self.manifest = plan.read_json(plan.PACKAGE / "tasks.json")
        self.baseline = plan.read_json(plan.PACKAGE / "baseline.json")

    def test_checked_in_package_has_complete_inventory(self):
        result = plan.validate(self.manifest, self.baseline)
        self.assertEqual(result["tasks"], 10)
        self.assertGreaterEqual(result["required_cases"], 50)
        self.assertGreater(plan.check_links(), 0)

    def test_dependency_cycle_is_rejected(self):
        self.manifest["tasks"][0]["depends_on"] = ["FO-10"]
        with self.assertRaisesRegex(plan.PlanError, "cycle"):
            plan.validate(self.manifest, self.baseline)

    def test_unknown_dependency_is_rejected(self):
        self.manifest["tasks"][0]["depends_on"] = ["FO-99"]
        with self.assertRaisesRegex(plan.PlanError, "Unknown dependency"):
            plan.validate(self.manifest, self.baseline)

    def test_done_without_evidence_is_rejected(self):
        self.manifest["tasks"][0]["status"] = "DONE"
        with self.assertRaisesRegex(plan.PlanError, "retained evidence"):
            plan.validate(self.manifest, self.baseline)

    def test_listing_must_include_all_required_cases_once(self):
        task = self.manifest["tasks"][0]
        listing = "\n".join(f"fixture::{case['name']}: test" for case in task["test_cases"])
        plan.check_listing(task, listing)
        for bad in ("0 tests", listing.splitlines()[0], listing + "\n" + listing.splitlines()[0]):
            with self.assertRaises(plan.PlanError):
                plan.check_listing(task, bad)

    def test_duplicate_json_fields_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "bad.json"
            path.write_text('{"format":1,"format":1}')
            with self.assertRaisesRegex(plan.PlanError, "Duplicate JSON"):
                plan.read_json(path)

    def test_source_drift_requires_review(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.rs"
            source.write_text("original")
            baseline = {"files": [{"path": "source.rs", "sha256": plan.sha256(source)}]}
            plan.preflight(baseline, root)
            source.write_text("changed")
            with self.assertRaisesRegex(plan.PlanError, "source drift"):
                plan.preflight(baseline, root)

    def test_artifact_escape_and_hash_change_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(plan.PlanError, "escapes"):
                plan.within(root, "../outside")
            log = root / "test.log"
            log.write_text("original result")
            digest = plan.sha256(log)
            plan.verified_file(root, "test.log", digest)
            log.write_text("replacement result")
            with self.assertRaisesRegex(plan.PlanError, "hash mismatch"):
                plan.verified_file(root, "test.log", digest)

    def evidence_fixture(self, root, task):
        log = root / "tests.log"
        log.write_text("retained unit fixture output; not actual runtime qualification")
        report = root / "report.md"
        report.write_text("test fixture report")
        return {
            "format": 1, "task_id": task["id"], "source_revision": "a" * 40,
            "executable_digest": "b" * 64, "environment": "validator unit fixture",
            "limitations": "synthetic record tests validation only",
            "tests": [{"name": case["name"], "result": "passed"} for case in task["test_cases"]],
            "commands": [{"argv": command, "exit_code": 0, "output": "tests.log",
                          "sha256": plan.sha256(log)} for command in task["verification"]],
            "qualification_report": "report.md", "qualification_sha256": plan.sha256(report),
            "gates": {gate: "passed" for gate in plan.GATES},
        }

    def test_completion_requires_commands_cases_and_retained_outputs(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task = copy.deepcopy(self.manifest["tasks"][0])
            task["evidence"] = ["evidence.json"]
            record = self.evidence_fixture(root, task)
            path = root / "evidence.json"
            path.write_text(json.dumps(record))
            plan.validate_evidence(task, root)
            variants = []
            missing_case = copy.deepcopy(record)
            missing_case["tests"].pop()
            variants.append(missing_case)
            missing_execution = copy.deepcopy(record)
            missing_execution["commands"].pop()
            variants.append(missing_execution)
            failed = copy.deepcopy(record)
            failed["commands"][0]["exit_code"] = 1
            variants.append(failed)
            for variant in variants:
                path.write_text(json.dumps(variant))
                with self.assertRaises(plan.PlanError):
                    plan.validate_evidence(task, root)
            path.write_text(json.dumps(record))
            (root / "tests.log").unlink()
            with self.assertRaisesRegex(plan.PlanError, "Missing/empty artifact"):
                plan.validate_evidence(task, root)

    def test_qualification_cannot_omit_lifecycle_gates_or_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task = copy.deepcopy(self.manifest["tasks"][-1])
            task["evidence"] = ["evidence.json"]
            record = self.evidence_fixture(root, task)
            path = root / "evidence.json"
            path.write_text(json.dumps(record))
            plan.validate_evidence(task, root)
            for gate in ("soak_24h", "vertical_resize", "lifecycle_fencing",
                         "low_load_distribution", "receipt_finalization"):
                with self.subTest(missing_gate=gate):
                    incomplete = copy.deepcopy(record)
                    del incomplete["gates"][gate]
                    path.write_text(json.dumps(incomplete))
                    with self.assertRaisesRegex(plan.PlanError, "Missing qualification gates"):
                        plan.validate_evidence(task, root)
            path.write_text(json.dumps(record))
            (root / "report.md").unlink()
            with self.assertRaisesRegex(plan.PlanError, "Missing/empty artifact"):
                plan.validate_evidence(task, root)


if __name__ == "__main__":
    unittest.main()

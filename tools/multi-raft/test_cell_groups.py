# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Qualification campaigns preserve provenance and report their actual scope."""

import contextlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import cell_groups


def queue_observations():
    return [{"extents": extents, "realRustFS": True,
             "steadyMessages": 512, "archiveOutageMessages": 16,
             "uniqueMessageIds": 528, "delivered": 528,
             "inFlightRequests": 2, "perQueueInFlight": 1,
             "elapsedMs": 1000} for extents in [False, True]]


class SqlCampaignTests(unittest.TestCase):
    def campaign(self, fault=None, suite="ordinary-sql-bindings", keep_going=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            target, cases, _, _ = cell_groups.SUITES[suite]
            executable = root / f"target/debug/deps/{target}-abcdef"
            executable.parent.mkdir(parents=True)
            executable.write_bytes(b"test executable fixture")
            cli = executable.parent.parent / "cellar"
            cli.write_bytes(b"SQL CLI fixture")
            output = root / "evidence"
            executed = []

            def run(argv, **kwargs):
                if argv[0] == "git":
                    return subprocess.CompletedProcess(argv, 0, "")
                self.assertEqual(argv[0], "cargo")
                self.assertEqual(argv[argv.index("--test") + 1], target)
                selection = argv[argv.index("--") + 1]
                env = kwargs["env"]
                if suite in ("ordinary-sql-bindings", "ordinary-kv-bindings", "ordinary-alarms", "ordinary-workflows", "immutable-payloads", "queue-primitives"):
                    self.assertEqual(env["CARGO_PROFILE_TEST_OPT_LEVEL"], "1")
                    self.assertEqual(env["CARGO_PROFILE_TEST_DEBUG_ASSERTIONS"], "true")
                    self.assertEqual(env["CARGO_PROFILE_TEST_OVERFLOW_CHECKS"], "true")
                prefix = f"Running test ({executable})\n"
                if selection == "--list":
                    return subprocess.CompletedProcess(
                        argv, 0, prefix + "\n".join(f"{case}: test" for case in cases)
                    )
                if suite == "queue-primitives":
                    self.assertEqual("--nocapture" in argv,
                                     selection == "rustfs_queue_load_preserves_acknowledged_messages")
                if suite in ("ordinary-kv-bindings", "ordinary-alarms", "ordinary-workflows"):
                    self.assertIn("--show-output", argv)
                    self.assertNotIn("--nocapture", argv)
                if suite in ("ordinary-sql-bindings", "ordinary-kv-bindings", "queue-primitives"):
                    retained = Path(env[{"ordinary-sql-bindings": "CELLAR_CELL_GROUPS_SQL_CLI", "ordinary-kv-bindings": "CELLAR_CELL_GROUPS_KV_CLI", "queue-primitives": "CELLAR_CELL_GROUPS_QUEUE_CLI"}[suite]])
                    self.assertEqual(retained, output / "cellar")
                    self.assertNotEqual(retained, cli)
                    self.assertEqual(retained.read_bytes(), b"SQL CLI fixture")
                executed.append(selection)
                if fault == "tamper":
                    retained.write_bytes(b"changed after capture")
                body = ("test result: ok. 0 passed; 0 failed; 0 ignored;\n" if fault == "zero"
                        else f"test {selection} ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n")
                if selection == "rustfs_queue_load_preserves_acknowledged_messages":
                    body = "".join("QUEUE_LOAD " + json.dumps(value) + "\n"
                                   for value in queue_observations()) + body
                return subprocess.CompletedProcess(argv, 0, prefix + body)

            def git(argv, **kwargs):
                return "a" * 40 + "\n" if "rev-parse" in argv else b""

            remote = {"owner": "cellar-multi-raft-" + "a" * 16,
                      "endpoint": "http://127.0.0.1:1234"}
            credentials = {"RUSTFS_ACCESS_KEY": "access-fixture",
                           "RUSTFS_SECRET_KEY": "secret-fixture"}
            argv = ["cell_groups.py", "--suite", suite,
                    "--backend", str(root / "backend"), "--output", str(output),
                    "--target-directory", str(root / "target")]
            if keep_going:
                argv.append("--keep-going")
            report = io.StringIO()
            with (patch.object(sys, "argv", argv),
                  patch.object(cell_groups, "backend", return_value=(remote, credentials)),
                  patch.object(cell_groups, "sources", return_value=[]),
                  patch.object(cell_groups, "probe", return_value={"fixture": True}),
                  patch.object(cell_groups.subprocess, "check_output", side_effect=git),
                  patch.object(cell_groups.subprocess, "run", side_effect=run),
                  contextlib.redirect_stdout(report)):
                if fault:
                    with self.assertRaises(RuntimeError):
                        cell_groups.main()
                else:
                    cell_groups.main()
            record = json.loads((output / "record.json").read_text())
            if fault:
                self.assertEqual(record["status"], "FAILED")
                self.assertEqual(len(executed), len(cases) if keep_going else 1)
                if keep_going:
                    self.assertEqual(record["failed_cases"], list(cases))
            else:
                self.assertEqual(record["status"], "PASSED")
                self.assertEqual(executed, list(cases))
                summary = json.loads(report.getvalue().splitlines()[-1])
                self.assertEqual(summary["image_modes_per_case"], 0 if suite == "immutable-payloads" else 2)
                if suite in ("ordinary-sql-bindings", "ordinary-kv-bindings", "queue-primitives"):
                    self.assertEqual(record["operator_cli"]["sha256"], cell_groups.sha(output / "cellar"))
                else:
                    self.assertNotIn("operator_cli", record)
                    if suite == "immutable-payloads":
                        self.assertNotIn("three-voter", record["limitations"])
                        self.assertIn("no Raft voters", record["limitations"])
                    elif suite == "ordinary-alarms":
                        self.assertIn("Quorum/S3 namespace minima", record["limitations"])
                        self.assertIn("No operator CLI", record["limitations"])

    def test_sql_uses_the_frozen_cli_for_every_case(self):
        self.campaign()

    def test_sql_refuses_a_changed_frozen_cli(self):
        self.campaign("tamper")

    def test_sql_refuses_success_with_no_selected_test(self):
        self.campaign("zero")

    def test_payload_campaign_does_not_claim_voter_or_cli_qualification(self):
        self.campaign(suite="immutable-payloads")


class AlarmCampaignTests(unittest.TestCase):
    campaign = SqlCampaignTests.campaign

    def test_alarms_capture_exact_cases_without_claiming_cli_qualification(self):
        self.campaign(suite="ordinary-alarms")

    def test_alarms_refuse_success_with_no_selected_test(self):
        self.campaign("zero", "ordinary-alarms")

    def test_alarms_collect_independent_cases_without_waiving_failures(self):
        self.campaign("zero", "ordinary-alarms", keep_going=True)


class QueueCampaignTests(unittest.TestCase):
    campaign = SqlCampaignTests.campaign

    def test_queue_uses_the_frozen_cli_for_every_case(self):
        self.campaign(suite="queue-primitives")

    def test_queue_refuses_a_changed_frozen_cli(self):
        self.campaign("tamper", "queue-primitives")

    def test_queue_refuses_success_with_no_selected_test(self):
        self.campaign("zero", "queue-primitives")

    def test_queue_collects_independent_cases_without_waiving_failures(self):
        self.campaign("zero", "queue-primitives", keep_going=True)

    def test_queue_load_proof_validates_interleaved_observations(self):
        case = "rustfs_queue_load_preserves_acknowledged_messages"
        observations = queue_observations()
        def output(values, summary="ok. 1 passed; 0 failed; 0 ignored;"):
            return (f"test {case} ... "
                    + "".join("QUEUE_LOAD " + json.dumps(value) + "\n" for value in values)
                    + "ok\ntest result: " + summary + "\n")
        self.assertTrue(cell_groups.exact_case_passed(output(observations), case))
        with self.assertRaises(ValueError):
            cell_groups.exact_case_passed(output([]), case)
        self.assertFalse(cell_groups.exact_case_passed(output(observations, "FAILED. 0 passed; 1 failed;"), case))
        with self.assertRaises(ValueError):
            cell_groups.exact_case_passed(output(observations[:1]), case)
        observations[1]["uniqueMessageIds"] = 527
        with self.assertRaises(ValueError):
            cell_groups.exact_case_passed(output(observations), case)


class KvCampaignTests(unittest.TestCase):
    campaign = SqlCampaignTests.campaign

    def test_kv_capture_preserves_diagnostics_and_strict_success(self):
        case = "rustfs_ordinary_kv_bindings_use_managed_shared_groups"
        diagnostics = "public KV Contract node=0: status=200\n"
        summary = "test result: ok. 1 passed; 0 failed; 0 ignored;\n"
        captured = (f"test {case} ... ok\n\nsuccesses:\n\n"
                    f"---- {case} stdout ----\n" + diagnostics + summary)
        self.assertTrue(cell_groups.exact_case_passed(captured, case))
        interleaved = f"test {case} ... " + diagnostics + "ok\n" + summary
        self.assertFalse(cell_groups.exact_case_passed(interleaved, case))
        self.assertFalse(cell_groups.exact_case_passed(
            captured.replace("1 passed;", "0 passed;"), case))

    def test_kv_uses_the_frozen_cli_for_every_case(self):
        self.campaign(suite="ordinary-kv-bindings")

    def test_kv_refuses_a_changed_frozen_cli(self):
        self.campaign("tamper", "ordinary-kv-bindings")

    def test_kv_refuses_success_with_no_selected_test(self):
        self.campaign("zero", "ordinary-kv-bindings")




class WorkflowCampaignTests(unittest.TestCase):
    campaign = SqlCampaignTests.campaign

    def test_workflows_capture_exact_native_and_axum_cases(self):
        self.campaign(suite="ordinary-workflows")

    def test_workflows_refuse_success_with_no_selected_test(self):
        self.campaign("zero", "ordinary-workflows")

    def test_workflows_preserve_each_failed_case(self):
        self.campaign("zero", "ordinary-workflows", keep_going=True)


if __name__ == "__main__":
    unittest.main()

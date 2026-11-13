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


def queue_observations(per_queue_in_flight=1):
    return [{"extents": extents, "realRustFS": True,
             "steadyMessages": 512, "archiveOutageMessages": 16,
             "uniqueMessageIds": 528, "delivered": 528,
             "producers": 4, "queues": 2, "batchMessages": 8,
             "ordinarySlots": 8, "ordinaryBytes": 131072, "callbackSlots": 2,
             "inFlightRequests": 2 * per_queue_in_flight,
             "perQueueInFlight": per_queue_in_flight,
             "peakInFlightRequests": 2 * per_queue_in_flight,
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
                if suite in ("ordinary-sql-bindings", "ordinary-kv-bindings", "immutable-payloads", "queue-primitives"):
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
                                     selection in ("rustfs_queue_load_preserves_acknowledged_messages",
                                                   "rustfs_queue_four_in_flight_preserves_acknowledged_messages"))
                if suite == "ordinary-kv-bindings":
                    self.assertIn("--nocapture", argv)
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
                if selection in ("rustfs_queue_load_preserves_acknowledged_messages",
                                 "rustfs_queue_four_in_flight_preserves_acknowledged_messages"):
                    per_queue = 2 if "four_in_flight" in selection else 1
                    body = "".join("QUEUE_LOAD " + json.dumps(value) + "\n"
                                   for value in queue_observations(per_queue)) + body
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
                    self.assertNotIn("three-voter", record["limitations"])
                    self.assertIn("no Raft voters", record["limitations"])

    def test_sql_uses_the_frozen_cli_for_every_case(self):
        self.campaign()

    def test_sql_refuses_a_changed_frozen_cli(self):
        self.campaign("tamper")

    def test_sql_refuses_success_with_no_selected_test(self):
        self.campaign("zero")

    def test_payload_campaign_does_not_claim_voter_or_cli_qualification(self):
        self.campaign(suite="immutable-payloads")


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

    def test_four_in_flight_control_requires_observed_concurrency_and_original_limits(self):
        case = "rustfs_queue_four_in_flight_preserves_acknowledged_messages"

        def output(values):
            return (f"test {case} ... "
                    + "".join("QUEUE_LOAD " + json.dumps(value) + "\n" for value in values)
                    + "ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n")

        self.assertTrue(cell_groups.exact_case_passed(output(queue_observations(2)), case))
        for key, value in (("peakInFlightRequests", 3), ("inFlightRequests", 2),
                           ("perQueueInFlight", 1), ("ordinarySlots", 9),
                           ("ordinaryBytes", 262144), ("callbackSlots", 4),
                           ("producers", 2), ("queues", 1), ("batchMessages", 4),
                           ("uniqueMessageIds", 527)):
            with self.subTest(key=key):
                observations = queue_observations(2)
                observations[1][key] = value
                with self.assertRaises(ValueError):
                    cell_groups.exact_case_passed(output(observations), case)
        with self.assertRaises(ValueError):
            cell_groups.exact_case_passed(output(queue_observations()), case)
        with self.assertRaises(ValueError):
            cell_groups.exact_case_passed(output(queue_observations(2)[:1]), case)


class KvCampaignTests(unittest.TestCase):
    campaign = SqlCampaignTests.campaign

    def test_kv_uses_the_frozen_cli_for_every_case(self):
        self.campaign(suite="ordinary-kv-bindings")

    def test_kv_refuses_a_changed_frozen_cli(self):
        self.campaign("tamper", "ordinary-kv-bindings")

    def test_kv_refuses_success_with_no_selected_test(self):
        self.campaign("zero", "ordinary-kv-bindings")


if __name__ == "__main__":
    unittest.main()

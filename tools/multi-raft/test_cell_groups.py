# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""The SQL campaign uses its captured CLI and refuses invalid provenance."""

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


class SqlCampaignTests(unittest.TestCase):
    def campaign(self, fault=None, suite="ordinary-sql-bindings"):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            executable = root / "target/debug/deps/native_cell_groups_primitives-abcdef"
            executable.parent.mkdir(parents=True)
            executable.write_bytes(b"test executable fixture")
            cli = executable.parent.parent / "cellar"
            cli.write_bytes(b"SQL CLI fixture")
            output = root / "evidence"
            target, cases, _, _ = cell_groups.SUITES[suite]
            executed = []

            def run(argv, **kwargs):
                if argv[0] == "git":
                    return subprocess.CompletedProcess(argv, 0, "")
                self.assertEqual(argv[0], "cargo")
                self.assertEqual(argv[argv.index("--test") + 1], target)
                selection = argv[argv.index("--") + 1]
                env = kwargs["env"]
                if suite == "ordinary-sql-bindings":
                    self.assertEqual(env["CARGO_PROFILE_TEST_OPT_LEVEL"], "1")
                    self.assertEqual(env["CARGO_PROFILE_TEST_DEBUG_ASSERTIONS"], "true")
                    self.assertEqual(env["CARGO_PROFILE_TEST_OVERFLOW_CHECKS"], "true")
                prefix = f"Running test ({executable})\n"
                if selection == "--list":
                    return subprocess.CompletedProcess(
                        argv, 0, prefix + "\n".join(f"{case}: test" for case in cases)
                    )
                retained = Path(env["CELLAR_CELL_GROUPS_SQL_CLI" if suite == "ordinary-sql-bindings" else "CELLAR_CELL_GROUPS_QUEUE_CLI"])
                self.assertEqual(retained, output / "cellar")
                self.assertNotEqual(retained, cli)
                self.assertEqual(retained.read_bytes(), b"SQL CLI fixture")
                executed.append(selection)
                if fault == "tamper":
                    retained.write_bytes(b"changed after capture")
                body = ("test result: ok. 0 passed; 0 failed; 0 ignored;\n" if fault == "zero"
                        else f"test {selection} ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n")
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
            with (patch.object(sys, "argv", argv),
                  patch.object(cell_groups, "backend", return_value=(remote, credentials)),
                  patch.object(cell_groups, "sources", return_value=[]),
                  patch.object(cell_groups, "probe", return_value={"fixture": True}),
                  patch.object(cell_groups.subprocess, "check_output", side_effect=git),
                  patch.object(cell_groups.subprocess, "run", side_effect=run),
                  contextlib.redirect_stdout(io.StringIO())):
                if fault:
                    with self.assertRaises(RuntimeError):
                        cell_groups.main()
                else:
                    cell_groups.main()
            record = json.loads((output / "record.json").read_text())
            if fault:
                self.assertEqual(record["status"], "FAILED")
                self.assertEqual(len(executed), 1)
            else:
                self.assertEqual(record["status"], "PASSED")
                self.assertEqual(executed, list(cases))
                self.assertEqual(record["operator_cli"]["sha256"], cell_groups.sha(output / "cellar"))

    def test_sql_uses_the_frozen_cli_for_every_case(self):
        self.campaign()

    def test_sql_refuses_a_changed_frozen_cli(self):
        self.campaign("tamper")

    def test_sql_refuses_success_with_no_selected_test(self):
        self.campaign("zero")


class QueueCampaignTests(SqlCampaignTests):
    def campaign(self, fault=None):
        return super().campaign(fault, "queue-primitives")


if __name__ == "__main__":
    unittest.main()

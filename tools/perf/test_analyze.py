"""A refused load or incomplete recovery cannot appear as a passing report."""

import json
import pathlib
import tempfile
import unittest

from analyze import summarize


class AnalysisFailureTests(unittest.TestCase):
    def fixture(self, root, completed):
        metadata = {
            "started_utc": "2026-10-12", "revision": "fixture", "dirty": False,
            "source_sha256": {}, "image": "fixture-image", "rustfs_image": "fixture-store",
            "execution": "sdk::Node", "node_limits": {}, "load_generator_limits": {},
            "rustfs_limits": {}, "docker": {"Architecture": "aarch64", "NCPU": 8, "MemTotal": 1},
            "arguments": {"cells": 1000, "bytes": 1024, "seconds": 20, "repeats": 2, "concurrency": [32, 128]},
        }
        (root / "metadata.json").write_text(json.dumps(metadata))
        (root / "results.json").write_text(json.dumps({
            "completed": completed, "measurements": [], "recovery": [], "shutdown_outcomes": [],
        }))

    def test_failure_saved_before_load_raised_is_not_omitted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            self.fixture(root, True)
            failed = {
                "scenario": "three_node_fleet", "label": "c128-r2", "operation": "write",
                "successful_writes": 855,
                "errors": {"HTTP 503 Service Unavailable: Worker refused: node is at its stateless request limit": 37},
            }
            (root / "three_node_fleet-c128-r2.json").write_text(json.dumps(failed))
            report = summarize(root)
            self.assertIs(report.get("completed"), False)
            self.assertEqual(report.get("failures"), [failed])

    def test_unfinished_recovery_is_incomplete_even_without_load_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            self.fixture(root, False)
            self.assertIs(summarize(root).get("completed"), False)


if __name__ == "__main__":
    unittest.main()

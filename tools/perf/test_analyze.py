# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""A refused load or incomplete recovery cannot appear as a passing report."""

import json
import pathlib
import tempfile
import unittest

from analyze import summarize
from run import routing_cache_checkpoints


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

    def cache_fixture(self, root):
        self.fixture(root, True)
        metadata = json.loads((root / "metadata.json").read_text())
        metadata["arguments"].update(max_cached_routes=32, scenario=["one_node_bucket"])
        (root / "metadata.json").write_text(json.dumps(metadata))
        state = {"cached_routes": 32, "occupied": 16, "owned_cells": 16,
                 "phases": {"resident": 16, "remote": 32}, "residents": list(range(16)),
                 "rss_bytes": 100, "cgroup_working_set_bytes": 100, "cgroup_current_bytes": 200,
                 "node_load": {"shed_cells": 0}, "stops": 100}
        for checkpoint in routing_cache_checkpoints(["one_node_bucket"]):
            (root / checkpoint["file"]).write_text(json.dumps(state))
        (root / "one_node_bucket-measured-node1-disk-cpu.txt").write_text(
            "Filesystem 1B-blocks Used Available Use% Mounted\n/dev/loop0 1000 100 900 10% /data\n100\t/data/state\n200\nlow 0\noom 0\noom_kill 0\n")

    def test_cache_report_checks_all_checkpoints_and_keeps_live_cells_separate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            self.cache_fixture(root)
            report = summarize(root)
            self.assertIs(report["completed"], True)
            self.assertEqual(len(report["routing_cache_snapshots"]), 4)
            self.assertTrue(all(v["cached_routes"] == 32 and v["tracked_cells"] == 48 for v in report["routing_cache_snapshots"]))

    def test_missing_recovery_snapshot_overrides_a_passing_completion_flag(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            self.cache_fixture(root)
            (root / "one_node_bucket-recovered-node1-state.json").unlink()
            report = summarize(root)
            self.assertIs(report["completed"], False)
            self.assertEqual(report["failures"][0]["label"], "recovered-node1")

    def test_missing_or_unbounded_cache_metric_overrides_a_passing_completion_flag(self):
        for count in (None, 33):
            with self.subTest(count=count), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                self.cache_fixture(root)
                snapshot = root / "one_node_bucket-warm-node1-state.json"
                state = json.loads(snapshot.read_text())
                if count is None:
                    del state["cached_routes"]
                else:
                    state["cached_routes"] = count
                snapshot.write_text(json.dumps(state))
                report = summarize(root)
                self.assertIs(report["completed"], False)
                self.assertEqual(report["failures"][0]["label"], "warm-node1")


if __name__ == "__main__":
    unittest.main()

# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""The test runner must never control a foreign or substituted container."""

import copy
import json
import os
from pathlib import Path
import tempfile
import sys
import unittest
from unittest.mock import patch

from backend_control import BackendControl, TRANSITIONS


class BackendOwnershipTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        owner = "cellar-multi-raft-" + "a" * 16
        self.remote = {"owner": owner, "container": owner + "-rustfs",
                       "context": "colima-cellar-multi-raft", "endpoint": "http://127.0.0.1:9123",
                       "image_id": "sha256:" + "b" * 64}
        self.actual = {"id": "c" * 64, "owner": owner, "image": self.remote["image_id"],
                       "running": True, "cpus": 10**9, "memory": 2 * 1024**3,
                       "swap": 2 * 1024**3,
                       "ports": {"9000/tcp": [{"HostIp": "127.0.0.1", "HostPort": "9123"}]},
                       "mounts": [{"Name": owner + "-objects", "Destination": "/data"}]}
        self.actions = []
        self.mock = patch("backend_control.command", self.command)
        self.mock.start()
        self.addCleanup(self.mock.stop)

    def command(self, argv):
        if "inspect" in argv:
            return json.dumps(self.actual)
        action = next(value for value in ("start", "stop") if value in argv)
        self.actions.append((action, argv[-1]))
        self.actual["running"] = action == "start"
        return argv[-1]

    def control(self):
        return BackendControl(self.remote, Path(self.root.name) / "control")

    def test_foreign_owner_cannot_be_stopped(self):
        self.actual["owner"] = "foreign"
        with self.assertRaises(ValueError):
            self.control()
        self.assertEqual(self.actions, [])

    def test_substituted_container_cannot_be_stopped(self):
        control = self.control()
        self.actual["id"] = "d" * 64
        with self.assertRaises(ValueError):
            control.transition({"format": 1, "sequence": 0, "mode": "materialized", "action": "stop"})
        self.assertEqual(self.actions, [])

    def test_unexpected_operations_never_reach_docker(self):
        control = self.control()
        valid = {"format": 1, "sequence": 0, "mode": "materialized", "action": "stop"}
        for changes in [{"action": "rm"}, {"sequence": 2}, {"sequence": False},
                        {"container": "foreign"}, {"mode": "extents"}]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                control.transition({**valid, **changes})
        self.assertEqual(self.actions, [])

    def test_fixed_transitions_preserve_identity_resources_and_mounts(self):
        control = self.control()
        identity = copy.deepcopy(self.actual)
        with patch.object(control, "healthy"):
            for sequence, (mode, action) in enumerate(TRANSITIONS):
                control.transition({"format": 1, "sequence": sequence, "mode": mode, "action": action})
        self.assertEqual(self.actions, [(action, identity["id"]) for _, action in TRANSITIONS])
        self.assertEqual(self.actual, identity)
        self.assertEqual([event["after"]["running"] for event in control.events],
                         [False, True, False, True])
        changed = copy.deepcopy(self.actual)
        self.actual["mounts"] = []
        with self.assertRaises(ValueError):
            control.verify(True)
        self.actual = changed

    def test_failed_child_keeps_output_and_running_backend(self):
        control = self.control()
        result = control.run(
            [sys.executable, "-c", "print('failure-provenance'); raise SystemExit(9)"],
            Path(self.root.name), os.environ,
        )
        self.assertEqual(result.returncode, 9)
        self.assertIn("failure-provenance", result.stdout)
        record = json.loads((control.directory / "record.json").read_text())
        self.assertEqual(record["status"], "FAILED")
        self.assertEqual(record["cleanup"], [{"already_running": True}])
        self.assertEqual(self.actions, [])


if __name__ == "__main__":
    unittest.main()

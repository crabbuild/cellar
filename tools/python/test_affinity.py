# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""CPU placement validation and real exec/thread inheritance on Linux."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import affinity


class PlacementTests(unittest.TestCase):
    def test_default_keeps_platform_behavior(self):
        with patch.object(affinity, "available", side_effect=AssertionError("must not query affinity")):
            self.assertIsNone(affinity.placement(None, None))
            self.assertEqual(affinity.prefix(None, Path("unused")), [])

    def test_invalid_and_overlapping_masks_refuse(self):
        with patch.object(affinity, "available", return_value=[2, 4, 6, 8]):
            for host, client in [(None, "2"), ("2", None), ("2", "2,4"), ("0", "4"),
                                 ("2,2", "4"), ("2-4", "6"), ("", "4"), ("-2", "4"),
                                 ("２", "4"), ("2,", "4")]:
                with self.subTest(host=host, client=client), self.assertRaises(ValueError):
                    affinity.placement(host, client)
            self.assertEqual(affinity.placement("6,2", "8,4"),
                             dict(host=[2, 6], client=[4, 8], available=[2, 4, 6, 8]))

    def test_non_linux_refuses_explicit_placement(self):
        with patch.object(affinity.sys, "platform", "darwin"), self.assertRaises(ValueError):
            affinity.placement("0", "1")

    def test_fleet_requires_three_available_disjoint_pools(self):
        self.assertIsNone(affinity.fleet_placement(None, None, None))
        with patch.object(affinity, "available", return_value=[0, 1, 2, 3]):
            for host, client, store in [("1,2", "0", None), (None, None, "3"),
                                        ("1,2", "0", "2"), ("1,2", "0", "0"),
                                        ("1,2", "0", "4"), ("1,2", "0", "3,3")]:
                with self.subTest(host=host, client=client, store=store), self.assertRaises(ValueError):
                    affinity.fleet_placement(host, client, store)
            self.assertEqual(affinity.fleet_placement("2,1", "0", "3"),
                             dict(host=[1, 2], client=[0], store=[3], available=[0, 1, 2, 3]))

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux kernel affinity required")
    def test_live_thread_observation_refuses_changed_masks(self):
        import os
        requested = affinity.available()
        proof = affinity.verify_threads(os.getpid(), requested)
        self.assertIn(str(os.getpid()), proof)
        with self.assertRaises(RuntimeError):
            affinity.verify_threads(os.getpid(), [max(requested) + 1])

    def test_changed_proof_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = Path(root) / "proof.json"
            evidence.write_text(json.dumps(dict(requested=[1], observed=[1], available_before=[0, 1], pid=42)))
            self.assertEqual(affinity.verify(evidence, [1], 42)["observed"], [1])
            with self.assertRaises(RuntimeError):
                affinity.verify(evidence, [0], 42)
            with self.assertRaises(RuntimeError):
                affinity.verify(evidence, [1], 43)

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux kernel affinity required")
    def test_exec_and_new_thread_inherit_exact_mask(self):
        parent = affinity.available()
        requested = parent[:1]
        with tempfile.TemporaryDirectory() as root:
            evidence = Path(root) / "proof.json"
            program = """import json, os, threading
result = dict(pid=os.getpid(), main=sorted(os.sched_getaffinity(0)))
def check():
    result['thread'] = sorted(os.sched_getaffinity(0))
thread = threading.Thread(target=check)
thread.start()
thread.join()
print(json.dumps(result))
"""
            child = subprocess.run([*affinity.prefix(requested, evidence), sys.executable, "-I", "-c", program],
                                   capture_output=True, text=True, check=True, timeout=10)
            actual = json.loads(child.stdout)
            affinity.verify(evidence, requested, actual["pid"])
            self.assertEqual(actual["main"], requested)
            self.assertEqual(actual["thread"], requested)
            self.assertEqual(affinity.available(), parent, "launcher changed the parent affinity")

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux kernel affinity required")
    def test_unavailable_cpu_never_executes_target(self):
        with tempfile.TemporaryDirectory() as root:
            evidence = Path(root) / "proof.json"
            child = subprocess.run([*affinity.prefix([max(affinity.available()) + 1], evidence),
                                    sys.executable, "-c", "print('executed')"],
                                   capture_output=True, text=True, timeout=10)
            self.assertNotEqual(child.returncode, 0)
            self.assertNotIn("executed", child.stdout)
            self.assertFalse(evidence.exists())


if __name__ == "__main__":
    unittest.main()

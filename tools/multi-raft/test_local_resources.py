# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Explicit small-voter profiles retain a bounded VM and object-store reserve."""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError

from local import GIB, RUSTFS_IMAGE, prepare, required_resources


class LocalResourceTests(unittest.TestCase):
    def test_three_two_gib_voters_keep_the_store_and_observer_reserve(self):
        self.assertEqual(required_resources(3, 2), (5, 8 * GIB + GIB // 2))
        self.assertEqual(required_resources(3), (5, 11 * GIB + GIB // 2))

    def test_refuses_unqualified_node_and_memory_profiles(self):
        for nodes, memory in [(2, 2), (3, 1), (3, 4), (3, True), (True, 2)]:
            with self.assertRaises(ValueError):
                required_resources(nodes, memory)

    def test_prepare_waits_for_storage_readiness_before_marking_backend_ready(self):
        endpoint = "http://127.0.0.1:9001"
        ready = MagicMock()
        ready.__enter__.return_value.status = 200
        attempted = []
        with TemporaryDirectory() as directory:
            output = Path(directory) / "rustfs"
            record = output / "environment.json"

            def open_health(url, *, timeout):
                self.assertEqual(json.loads(record.read_text())["phase"], "preparing-backend")
                attempted.append(url)
                if url.endswith("/health/ready") and len(attempted) == 1:
                    raise HTTPError(url, 503, "storage initializing", {}, None)
                return ready

            args = SimpleNamespace(profile="cellar-multi-raft", nodes=3, node_memory_gib=2,
                                   rustfs_image=RUSTFS_IMAGE, output=output)
            docker_results = ["", "", "", "", '[{"HostIp":"127.0.0.1","HostPort":"9001"}]',
                              "sha256:test", json.dumps({"NanoCpus": 10**9, "Memory": 2 * GIB, "MemorySwap": 2 * GIB})]
            with (patch("local.preflight", return_value=(["docker"], {})),
                  patch("local.command", side_effect=docker_results),
                  patch("urllib.request.urlopen", side_effect=open_health),
                  patch("time.sleep"), patch("builtins.print")):
                prepare(args)
            self.assertEqual(attempted, [endpoint + "/health/ready"] * 2)
            self.assertEqual(json.loads(record.read_text())["phase"], "backend-ready")


if __name__ == '__main__':
    unittest.main()

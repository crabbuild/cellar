# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""CLI-boundary regressions for recording the real qualifier build input."""

import json
import subprocess
import unittest
from unittest.mock import patch

import qualify


class RustInputTests(unittest.TestCase):
    docker = ["docker", "--context", "owned-qualifier"]
    requested = "rust:1.98.1-bookworm"
    image_id = "sha256:" + "a" * 64
    digest = "rust@sha256:" + "b" * 64
    local_source = "cellar-egress-rust-input:owned-campaign"

    def resolve(self, requested=None):
        return qualify.resolve_rust_image(
            self.docker, requested or self.requested, self.local_source,
        )

    def descriptor(self, digests=None):
        return json.dumps({
            "id": self.image_id, "digests": [self.digest] if digests is None else digests,
            "os": "linux", "arch": "amd64",
        })

    @patch.object(qualify, "run")
    def test_fresh_daemon_loads_missing_base_before_pinning_its_identity(self, run):
        missing = subprocess.CalledProcessError(
            1, ["docker", "image", "inspect"], stderr="Error response from daemon: No such image"
        )
        run.side_effect = [missing, "downloaded", self.descriptor()]
        result = self.resolve()
        self.assertEqual(result["id"], self.image_id)
        self.assertEqual(result["source"], self.local_source)
        self.assertEqual(result["digests"], [self.digest])
        self.assertEqual(result["platform"], "linux/amd64")
        self.assertEqual(run.call_count, 3)
        self.assertEqual(run.call_args_list[0].args[0], run.call_args_list[2].args[0])
        self.assertEqual(run.call_args_list[1].args[0], [*self.docker, "pull", self.requested])

    @patch.object(qualify, "run")
    def test_loaded_image_selects_owned_alias_and_records_provenance(self, run):
        run.return_value = self.descriptor()
        result = self.resolve()
        self.assertEqual(result["requested"], self.requested)
        self.assertEqual(result["source"], self.local_source)
        self.assertEqual(result["digests"], [self.digest])
        self.assertNotEqual(result["source"], result["requested"])
        run.assert_called_once()

    @patch.object(qualify, "run")
    def test_local_only_image_selects_owned_alias_for_its_config_identity(self, run):
        run.return_value = self.descriptor(digests=[])
        result = self.resolve("local-rust:qualification")
        self.assertEqual(result["source"], self.local_source)
        self.assertEqual(result["id"], self.image_id)
        run.assert_called_once()

    @patch.object(qualify, "run")
    def test_daemon_failure_is_not_reclassified_as_missing_image(self, run):
        run.side_effect = subprocess.CalledProcessError(
            1, ["docker", "image", "inspect"], stderr="Cannot connect to the Docker daemon"
        )
        with self.assertRaises(subprocess.CalledProcessError):
            self.resolve()
        run.assert_called_once()

    @patch.object(qualify, "run")
    def test_malformed_registry_provenance_refuses_before_build(self, run):
        run.return_value = self.descriptor(digests=["rust:tag\nRUN unexpected"])
        with self.assertRaisesRegex(RuntimeError, "invalid registry provenance"):
            self.resolve()
        run.assert_called_once()

    @patch.object(qualify, "run")
    def test_cleanup_accepts_absent_owned_alias_after_a_failed_tag(self, run):
        run.side_effect = subprocess.CalledProcessError(
            1, ["docker", "image", "rm"], stderr="Error response from daemon: No such image"
        )
        qualify.remove_owned_image_tag(self.docker, self.local_source)
        run.assert_called_once_with([*self.docker, "image", "rm", self.local_source])

    @patch.object(qualify, "run")
    def test_cleanup_propagates_daemon_failures(self, run):
        run.side_effect = subprocess.CalledProcessError(
            1, ["docker", "image", "rm"], stderr="Cannot connect to the Docker daemon"
        )
        with self.assertRaises(subprocess.CalledProcessError):
            qualify.remove_owned_image_tag(self.docker, self.local_source)


if __name__ == "__main__":
    unittest.main()

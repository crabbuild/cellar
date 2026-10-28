# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""CLI-boundary regressions for recording the real qualifier build input."""

import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
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


class ProductionCleanupTests(unittest.TestCase):
    docker = ["docker", "--context", "owned-qualifier"]
    owner = "123456abcdef"
    namespace = "a" * 64

    @patch.object(qualify, "run")
    def test_foreign_published_owner_refuses_before_any_daemon_call(self, run):
        output = 'CELLAR_EGRESS_PRODUCTION_OWNER=' + json.dumps({
            "node": "another-node", "host": self.namespace,
        })
        with self.assertRaisesRegex(RuntimeError, "invalid cleanup ownership"):
            qualify.cleanup_production(self.docker, "image", self.owner, output)
        run.assert_not_called()

    @patch.object(qualify, "cleanup_namespace")
    def test_owner_marker_survives_the_native_test_harness_line_prefix(self, cleanup):
        output = 'test production_profile ... CELLAR_EGRESS_PRODUCTION_OWNER=' + json.dumps({
            "node": f"network-{self.owner}", "host": self.namespace,
        })
        qualify.cleanup_production(self.docker, "image", self.owner, output)
        cleanup.assert_called_once_with(self.docker, "image", self.owner, self.namespace)

    @patch.object(qualify, "subprocess")
    @patch.object(qualify, "run")
    def test_application_removal_precedes_helper_and_uses_full_owned_ids(self, run, process):
        app, helper, network = "b" * 64, "c" * 64, "d" * 64
        labels = {"cellar.host": self.namespace, "cellar.node": f"network-{self.owner}"}
        def inspect(resource, role):
            return json.dumps({"id": resource, "labels": {**labels, "cellar.network.role": role}})
        run.side_effect = [
            helper + '\n' + app, inspect(helper, "helper"), inspect(app, "application"),
            "", "", network, json.dumps({"id": network, "labels": labels}), "",
        ]
        qualify.cleanup_namespace(self.docker, "image", self.owner, self.namespace)
        removals = [call.args[0] for call in run.call_args_list if "rm" in call.args[0]]
        self.assertEqual(removals, [
            [*self.docker, "rm", "-f", app], [*self.docker, "rm", "-f", helper],
            [*self.docker, "network", "rm", network],
        ])
        self.assertEqual(process.run.call_args.kwargs["input"],
            f"table inet cellar_{self.namespace[:24]}\ndelete table inet cellar_{self.namespace[:24]}\n")

    @patch.object(qualify, "subprocess")
    @patch.object(qualify, "run")
    def test_foreign_daemon_labels_refuse_before_removal(self, run, process):
        run.side_effect = ["b" * 64, json.dumps({"id": "b" * 64, "labels": {}})]
        with self.assertRaisesRegex(RuntimeError, "foreign container"):
            qualify.cleanup_namespace(self.docker, "image", self.owner, self.namespace)
        self.assertEqual(run.call_count, 2)
        process.run.assert_not_called()

    @patch.object(qualify, "cleanup_production")
    @patch.object(qualify.subprocess, "run")
    @patch.object(qualify, "run")
    def test_timeout_preserves_output_and_original_failure_during_recovery(self, run, process, cleanup):
        run.return_value = "unix:///owned.sock"
        output = b"fixed native failure evidence\n"
        process.side_effect = subprocess.TimeoutExpired(["native-test"], 90, output=output)
        cleanup.side_effect = RuntimeError("fixed recovery failure")
        with tempfile.TemporaryDirectory() as directory:
            evidence = Path(directory)
            with self.assertRaises(subprocess.TimeoutExpired) as caught:
                qualify.qualify_engine("native-test", self.docker, "image", "network", self.owner,
                    SimpleNamespace(context="owned", node_image="node-image"), evidence)
            self.assertEqual((evidence / "engine.log").read_bytes(), output)
        cleanup.assert_called_once_with(self.docker, "image", self.owner, output.decode())
        self.assertIn("Owned production cleanup failed", caught.exception.__notes__[0])

    @patch.object(qualify, "cleanup_namespace")
    def test_one_cleanup_failure_does_not_skip_other_published_namespaces(self, cleanup):
        other = "b" * 64
        output = '\n'.join('CELLAR_EGRESS_PRODUCTION_OWNER=' + json.dumps({
            "node": f"network-{self.owner}", "host": host,
        }) for host in [self.namespace, other])
        cleanup.side_effect = [RuntimeError("first namespace unavailable"), None]
        with self.assertRaisesRegex(RuntimeError, "first namespace unavailable"):
            qualify.cleanup_production(self.docker, "image", self.owner, output)
        self.assertEqual(cleanup.call_count, 2)


if __name__ == "__main__":
    unittest.main()

# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Qualification campaigns preserve provenance and report their actual scope."""

import contextlib
import io
import json
from pathlib import Path
import subprocess
import socket
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
                if suite in ("native-docker", "controller-provider", "provider-journals", "binding-ack-recovery", "binding-settlement-output-loss", "binding-settlement", "binding-settlement-diagnostic", "live-binding-recovery", "ordinary-sql-bindings", "ordinary-kv-bindings", "ordinary-alarms", "ordinary-workflows", "workflow-lifecycle", "workflow-deletion", "queue-transfer-identity", "ordinary-event-stream", "ordinary-filesystem", "ordinary-blob", "external-payload-recovery", "filesystem-disaster", "immutable-payloads", "queue-primitives"):
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
                if suite in ("native-docker", "controller-provider", "ordinary-filesystem", "named-bindings", "workflow-deletion", "provider-journals", "binding-ack-recovery", "binding-settlement-output-loss", "binding-settlement", "binding-settlement-diagnostic", "live-binding-recovery", "ordinary-kv-bindings", "ordinary-alarms", "ordinary-workflows", "workflow-lifecycle"):
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
                if selection in ("rustfs_queue_load_preserves_acknowledged_messages",
                                 "rustfs_queue_four_in_flight_preserves_acknowledged_messages"):
                    per_queue = 2 if "four_in_flight" in selection else 1
                    body = "".join("QUEUE_LOAD " + json.dumps(value) + "\n"
                                   for value in queue_observations(per_queue)) + body
                if selection in cell_groups.NAMED_BINDING_CASES and fault != "zero":
                    recovery = str(cell_groups.NAMED_BINDING_CASES[selection]).lower()
                    body = "".join(
                        f"named binding configuration recovery={recovery} extents={str(mode).lower()} source={policy}: PASS\n"
                        for mode in (False, True) for policy in ("Quorum", "S3")
                    ) + body
                return subprocess.CompletedProcess(argv, 0, prefix + body)

            def git(argv, **kwargs):
                return "a" * 40 + "\n" if "rev-parse" in argv else b""

            remote = {"owner": "cellar-multi-raft-" + "a" * 16,
                      "profile": "cellar-multi-raft-fixture",
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
                  patch.object(cell_groups, "docker_configuration", return_value={
                      "context": "colima-cellar-multi-raft-fixture",
                      "socket": "/owned/docker.sock", "image": "sha256:" + "b" * 64}),
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
                self.assertEqual(summary["image_modes_per_case"],
                                 None if suite == "native-docker" else
                                 0 if suite == "immutable-payloads" else
                                 1 if suite == "binding-settlement-diagnostic" else 2)
                if suite == "native-docker":
                    self.assertEqual(summary["rustfs_cases"], 6)
                    self.assertEqual(record["planned_configurations"], {
                        "docker_backend_joins_processes_and_retains_original_receipts": 1,
                        "docker_backend_shutdown_fences_processes_when_journal_fails": 1,
                        "rustfs_native_docker_executes_and_recovers_shared_quorum": 4,
                        "rustfs_native_docker_refuses_execution_without_source_proof": 4,
                        "rustfs_native_sandboxes_execute_and_cleanup_through_shared_quorum": 4,
                        "rustfs_native_sandboxes_refuse_unproved_parent_dispatch": 4,
                        "rustfs_native_sandbox_retirement_preserves_requested_s3_minimum": 2,
                        "rustfs_native_sandbox_deletion_preserves_input_proof": 2,
                    })
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


class LiveBindingRecoveryCampaignTests(unittest.TestCase):
    campaign = SqlCampaignTests.campaign

    def test_recovery_captures_both_image_cases_without_claiming_cli(self):
        self.assertEqual(cell_groups.SUITES["live-binding-recovery"][1], (
            "rustfs_live_materialized_binding_recovery_and_unused_sequence",
            "rustfs_live_extent_binding_recovery_and_unused_sequence",
        ))
        self.campaign(suite="live-binding-recovery")

    def test_recovery_refuses_success_with_no_selected_test(self):
        self.campaign("zero", "live-binding-recovery")

    def test_recovery_preserves_each_failed_case(self):
        self.campaign("zero", "live-binding-recovery", keep_going=True)


class EventStreamCampaignTests(unittest.TestCase):
    campaign = SqlCampaignTests.campaign

    def test_events_capture_native_and_axum_cases(self):
        self.campaign(suite="ordinary-event-stream")

    def test_events_refuse_success_with_no_selected_test(self):
        self.campaign("zero", "ordinary-event-stream")

    def test_events_collect_independent_cases_without_waiving_failures(self):
        self.campaign("zero", "ordinary-event-stream", keep_going=True)


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


class WorkflowLifecycleCampaignTests(unittest.TestCase):
    campaign = SqlCampaignTests.campaign

    def test_lifecycle_captures_native_and_axum_cases(self):
        for suite in ("workflow-lifecycle", "workflow-deletion", "queue-transfer-identity", "binding-settlement", "binding-settlement-diagnostic", "binding-settlement-output-loss", "binding-ack-recovery"):
            with self.subTest(suite=suite):
                self.campaign(suite=suite)

    def test_lifecycle_refuses_zero_selection(self):
        for suite in ("workflow-lifecycle", "workflow-deletion", "queue-transfer-identity", "binding-settlement", "binding-settlement-diagnostic", "binding-settlement-output-loss", "binding-ack-recovery"):
            with self.subTest(suite=suite):
                self.campaign("zero", suite)

    def test_lifecycle_preserves_each_failed_case(self):
        for suite in ("workflow-lifecycle", "workflow-deletion", "queue-transfer-identity", "binding-settlement", "binding-settlement-diagnostic", "binding-settlement-output-loss", "binding-ack-recovery"):
            with self.subTest(suite=suite):
                self.campaign("zero", suite, keep_going=True)


class ProviderJournalCampaignTests(unittest.TestCase):
    campaign = SqlCampaignTests.campaign

    def test_provider_suite_selects_the_fixed_public_entrypoints(self):
        self.assertEqual(cell_groups.SUITES["provider-journals"][1], (
            "rustfs_provider_binding_journals_use_shared_quorum",
            "rustfs_provider_effects_reuse_only_unchanged_invocation_checkpoints",
            "rustfs_provider_supervision_recovers_without_application_requests",
        ))
        self.campaign(suite="provider-journals")

    def test_provider_suite_refuses_zero_selection(self):
        self.campaign("zero", "provider-journals")

    def test_provider_suite_preserves_every_failure(self):
        self.campaign("zero", "provider-journals", keep_going=True)

    def test_native_docker_selects_fixed_physical_and_public_cases(self):
        target, cases, _, _ = cell_groups.SUITES["native-docker"]
        self.assertEqual(target, "native_cell_groups_docker")
        self.assertEqual(cases, (
            "docker_backend_joins_processes_and_retains_original_receipts",
            "docker_backend_shutdown_fences_processes_when_journal_fails",
            "rustfs_native_docker_executes_and_recovers_shared_quorum",
            "rustfs_native_docker_refuses_execution_without_source_proof",
            "rustfs_native_sandboxes_execute_and_cleanup_through_shared_quorum",
            "rustfs_native_sandboxes_refuse_unproved_parent_dispatch",
            "rustfs_native_sandbox_retirement_preserves_requested_s3_minimum",
            "rustfs_native_sandbox_deletion_preserves_input_proof",
        ))
        self.campaign(suite="native-docker")

    def test_native_docker_refuses_zero_selection(self):
        self.campaign("zero", suite="native-docker")

    def test_native_docker_preserves_all_failures(self):
        self.campaign("zero", suite="native-docker", keep_going=True)

    def test_controller_provider_selects_the_fixed_public_entrypoints(self):
        self.assertEqual(cell_groups.SUITES["controller-provider"][1], (
            "rustfs_controller_provider_sessions_preserve_shared_group_durability",
            "rustfs_controller_provider_recovers_without_replaying_compute",
            "rustfs_controller_provider_expiry_seals_unsubmitted_creates",
            "rustfs_controller_provider_recovers_live_write_without_replay",
            "rustfs_controller_provider_reconciles_live_exec_before_destroy",
            "rustfs_controller_retirement_confirmation_survives_lost_s3_proof",
            "rustfs_controller_retirement_confirmation_waits_for_independent_fence",
            "rustfs_controller_routine_inspection_yields_after_fencing_to_foreground",
            "rustfs_controller_routine_inspection_yields_after_overlapping_s3_checkpoint",
        ))
        self.campaign(suite="controller-provider")

    def test_controller_provider_refuses_zero_selection(self):
        self.campaign("zero", "controller-provider")

    def test_controller_provider_preserves_every_failure(self):
        self.campaign("zero", "controller-provider", keep_going=True)


class PayloadLifecycleCampaignTests(unittest.TestCase):
    campaign = SqlCampaignTests.campaign

    def test_payload_and_filesystem_campaigns_capture_exact_cases(self):
        for suite in ("ordinary-filesystem", "ordinary-blob", "external-payload-recovery", "filesystem-disaster"):
            with self.subTest(suite=suite):
                self.campaign(suite=suite)

    def test_payload_and_filesystem_campaigns_refuse_empty_success(self):
        for suite in ("ordinary-filesystem", "ordinary-blob", "external-payload-recovery", "filesystem-disaster"):
            with self.subTest(suite=suite):
                self.campaign("zero", suite=suite)


class NativeDockerConfigurationTests(unittest.TestCase):
    def configuration(self, *, mismatch=False, unpinned=False, changed_image=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            selected = root / "selected.sock"
            other = root / "other.sock"
            with socket.socket(socket.AF_UNIX) as first, socket.socket(socket.AF_UNIX) as second:
                first.bind(str(selected))
                second.bind(str(other))
                image = "sha256:" + "b" * 64
                environment = {"CELLAR_DOCKER_BACKEND_SOCKET": str(other if mismatch else selected),
                               "CELLAR_DOCKER_BACKEND_IMAGE": "busybox:latest" if unpinned else image}

                def command(argv):
                    if argv[1] == "context":
                        self.assertEqual(argv[-1], "colima-cellar-multi-raft-fixture")
                        return json.dumps([{"Endpoints": {"docker": {"Host": "unix://" + str(selected)}}}])
                    self.assertEqual(argv[1:3], ["--context", "colima-cellar-multi-raft-fixture"])
                    return "sha256:" + "c" * 64 if changed_image else image

                with patch.object(cell_groups, "command", side_effect=command):
                    return cell_groups.docker_configuration("cellar-multi-raft-fixture", environment)

    def test_accepts_the_selected_owned_socket_and_preinstalled_digest(self):
        selected = self.configuration()
        self.assertEqual(selected["image"], "sha256:" + "b" * 64)

    def test_refuses_another_engine_before_physical_entry(self):
        with self.assertRaisesRegex(RuntimeError, "differs from the owned"):
            self.configuration(mismatch=True)

    def test_refuses_mutable_image_tags(self):
        with self.assertRaisesRegex(RuntimeError, "immutable"):
            self.configuration(unpinned=True)

    def test_refuses_a_different_preinstalled_image(self):
        with self.assertRaisesRegex(RuntimeError, "pinned"):
            self.configuration(changed_image=True)

    def test_refuses_missing_explicit_configuration(self):
        with self.assertRaisesRegex(RuntimeError, "explicit"):
            cell_groups.docker_configuration("cellar-multi-raft-fixture", {})


if __name__ == "__main__":
    unittest.main()

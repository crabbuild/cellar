# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Cold recovery is complete only after local verified reads and durable writes."""
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
from types import SimpleNamespace
import sys
import tempfile
import time
import unittest

from analyze_recovery import analyze, PHASES, CHECKS
from recovery import PROFILES, binary_for, run_process, selected_results, select_test, sha256, worker_environment


def campaign(count=3, *, elapsed_us=80_000_000):
    trial = {
        'format': 1, 'backend': 'peer', 'storage': 'materialized',
        'image_bytes': 9_999_998_976, 'failure_mode': 'joined_domain_loss',
        'events': [{'name': name, 'elapsed_us': index * 1_000_000}
                   for index, name in enumerate(PHASES)],
        'checks': dict.fromkeys(CHECKS, True), 'complete': True,
    }
    for event in trial['events']:
        if event['name'] == 'first_write_verified':
            event['elapsed_us'] = elapsed_us
        elif event['name'] in ('receipt_verified', 'payloads_verified', 'cleanup_joined'):
            event['elapsed_us'] = elapsed_us + PHASES.index(event['name']) * 1_000_000
    profile = {'image_bytes': 9_999_998_976, 'logical_quota_bytes': 10_000_000_000,
               'backends': ['peer'], 'failure_mode': 'joined_domain_loss'}
    profile_sha256 = hashlib.sha256(json.dumps(profile, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return {
        'format': 1, 'backend': 'peer', 'storage': 'materialized',
        'image_bytes': 9_999_998_976, 'requested_repetitions': count,
        'source': {'commit': 'a' * 40, 'inputs_sha256': 'b' * 64, 'binary_sha256': 'c' * 64, 'build_profile': 'release'},
        'profile': profile, 'profile_sha256': profile_sha256, 'reference_profile_verified': False,
        'fault_suite': {'complete': False},
        'trials': [{'returncode': 0, 'selected_tests': 1, 'passed_tests': 1,
                    'source_unchanged': True, 'evidence': copy.deepcopy(trial)} for _ in range(count)],
    }


class RecoveryAnalysis(unittest.TestCase):
    def test_pilot_reports_exact_read_and_write_duration_without_qualification(self):
        report = analyze(campaign(), mode='pilot', target_seconds=120, minimum_samples=3)
        self.assertFalse(report['qualified'])
        self.assertTrue(report['target_met'])
        self.assertEqual(report['successful_trials'], 3)
        self.assertEqual(report['cold_failure_to_ready_seconds']['p95'], 80)

    def test_write_time_cannot_be_replaced_by_the_first_read(self):
        report = analyze(campaign(elapsed_us=125_000_000), mode='pilot', target_seconds=120, minimum_samples=3)
        self.assertFalse(report['target_met'])
        self.assertEqual(report['target_misses'], 3)

    def test_failures_stay_in_the_percentile_denominator(self):
        raw = campaign(count=20)
        raw['trials'][0]['returncode'] = 101
        raw['trials'][1]['returncode'] = -9
        raw['trials'][1]['evidence'] = None
        report = analyze(raw, mode='pilot', target_seconds=120, minimum_samples=3)
        self.assertEqual(report['requested_trials'], 20)
        self.assertEqual(report['successful_trials'], 18)
        self.assertEqual(report['target_misses'], 2)
        self.assertIsNone(report['cold_failure_to_ready_seconds']['p95'])
        self.assertFalse(report['target_met'])

    def test_incomplete_or_wrong_evidence_fails_closed(self):
        mutations = {
            'zero-selected': lambda r: r['trials'][0].update(selected_tests=0),
            'controller-timeout': lambda r: r['trials'][0].update(timed_out=True),
            'no-pass': lambda r: r['trials'][0].update(passed_tests=0),
            'source-drift': lambda r: r['trials'][0].update(source_unchanged=False),
            'missing-phase': lambda r: r['trials'][0]['evidence']['events'].pop(),
            'duplicate-phase': lambda r: r['trials'][0]['evidence']['events'].append(r['trials'][0]['evidence']['events'][0]),
            'clock-regression': lambda r: r['trials'][0]['evidence']['events'][2].update(elapsed_us=0),
            'bool-clock': lambda r: r['trials'][0]['evidence']['events'][0].update(elapsed_us=False),
            'missing-receipt': lambda r: r['trials'][0]['evidence']['checks'].update(receipt_verified=False),
            'wrong-backend': lambda r: r['trials'][0]['evidence'].update(backend='rustfs'),
            'tiny-fixture': lambda r: r['trials'][0]['evidence'].update(image_bytes=12 * 1024 * 1024),
            'partial': lambda r: r['trials'][0]['evidence'].update(complete=False),
        }
        for name, change in mutations.items():
            with self.subTest(name=name):
                raw = campaign()
                change(raw)
                report = analyze(raw, mode='pilot', target_seconds=120, minimum_samples=3)
                self.assertFalse(report['target_met'])
                self.assertEqual(report['successful_trials'], 2)
                self.assertEqual(report['target_misses'], 1)
                self.assertTrue(report['failures'])

    def test_missing_trials_and_invalid_manifests_are_errors(self):
        for change in [lambda r: r['trials'].pop(), lambda r: r.update(requested_repetitions=0),
                       lambda r: r['source'].update(binary_sha256='invalid'),
                       lambda r: r['source'].update(build_profile='debug'),
                       lambda r: r.update(profile_sha256=''), lambda r: r['profile'].update(logical_quota_bytes=1),
                       lambda r: r.update(storage='unknown')]:
            raw = campaign()
            change(raw)
            with self.assertRaises(ValueError):
                analyze(raw, mode='pilot', target_seconds=120, minimum_samples=3)

    def test_small_pilot_or_joined_shutdown_cannot_qualify_hard_crash_rto(self):
        for count in (3, 100):
            with self.subTest(count=count):
                report = analyze(campaign(count), mode='qualify', target_seconds=120, minimum_samples=100)
                self.assertFalse(report['qualified'])
                self.assertTrue(report['qualification_blockers'])

    def test_complete_extent_recovery_keeps_the_same_read_write_and_failure_oracle(self):
        raw = campaign()
        raw['storage'] = 'extents'
        for trial in raw['trials']:
            trial['evidence']['storage'] = 'extents'
        report = analyze(raw, mode='pilot', target_seconds=120, minimum_samples=3)
        self.assertTrue(report['target_met'])
        self.assertFalse(report['qualified'])
        raw['trials'][0]['evidence']['storage'] = 'materialized'
        report = analyze(raw, mode='pilot', target_seconds=120, minimum_samples=3)
        self.assertEqual(report['successful_trials'], 2)
        self.assertFalse(report['target_met'])

    def test_extent_profile_still_requires_complete_local_hydration(self):
        raw = campaign()
        raw['storage'] = 'extents'
        for trial in raw['trials']:
            trial['evidence']['storage'] = 'extents'
            for event in trial['evidence']['events']:
                if event['name'] == 'hydrated':
                    event['elapsed_us'] = 0
        report = analyze(raw, mode='pilot', target_seconds=120, minimum_samples=3)
        self.assertEqual(report['successful_trials'], 0)
        self.assertFalse(report['qualified'])

    def test_qualification_requires_both_hardware_and_matching_hard_failure_evidence(self):
        raw = campaign(100)
        raw['reference_profile_verified'] = True
        raw['fault_suite'] = {'complete': True, 'source_inputs_sha256': 'b' * 64,
                             'cases': ['process_loss', 'corruption', 'stale_authority', 'disk_full',
                                       'duplicate_commands', 'concurrent_recovery']}
        for trial in raw['trials']:
            trial['evidence']['failure_mode'] = 'process_kill'
        raw['profile']['failure_mode'] = 'process_kill'
        raw['profile_sha256'] = hashlib.sha256(json.dumps(raw['profile'], sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        report = analyze(raw, mode='qualify', target_seconds=120, minimum_samples=100)
        self.assertTrue(report['qualified'])
        raw['fault_suite']['source_inputs_sha256'] = 'e' * 64
        self.assertFalse(analyze(raw, mode='qualify', target_seconds=120, minimum_samples=100)['qualified'])

    def test_invalid_target_or_mode_is_rejected(self):
        for kwargs in [{'target_seconds': float('nan')}, {'target_seconds': 0},
                       {'minimum_samples': 0}, {'mode': 'unknown'}]:
            args = dict(mode='pilot', target_seconds=120, minimum_samples=3)
            args.update(kwargs)
            with self.assertRaises(ValueError):
                analyze(campaign(), **args)


class RecoveryRunner(unittest.TestCase):
    def test_controller_protocol_matches_the_native_fixture(self):
        source = (Path(__file__).resolve().parents[2] / 'crates/cellar/tests/support/raft_recovery.rs').read_text()
        for name, expected in [('PHASES', PHASES), ('CHECKS', CHECKS)]:
            body = source.split('const ' + name + ':', 1)[1].split('= &[', 1)[1].split('];', 1)[0]
            self.assertEqual(tuple(re.findall(r'"([a-z_]+)"', body)), expected)

    def test_profiles_select_existing_exact_tests_and_correct_physical_sizes(self):
        source = (Path(__file__).resolve().parents[2] / 'crates/cellar/tests/support/raft_recovery.rs').read_text()
        for profile in json.loads(PROFILES.read_text())['profiles'].values():
            self.assertEqual(profile['image_bytes'], profile['logical_quota_bytes'] // 4096 * 4096)
            for backend in profile['backends']:
                for storage in ('materialized', 'extents'):
                    name = select_test(profile, backend, storage).split('::')[1]
                    self.assertEqual(len(re.findall(r'async fn ' + name + r'\(', source)), 1)
            with self.assertRaises(ValueError):
                select_test(profile, 'unknown')
            with self.assertRaises(ValueError):
                select_test(profile, profile['backends'][0], 'unknown')

    def test_zero_or_multiple_test_selections_never_count_as_a_single_pass(self):
        self.assertEqual(selected_results('running 0 tests\ntest result: ok. 0 passed; 0 failed;'), (0, 0))
        self.assertEqual(selected_results('running 1 test\ntest result: ok. 1 passed; 0 failed;'), (1, 1))
        self.assertEqual(selected_results('running 1 test\nrunning 1 test\ntest result: ok. 1 passed; 0 failed;'), (0, 1))

    def test_timed_out_trial_process_is_stopped_and_joined(self):
        with tempfile.TemporaryDirectory() as directory:
            code, timed_out = run_process([sys.executable, '-c', 'import time; time.sleep(30)'],
                                           Path(directory) / 'trial.log', timeout=.05)
            self.assertTrue(timed_out)
            self.assertNotEqual(code, 0)
            self.assertIsInstance(code, int)

    @unittest.skipUnless(os.name == 'posix', 'requires POSIX process groups')
    def test_signaled_runner_joins_child_before_removing_worker_data(self):
        for signum in (signal.SIGTERM, signal.SIGINT):
            with self.subTest(signal=signum), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                marker, stopped, owned = (root / name for name in ('child.pid', 'stopped', 'owned'))
                child = (
                    'import os,signal,sys,time; from pathlib import Path; '
                    f'root=Path({str(root)!r}); '
                    'signal.signal(signal.SIGTERM, lambda *_: '
                    '(root.joinpath("stopped").write_text(str(Path(root.joinpath("owned").read_text()).exists())), sys.exit(0))); '
                    'root.joinpath("child.pid").write_text(str(os.getpid())); time.sleep(30)'
                )
                # Exercise the CLI's signal boundary with its real process and
                # directory owners, without building or launching native nodes.
                script = (
                    f'import sys; from pathlib import Path; sys.path.insert(0, {str(PROFILES.parent)!r}); import recovery\n'
                    'def trial(_):\n'
                    f'    root=Path({str(root)!r})\n'
                    '    with recovery.worker_environment({}, root, physical=False) as (env, paths):\n'
                    '        root.joinpath("owned").write_text(str(paths[0]))\n'
                    f'        recovery.run_process([{sys.executable!r}, "-c", {child!r}], root / "native.log", timeout=30)\n'
                    'recovery.run=trial\n'
                    f'sys.argv=["recovery.py", "--backend", "peer", "--output", {str(root)!r}]\n'
                    'raise SystemExit(recovery.main())\n'
                )
                process = subprocess.Popen([sys.executable, '-c', script], start_new_session=True,
                                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                child_pid = None
                try:
                    deadline = time.monotonic() + 10
                    while not marker.exists() and time.monotonic() < deadline:
                        self.assertIsNone(process.poll(), 'runner exited before the child started')
                        time.sleep(.01)
                    self.assertTrue(marker.exists(), 'child startup was not observed')
                    child_pid = int(marker.read_text())
                    process.send_signal(signum)
                    process.wait(timeout=10)
                    self.assertTrue(stopped.exists(), 'owned child survived runner interruption')
                    self.assertEqual(stopped.read_text(), 'True', 'worker data was erased before its child stopped')
                    self.assertFalse(Path(owned.read_text()).exists())
                    with self.assertRaises(ProcessLookupError):
                        os.kill(child_pid, 0)
                finally:
                    if process.poll() is None:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
                    if child_pid is not None:
                        try:
                            os.killpg(child_pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass

    def test_prebuilt_binaries_require_matching_source_and_byte_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            binary = directory / 'fixture'
            binary.write_bytes(b'fixture')
            identity = {'commit': 'a' * 40, 'inputs_sha256': 'b' * 64}
            args = SimpleNamespace(binary=binary, binary_manifest=None)
            with self.assertRaisesRegex(ValueError, 'binary-manifest'):
                binary_for(args, directory, identity)
            args.binary_manifest = directory / 'manifest.json'
            args.binary_manifest.write_text(json.dumps({**identity, 'binary_sha256': sha256(binary), 'build_profile': 'release'}))
            self.assertEqual(binary_for(args, directory, identity), binary.resolve())
            binary.write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError, 'bytes differ'):
                binary_for(args, directory, identity)


class WorkerDirectoryOwnership(unittest.TestCase):
    def test_failure_removes_only_new_owned_directories_and_keeps_fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            primary, secondary, evidence = (base / name for name in ('primary', 'secondary', 'evidence'))
            for path in (primary, secondary, evidence): path.mkdir()
            fixture = primary / 'source.sqlite'
            fixture.write_bytes(b'immutable fixture')
            parent = {'CELLAR_LARGE_IMAGE_WORKDIR': str(primary),
                      'CELLAR_LARGE_IMAGE_SECONDARY_WORKDIR': str(secondary),
                      'CELLAR_LARGE_IMAGE_FIXTURE': str(fixture)}
            owned = []
            with self.assertRaisesRegex(RuntimeError, 'interrupted'):
                with worker_environment(parent, evidence, physical=True) as (env, owned):
                    self.assertEqual(env['CELLAR_RECOVERY_KEEP_NODE_STATE'], '0')
                    self.assertEqual(env['CELLAR_LARGE_IMAGE_FIXTURE'], str(fixture))
                    self.assertEqual(len(owned), 2)
                    for path in owned:
                        (path / 'temporary.sqlite').write_bytes(b'partial state')
                    raise RuntimeError('interrupted')
            self.assertTrue(all(not path.exists() for path in owned))
            self.assertEqual(fixture.read_bytes(), b'immutable fixture')
            self.assertEqual(parent['CELLAR_LARGE_IMAGE_WORKDIR'], str(primary))

    def test_explicit_retention_preserves_just_its_own_worker_data(self):
        with tempfile.TemporaryDirectory() as directory:
            with worker_environment({}, Path(directory), physical=False, keep=True) as (env, owned):
                self.assertEqual(env['CELLAR_RECOVERY_KEEP_NODE_STATE'], '1')
                self.assertEqual(env['CELLAR_RECOVERY_WORKDIR'], str(owned[0]))
                (owned[0] / 'local.sqlite').write_bytes(b'diagnostic state')
            self.assertEqual((owned[0] / 'local.sqlite').read_bytes(), b'diagnostic state')

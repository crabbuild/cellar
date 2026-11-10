# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Cold recovery is complete only after local verified reads and durable writes."""
import copy
import hashlib
import json
import unittest

from analyze_recovery import analyze, PHASES, CHECKS


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


if __name__ == "__main__":
    unittest.main()

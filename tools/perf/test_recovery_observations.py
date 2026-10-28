# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import copy
import unittest

from raft_metrics import BOUNDS, COUNTERS, FIELDS, OPERATIONS
from recovery_observations import PHASES, phase_work


def host(source, stamp, value):
    return {'source': source, 'available': True, 'saturated': False,
            'sampled_at_us': str(stamp),
            'counters': [{'name': name, 'value': str(value)} for name in COUNTERS],
            'operations': [{'name': name, **{field: '0' for field in FIELDS},
                            'buckets': [{'upper_us': None if bound is None else str(bound), 'count': '0'}
                                        for bound in BOUNDS]} for name in OPERATIONS]}


def windows():
    result = []
    for index, phase in enumerate(PHASES):
        hosts = [host('1' * 32, 100 + index * 100, index)]
        if index >= 2:
            hosts.append(host('2' * 32, 100 + index * 100, 700 + index))
        result.append({'phase': phase, 'capture_started_us': index * 1000,
                       'capture_finished_us': index * 1000 + 10, 'hosts': hosts,
                       'new_sources': ['1' * 32] if index == 0 else ['2' * 32] if index == 2 else []})
    return result


class ObservationWindows(unittest.TestCase):
    def test_source_stable_deltas_and_new_host_lifetime_are_distinct(self):
        result = phase_work(windows())
        old, new = result['windows'][1]['hosts']
        self.assertEqual(old['coverage'], 'completed_operation_window')
        self.assertEqual(old['work']['counters'][COUNTERS[0]], 1)
        self.assertEqual(new['coverage'], 'new_host_lifetime')
        self.assertEqual(new['work']['counters'][COUNTERS[0]], 702)
        self.assertEqual(result['capture_overhead'][0]['capture_us'], 10)

    def test_closed_host_cannot_disappear(self):
        value = windows()
        value[3]['hosts'].pop(0)
        with self.assertRaisesRegex(ValueError, 'disappeared'):
            phase_work(value)

    def test_duplicate_source_is_refused(self):
        value = windows()
        value[0]['hosts'].append(copy.deepcopy(value[0]['hosts'][0]))
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            phase_work(value)

    def test_source_restart_cannot_be_subtracted(self):
        value = windows()
        value[1]['hosts'][0]['source'] = '3' * 32
        value[1]['new_sources'] = ['3' * 32]
        with self.assertRaisesRegex(ValueError, 'disappeared'):
            phase_work(value)

    def test_clock_and_counter_regressions_are_refused(self):
        for mutate in (
            lambda row: row.update(capture_started_us=-1),
            lambda row: row['hosts'][0].update(sampled_at_us='50'),
            lambda row: row['hosts'][0]['counters'][0].update(value='0'),
        ):
            value = windows()
            mutate(value[3])
            with self.subTest(mutate=mutate), self.assertRaises(ValueError):
                phase_work(value)

    def test_unavailable_owners_never_create_measured_deltas(self):
        for field, content in [('available', False), ('saturated', True), ('sampled_at_us', None)]:
            value = windows()
            value[2]['hosts'][0][field] = content
            with self.subTest(field=field):
                report = phase_work(value)
                missing = report['windows'][1]['hosts'][0]
                self.assertEqual(missing['coverage'], 'unavailable')
                self.assertNotIn('work', missing)
                self.assertEqual(len(report['unavailable_samples']), 1)
                self.assertEqual(report['windows'][2]['hosts'][0]['coverage'], 'resumed_host_lifetime')

    def test_live_owner_missing_durations_remain_invalid(self):
        value = windows()
        value[2]['hosts'][0]['operations'][0]['timing_unavailable'] = '1'
        with self.assertRaises(ValueError):
            phase_work(value)

    def test_closed_clock_preserves_raw_unmeasured_outcomes(self):
        value = windows()
        for row in value[1:]:
            raw = row['hosts'][0]
            raw['sampled_at_us'] = None
            raw['operations'][0].update(started='1', succeeded='1', timing_unavailable='1')
        report = phase_work(value)
        self.assertEqual(len(report['unavailable_samples']), 6)
        for window in report['windows']:
            self.assertEqual(window['hosts'][0]['coverage'], 'unavailable')
            self.assertNotIn('work', window['hosts'][0])

    def test_missing_and_forged_windows_are_refused(self):
        for value in (windows()[:-1], [{**windows()[0], 'phase': 'seed_started'}, *windows()[1:]]):
            with self.assertRaises(ValueError):
                phase_work(value)
        value = windows()
        value[1]['new_sources'] = ['1' * 32]
        with self.assertRaisesRegex(ValueError, 'sources differ'):
            phase_work(value)

    def test_controller_phase_bounds_are_checked(self):
        value = windows()
        events = [{'name': name, 'elapsed_us': stamp} for name, stamp in (
            ('fault', 100), ('seed_started', 1000), ('seed_installed', 2000),
            ('hydrated', 2100), ('first_write_verified', 3000), ('receipt_verified', 3100),
            ('payloads_verified', 4000), ('cleanup_joined', 7000))]
        phase_work(value, events)
        value[2]['capture_finished_us'] = 2200
        with self.assertRaisesRegex(ValueError, 'outside'):
            phase_work(value, events)

    def test_valid_phase_work_is_reported_beside_controller_rto(self):
        from analyze_recovery import analyze
        from test_recovery import campaign
        raw = campaign(count=1)
        evidence = raw['trials'][0]['evidence']
        stamps = {row['name']: row['elapsed_us'] for row in evidence['events']}
        value = windows()
        for index, row in enumerate(value):
            event = row['phase']
            stamp = 0 if event == 'before_fault' else stamps.get(event, stamps['payloads_verified'])
            row['capture_started_us'] = row['capture_finished_us'] = stamp
        evidence['observation_windows'] = value
        report = analyze(raw, minimum_samples=1)
        self.assertEqual(report['phase_observation_failures'], [])
        self.assertEqual(len(report['phase_observation_work'][0]['work']['windows']), 6)
        self.assertEqual(report['cold_failure_to_ready_seconds']['p95'], 80)

    def test_metrics_cannot_replace_native_correctness_or_controller_timing(self):
        from analyze_recovery import analyze
        from test_recovery import campaign
        raw = campaign(count=1)
        evidence = raw['trials'][0]['evidence']
        # Invalid optional metrics are reported separately from a valid native
        # timing sample. They cannot repair a failed receipt or create RTO.
        evidence['observation_windows'] = windows()
        report = analyze(raw, minimum_samples=1)
        self.assertEqual(len(report['phase_observation_failures']), 1)
        self.assertEqual(report['successful_trials'], 1)
        self.assertEqual(report['cold_failure_to_ready_seconds']['p95'], 80)
        evidence['checks']['receipt_verified'] = False
        report = analyze(raw, minimum_samples=1)
        self.assertEqual(report['successful_trials'], 0)
        self.assertIsNone(report['cold_failure_to_ready_seconds']['p95'])


if __name__ == '__main__':
    unittest.main()

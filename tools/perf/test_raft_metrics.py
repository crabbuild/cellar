# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Measurement evidence must fail closed on reset, loss or impossible samples."""
import copy
from pathlib import Path
import re
import unittest
from raft_metrics import BOUNDS, COUNTERS, FIELDS, OPERATIONS, delta, phases, periodic, samples, snapshot


def observation(source='1' * 32, stamp=1):
    return {'source': source, 'available': True, 'saturated': False, 'sampled_at_us': str(stamp),
            'counters': [{'name': name, 'value': '0'} for name in COUNTERS],
            'operations': [{'name': name, **dict.fromkeys(FIELDS, '0'),
                            'buckets': [{'upper_us': None if upper is None else str(upper), 'count': '0'} for upper in BOUNDS]}
                           for name in OPERATIONS]}


def completed(raw, duration, outcome='succeeded'):
    operation = raw['operations'][1]
    for field in ('started', outcome, 'count'):
        operation[field] = str(int(operation[field]) + 1)
    operation['sum_us'] = str(int(operation['sum_us']) + duration)
    for bucket in operation['buckets']:
        if bucket['upper_us'] is None or duration <= int(bucket['upper_us']):
            bucket['count'] = str(int(bucket['count']) + 1)


def rows(stamp, node_count=3):
    return [{'node': node, 'started_ns': stamp * 10 + node, 'finished_ns': stamp * 10 + node + 1,
             'sample': {'performance': observation(str(node) * 32, stamp), 'resources': {'sql': {}, 'groups': {}}}}
            for node in range(1, node_count + 1)]


class Measurements(unittest.TestCase):
    def test_metric_contract_matches_the_native_owner(self):
        rust = (Path(__file__).resolve().parents[2] / 'crates/cellar/durability/raft/performance.rs').read_text()
        operations = rust.split('impl Operation {', 1)[1].split('#[derive', 1)[0]
        counters = rust.split('impl Counter {', 1)[1].split('#[derive', 1)[0]
        self.assertEqual(OPERATIONS, tuple(re.findall(r'"([a-z_]+)"', operations)))
        self.assertEqual(COUNTERS, tuple(re.findall(r'"([a-z_]+)"', counters)))
        bounds = rust.split('const BOUNDS_US:', 1)[1].split('= [', 1)[1].split('];', 1)[0]
        values = [value.strip().replace('_', '') for value in bounds.split(',') if value.strip()]
        self.assertEqual(BOUNDS, tuple(None if value == 'u64::MAX' else int(value) for value in values))

    def test_integer_precision_and_bucket_upper_bounds(self):
        before, after = observation(), observation(stamp=2)
        before['counters'][0]['value'] = str(2**63)
        after['counters'][0]['value'] = str(2**63 + 3)
        for duration in (50, 51, 100, 101):
            completed(after, duration)
        result = delta(before, after)
        self.assertEqual(result['counters'][COUNTERS[0]], 3)
        self.assertEqual(result['operations']['engine_file_sync']['quantile_upper_us'],
                         {'p50': 100, 'p95': 250, 'p99': 250, 'p99_9': 250})
        self.assertEqual(result['operations']['engine_file_sync']['sum_us'], 302)
        self.assertIsNone(result['operations']['sql_query']['quantile_upper_us']['p50'])

    def test_inflight_crosses_window_and_failure_is_not_success(self):
        before, after = observation(), observation(stamp=2)
        before['operations'][1]['started'] = '1'
        completed(after, 40_000_000, 'failed')
        operation = delta(before, after)['operations']['engine_file_sync']
        self.assertEqual((operation['started'], operation['failed'], operation['succeeded']), (0, 1, 0))
        self.assertEqual((operation['inflight_before'], operation['inflight_after']), (1, 0))
        self.assertEqual(operation['quantile_upper_us']['p99'], 60_000_000)

    def test_missing_unavailable_saturated_and_clock_samples_refuse(self):
        for field, value in [('available', False), ('saturated', True), ('sampled_at_us', None),
                             ('available', 1), ('source', 'invalid')]:
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                raw = observation()
                raw[field] = value
                snapshot(raw)
        raw = observation()
        del raw['operations']
        with self.assertRaises(ValueError):
            snapshot(raw)

    def test_bad_decimal_observations_refuse(self):
        for value in (0, True, '-1', '1.0', '01', '1e3', str(2**64 - 1), str(2**64), ''):
            with self.subTest(value=value), self.assertRaises(ValueError):
                raw = observation()
                raw['counters'][0]['value'] = value
                snapshot(raw)

    def test_duplicate_or_missing_vocabulary_refuses(self):
        for field in ('operations', 'counters'):
            for duplicate in (False, True):
                with self.subTest(field=field, duplicate=duplicate), self.assertRaises(ValueError):
                    raw = observation()
                    if duplicate:
                        raw[field][1] = raw[field][0]
                    else:
                        raw[field].pop()
                    snapshot(raw)

    def test_changed_source_and_reset_refuse(self):
        before, after = observation(), observation(stamp=2)
        for mutate in (lambda r: r.update(source='2' * 32), lambda r: r.update(sampled_at_us='1')):
            changed = copy.deepcopy(after)
            mutate(changed)
            with self.assertRaises(ValueError):
                delta(before, changed)
        before['counters'][0]['value'] = '1'
        with self.assertRaises(ValueError):
            delta(before, after)

    def test_impossible_histogram_and_missing_clock_refuse(self):
        for field, value in [('count', '2'), ('sum_us', '49'), ('started', '0'), ('timing_unavailable', '1')]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                raw = observation()
                completed(raw, 51)
                raw['operations'][1][field] = value
                snapshot(raw)
        raw = observation()
        raw['operations'][1]['buckets'][0]['upper_us'] = '51'
        with self.assertRaises(ValueError):
            snapshot(raw)

    def test_monotone_snapshots_with_impossible_bucket_delta_refuse(self):
        before, after = observation(), observation(stamp=2)
        completed(before, 100)
        completed(after, 50)
        completed(after, 101)
        # Both snapshots and each cumulative count are valid independently.
        # Their delta has [1, 0, 1], which cannot be a histogram.
        snapshot(before)
        snapshot(after)
        with self.assertRaises(ValueError):
            delta(before, after)

    def test_phases_require_every_node_and_continuous_sources(self):
        raw = {'warmup': {'before': rows(1), 'after': rows(2)},
               'measurement': {'before': rows(3), 'after': rows(4)}}
        self.assertEqual(len(phases(raw, True)['measurement']), 3)
        raw['measurement']['before'][0]['sample']['performance']['source'] = '4' * 32
        with self.assertRaises(ValueError):
            phases(raw, True)
        for modify in (lambda r: r.pop(), lambda r: r[0].update(node=2),
                       lambda r: r[0]['sample'].update(resources={'groups': None}),
                       lambda r: r[0].update(finished_ns=0)):
            raw = rows(1)
            modify(raw)
            with self.assertRaises(ValueError):
                samples(raw)

    def test_periodic_samples_require_nonempty_continuous_phase_sources(self):
        sources = {node: str(node) * 32 for node in (1, 2, 3)}
        self.assertEqual(periodic([rows(1), rows(2)], sources), 2)
        for evidence in ([], [rows(2), rows(1)]):
            with self.assertRaises(ValueError):
                periodic(evidence, sources)
        bad = rows(1)
        bad[0]['sample']['performance']['source'] = '4' * 32
        with self.assertRaises(ValueError):
            periodic([bad], sources)

    def test_overlapping_boundaries_or_missing_phase_refuse(self):
        for raw in ({}, {'measurement': {'before': rows(2), 'after': rows(1)}},
                    {'measurement': {'before': rows(1)}}):
            with self.assertRaises(ValueError):
                phases(raw, False)


if __name__ == '__main__':
    unittest.main()

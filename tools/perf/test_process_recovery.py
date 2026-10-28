# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import copy
import unittest

from process_recovery import validate


def evidence():
    details = [
        ('request_before_loss', {}),
        ('before_fault', {'pids': [10, 11, 12]}),
        ('fault', {'pid': 10, 'signal': 9}),
        ('owner_killed', {'pid': 10, 'signal': 9}),
        ('local_state_erased', {}), ('admitted', {}), ('archive_ready', {}),
        ('replacement_spawn_started', {'absent': True}),
        ('replacement_process_ready', {'pid': 13, 'initially_empty': True}),
        ('seed_installed', {'pid': 13, 'from_s3': True, 'performance': {'counters': [
            {'name': 'object_get_body_bytes', 'value': '10000'},
            {'name': 'recovery_peer_received_bytes', 'value': '0'},
        ]}}),
        ('first_read', {'served_by': 4, 'value': 8, 'verified': True}),
        ('promoted', {'pid': 13, 'member': 4}), ('request_after_recovery', {}),
        ('first_write_verified', {'value': 9, 'durability': 'quorum'}),
        ('receipts_and_quorum_reads_verified', {'callbacks_before_kill': 1, 'replacement_callbacks': 1}),
        ('payloads_verified', {}),
        *[('worker_joined', {'pid': pid, 'report': {'owners_closed': True}}) for pid in [11, 12, 13]],
        ('cleanup_joined', {'workers': 4, 'hard_killed': 1, 'gracefully_closed': 3}),
    ]
    return {'complete': True, 'failure_mode': 'sigkill', 'backend': 'rustfs', 'storage': 'extents',
            'image_bytes': 10000, 'events': [
                {'name': name, 'elapsed_us': i * 1000000, 'detail': detail}
                for i, (name, detail) in enumerate(details)
            ]}


class ProcessRecoveryEvidenceTests(unittest.TestCase):
    def test_rto_starts_at_fault_and_includes_process_start(self):
        result = validate(evidence(), 10000)
        self.assertEqual(result['fault_to_verified_write_seconds'], 11)
        self.assertEqual(result['fresh_process_start_through_seed_seconds'], 2)

    def test_wrong_process_kill_source_and_cleanup_are_rejected(self):
        edits = [
            (2, 'signal', 15), (3, 'pid', 99),
            (7, 'absent', False), (8, 'pid', 11), (8, 'pid', 0), (8, 'initially_empty', False),
            (9, 'pid', 11), (9, 'from_s3', False), (10, 'served_by', 2),
            (11, 'pid', 10), (13, 'durability', 'local'),
            (14, 'replacement_callbacks', 2), (16, 'pid', 10),
        ]
        for index, name, value in edits:
            with self.subTest(index=index, name=name):
                value_evidence = evidence()
                value_evidence['events'][index]['detail'][name] = value
                with self.assertRaises(ValueError):
                    validate(value_evidence, 10000)

    def test_missing_phase_and_regressed_clock_are_rejected(self):
        value = evidence()
        del value['events'][16]
        with self.assertRaises(ValueError):
            validate(value, 10000)
        value = evidence()
        value['events'][5]['elapsed_us'] = 0
        with self.assertRaises(ValueError):
            validate(value, 10000)

    def test_peer_fallback_and_incomplete_object_reads_are_rejected(self):
        for counter, changed in [(0, '9999'), (1, '1')]:
            value = copy.deepcopy(evidence())
            value['events'][9]['detail']['performance']['counters'][counter]['value'] = changed
            with self.assertRaises(ValueError):
                validate(value, 10000)


if __name__ == '__main__':
    unittest.main()

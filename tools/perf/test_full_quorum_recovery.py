# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
import copy
import unittest

from full_quorum_recovery import validate


def evidence():
    old = {'value': 8, 'durability': 'quorum', 'index': 7, 'group': 'old', 'incarnation': 'old'}
    new = {'group': 'new', 'incarnation': 'new'}
    current = {'archived': 7, 'committed': 7, 'archive': {'readiness': 'current', 'lag_entries': 0}}
    details = [
        ('quorum_write_before_loss', {'pids': [10, 11, 12], 'receipt': old}),
        ('archive_current_before_loss', {'status': current}), ('fault', {'pids': [10, 11, 12], 'signal': 9}),
        *[('replica_killed_and_erased', {'pid': pid, 'signal': 9, 'directory': str(pid), 'absent': True}) for pid in [10, 11, 12]],
        ('disaster_preflight_started', {}), ('new_history_committed', new),
        ('fresh_quorum_spawn_started', {'all_absent': True}),
        *[('fresh_process_ready', {'pid': pid, 'initially_empty': True, 'member': index + 1}) for index, pid in enumerate([20, 21, 22])],
        *[('fresh_seed_installed', {'pid': pid, 'from_s3': True, 'snapshot_timeout_ms': 3600000,
                                  'performance': {'counters': [{'name': 'object_get_body_bytes', 'value': '10000'}, {'name': 'recovery_peer_received_bytes', 'value': '0'}]}}) for pid in [20, 21, 22]],
        *[('restored_read', {'served_by': member, 'verified': True, 'value': 8}) for member in [1, 2, 3]],
        ('old_receipt_and_token_verified', {'wrong_incarnation': True, 'callbacks': 0, 'receipt': {**new, 'value': 8}}),
        ('first_write_verified', {**new, 'value': 9, 'durability': 'quorum', 'index': 6}),
        ('quorum_payloads_and_receipts_verified', {'workers': 3, 'callbacks': 1, 'payload_bytes': 9000, 'sample_ids': [1, 2, 3], 'sample_hashes': [[1], [2], [3]]}),
        ('new_history_archive_current', current),
        *[('fresh_worker_joined', {'pid': pid, 'report': {'owners_closed': True}}) for pid in [20, 21, 22]],
        ('restart_reads_and_receipts_verified', {'pids': [30, 31, 32], 'value': 9, 'callbacks': 0}),
        *[('resumed_worker_joined', {'pid': pid, 'report': {'owners_closed': True}}) for pid in [30, 31, 32]],
        ('cleanup_joined', {'workers': 9, 'hard_killed': 3, 'gracefully_closed': 6}),
    ]
    return {'complete': True, 'failure_mode': 'full_quorum_sigkill', 'backend': 'rustfs', 'storage': 'extents', 'image_bytes': 10000,
            'events': [{'name': name, 'elapsed_us': index * 1000000, 'detail': copy.deepcopy(detail)} for index, (name, detail) in enumerate(details)]}


class FullQuorumEvidenceTests(unittest.TestCase):
    def test_rto_includes_fence_and_all_three_cold_workers(self):
        result = validate(evidence(), 10000)
        self.assertEqual(result['fault_to_verified_write_seconds'], 17)
        self.assertEqual(result['operator_preflight_and_fence_seconds'], 1)
        self.assertEqual(result['fresh_quorum_start_through_all_seeds_seconds'], 6)

    def test_missing_reordered_phase_or_regressed_clock_fails(self):
        for index in range(len(evidence()['events'])):
            value = evidence()
            del value['events'][index]
            with self.assertRaises(ValueError):
                validate(value, 10000)
        value = evidence()
        value['events'][10]['elapsed_us'] = 0
        with self.assertRaises(ValueError):
            validate(value, 10000)

    def test_surviving_replica_stale_archive_authority_and_restart_fail(self):
        edits = [(1, ['status', 'archived'], 6), (1, ['status', 'archive', 'readiness'], 'lagging'),
                 (2, ['signal'], 15), (3, ['signal'], 15), (4, ['absent'], False), (5, ['pid'], 99),
                 (7, ['group'], 'old'), (8, ['all_absent'], False), (9, ['pid'], 10), (10, ['initially_empty'], False),
                 (11, ['member'], 2), (12, ['from_s3'], False), (13, ['pid'], 12),
                 (14, ['performance', 'counters', 0, 'value'], '9999'), (14, ['performance', 'counters', 1, 'value'], '1'),
                 (15, ['value'], 7), (18, ['wrong_incarnation'], False), (18, ['callbacks'], 1),
                 (19, ['durability'], 'local'), (20, ['workers'], 2), (21, ['archived'], 0),
                 (22, ['report', 'owners_closed'], False), (25, ['value'], 8), (25, ['pids'], [10, 31, 32]),
                 (26, ['report', 'owners_closed'], False)]
        for index, keys, replacement in edits:
            with self.subTest(index=index, keys=keys):
                value = evidence()
                target = value['events'][index]['detail']
                for key in keys[:-1]:
                    target = target[key]
                target[keys[-1]] = replacement
                with self.assertRaises(ValueError):
                    validate(value, 10000)


if __name__ == '__main__':
    unittest.main()

# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import raft_fleet_operations as operations
from raft_groups import Layout
from raft_run import read_json


class OperationsTests(unittest.TestCase):
    def test_shared_seed_reserves_complete_population_before_activation_and_enrollment(self):
        run = object.__new__(operations.Run)
        layout = Layout(65, 32, 'Counter')
        run.policy = {'nodes': 5, 'cells': 65, 'cells_per_group': 32,
                      'incarnation': [8] * 16, 'owner': 'test'}
        run.report = {'run_id': 'frozen', 'layout': layout.json(5)}
        run.status = Mock(return_value={'state': 'Active', 'revision': 10})
        commands = []
        run.command = lambda node, key, make: commands.append((node, key, make()))
        run.seed()
        actions = [body['action'] for _, _, body in commands]
        self.assertEqual(actions[:3], ['Reserve'] * 3)
        self.assertEqual(actions[3:9], ['Confirm', 'InitializeArchive'] * 3)
        self.assertEqual(actions[9:18], ['ActivateInitial'] * 9)
        self.assertEqual(actions[18:], ['Enroll'] * 65)
        self.assertEqual(len({key for _, key, _ in commands}), len(commands))
        for node, _, body in commands[18:]:
            self.assertEqual(node, layout.members(body['cell'], 5)[0])
        commands.clear()
        run.report['layout']['cells_per_group'] = 1
        with self.assertRaisesRegex(ValueError, 'frozen population changed'):
            run.seed()
        self.assertEqual(commands, [])

    def test_mount_identity_ignores_order_but_preserves_every_binding_field(self):
        mounts = [{'Destination': '/data', 'Name': 'metadata', 'RW': True},
                  {'Destination': '/data/node/raft', 'Name': 'raft', 'RW': True}]
        expected = operations.mount_bindings(mounts)
        self.assertEqual(operations.mount_bindings(list(reversed(mounts))), expected)
        for field, value in [('Name', 'other'), ('RW', False), ('Destination', '/other')]:
            changed = [{**mounts[0], field: value}, mounts[1]]
            with self.subTest(field=field):
                self.assertNotEqual(operations.mount_bindings(changed), expected)
        with self.assertRaisesRegex(ValueError, 'duplicate mount destination'):
            operations.mount_bindings(mounts + [mounts[0]])

    def test_kernel_observations_must_match_enforced_profile(self):
        valid = ['100000 100000', '3221225472', '0', '1048576']
        self.assertEqual(operations.kernel_limits(valid)['cpu_millis'], 1000)
        for index, value in [(0, 'max 100000'), (0, '200000 100000'), (1, '1073741824'),
                             (2, '1073741824'), (3, '0'), (3, '4294967296')]:
            lines = valid.copy()
            lines[index] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                operations.kernel_limits(lines)

    def test_unknown_result_reconciles_original_request_without_new_revision(self):
        with tempfile.TemporaryDirectory() as folder:
            run = object.__new__(operations.Run)
            run.root = Path(folder)
            original = {'action': 'Capacity', 'request': {'identity': {'expected_revision': 12}}}
            factory = Mock(return_value=original)
            with patch.object(operations, 'apply', side_effect=[OSError('lost reply'), None]) as apply:
                with self.assertRaises(OSError):
                    run.command(1, 'capacity-1-Qualify', factory)
                factory.side_effect = AssertionError('must not resample/revise an unknown request')
                run.command(1, 'capacity-1-Qualify', factory)
                sent = [read_json(call.args[0].command) for call in apply.call_args_list]
                self.assertEqual(sent, [original, original])
                self.assertEqual(factory.call_count, 1)
                with self.assertRaisesRegex(ValueError, 'intent conflict'):
                    run.command(2, 'capacity-1-Qualify', factory)
                self.assertEqual(apply.call_count, 2)


if __name__ == '__main__':
    unittest.main()

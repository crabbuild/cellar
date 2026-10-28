# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import argparse
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import raft_fleet


class ManagedOperatorTests(unittest.TestCase):
    def test_unknown_response_retry_preserves_original_request(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'config').mkdir()
            (root / 'config/token').write_text('private-token')
            raft_fleet.save_new(root / 'report.json', {
                'registered': True, 'containers': ['container'], 'port_base': 19800})
            command = {'action': 'Capacity', 'request': {'identity': {
                'id': [7] * 16, 'operation': [8] * 16, 'expected_revision': 19}}}
            raft_fleet.save_new(root / 'command.json', command)
            args = argparse.Namespace(run=root, node=1, command=root / 'command.json')
            calls = []

            def send(*positional, **kwargs):
                intents = list(root.glob('operator-*.json'))
                self.assertTrue(any(json.loads(p.read_text()) == {'node': 1, 'command': command}
                                    for p in intents))
                calls.append(kwargs['body'])
                return (503, {'outcome': 'unconfirmed'}) if len(calls) == 1 else (200, {'revision': 20})

            with patch.object(raft_fleet, 'request', side_effect=send), patch('builtins.print'):
                with self.assertRaises(ValueError):
                    raft_fleet.apply(args)
                raft_fleet.apply(args)
            self.assertEqual(calls, [command, command])
            self.assertEqual(len(list(root.glob('operator-*-response-*.json'))), 2)

    def test_unsynced_intent_never_dispatches(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'intent.json'
            with patch.object(raft_fleet.os, 'fsync', side_effect=OSError('disk failure')):
                with self.assertRaises(OSError):
                    raft_fleet.save_new(path, {'identity': [1] * 16})
            with self.assertRaises(FileExistsError):
                raft_fleet.save_new(path, {'identity': [2] * 16})

    def test_shared_or_unrelated_docker_context_is_refused(self):
        for context in ('default', 'colima', 'colima-cellar-dr-3617'):
            with self.assertRaises(ValueError):
                raft_fleet.engine(context)


if __name__ == '__main__':
    unittest.main()

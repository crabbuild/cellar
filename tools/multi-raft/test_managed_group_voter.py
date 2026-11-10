# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Actual assignment boundary used by the managed voter process adapter."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

import group_disaster
from managed_group_fence import ManagedLauncher
from managed_group_voter import ManagedVoter, VoterFailure
from test_managed_group_fence import history, identity, lineage, request, word


class ManagedVoterTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.launcher = ManagedLauncher(self.root, 'owned')
        self.launcher.declare(0, word(1, 32), word(2, 32), [history(10)])
        container = self.launcher.launch(0, 'voter', 'disk', lambda: 'a'*64, identity=identity(4))
        self.voter = ManagedVoter(self.launcher, 0, container, 'http://127.0.0.1:8080',
                                  word(80, 32), word(81))

    def test_assignment_precedes_unknown_prepare_and_is_reloaded_for_fencing(self):
        def lost(_action):
            self.assertEqual(len(self.launcher.read()['deployments']['0']), 2)
            raise VoterFailure('lost prepare reply')
        self.voter._request = Mock(side_effect=lost)
        with self.assertRaises(VoterFailure) as caught:
            self.voter.prepare_seed(word(1, 32), word(2, 32), word(12), word(13))
        self.assertEqual(caught.exception.outcome, 'unknown')
        self.assertEqual(self.voter._request.call_count, 1)
        digest, challenge = request([lineage(4, 12)])
        reloaded = ManagedLauncher(self.root, 'owned')
        self.assertEqual(reloaded.bind(digest, challenge), [('a'*64, 'disk')])
        with self.assertRaisesRegex(ValueError, 'deployment permanently revoked'):
            self.voter.attach(word(1, 32), word(2, 32), word(12), word(13), word(70), word(71, 32), word(72, 32))
        self.assertEqual(self.voter._request.call_count, 1)

    def test_attach_and_activate_record_history_before_sdk_callback(self):
        def acknowledged(action):
            self.assertEqual(len(self.launcher.read()['deployments']['0']), 2)
            return {'action': action['action']}
        self.voter._request = Mock(side_effect=acknowledged)
        for action in (self.voter.attach, self.voter.activate):
            self.assertEqual(action(word(1, 32), word(2, 32), word(12), word(13),
                                    word(70), word(71, 32), word(72, 32))['action'],
                             action.__name__)

    def test_raw_control_call_cannot_bypass_assignment_for_physical_operations(self):
        self.voter._request = Mock(side_effect=AssertionError('physical admission bypassed'))
        for action in ('start', 'prepare_seed', 'seed', 'attach', 'activate'):
            with self.subTest(action=action), self.assertRaisesRegex(ValueError, 'durable assignment'):
                self.voter.call({'action': action})
        self.voter._request.assert_not_called()

    def test_wire_request_pins_launch_identity_and_never_retries_http_failure(self):
        import io
        import urllib.error
        failure = urllib.error.HTTPError(self.voter.endpoint + '/control', 503, 'refused', {},
                                         io.BytesIO(b'{"error":"pending","outcome":"unknown"}'))
        self.voter.client.open = Mock(side_effect=failure)
        with self.assertRaises(VoterFailure) as caught:
            self.voter.call({'action': 'observe'})
        self.assertEqual(caught.exception.outcome, 'unknown')
        self.assertEqual(caught.exception.status, 503)
        self.assertEqual(self.voter.client.open.call_count, 1)
        wire = self.voter.client.open.call_args.args[0]
        self.assertEqual(wire.get_header('X-cellar-node'), identity(4)['node'])
        self.assertEqual(wire.get_header('X-cellar-disk'), identity(4)['disk'])
        self.assertEqual(wire.get_header('X-cellar-process'), word(81))


if __name__ == '__main__':
    unittest.main()

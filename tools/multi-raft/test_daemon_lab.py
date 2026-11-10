# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Daemon commands must use the backend's exact Engine and Colima home."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from daemon import Lab


class DaemonLabTests(unittest.TestCase):
    def test_refuses_unrelated_or_mismatched_profiles_before_any_command(self):
        with tempfile.TemporaryDirectory() as directory:
            for profile, context in [
                ('default', 'colima-default'),
                ('cellar-multi-raft-owned', 'colima-cellar-multi-raft-other'),
                ('cellar-multi-raft-owned', 'default'),
            ]:
                with patch('daemon.subprocess.check_output') as docker, patch('daemon.subprocess.run') as guest:
                    with self.assertRaises(ValueError):
                        Lab(profile, context, Path(directory).resolve())
                    docker.assert_not_called()
                    guest.assert_not_called()

    def test_requires_an_existing_explicit_colima_home(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            file = root / 'file'
            file.write_text('not a directory')
            for home in [Path('relative'), root / 'missing', file]:
                with self.assertRaises(ValueError):
                    Lab('cellar-multi-raft-owned', 'colima-cellar-multi-raft-owned', home)

    def test_docker_ignores_the_active_context_and_guest_binds_the_same_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            lab = Lab('cellar-multi-raft-owned', 'colima-cellar-multi-raft-owned', home)
            with patch.dict(os.environ, {'DOCKER_CONTEXT': 'foreign', 'COLIMA_HOME': '/foreign', 'LIMA_HOME': '/foreign/lima'}):
                with patch('daemon.subprocess.check_output', return_value='own-engine\n') as docker:
                    self.assertEqual(lab.docker('info', '--format', '{{.ID}}'), 'own-engine')
                    self.assertEqual(docker.call_args.args[0], [
                        'docker', '--context', lab.context, 'info', '--format', '{{.ID}}'])
                with patch('daemon.subprocess.run', return_value=subprocess.CompletedProcess([], 0)) as guest:
                    lab.guest('printf own-engine\n')
                    self.assertEqual(guest.call_args.args[0], [
                        'colima', 'ssh', '--profile', lab.profile, '--', 'sh', '-s'])
                    self.assertEqual(guest.call_args.kwargs['env']['COLIMA_HOME'], str(home))
                    self.assertEqual(guest.call_args.kwargs['env']['LIMA_HOME'], str(home / '_lima'))
                    self.assertEqual(guest.call_args.kwargs['input'], 'printf own-engine\n')
            self.assertNotEqual(os.environ.get('COLIMA_HOME'), str(home))


if __name__ == '__main__':
    unittest.main()

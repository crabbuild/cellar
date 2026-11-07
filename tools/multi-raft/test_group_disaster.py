# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""The trusted launcher's complete inventory and durable, irreversible revocation."""
import json
from pathlib import Path
import tempfile
import unittest

from group_disaster import Launcher


class LauncherTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.launcher = Launcher(self.root, 'owned')

    def populate(self):
        return [(self.launcher.launch(0, f'node{n}', f'disk{n}', lambda n=n: str(n)*64), f'disk{n}')
                for n in range(1, 4)]

    def test_reload_refuses_revoked_generation_before_invoking_docker(self):
        old = self.populate()
        self.assertEqual(self.launcher.revoke(0), old)
        reloaded = Launcher(self.root, 'owned')
        with self.assertRaisesRegex(ValueError, 'deployment permanently revoked'):
            reloaded.launch(0, 'recreated', 'lost', lambda: self.fail('Docker called after revocation'))
        self.assertEqual(reloaded.revoke(0), old)
        self.assertEqual(reloaded.launch(1, 'fresh', 'new', lambda: 'a'*64), 'a'*64)

    def test_incomplete_inventory_cannot_certify_a_fence(self):
        self.launcher.launch(0, 'only-one', 'disk', lambda: '1'*64)
        with self.assertRaisesRegex(ValueError, 'incomplete writer inventory'):
            self.launcher.revoke(0)
        self.assertEqual(json.loads(self.launcher.path.read_text())['revoked'], [])

    def test_unknown_launch_outcome_prevents_fence(self):
        self.populate()
        with self.assertRaisesRegex(RuntimeError, 'unknown outcome'):
            self.launcher.launch(0, 'uncertain', 'disk4', lambda: (_ for _ in ()).throw(RuntimeError('unknown outcome')))
        with self.assertRaisesRegex(ValueError, 'incomplete writer inventory'):
            Launcher(self.root, 'owned').revoke(0)

    def test_duplicate_container_ids_prevent_fence(self):
        for n in range(3):
            self.launcher.launch(0, f'node{n}', f'disk{n}', lambda: 'a'*64)
        with self.assertRaisesRegex(ValueError, 'incomplete writer inventory'):
            self.launcher.revoke(0)

    def test_foreign_run_cannot_adopt_the_launch_ledger(self):
        with self.assertRaisesRegex(ValueError, 'foreign launcher ledger'):
            Launcher(self.root, 'foreign')


if __name__ == '__main__':
    unittest.main()

# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Real Docker regression for snapshot configuration through changing host files.

Set CELLAR_GROUP_TEST_IMAGE to an immutable measurement image and explicitly set
CELLAR_GROUP_TEST_CONTEXT to the dedicated qualification context. No backend or
voter is started. Image contents are irrelevant to this Docker API boundary.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

from group_configuration import copy_configuration, read_copy
from group_disaster import save


class DockerConfigurationTests(unittest.TestCase):
    def test_host_replacement_cannot_change_a_copied_process_snapshot(self):
        image = os.environ.get('CELLAR_GROUP_TEST_IMAGE')
        context = os.environ.get('CELLAR_GROUP_TEST_CONTEXT')
        if not image or not context:
            self.skipTest('requires explicit isolated Docker image/context')
        self.assertRegex(image, r'^sha256:[0-9a-f]{64}$')
        self.assertEqual(context, 'colima-cellar-multi-raft')
        with tempfile.TemporaryDirectory(prefix='cellar-config-regression-') as temporary:
            source = Path(temporary)
            config = {'values':[17]*9000, 'certificate':[]}
            save(source/'config.json', config)
            save(source/'ledger.json', {'tokens':[]})
            (source/'private-key').write_text('must stay on the controller')
            result = subprocess.run(['docker','--context',context,'create','--network','none',
                '--label','io.cellar.config.regression=owned',image],capture_output=True,text=True,check=True)
            identifier = result.stdout.strip()
            self.assertTrue(re.fullmatch('[0-9a-f]{64}',identifier))
            try:
                first = (source/'config.json').read_bytes()
                copied = copy_configuration(context,identifier,source)
                self.assertEqual(copied['config.json'],hashlib.sha256(first).hexdigest())
                config['certificate'] = [0]*96
                save(source/'config.json',config)
                self.assertEqual(read_copy(context,identifier,'config.json'),first)
                copy_configuration(context,identifier,source)
                actual = read_copy(context,identifier,'config.json')
                self.assertEqual(json.loads(actual),config)
                self.assertEqual(actual,(source/'config.json').read_bytes())
                absent = subprocess.run(['docker','--context',context,'cp',identifier+':/config/private-key','-'],capture_output=True)
                self.assertNotEqual(absent.returncode,0)
            finally:
                subprocess.run(['docker','--context',context,'rm',identifier],capture_output=True,check=True)


if __name__ == '__main__':
    unittest.main()

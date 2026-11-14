# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Fence keys and mutable live configurations must never enter voter copies."""
import sys
from pathlib import Path
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'perf'))
from managed_group_configuration import copy


class ManagedConfigurationTests(unittest.TestCase):
    def test_authority_files_and_partial_tls_are_refused_before_docker(self):
        with patch('managed_group_configuration.subprocess.run', side_effect=AssertionError('unsafe copy entered')):
            for extra in ('controller-private.pem', 'managed-launcher.json', 'ca.der', '../config.json'):
                with self.subTest(extra=extra), self.assertRaisesRegex(ValueError, 'only bounded configuration'):
                    copy('colima-cellar-multi-raft-cg13', 'a'*64, 'owned',
                         {'config.json': b'{}', extra: b'private'})

    def test_running_container_refuses_even_valid_tls_snapshot(self):
        with patch('managed_group_configuration.owned_container', return_value={'State':{'Running':True}}), \
                patch('managed_group_configuration.subprocess.run', side_effect=AssertionError('live copy entered')):
            with self.assertRaisesRegex(ValueError, 'under a live process'):
                copy('colima-cellar-multi-raft-cg13', 'a'*64, 'owned', {'config.json':b'{}'})


if __name__ == '__main__':
    unittest.main()

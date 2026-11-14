# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""VM lifecycle cannot escape its existing dedicated profile or weaken limits."""
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock,patch

from managed_group_vm import DedicatedVM,scope


class ManagedVMTests(unittest.TestCase):
    def test_context_and_local_socket_must_name_the_same_dedicated_profile(self):
        context='colima-cellar-multi-raft-owned'
        self.assertEqual(scope(context,'unix:///tmp/owned/cellar-multi-raft-owned/docker.sock'),
                         (Path('/tmp/owned'),context))
        for name,endpoint in [('default','unix:///tmp/owned/default/docker.sock'),
            (context,'tcp://127.0.0.1:2375'),(context,'unix:///tmp/owned/default/docker.sock'),
            (context,'unix:///tmp/../cellar-multi-raft-owned/docker.sock')]:
            with self.subTest(name=name,endpoint=endpoint),self.assertRaises(ValueError):scope(name,endpoint)

    def test_running_vm_must_already_have_exact_requested_resources(self):
        vm=object.__new__(DedicatedVM);vm.status=Mock(return_value={'status':'Running','cpus':6,'memory':16*1024**3})
        vm.call=Mock(side_effect=AssertionError('running VM was edited'))
        with self.assertRaisesRegex(ValueError,'exact declared resources'):vm.start(4,8)
        vm.call.assert_not_called()

    def test_unknown_vm_state_refuses_stop_without_a_mutation(self):
        vm=object.__new__(DedicatedVM);vm.status=Mock(return_value={'status':'Broken'});vm.call=Mock()
        with self.assertRaisesRegex(ValueError,'known running state'):vm.stop()
        vm.call.assert_not_called()

    def test_running_containers_refuse_vm_stop(self):
        vm=object.__new__(DedicatedVM);vm.context='colima-cellar-multi-raft-owned'
        vm.status=Mock(return_value={'status':'Running'});vm.call=Mock()
        with patch('managed_group_vm.command',return_value=SimpleNamespace(stdout='unjoined-container\n')):
            with self.assertRaisesRegex(ValueError,'every container'):vm.stop()
        vm.call.assert_not_called()

    def test_resource_metadata_preserves_disk_and_other_settings(self):
        with tempfile.TemporaryDirectory() as temporary:
            vm=object.__new__(DedicatedVM);vm.config=Path(temporary)/'colima.yaml'
            vm.config.write_text('# owned VM\ncpu: 8\nmemory: 24\ndisk: 100\nmountType: virtiofs\n')
            vm.resources(4,8)
            self.assertEqual(vm.config.read_text(),'# owned VM\ncpu: 4\nmemory: 8\ndisk: 100\nmountType: virtiofs\n')


if __name__=='__main__':unittest.main()

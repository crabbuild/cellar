# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Deployment inputs must satisfy the physical group's transport identity."""
import unittest

from managed_group_drill import ManagedQualification, verify_retry


class ManagedDrillTests(unittest.TestCase):
    def test_container_peer_listeners_bind_the_advertised_identity(self):
        driver=object.__new__(ManagedQualification)
        driver.config={key:'fixture' for key in ('endpoint','bucket','prefix','access','secret','token')}
        driver.config['fence_public']=[1]*32
        driver.model={'bootstrap':{'connection':{}}}
        row={'node':'01'*16,'disk':'02'*16,'process':'03'*16,
             'label':'managed-fixture','ip':'172.28.1.11'}
        config=driver.node_config(row,False)
        self.assertEqual(config['bind'],row['ip'])
        self.assertEqual(config['bind'],config['advertise'])

    def test_retry_preserves_result_and_history_with_a_later_archive_observation(self):
        original={'result':'0000000000000001','version':2,'group':[1]*16,
                  'incarnation':[2]*16,'durability':'s3'}
        verify_retry(original,dict(original,version=3))

    def test_retry_refuses_changed_results_histories_proofs_and_regressing_observations(self):
        original={'result':'0000000000000001','version':2,'group':[1]*16,
                  'incarnation':[2]*16,'durability':'s3'}
        for changed in ({'result':'0000000000000002'},{'group':[3]*16},
                        {'incarnation':[3]*16},{'durability':'quorum'},{'version':1}):
            with self.subTest(changed=changed),self.assertRaisesRegex(ValueError,'original command receipt'):
                verify_retry(original,dict(original,**changed))


if __name__=='__main__':
    unittest.main()

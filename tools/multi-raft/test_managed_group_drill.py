# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Deployment inputs must satisfy the physical group's transport identity."""
import unittest
from unittest.mock import Mock, patch

from managed_group_drill import ManagedQualification, verify_retry
from managed_group_transition import TransitionQualification
from managed_group_voter import VoterFailure


class ManagedDrillTests(unittest.TestCase):
    def activation_driver(self):
        driver=object.__new__(TransitionQualification)
        driver.model={'attach_request':{'operation':'original','plan_digest':'plan','seed_digest':'seeds'}}
        driver.client=Mock()
        driver.event=Mock(side_effect=lambda _label,_request,invoke:invoke())
        return driver

    def test_activation_reconciles_only_a_known_accepted_original_request(self):
        driver=self.activation_driver()
        applied={'applied':{'index':2,'term':3}}
        driver.physical=Mock(side_effect=[{'applied':None},applied])
        with patch('managed_group_transition.time.sleep'):
            self.assertEqual(driver.activate_target({'node':'leader'}),applied)
        self.assertEqual(driver.physical.call_count,2)
        for invocation in driver.event.call_args_list:
            self.assertIs(invocation.args[1],driver.model['attach_request'])

    def test_unknown_activation_stops_without_automatic_mutation_retry(self):
        driver=self.activation_driver()
        driver.physical=Mock(side_effect=VoterFailure('lost activation reply'))
        with self.assertRaises(VoterFailure):driver.activate_target({'node':'leader'})
        self.assertEqual(driver.physical.call_count,1)
        self.assertEqual(driver.event.call_count,1)

    def test_source_quorum_and_replacement_cohort_remain_separate(self):
        driver=object.__new__(TransitionQualification)
        rows=[{'generation':generation,'index':index} for generation,count in ((0,5),(1,3))
              for index in range(count)]
        driver.model={'nodes':rows}
        self.assertEqual(driver.source_nodes(),rows[:3])
        self.assertEqual(driver.target_nodes(),rows[5:])
        self.assertEqual([row for row in rows if row['generation']==0],rows[:5])

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

# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Deployment inputs must satisfy the physical group's transport identity."""
import unittest
from unittest.mock import Mock, patch

from managed_group_drill import ManagedQualification, verify_retry
from managed_group_transition import TransitionQualification
from managed_group_voter import VoterFailure
from managed_group_deployment import profile


class ManagedDrillTests(unittest.TestCase):
    def test_vm_handoff_refuses_default_and_unscoped_contexts(self):
        self.assertEqual(profile('colima-cellar-multi-raft-owned'),'cellar-multi-raft-owned')
        for context in ('default','colima','colima-default','colima-cellar-multi-raft'):
            with self.subTest(context=context),self.assertRaisesRegex(ValueError,'dedicated VM'):
                profile(context)

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

    def recovery_driver(self,release_error=None):
        driver=object.__new__(TransitionQualification)
        rows=[{'node':str(i+1).zfill(32),'process':str(i+4).zfill(32),'member':11+i} for i in range(3)]
        driver.model={'bootstrap':{'catalog_id':'catalog','namespace_digest':'namespace'},
            'replacement':{'group':'01'*16,'incarnation':'02'*16,'creation':'03'*16},
            'fenced':{'cuts':[{'epoch':1}]},'selection':{},'recovery':'original'}
        driver.report={'checks':{}};driver.launcher=Mock();driver.save=Mock()
        driver.launch_node=Mock(side_effect=rows);driver.admit=Mock();driver.target_nodes=Mock(return_value=rows)
        driver.event=Mock(side_effect=lambda _label,_request,invoke:invoke())
        driver.client=lambda row:Mock(seed=Mock(return_value={'seeded':True,'member':row['member'],'commitment':'same'}))
        replies={'recovery-stage':{'plan_digest':'plan'},'recovery-select-seeds':{'seeds':{'seed_digest':'seeds'}},
            'recovery-activation':{'activation':{'index':3,'term':2}},
            'recovery-settle':{'promoted_ranges':1,'total_ranges':1},'recovery-complete':{'completion':'complete'},
            'recovery-retire':{'retired_histories':3,'total_histories':3,'fleet_slot_released':True}}
        def control(label,_request):
            if label=='recovery-release' and release_error:raise release_error
            return replies.get(label,{})
        driver.control=Mock(side_effect=control);driver.attach_targets=Mock();driver.activate_target=Mock()
        driver.target_leader=Mock(return_value=rows[0]);driver.grant_origins=Mock()
        driver.until=Mock(return_value=rows[0]);driver.fresh_writes=Mock()
        driver.restart_targets=Mock();driver.verify_targets=Mock()
        return driver

    def test_released_recovery_reopens_serving_owners_before_reading(self):
        driver=self.recovery_driver()
        def read(**_options):
            self.assertEqual(driver.model['completion'],'complete')
            self.assertEqual(driver.restart_targets.call_count,1,
                'pre-release owners remain fenced; reopen them under the serving receipt')
        driver.verify_targets.side_effect=read
        driver.recover()
        self.assertEqual(driver.restart_targets.call_count,2)
        self.assertTrue(driver.report['full_dr_completed'])

    def test_unknown_serving_release_never_reboots_or_reads(self):
        driver=self.recovery_driver(VoterFailure('lost serving release reply'))
        with self.assertRaises(VoterFailure):driver.recover()
        driver.restart_targets.assert_not_called();driver.verify_targets.assert_not_called()
        self.assertNotIn('completion',driver.model)

    def test_retained_serving_startup_does_not_reattach_as_pending_recovery(self):
        driver=object.__new__(TransitionQualification)
        row={'process':'01'*16,'container':'owned'}
        driver.target_nodes=Mock(return_value=[row]);driver.client=Mock()
        driver.event=Mock(side_effect=lambda _label,_request,invoke:invoke())
        driver.until=Mock(return_value=True);driver.joined_voter=Mock();driver.save=Mock()
        driver.copy_node=Mock();driver.docker=Mock();driver.launcher=Mock()
        driver.launcher.restart.side_effect=lambda _container,invoke:invoke()
        driver.admit=Mock();driver.grant_origins=Mock();driver.target_leader=Mock();driver.verify_targets=Mock()
        driver.attach_targets=Mock(side_effect=AssertionError('serving startup re-entered pending recovery'))
        driver.restart_targets()
        driver.copy_node.assert_called_once_with(row,True)
        driver.verify_targets.assert_called_once_with()

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

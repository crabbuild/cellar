# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Deployment inputs must satisfy the physical group's transport identity."""
import unittest
from unittest.mock import Mock, patch

from managed_group_drill import ManagedQualification, verify_retry
from managed_group_transition import TransitionQualification
from managed_group_published import PublishedTransitionQualification
from managed_group_voter import VoterFailure
from managed_group_deployment import profile


class ManagedDrillTests(unittest.TestCase):
    def test_permission_grants_have_distinct_phase_journals_without_reusing_mutations(self):
        driver=object.__new__(ManagedQualification);driver.model={'events':{}};driver.save=Mock()
        rows=[{'node':str(i+1).zfill(32),'disk':str(i+3).zfill(32),'process':str(i+5).zfill(32),
               'label':'voter'+str(i),'ip':'172.20.0.'+str(11+i)} for i in range(2)]
        revision=[8];mutations=[]
        def call(request):
            if request['action']=='observe':return {'fleet_revision':revision[0]}
            mutations.append(request);return {'granted':True}
        driver.client=Mock(return_value=Mock(call=Mock(side_effect=call)))
        driver.grant_origins(rows);revision[0]=9
        driver.grant_origins(rows,phase='published')
        self.assertEqual(len(driver.model['events']),4)
        self.assertEqual([request['revision'] for request in mutations],[8,8,9,9])
        self.assertTrue(all(request['read'] and request['write'] for request in mutations))
        with self.assertRaisesRegex(ValueError,'operation already entered'):
            driver.grant_origins(rows,phase='published')
        self.assertEqual(len(mutations),4)

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
        driver.client=lambda row:Mock(release_seed=Mock(return_value={'released':True}),seed=Mock(side_effect=lambda _catalog,_namespace,group,incarnation:{
            'seeded':True,'member':row['member'],'commitment':'same','plan_digest':'plan',
            'group':list(bytes.fromhex(group)),'incarnation':list(bytes.fromhex(incarnation))}))
        replies={'recovery-stage':{'plan_digest':'plan'},'recovery-select-seeds':{'seeds':{'seed_digest':'seeds'}},
            'recovery-activation':{'activation':{'index':3,'term':2}},
            'recovery-settle':{'promoted_ranges':1,'total_ranges':1},'recovery-complete':{'completion':'complete'},
            'recovery-retire':{'retired_histories':3,'total_histories':3,'fleet_slot_released':True}}
        def control(label,_request):
            if label=='recovery-release' and release_error:raise release_error
            if label.startswith('recovery-activation'):return replies['recovery-activation']
            if label=='recovery-settle':
                count=len(driver.recovery_histories());return {'promoted_ranges':count,'total_ranges':count}
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
        driver.model={'replacement':{'group':'01'*16,'incarnation':'02'*16}}
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

    def test_two_recovered_ranges_keep_one_preparation_slot_per_voter(self):
        driver=self.recovery_driver()
        driver.model['fenced']['cuts']=[{'group':'04'*16,'incarnation':'05'*16,'epoch':7},
            {'group':'06'*16,'incarnation':'07'*16,'epoch':3}]
        driver.model['replacements']=[dict(driver.model['replacement'],
            source={'group_id':'06'*16,'incarnation_id':'07'*16},cells=['Counter:alpha']),
            {'group':'08'*16,'incarnation':'09'*16,'creation':'0a'*16,
             'source':{'group_id':'04'*16,'incarnation_id':'05'*16},'cells':['Counter:beta']}]
        clients={}
        for row in driver.target_nodes():
            voter=Mock();pending=[];calls=[]
            def prepare(_catalog,_namespace,group,incarnation,pending=pending,calls=calls):
                self.assertFalse(pending,'second preparation bypassed the one-operation pool')
                pending.append((group,incarnation));calls.append(('prepare',group));return {}
            def seed(_catalog,_namespace,group,incarnation,pending=pending,calls=calls,row=row):
                self.assertEqual(pending[-1],(group,incarnation));calls.append(('seed',group))
                return {'seeded':True,'member':row['member'],'commitment':group,'plan_digest':'plan',
                    'group':list(bytes.fromhex(group)),'incarnation':list(bytes.fromhex(incarnation))}
            def release(_catalog,_namespace,group,incarnation,_plan,commitment,pending=pending,calls=calls):
                self.assertEqual(pending.pop(),(group,incarnation));self.assertEqual(commitment,group)
                calls.append(('release',group));return {'released':True}
            voter.prepare_seed.side_effect=prepare;voter.seed.side_effect=seed;voter.release_seed.side_effect=release
            clients[row['node']]=(voter,calls)
        driver.client=lambda row:clients[row['node']][0]
        driver.recover()
        histories=driver.recovery_histories()
        for _voter,calls in clients.values():
            self.assertEqual(calls,[(step,h['group']) for h in histories for step in ('prepare','seed','release')])
        staged=next(c.args[1]['replacements'] for c in driver.control.call_args_list if c.args[0]=='recovery-stage')
        self.assertEqual([h['epoch'] for h in staged],[4,8])
        self.assertEqual(driver.activate_target.call_count,2)

    def test_unknown_seed_never_releases_original_input_or_prepares_another_range(self):
        driver=self.recovery_driver();voter=Mock()
        voter.seed.side_effect=VoterFailure('lost seed reply')
        driver.client=Mock(return_value=voter)
        with self.assertRaises(VoterFailure):driver.recover()
        voter.release_seed.assert_not_called();self.assertEqual(voter.prepare_seed.call_count,1)
        driver.attach_targets.assert_not_called()

    def test_recovered_cell_requests_use_their_own_new_incarnation(self):
        driver=self.recovery_driver()
        driver.model['replacements']=[dict(driver.model['replacement'],cells=['Counter:alpha']),
            {'group':'06'*16,'incarnation':'07'*16,'cells':['Counter:beta']}]
        client=Mock();driver.client=Mock(return_value=client)
        for cell,expected in [('Counter:alpha','02'*16),('Counter:beta','07'*16)]:
            driver.query_target({},cell)
            self.assertEqual(bytes(client.call.call_args.args[0]['incarnation']).hex(),expected)
        with self.assertRaisesRegex(ValueError,'exactly one restored range'):driver.query_target({},'Counter:missing')

    def test_published_child_activation_reconciles_only_a_returned_proposal(self):
        driver=object.__new__(PublishedTransitionQualification)
        driver.model={'bootstrap':{'catalog_id':'catalog','namespace_digest':'namespace'}}
        client=Mock();driver.client=Mock(return_value=client)
        driver.event=Mock(side_effect=lambda _label,_request,invoke:invoke())
        child={'group':{'group_id':'01'*16,'incarnation_id':'02'*16}}
        applied={'applied':{'term':2,'index':3}}
        client.activate_child.side_effect=[{'applied':None},applied]
        with patch('managed_group_published.time.sleep'):
            self.assertEqual(driver.activate_child({},child,{}),applied)
        first,second=driver.event.call_args_list
        self.assertIs(first.args[1],second.args[1])
        client.activate_child.reset_mock(side_effect=True)
        client.activate_child.side_effect=VoterFailure('lost child activation reply')
        with self.assertRaises(VoterFailure):driver.activate_child({},child,{})
        self.assertEqual(client.activate_child.call_count,1)

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

# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import copy
import io
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
import urllib.error
from unittest.mock import patch

from primitive_compare import Engine, GIB, parse_cgroup, validate_rows, wait_store_ready, request, serving_leaders, sample_container, record_load_evidence
from primitive_load import latency_summary, value_for, window


def rows(family='sql'):
    result = []
    for phase in (['warmup', 'enqueue'] if family == 'queue' else ['warmup', 'write', 'read']):
        samples = [1000, 4000]
        row = {'repeat':0,'family':family,'concurrency':1,'phase':phase,'requested_seconds':3,
               'elapsed_seconds':3,'verified':True,'errors':[],'attempts':2,'successful_requests':2,
               'attempt_latency_us':samples,'success_latency_us':samples,'attempt_latency':latency_summary(samples),
               'success_latency':latency_summary(samples),'requests_per_second':2/3}
        if family == 'queue':
            row.update(acknowledged_messages=16,delivered_messages=16,missing_messages=[],extra_messages=[],
                       queue_metrics=[{'backlog':0,'dead':0}],enqueue_and_drain_seconds=4,
                       enqueue_messages_per_second=16/3,verified_messages_per_second_including_drain=4)
        result.append(row)
    return result


def topology(family='sql', leaders=(1,2,3)):
    total = 48 if family == 'queue' else 24
    profile = {'family':family,'logicalCells':total,'groups':3,'votersPerGroup':3,'cellsPerGroup':total//3}
    return [{'node':node,'profile':copy.deepcopy(profile),
             'groups':[{'cells':total//3,'leader':leader,'term':2,'ready':node==leader} for leader in leaders],
             'resources':{'physical_counts':{'durable_groups':3,'attached_groups':3,'unattached_groups':0,'dormant_groups':0,'pending_groups':0}}}
            for node in range(1,4)]


class ComparisonEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.args = SimpleNamespace(repetitions=1, concurrency=[1], warmup=3, seconds=3)

    def test_incomplete_or_manipulated_windows_cannot_qualify(self):
        validate_rows(rows(), 'sql', self.args)
        for mutation in ['missing','failed','errors','latency','rate','duration']:
            value = copy.deepcopy(rows())
            if mutation == 'missing':
                value.pop()
            elif mutation == 'failed':
                value[1]['verified'] = False
            elif mutation == 'errors':
                value[1]['errors'] = [{'mutation_outcome':'unconfirmed'}]
            elif mutation == 'latency':
                value[1]['success_latency']['p99_ms'] = 0
            elif mutation == 'rate':
                value[1]['requests_per_second'] = 99
            else:
                value[1]['elapsed_seconds'] = 1
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                validate_rows(value, 'sql', self.args)

    def test_queue_ack_delivery_and_drain_all_required(self):
        validate_rows(rows('queue'), 'queue', self.args)
        for field, replacement in [('delivered_messages',15),('acknowledged_messages',15),
                                   ('extra_messages',['unknown']),('queue_metrics',[{'backlog':1,'dead':0}]),
                                   ('enqueue_messages_per_second',99),('verified_messages_per_second_including_drain',99),
                                   ('enqueue_and_drain_seconds',1)]:
            value=copy.deepcopy(rows('queue'))
            value[1][field]=replacement
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_rows(value,'queue',self.args)

    def test_kernel_limits_are_checked_against_requested_profile(self):
        source=f'cpu.max\n100000 100000\nmemory.max\n{2*GIB}\nmemory.swap.max\n0\ncpu.stat\nusage_usec 12\nmemory.current\n1024\nmemory.peak\n2048\nmemory.events\noom 0\noom_kill 0\nmemory.stat\nanon 512\n'
        self.assertEqual(parse_cgroup(source)['cpu.stat']['usage_usec'],12)
        for bad in [source.replace('100000 100000','200000 100000'),source.replace('100000 100000','max 100000'),
                    source.replace(str(2*GIB),str(3*GIB)),source.replace('memory.swap.max\n0','memory.swap.max\n1')]:
            with self.assertRaises(ValueError):
                parse_cgroup(bad)

    def test_wrong_container_owner_image_or_limits_refuse(self):
        state={'id':'exact','owner':'mine','cpus':10**9,'memory':2*GIB,'swap':2*GIB,'cpuset':'1','image':'sha256:one'}
        engine=Engine('unused',{})
        engine.inspect=lambda _: state
        engine.owned('exact','mine',1,'sha256:one')
        for field,replacement in [('id','other'),('owner','foreign'),('cpus',0),('memory',0),('swap',3*GIB),('cpuset','0'),('image','sha256:other')]:
            old=state[field]
            state[field]=replacement
            with self.subTest(field=field),self.assertRaises(ValueError):
                engine.owned('exact','mine',1,'sha256:one')
            state[field]=old

    def test_healthy_followers_are_enrolled_but_only_leaders_are_ready(self):
        for family in ['sql','kv','queue']:
            self.assertEqual(serving_leaders(topology(family),family),[1,2,3])
            self.assertEqual(serving_leaders(topology(family,(1,1,2)),family),[1,1,2])

    def test_sampler_accepts_only_successful_owned_load_exit_race(self):
        initial={'id':'load','owner':'mine','cpus':10**9,'memory':2*GIB,'swap':2*GIB,
                 'cpuset':'4','image':'sha256:one','restarts':0,
                 'state':{'Running':True,'ExitCode':0,'OOMKilled':False}}
        final=copy.deepcopy(initial)
        final['state']['Running']=False
        engine=Engine('unused',{})
        with patch.object(engine,'inspect',side_effect=[initial,final]), patch.object(engine,'call',side_effect=ValueError('exec failed')):
            cgroup, terminal=sample_container(engine,'load','mine',4,'sha256:one','load')
        self.assertIsNone(cgroup)
        self.assertEqual(terminal,final)
        for mutation in ['running','exit','oom','restart','owner','image','limits','node']:
            stopped=copy.deepcopy(final)
            load='load'
            if mutation=='running':stopped['state']['Running']=True
            elif mutation=='exit':stopped['state']['ExitCode']=1
            elif mutation=='oom':stopped['state']['OOMKilled']=True
            elif mutation=='restart':stopped['restarts']=1
            elif mutation=='owner':stopped['owner']='foreign'
            elif mutation=='image':stopped['image']='sha256:other'
            elif mutation=='limits':stopped['memory']=3*GIB
            else:load='other-load'
            with self.subTest(mutation=mutation), patch.object(engine,'inspect',side_effect=[initial,stopped]), patch.object(engine,'call',side_effect=ValueError('exec failed')):
                with self.assertRaises(ValueError):
                    sample_container(engine,'load','mine',4,'sha256:one',load)

    def test_failed_controller_recovers_emitted_windows_without_promoting_case(self):
        with tempfile.TemporaryDirectory() as directory:
            folder=Path(directory)
            observed=rows()
            (folder/'load.log').write_text('\n'.join('PRIMITIVE_COMPARE '+json.dumps(row) for row in observed)+'\n')
            record={'status':'FAILED','error':'load exited during sampling'}
            self.assertEqual(record_load_evidence(folder,record),observed)
            self.assertEqual(json.loads((folder/'windows.json').read_text()),observed)
            self.assertEqual(record['status'],'FAILED')
            self.assertEqual(record['error'],'load exited during sampling')
            self.assertEqual(len(record['windows']),3)
            self.assertNotIn('attempt_latency_us',record['windows'][0])
            self.assertEqual(record_load_evidence(folder,record),observed)
            (folder/'windows.json').write_text('[]\n')
            with self.assertRaisesRegex(ValueError,'differs from retained load log'):
                record_load_evidence(folder,record)
            self.assertEqual((folder/'windows.json').read_text(),'[]\n')
            self.assertEqual(record['status'],'FAILED')

    def test_unready_leader_missing_replica_or_divergent_inventory_refuses(self):
        for mutation in ['leader_not_ready','follower_claims_ready','missing_node','missing_group','wrong_cells','leader_disagrees','term_disagrees','wrong_node','missing_inventory','pending_group','profile_mismatch']:
            states=topology()
            if mutation=='leader_not_ready':states[0]['groups'][0]['ready']=False
            elif mutation=='follower_claims_ready':states[1]['groups'][0]['ready']=True
            elif mutation=='missing_node':states.pop()
            elif mutation=='missing_group':states[1]['groups'].pop()
            elif mutation=='wrong_cells':states[1]['groups'][0]['cells']=7
            elif mutation=='leader_disagrees':states[1]['groups'][0]['leader']=2
            elif mutation=='term_disagrees':states[1]['groups'][0]['term']=3
            elif mutation=='wrong_node':states[1]['node']=1
            elif mutation=='missing_inventory':states[1]['resources']['physical_counts'].pop('durable_groups')
            elif mutation=='pending_group':states[1]['resources']['physical_counts']['pending_groups']=1
            else:states[1]['profile']['logicalCells']=23
            with self.subTest(mutation=mutation),self.assertRaises(ValueError):
                serving_leaders(states,'sql')

    def test_admin_refusal_keeps_the_typed_failure_without_retry(self):
        failure=urllib.error.HTTPError('http://owned/state',503,'unavailable',{},io.BytesIO(b'{"error":"UnsupportedCapability"}'))
        with patch('primitive_compare.urllib.request.urlopen',side_effect=failure) as call:
            with self.assertRaisesRegex(ValueError,'UnsupportedCapability'):
                request('http://owned/state','test-token')
        self.assertEqual(call.call_count,1)

    def test_store_readiness_does_not_accept_liveness_or_retry_mutations(self):
        class Ready:
            status=200
            def __enter__(self):return self
            def __exit__(self,*_):pass
        unavailable=urllib.error.HTTPError('http://store/health/ready',503,'not ready',{},None)
        with patch('primitive_compare.urllib.request.urlopen',side_effect=[unavailable,Ready()]) as request, patch('primitive_compare.time.sleep'):
            wait_store_ready('http://store')
        self.assertEqual(request.call_count,2)
        self.assertTrue(all(call.args[0]=='http://store/health/ready' for call in request.call_args_list))
        with patch('primitive_compare.urllib.request.urlopen',side_effect=unavailable), patch('primitive_compare.time.monotonic',side_effect=[0,61]):
            with self.assertRaisesRegex(RuntimeError,'readiness failed'):
                wait_store_ready('http://store')

    def test_unknown_mutation_stops_worker_without_fresh_identity_retry(self):
        calls=[]
        class FailingClient:
            def __init__(self,*_):pass
            def close(self):pass
            def request(self,node,operation,body):
                calls.append((node,operation,body))
                raise TimeoutError('response lost after possible commit')
        with patch('primitive_load.Client',FailingClient):
            result=window(['a','b','c'],'secret','kv','case','write',1,1)
        self.assertFalse(result['verified'])
        self.assertEqual(len(calls),1)
        self.assertEqual(result['attempts'],1)
        self.assertEqual(result['successful_requests'],0)
        self.assertEqual(result['errors'][0]['mutation_outcome'],'unconfirmed')
        self.assertEqual(result['errors'][0]['sequence'],1)
        self.assertEqual(len(value_for('any identity')),256)


if __name__=='__main__':
    unittest.main()

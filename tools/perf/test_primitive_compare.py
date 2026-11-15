# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import copy
import hashlib
import io
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import threading
import unittest
import urllib.error
from unittest.mock import patch

from primitive_compare import Engine, GIB, parse_cgroup, validate_rows, wait_store_ready, request, serving_leaders, sample_container, record_load_evidence, capture_raft_failure
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
            batches={f'fixture.{phase}.0.{sequence}':
                     [{'id':f'fixture.{phase}.0.{sequence}.{item}',
                       'value':value_for(f'fixture.{phase}.0.{sequence}')} for item in range(8)]
                     for sequence in [1,2]}
            hashes={message['id']:hashlib.sha256(message['value'].encode()).hexdigest()
                    for messages in batches.values() for message in messages}
            metrics=[{'backlog':0,'dead':0} for _ in range(24)]
            row.update(acknowledged_messages=16,delivered_messages=16,missing_messages=[],extra_messages=[],
                       queue_metrics=metrics,enqueue_and_drain_seconds=4,duplicate_deliveries=0,
                       enqueue_messages_per_second=16/3,verified_messages_per_second_including_drain=4,
                       attempted_batches=batches,acknowledged_message_hashes=hashes,
                       delivery_probe_round=1,delivery_probe_complete=True,delivery_set_verified=True,
                       delivery_observations={str(node):{'round':1,'messages':
                           {key:[value,1] for key,value in hashes.items()} if node==0 else {}}
                                              for node in range(3)},
                       queue_metrics_probe_round=1,queue_metrics_probe_complete=True,
                       queue_settlement_verified=True,
                       queue_metric_observations={str(entity):{'round':1,'metrics':metric}
                                                  for entity,metric in enumerate(metrics)})
        elif phase != 'read':
            row['prefix'] = f'fixture.{phase}'
            acknowledged = [{'entity': sequence - 1, 'key': row['prefix'] + '.0',
                             'identity': f"{row['prefix']}.0.{sequence}",
                             'value_sha256': hashlib.sha256(value_for(f"{row['prefix']}.0.{sequence}").encode()).hexdigest()}
                            for sequence in [1, 2]]
            row.update(producer_workers=[{'worker': 0, 'attempts': 2, 'acknowledged': 2}],
                       acknowledged_writes=acknowledged, unconfirmed_writes=[], verified_keys=2,
                       readback_probe_pending=None, readback_probe_complete=True,
                       readback_observations=[{'entity': item['entity'], 'key': item['key'],
                           'expected_identity': item['identity'], 'expected_sha256': item['value_sha256'],
                           'observed_type': 'str', 'observed_sha256': item['value_sha256'],
                           'matching_unconfirmed_identities': []} for item in acknowledged])
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

    def test_failed_node_observation_keeps_other_owners_and_missing_node_explicit(self):
        states=topology()
        with patch('primitive_compare.request',side_effect=[states[0],ValueError('owner closed'),states[2]]) as observe:
            evidence=capture_raft_failure(['http://node1','http://node2','http://node3'],'test-token')
        self.assertEqual(evidence['observations'],[states[0],states[2]])
        self.assertEqual(evidence['errors'],[{'node':2,'error':'owner closed'}])
        self.assertEqual(observe.call_count,3)
        self.assertEqual([call.args[0] for call in observe.call_args_list],['http://node1/state','http://node2/state','http://node3/state'])
        with self.assertRaises(ValueError):
            serving_leaders(evidence['observations'],'sql')

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
        duplicated=rows('queue')
        duplicated[1]['delivery_observations']['1']['messages']=copy.deepcopy(
            duplicated[1]['delivery_observations']['0']['messages'])
        duplicated[1]['duplicate_deliveries']=16
        validate_rows(duplicated,'queue',self.args)
        for field, replacement in [('delivered_messages',15),('acknowledged_messages',15),
                                   ('extra_messages',['unknown']),('queue_metrics',[{'backlog':1,'dead':0}]),
                                   ('enqueue_messages_per_second',99),('verified_messages_per_second_including_drain',99),
                                   ('enqueue_and_drain_seconds',1)]:
            value=copy.deepcopy(rows('queue'))
            value[1][field]=replacement
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_rows(value,'queue',self.args)

    def test_queue_api_float_counts_require_complete_zero_settlement(self):
        # QueueMetrics exposes f64 counts, so real JSON replies contain 0.0.
        value = rows('queue')
        for row in value:
            for metric in row['queue_metrics']:
                metric.update(backlog=0.0, dead=0.0)
        validate_rows(value, 'queue', self.args)
        for field in ['backlog', 'dead']:
            for count in [True, False, '0', None, -1, -1.0, 0.5,
                          float('nan'), float('inf'), float('-inf'), 1, 1.0]:
                invalid = copy.deepcopy(value)
                invalid[1]['queue_metrics'][0][field] = count
                # Keep the observer in agreement: invalid or unsettled counts
                # must fail independently of the existing observer proof.
                invalid[1]['queue_metric_observations']['0']['metrics'][field] = count
                with self.subTest(field=field, count=count), self.assertRaises(ValueError):
                    validate_rows(invalid, 'queue', self.args)

    def test_queue_ledger_mismatch_or_partial_probe_cannot_qualify(self):
        for mutation in ['missing-ledger','hash','body','duplicate-id','delivery-round',
                         'missing-node','delivery-count','duplicate-count','metrics-round',
                         'missing-metric','delivery-incomplete','settlement-unverified']:
            value=copy.deepcopy(rows('queue'))
            row=value[1]
            first=next(iter(row['acknowledged_message_hashes']))
            batch=next(iter(row['attempted_batches']))
            if mutation=='missing-ledger':row.pop('acknowledged_message_hashes')
            elif mutation=='hash':row['acknowledged_message_hashes'][first]='00'*32
            elif mutation=='body':row['attempted_batches'][batch][0]['value']='changed'
            elif mutation=='duplicate-id':row['attempted_batches'][batch][1]['id']=first
            elif mutation=='delivery-round':row['delivery_observations']['0']['round']=0
            elif mutation=='missing-node':row['delivery_observations'].pop('2')
            elif mutation=='delivery-count':row['delivery_observations']['0']['messages'][first][1]=True
            elif mutation=='duplicate-count':row['delivery_observations']['0']['messages'][first][1]=2
            elif mutation=='metrics-round':row['queue_metric_observations']['0']['round']=0
            elif mutation=='missing-metric':row['queue_metric_observations'].pop('23')
            elif mutation=='delivery-incomplete':row['delivery_probe_complete']=False
            else:row['queue_settlement_verified']=False
            with self.subTest(mutation=mutation),self.assertRaises(ValueError):
                validate_rows(value,'queue',self.args)

    def test_sql_kv_write_ledgers_cannot_be_missing_or_manipulated(self):
        for family in ['sql', 'kv']:
            validate_rows(rows(family), family, self.args)
            for mutation in ['missing', 'worker-count', 'worker-bool', 'ack-identity', 'ack-entity',
                             'ack-hash', 'duplicate-key', 'missing-readback', 'observed-hash',
                             'observed-type', 'pending-probe', 'incomplete-probe', 'unconfirmed-write']:
                value = rows(family)
                row = value[1]
                if mutation == 'missing': row.pop('acknowledged_writes')
                elif mutation == 'worker-count': row['producer_workers'][0]['acknowledged'] = 1
                elif mutation == 'worker-bool': row['producer_workers'][0]['worker'] = False
                elif mutation == 'ack-identity': row['acknowledged_writes'][0]['identity'] = 'foreign'
                elif mutation == 'ack-entity': row['acknowledged_writes'][0]['entity'] = 12
                elif mutation == 'ack-hash': row['acknowledged_writes'][0]['value_sha256'] = '00' * 32
                elif mutation == 'duplicate-key': row['acknowledged_writes'][1] = copy.deepcopy(row['acknowledged_writes'][0])
                elif mutation == 'missing-readback': row['readback_observations'].pop()
                elif mutation == 'observed-hash': row['readback_observations'][0]['observed_sha256'] = '00' * 32
                elif mutation == 'observed-type': row['readback_observations'][0]['observed_type'] = 'bytes'
                elif mutation == 'pending-probe': row['readback_probe_pending'] = {'entity': 0}
                elif mutation == 'incomplete-probe': row['readback_probe_complete'] = False
                else: row['unconfirmed_writes'] = [copy.deepcopy(row['acknowledged_writes'][0])]
                with self.subTest(family=family, mutation=mutation), self.assertRaises(ValueError):
                    validate_rows(value, family, self.args)

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

    def test_sql_kv_failures_keep_producer_and_readback_evidence(self):
        for family in ['sql', 'kv']:
            for failure in ['overwrite', 'constructor', 'partial']:
                with self.subTest(family=family, failure=failure):
                    clock, database, instances, writes, reads = [100.0], {}, [], [], []
                    class FailingClient:
                        def __init__(self, *_):
                            instances.append(self)
                            if failure == 'constructor' and len(instances) == 2:
                                raise ValueError('verifier unavailable')
                        def close(self): pass
                        def request(self, node, operation, body):
                            if operation == 'write':
                                writes.append(dict(body))
                                database[body['entity'], body['key']] = body['value']
                                clock[0] += .01 if failure == 'overwrite' else 2 if failure == 'constructor' else .6
                                if failure == 'overwrite' and len(writes) == 25:
                                    raise TimeoutError('response lost after possible commit')
                                return True
                            reads.append(dict(body))
                            if failure == 'partial' and len(reads) == 2:
                                raise TimeoutError('readback unavailable')
                            return database[body['entity'], body['key']]
                    with patch('primitive_load.Client', FailingClient), patch('primitive_load.time.monotonic', side_effect=lambda: clock[0]):
                        row = window(['a', 'b', 'c'], 'secret', family, 'proof', 'write', 1, 1)
                    self.assertFalse(row['verified'])
                    count = 24 if failure == 'overwrite' else 1 if failure == 'constructor' else 2
                    self.assertEqual(len(row['acknowledged_writes']), count)
                    self.assertFalse(row['readback_probe_complete'])
                    if failure == 'overwrite':
                        self.assertEqual(len(writes), 25)
                        self.assertEqual(row['successful_requests'], 24)
                        self.assertEqual(row['errors'][0]['sequence'], 25)
                        self.assertEqual(row['errors'][0]['mutation_outcome'], 'unconfirmed')
                        observation = row['readback_observations'][0]
                        self.assertEqual(observation['expected_identity'], 'proof.0.1')
                        self.assertEqual(observation['expected_sha256'], hashlib.sha256(value_for('proof.0.1').encode()).hexdigest())
                        self.assertEqual(observation['observed_sha256'], hashlib.sha256(value_for('proof.0.25').encode()).hexdigest())
                        self.assertEqual(observation['matching_unconfirmed_identities'], ['proof.0.25'])
                        self.assertIn('write readback mismatch', row['verification_error'])
                    elif failure == 'partial':
                        self.assertEqual(len(row['readback_observations']), 1)
                        self.assertEqual(row['readback_probe_pending'],
                                         {'entity': 1, 'key': 'proof.0', 'expected_identity': 'proof.0.2'})
                    else:
                        self.assertEqual(row['readback_observations'], [])
                        self.assertIsNone(row['readback_probe_pending'])

    def test_sql_kv_write_readback_qualifies_with_wrapped_worker_keys(self):
        for family in ['sql', 'kv']:
            for concurrency in [1, 4, 16]:
                with self.subTest(family=family, concurrency=concurrency):
                    clock, database, lock = [100.0], {}, threading.Lock()
                    class VerifiedClient:
                        def __init__(self, *_): pass
                        def close(self): pass
                        def request(self, node, operation, body):
                            with lock:
                                if operation == 'write':
                                    database[body['entity'], body['key']] = body['value']
                                    clock[0] += 1 / (32 * concurrency)
                                    return True
                                if body['key'] == 'readseed':
                                    clock[0] += 1 / (32 * concurrency)
                                    return value_for('readseed')
                                return database[body['entity'], body['key']]
                    observed = []
                    with patch('primitive_load.Client', VerifiedClient), patch('primitive_load.time.monotonic', side_effect=lambda: clock[0]):
                        for phase in ['warmup', 'write', 'read']:
                            row = window(['a', 'b', 'c'], 'secret', family, 'healthy.' + phase, phase, concurrency, 1)
                            row.update(family=family, repeat=0)
                            observed.append(row)
                    validate_rows(observed, family, SimpleNamespace(repetitions=1, concurrency=[concurrency], warmup=1, seconds=1))
                    for row in observed[:2]:
                        self.assertTrue(row['readback_probe_complete'])
                        self.assertEqual(sum(p['acknowledged'] for p in row['producer_workers']), row['successful_requests'])
                        self.assertEqual(len(row['acknowledged_writes']), row['verified_keys'])

    def test_sql_kv_complete_readback_of_wrong_or_missing_value_still_fails(self):
        for family in ['sql', 'kv']:
            for observed in ['wrong value', None]:
                with self.subTest(family=family, observed=observed):
                    clock = [100.0]
                    class MismatchedClient:
                        def __init__(self, *_): pass
                        def close(self): pass
                        def request(self, node, operation, body):
                            if operation == 'write':
                                clock[0] += 2
                                return True
                            return observed
                    with patch('primitive_load.Client', MismatchedClient), patch('primitive_load.time.monotonic', side_effect=lambda: clock[0]):
                        row = window(['a', 'b', 'c'], 'secret', family, 'mismatch', 'write', 1, 1)
                    self.assertFalse(row['verified'])
                    self.assertEqual(row['errors'], [])
                    self.assertTrue(row['readback_probe_complete'])
                    self.assertEqual(row['readback_observations'][0]['matching_unconfirmed_identities'], [])
                    self.assertEqual(row['readback_observations'][0]['observed_sha256'],
                                     hashlib.sha256(observed.encode()).hexdigest() if observed is not None else None)
                    self.assertIn('write readback mismatch', row['verification_error'])

    def test_queue_failures_keep_producer_and_partial_verifier_ledgers(self):
        for failure in ['constructor', 'delivery', 'metrics', 'partial-metrics', 'unknown-enqueue']:
            with self.subTest(failure=failure):
                attempted=[]
                instances=[]
                class FailingVerifier:
                    def __init__(self,*_):
                        instances.append(self)
                        if failure=='constructor' and len(instances)==2:
                            raise ValueError('verifier unavailable')
                    def close(self):pass
                    def request(self,node,operation,body):
                        if operation=='write':
                            attempted.append(body['messages'])
                            if len(attempted)==2:
                                raise TimeoutError('enqueue response lost')
                            return True
                        if operation=='deliveries':
                            if failure=='delivery' and node==1:
                                raise ValueError('delivery probe unavailable')
                            messages=attempted[0][:4] if failure=='delivery' else attempted[0]
                            return {m['id']:[hashlib.sha256(m['value'].encode()).hexdigest(),1]
                                    for m in messages} if node==0 else {}
                        if operation=='metrics':
                            if failure=='metrics' or (failure=='partial-metrics' and body['entity']==3):
                                raise ValueError('metrics probe unavailable')
                            return {'backlog':0,'dead':0}
                        raise AssertionError(operation)
                with patch('primitive_load.Client',FailingVerifier):
                    result=window(['a','b','c'],'secret','queue','queue-proof','enqueue',1,1)
                self.assertFalse(result['verified'])
                self.assertEqual(len(attempted),2)
                self.assertEqual(result['attempts'],2)
                self.assertEqual(result['successful_requests'],1)
                self.assertEqual(result['errors'][0]['mutation_outcome'],'unconfirmed')
                self.assertEqual(result['errors'][0]['sequence'],2)
                self.assertEqual(result['acknowledged_messages'],8)
                self.assertEqual(result['acknowledged_message_hashes'],
                                 {m['id']:hashlib.sha256(m['value'].encode()).hexdigest()
                                  for m in attempted[0]})
                self.assertEqual(result['attempted_batches'],
                                 {'queue-proof.0.1':attempted[0],'queue-proof.0.2':attempted[1]})
                self.assertEqual(result['queue_settlement_verified'],failure=='unknown-enqueue')
                self.assertEqual(result['queue_metrics_probe_complete'],failure=='unknown-enqueue')
                if failure=='constructor':
                    self.assertFalse(result['delivery_probe_complete'])
                    self.assertEqual(result['delivery_observations'],{})
                elif failure=='delivery':
                    self.assertFalse(result['delivery_probe_complete'])
                    observation=result['delivery_observations']['0']
                    self.assertEqual(observation['round'],1)
                    self.assertEqual(len(observation['messages']),4)
                elif failure=='unknown-enqueue':
                    self.assertTrue(result['delivery_set_verified'])
                    self.assertEqual(len(result['queue_metric_observations']),24)
                    self.assertIn('request failures retained',result['verification_error'])
                else:
                    self.assertTrue(result['delivery_probe_complete'])
                    self.assertEqual(len(result['queue_metric_observations']),
                                     3 if failure=='partial-metrics' else 0)
                    self.assertEqual(result['queue_metrics_probe_round'],1)

    def test_queue_complete_delivery_and_settlement_still_qualifies(self):
        clock=[100.0]
        messages={}
        class SettledClient:
            def __init__(self,*_):pass
            def close(self):pass
            def request(self,node,operation,body):
                if operation=='write':
                    for message in body['messages']:
                        messages[message['id']]=[hashlib.sha256(message['value'].encode()).hexdigest(),1]
                    clock[0]+=2
                    return True
                if operation=='deliveries':
                    return {key:value for key,value in messages.items()
                            if key.startswith(body['prefix'])} if node==0 else {}
                if operation=='metrics':return {'backlog':0,'dead':0}
                raise AssertionError(operation)
        observed=[]
        with patch('primitive_load.Client',SettledClient),patch('primitive_load.time.monotonic',side_effect=lambda:clock[0]):
            for phase in ['warmup','enqueue']:
                row=window(['a','b','c'],'secret','queue','settled.'+phase,phase,1,1)
                row.update(family='queue',repeat=0)
                observed.append(row)
        validate_rows(observed,'queue',SimpleNamespace(repetitions=1,concurrency=[1],warmup=1,seconds=1))
        for row in observed:
            self.assertTrue(row['verified'])
            self.assertTrue(row['delivery_probe_complete'])
            self.assertTrue(row['delivery_set_verified'])
            self.assertTrue(row['queue_metrics_probe_complete'])
            self.assertTrue(row['queue_settlement_verified'])
            self.assertEqual(row['acknowledged_messages'],8)
            self.assertEqual(len(row['queue_metric_observations']),24)


if __name__=='__main__':
    unittest.main()

#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Real managed SDK RF3 source cohort for the fenced recovery qualification.

Bootstraps an explicit colocated-development namespace, admits measured Docker
voters, provisions one shared group and verifies SQL/receipts through retained
restart. This establishes the source side of the managed disaster drill. Full
transition recovery and all-source-disk deletion remain separate required gates.
"""
import argparse
import hashlib
import ipaddress
import json
from pathlib import Path
import secrets
import socket
import time

from group_disaster import Qualification, qualification_subnet
from group_fence import save
from managed_group_configuration import copy as copy_managed
from managed_group_fence import ManagedLauncher
from managed_group_voter import ManagedVoter, VoterFailure, VoterCohortFailure
from raft_disaster import command, owned_container
from raft_run import ROOT, require

GIB = 1024**3


def nonce():
    value = secrets.token_hex(16)
    require(value != '0'*32, 'invalid generated identity')
    return value


def verify_retry(original, replay):
    # CommandReceipt.version is an observation. S3 settlement may return a
    # later complete image containing the same command receipt, not its first
    # observation. The surrounding SQL/version checks detect extra application.
    require(all(replay[key]==original[key]
            for key in ('result','group','incarnation','durability'))
            and replay['version']>=original['version'],
            'original command receipt changed')


class ManagedQualification(Qualification):
    def __init__(self, args):
        super().__init__(args)
        self.launcher = ManagedLauncher(self.root, self.run_id)
        self.model = {'format':1, 'cluster':nonce(), 'fleet_incarnation':nonce(),
                      'namespace':nonce(), 'group':nonce(), 'incarnation':nonce(),
                      'namespace_request':nonce(), 'namespace_operation':nonce(),
                      'events':{}, 'nodes':[], 'cells':{}}
        self.report.update(suite='managed-source', full_dr_completed=False,
            limitations=['Single VM with explicit Nodes separation; no independent host failure qualification.',
                'Counter/one namespace functional envelope; positive managed recovery and all six CG-13 cases remain unqualified.',
                'CPU/memory/swap limits are kernel enforced. The 1-GiB durable budget is an explicit deployment admission budget, not a filesystem quota.',
                'Correctness qualification, not load, throughput or wake-latency qualification.'])

    def save(self):
        super().save()
        save(self.root/'state.json', self.model)

    def event(self, label, request, invoke):
        require(label not in self.model['events'], 'operation already entered; reconcile its original request')
        self.model['events'][label] = {'request':request, 'outcome':'unknown'}
        self.save()
        try:
            result = invoke()
        except VoterCohortFailure as error:
            self.model['events'][label]['voter_outcomes'] = error.outcomes
            self.save()
            raise
        self.model['events'][label].update(outcome='returned', result=result)
        self.save()
        return result

    def setup(self):
        info = json.loads(self.docker('info', '--format', '{{json .}}'))
        require(info['NCPU'] >= 6 and info['MemTotal'] >= 14*GIB+GIB//2,
                'managed voters, operator and RustFS require 6 CPUs / 14.5 GiB')
        self.physical_host = hashlib.sha256(('docker-daemon/'+info['ID']).encode()).hexdigest()[:32]
        super().setup()

    def common(self):
        return {key:self.config[key] for key in ('endpoint','bucket','prefix','access','secret')}

    def operator(self, action, *_args, **_kwargs):
        # Parent setup invokes only this explicit bootstrap. No unmanaged init.
        require(action == 'init', 'managed setup cannot invoke an unmanaged operator')
        body = dict(self.common(), bucket_prefix='tenant/',
            cluster=list(bytes.fromhex(self.model['cluster'])),
            incarnation=list(bytes.fromhex(self.model['fleet_incarnation'])),
            namespace=list(bytes.fromhex(self.model['namespace'])),
            coordination_prefix='hosts/', namespace_prefix='managed/',
            image_storage='Extents' if self.config['rooted'] else 'Materialized', replica_separation='Nodes',
            request={'id':list(bytes.fromhex(self.model['namespace_request'])),
                'operation':list(bytes.fromhex(self.model['namespace_operation'])),
                'principal':'managed-drill','expected_revision':1})
        result = self._operator('bootstrap', 'managed-bootstrap', body)
        self.model['bootstrap'] = result
        self.model['revision'] = result['fleet_revision']
        self.save()
        return result

    def control(self, label, payload):
        body = dict(self.common(), bucket_prefix='tenant/', connection=self.model['bootstrap']['connection'],
            principal='managed-drill', namespace_id=self.model['namespace'],
            fence_public=self.config['fence_public'], command=payload)
        return self._operator(label, 'managed-recovery', body)

    def _operator(self, label, mode, body):
        def invoke():
            identifier = self.docker('create', '--label', self.owner, '--network', self.prefix,
                '--cpus','1','--memory','3g','--memory-swap','3g',
                '-e','CELLAR_GROUP_ACTION='+mode, self.args.image)
            self.capture_resources(owned_container(self.docker, identifier, self.run_id), 3)
            hashes = copy_managed(self.args.context, identifier, self.run_id,
                {'config.json':json.dumps(body).encode()})
            self.report.setdefault('configuration_copies', []).append({'container_id':identifier,'sha256':hashes})
            result = command(['docker','--context',self.args.context,'start','--attach',identifier],check=False)
            actual = owned_container(self.docker, identifier, self.run_id)
            require(not actual['State']['Running'] and not actual['State']['OOMKilled'], 'operator did not join')
            (self.root/(label+'.log')).write_text(self.redact(result.stdout+result.stderr))
            self.docker('rm',identifier)
            require(actual['State']['ExitCode'] == 0, 'managed operator failed: '+label)
            reply = json.loads(result.stdout)
            require(reply['managed_recovery'] == 'ok', 'unproved operator reply')
            return reply['result']
        # Credentials stay only in the private config; journal the command hash.
        declaration = body.get('command', {'bootstrap_request':body.get('request')})
        return self.event('operator/'+label, declaration, invoke)

    def client(self, row):
        scope = self.model['bootstrap']
        return ManagedVoter(self.launcher, row['generation'], row['container'], row['endpoint'], self.config['token'],
            row['process'], catalog=scope['catalog_id'], namespace=scope['namespace_digest'])

    def node_config(self, row, retained):
        return dict(self.common(), retained=retained, bucket_prefix='tenant/', connection=self.model['bootstrap']['connection'],
            principal='managed-drill', token=self.config['token'], node=list(bytes.fromhex(row['node'])),
            disk=list(bytes.fromhex(row['disk'])), process=list(bytes.fromhex(row['process'])),
            label=row['label'], server_name='localhost', bind=row['ip'], advertise=row['ip'],
            peer_ports=[17000,17001,17002], api_port=8080,
            ca='/config/ca.der', certificate='/config/server.der', key='/config/key.der',
            fence_public=self.config['fence_public'])

    def copy_node(self, row, retained):
        files={'config.json':json.dumps(self.node_config(row, retained)).encode()}
        for name in ('ca.der','server.der','key.der'):
            files[name]=(ROOT/'crates/cellar/transport/tcp/fixtures'/name).read_bytes()
        hashes=copy_managed(self.args.context,row['container'],self.run_id,files)
        self.report.setdefault('configuration_copies',[]).append({'container_id':row['container'],'sha256':hashes})
        self.save()

    def launch_source(self):
        scope=self.model['bootstrap']
        self.launcher.declare(0,scope['catalog_id'],scope['namespace_digest'],
            [{'group':self.model['group'],'incarnation':self.model['incarnation']}])
        for index in range(3):self.launch_node(index,0)
        for row in self.model['nodes']:self.admit(row)
        self.grant_origins()

    def launch_node(self, index, generation):
        scope=self.model['bootstrap']
        row={'node':nonce(),'disk':nonce(),'process':nonce(),'generation':generation,'index':index,
            'label':f'managed-{self.run_id}-{generation}-{index}',
            'ip':str(ipaddress.ip_network(self.report['network']['subnet'])[11+10*generation+index])}
        descriptor={'node_id':row['node'],'disk_id':row['disk'],'physical_host':self.physical_host,
            'label':row['label'],'server_name':'localhost',
            'capability_digest':scope['capability_digest'],'storage_instance':None}
        basis=self.control(f'register-basis-{generation}-{index}',{'step':'fleet_status'})
        self.model['revision']=basis['observed_revision'];self.save()
        admitted=self.control(f'register-{generation}-{index}',{'step':'register_node','request':{
            'catalog_id':scope['catalog_id'],'request_id':nonce(),
            'expected_revision':self.model['revision'],'descriptor':descriptor}})
        self.model['revision']=admitted['admission']['applied_revision']
        with socket.socket() as listener:
            listener.bind(('127.0.0.1',0));port=listener.getsockname()[1]
        row.update(endpoint='http://127.0.0.1:'+str(port),volume=f'{self.prefix}-managed-{generation}-{index}')
        self.model['nodes'].append(row);self.save()
        self.docker('volume','create','--label',self.owner,row['volume'])

        def invoke():
            identifier=self.docker('create','--label',self.owner,'--restart=no',
                '--name',row['volume'],'--network',self.prefix,'--ip',row['ip'],
                '--cpus','1','--memory','3g','--memory-swap','3g',
                '-p',f'127.0.0.1:{port}:8080','-v',row['volume']+':/data',
                '-e','CELLAR_GROUP_ACTION=managed-node',self.args.image)
            row['container']=identifier;self.save()
            self.copy_node(row,False)
            self.docker('start',identifier)
            return identifier
        self.launcher.launch(generation,row['volume'],row['volume'],invoke,identity={
            'node':row['node'],'disk':row['disk'],'physical_host':self.physical_host})
        self.capture_resources(owned_container(self.docker,row['container'],self.run_id),3)
        observed=self.until(lambda:self.client(row).call({'action':'observe'}),60)
        require(observed['catalog_id']==scope['catalog_id']
                and observed['namespace_digest']==scope['namespace_digest'], 'different running namespace')
        self.model['revision']=observed['fleet_revision'];self.save()
        return row

    def source_nodes(self):
        return [row for row in self.model['nodes'] if row['generation']==0 and row['index']<3]

    def grant_origins(self, rows=None):
        rows=self.source_nodes() if rows is None else rows
        revision=self.client(rows[0]).call({'action':'observe'})['fleet_revision']
        for target in rows:
            for origin in rows:
                if origin is target:continue
                payload={'action':'grant_origin','revision':revision,
                    'node':list(bytes.fromhex(origin['node'])),'disk':list(bytes.fromhex(origin['disk'])),
                    'process':list(bytes.fromhex(origin['process'])),'label':origin['label'],
                    'server_name':'localhost','addresses':[origin['ip']+':'+str(port) for port in (17000,17001,17002)],
                    'read':True,'write':True}
                self.event('grant/'+target['process']+'/'+origin['process'],payload,
                    lambda:self.client(target).call(payload))

    def admit(self,row):
        client=self.client(row);observed=client.call({'action':'observe'})
        actual=owned_container(self.docker,row['container'],self.run_id);limits=actual['HostConfig']
        self.capture_resources(actual,3)
        measured={'container_id':row['container'],'limits':{'nano_cpus':limits['NanoCpus'],
            'memory':limits['Memory'],'memory_swap':limits['MemorySwap']},
            'memory_current':int(self.docker('exec',row['container'],'cat','/sys/fs/cgroup/memory.current')),
            'durable_bytes_used':int(self.docker('exec',row['container'],'du','-sb','/data').split()[0]),
            'declared_durable_budget':GIB,'profile_digest':observed['profile_digest'],'boot':observed['boot']}
        evidence=json.dumps(measured,sort_keys=True).encode();digest=hashlib.sha256(evidence).hexdigest()
        save(self.root/('capacity-'+digest+'.json'),measured)
        attestation={'boot':observed['boot'],'profile_revision':1,
            'profile_digest':list(bytes.fromhex(observed['profile_digest'])),
            'cpu_millis':limits['NanoCpus']//1000000,'memory_bytes':limits['Memory'],
            'durable_bytes':GIB,'engine_slots':observed['group_capacity'],
            'memory_swap_disabled':limits['MemorySwap']==limits['Memory'],
            'used_memory_bytes':measured['memory_current'],'used_durable_bytes':measured['durable_bytes_used'],
            'evidence_digest':list(bytes.fromhex(digest)),'evidence_reference':'capacity-'+digest+'.json'}
        for action in ('qualify_node','activate_node'):
            current=client.call({'action':'observe'})
            payload={'action':action,'request':{'catalog_id':self.model['bootstrap']['catalog_id'],
                'request_id':nonce(),'expected_revision':current['fleet_revision'],'attestation':attestation}}
            label=action+'/'+row['process']
            self.event(label,payload,lambda:client.call(payload))

    def provision_source(self):
        standby=self.control('standby',{'step':'first_standby'})
        plan={**standby,'request_id':nonce(),'operation_id':nonce(),
            'expected_revision':self.client(self.model['nodes'][0]).call({'action':'observe'})['fleet_revision'],
            'publication_id':nonce(),'group_id':self.model['group'],
            'incarnation_id':self.model['incarnation'],'creation_id':nonce(),
            'voters':[{'node_id':row['node'],'member_id':index+1} for index,row in enumerate(self.source_nodes())]}
        self.model['first_plan']=plan;self.save()
        self.control('reserve-first',{'step':'first_reserve','plan':plan})
        for label,step in [('publish-first','first_publish'),('archive-first','first_archive')]:
            revision=self.client(self.model['nodes'][0]).call({'action':'observe'})['fleet_revision']
            self.control(label,{'step':step,'request':{'plan':plan,'request_id':nonce(),'expected_revision':revision}})
        self.start_source('initial')

    def start_source(self,label):
        scope=self.model['bootstrap'];voters=[self.client(row) for row in self.source_nodes()]
        self.event('start/'+label,{'group':self.model['group'],'incarnation':self.model['incarnation']},
            lambda:ManagedVoter.start_cohort(voters,scope['catalog_id'],scope['namespace_digest'],
                self.model['group'],self.model['incarnation']))
        self.until(self.source_leader,60)

    def source_leader(self):
        for index,row in enumerate(self.source_nodes()):
            try:
                value=self.client(row).call({'action':'status',
                    'group':list(bytes.fromhex(self.model['group'])),
                    'incarnation':list(bytes.fromhex(self.model['incarnation']))})
                if value['ready'] and value['leader']==index+1:return row
            except VoterFailure:pass
        return None

    def data(self):
        leader=self.until(self.source_leader,60)
        for name in ('alpha','beta','gamma'):
            cell='Counter:'+name
            common={'cell':cell,'incarnation':list(bytes.fromhex(self.model['incarnation']))}
            enrollment=dict(common, action='enroll',creation=list(bytes.fromhex(nonce())),s3=True)
            self.event('enroll/'+name,enrollment,lambda:self.client(leader).call(enrollment))
            payload=dict(common,action='write',session=list(bytes.fromhex(nonce())),sequence=1,
                retry_until_ms=int(time.time()*1000)+300000,delta=1,s3=True)
            receipt=self.event('write/'+name,payload,lambda:self.client(leader).call(payload))
            require(int.from_bytes(bytes.fromhex(receipt['result']),'big',signed=True)==1
                    and receipt['durability']=='s3' and bytes(receipt['group']).hex()==self.model['group'],
                    'write did not return the exact shared-group S3 result')
            quorum=dict(payload,session=list(bytes.fromhex(nonce())),delta=0,s3=False)
            committed=self.event('quorum-write/'+name,quorum,lambda:self.client(leader).call(quorum))
            require(int.from_bytes(bytes.fromhex(committed['result']),'big',signed=True)==1
                    and committed['durability']=='quorum'
                    and committed['version']==receipt['version']+1
                    and committed['group']==receipt['group'] and committed['incarnation']==receipt['incarnation'],
                    'quorum command did not preserve the applied SQL/group history')
            self.model['cells'][cell]={'request':payload,'receipt':receipt,
                'quorum':{'request':quorum,'receipt':committed}};self.save()
        self.verify_source()

    def verify_source(self):
        for row in self.source_nodes():
            require(self.client(row).call({'action':'observe'})['groups']==1,'expected one shared physical group')
            for cell,record in self.model['cells'].items():
                for minimum in (record['quorum']['receipt']['token'],None):
                    reply=self.client(row).call({'action':'read','cell':cell,
                        'incarnation':list(bytes.fromhex(self.model['incarnation'])),
                        'invocation':list(bytes.fromhex(nonce())),'minimum':minimum})
                    require(int.from_bytes(bytes.fromhex(reply['result']),'big',signed=True)==1
                            and reply['version']==record['quorum']['receipt']['version'],
                            'source SQL or minimum/linearizable read changed')

    def restart_source(self):
        for row in self.source_nodes():
            self.event('stop/'+row['process'], {'action':'stop'},
                lambda:self.client(row).call({'action':'stop'}))
            self.until(lambda:not owned_container(self.docker,row['container'],self.run_id)['State']['Running'],30)
            value=owned_container(self.docker,row['container'],self.run_id)
            require(value['State']['ExitCode']==0 and not value['State']['OOMKilled'],'source stop did not join')
            self.joined_voter(row, 1)
            old=row['process'];row['process']=nonce();self.save()
            def invoke():
                self.copy_node(row,True)
                # Keep the immutable container/volume and explicitly switch its
                # retained flag; Docker cannot mutate a container's environment.
                return self.docker('start',row['container'])
            # The stopped process receives an explicit next-launch flag through
            # its copied config; disk presence never infers the requested mode.
            self.launcher.restart(row['container'],invoke)
            self.until(lambda:self.client(row).call({'action':'observe'}),60)
            self.admit(row)
            require(row['process']!=old,'retained restart reused process identity')
        self.start_source('retained')
        self.grant_origins()
        # Prove the retained state before replay could mask a lost SQL image.
        self.verify_source()
        for cell,record in self.model['cells'].items():
            for kind,original in (('s3',record),('quorum',record['quorum'])):
                leader=self.until(self.source_leader,60)
                replay=self.event('retry/'+cell+'/'+kind,original['request'],
                    lambda:self.client(leader).call(original['request']))
                verify_retry(original['receipt'],replay)
        self.verify_source()

    def joined_voter(self, row, groups):
        logs=self.redact(self.docker('logs',row['container']))
        log_name='joined-'+row['process']+'.log'
        (self.root/log_name).write_text(logs)
        events=[]
        for line in logs.splitlines():
            try: value=json.loads(line)
            except ValueError: continue
            if isinstance(value,dict) and 'managed_voter' in value:events.append(value)
        ready=[index for index,value in enumerate(events)
               if value.get('managed_voter')=='control_ready' and value.get('process')==row['process']]
        require(bool(ready) and any(value.get('managed_voter')=='stopped'
                and value.get('joined') is True and value.get('groups')==groups
                for value in events[ready[-1]+1:]),'missing exact-process joined SDK close inventory')
        self.report.setdefault('joined_voters',[]).append({'container_id':row['container'],
            'process':row['process'],'groups':groups,'log':log_name,
            'log_sha256':hashlib.sha256(logs.encode()).hexdigest()})
        self.save()

    def close(self):
        super().close()
        if self.report['completed']:
            try:
                for row in self.model['nodes']:self.joined_voter(row,1)
            except BaseException as error:
                self.report.update(completed=False,shutdown_verified=False,
                    shutdown_failure=type(error).__name__+': '+self.redact(str(error)))
                self.save()
                raise

    def source_workflow(self):
        self.launch_source();self.provision_source();self.data();self.restart_source()
        self.report['checks'].update(managed_rf3=True,one_group_three_cells=True,
            sdk_s3_writes=True,sdk_quorum_writes=True,minimum_reads_each_ingress=True,
            linearizable_reads_each_ingress=True,retained_restart=True,exact_retry_results=True)
        self.save()

    def drill(self):
        self.source_workflow()
        self.report['completed']=True;self.save()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context',required=True);parser.add_argument('--image',required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--mode',choices=['materialized','extents'],required=True)
    parser.add_argument('--port-base',type=int,default=19800)
    parser.add_argument('--subnet',type=qualification_subnet)
    args=parser.parse_args();require(1024<=args.port_base<65530,'invalid loopback port range')
    q=ManagedQualification(args)
    try:q.setup();q.drill()
    except BaseException as error:
        q.report['failure']=type(error).__name__+': '+q.redact(str(error));q.report['completed']=False
        raise
    finally:q.close()
    require(q.report['completed'] and q.report['shutdown_verified'],'incomplete managed source cohort')
    print('PASS managed RF3 source cohort ('+args.mode+'); full recovery remains unqualified',flush=True)


if __name__=='__main__':main()

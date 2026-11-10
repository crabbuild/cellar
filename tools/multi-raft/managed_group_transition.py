#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Managed unpublished-transition recovery after permanent RF3 disk erasure.

Use the real SDK controller/voter protocol and RustFS. The controller persists
requests, owns all launch history and signs only after its exact old cohort and
disks are absent. This one unpublished split scenario does not accept CG-13's
published-tail, external-effect, complete native or release qualification gates.
"""
import argparse
import hashlib
import json
from pathlib import Path
import time

from group_disaster import Qualification, qualification_subnet
from group_fence import PURPOSE
from managed_group_drill import GIB, ManagedQualification, nonce
from managed_group_fence import ManagedLauncher
from managed_group_voter import VoterFailure
from raft_disaster import command, owned_container
from raft_run import require


class TransitionQualification(ManagedQualification):
    def __init__(self,args):
        super().__init__(args)
        self.model.update(transition=nonce(),recovery=nonce(),replacement={
            'group':nonce(),'incarnation':nonce(),'creation':nonce()},recovered_values={})
        self.report.update(suite='managed-unpublished-disaster',full_dr_completed=False)
        self.report['limitations'][1]='Counter/one namespace; one unpublished split recovery. Published child tails, external effects and all six CG-13 cases remain separate gates.'

    def setup(self):
        info=json.loads(self.docker('info','--format','{{json .}}'))
        require(info['NCPU']>=8 and info['MemTotal']>=21*GIB,
                'five voters, operator and RustFS require 8 CPUs / 21 GiB')
        super().setup()

    def transition(self):
        for index in (3,4):self.admit(self.launch_node(index,0))
        rows=self.model['nodes'];scope=self.model['bootstrap']
        basis=self.control('transition-basis',{'step':'transition_basis'})
        children=[]
        for roster in ((rows[0],rows[3],rows[4]),(rows[1],rows[3],rows[4])):
            children.append({'group':{'group_id':nonce(),'incarnation_id':nonce()},
                'request_id':nonce(),'creation_id':nonce(),
                'voters':[{'node_id':row['node'],'member_id':index+1} for index,row in enumerate(roster)]})
        request={'catalog_id':scope['catalog_id'],'namespace_id':self.model['namespace'],
            'operation_id':self.model['transition'],'request_id':nonce(),
            'expected_fleet_revision':basis['fleet_revision'],'expected_directory_digest':basis['directory_digest'],
            'class':'Counter','partition':{'split':{'parent':{
                'group_id':self.model['group'],'incarnation_id':self.model['incarnation']},'pivot_cell':'Counter:beta'}},
            'children':children}
        self.model['transition_request']=request;self.save()
        admitted=self.launcher.assign_histories(0,scope['catalog_id'],scope['namespace_digest'],
            [{'group':child['group']['group_id'],'incarnation':child['group']['incarnation_id']} for child in children],
            lambda:self.control('transition-admit',{'step':'transition_admit','request':request}))
        common={'catalog_id':scope['catalog_id'],'namespace_id':self.model['namespace'],
            'operation_id':self.model['transition'],'plan_digest':admitted['plan_digest']}
        prepare=dict(common,request_id=nonce())
        prepared=self.control('transition-prepare',{'step':'transition_prepare','request':prepare})
        leader=self.until(self.source_leader,60)
        authorization=dict(common,request_id=nonce(),preparation_digest=prepared['preparation_digest'])
        payload={'action':'authorize_seal','request':authorization}
        self.event('transition/authorize-seal',payload,lambda:self.client(leader).call(payload))
        seal=dict(common,request_id=nonce(),source=request['partition']['split']['parent'])
        payload={'action':'seal_source','request':seal}
        self.event('transition/seal-source',payload,lambda:self.client(leader).call(payload))
        self.control('transition-capture',{'step':'transition_capture','request':seal})
        basis=self.control('recovery-basis',{'step':'recovery_basis','operation':self.model['transition']})
        require(basis['publication_digest'] is None and len(basis['parents'])==1,'expected unpublished parent recovery')
        holds=[]
        for index,source in enumerate(basis['parents']):
            intent=self.control('hold-prepare-'+str(index),{'step':'hold_prepare','basis':basis,'source':source})
            holds.append(self.control('hold-acquire-'+str(index),{'step':'hold_acquire','basis':basis,'source':source,'intent':intent}))
        selected=self.control('recovery-intent',{'step':'intent','basis':basis,'operation':self.model['recovery'],'holds':holds})
        reopened=self.control('recovery-intent-status',{'step':'intent_status','operation':self.model['recovery']})
        require(reopened['intent']==selected,'independent controller changed the retained intent')
        challenge=self.control('recovery-challenge',{'step':'challenge','operation':self.model['recovery'],
            'intent_digest':selected['intent_digest']})
        self.model['selection']=challenge['selection'];self.save()
        self.permanent_fence(challenge)

    def permanent_fence(self,challenge):
        expected=challenge['selection']['fence_digest']
        encoded=json.dumps(challenge['challenge'],separators=(',',':')).encode()
        inventory=self.event('fence/bind',{'expected':expected,'challenge_sha256':hashlib.sha256(encoded).hexdigest()},
            lambda:self.launcher.bind(expected,encoded))
        old=[row for row in self.model['nodes'] if row['generation']==0]
        require(set(map(tuple,inventory))=={(row['container'],row['volume']) for row in old}
                and len(old)==5,'incomplete five-node writer cohort')
        for row in old:
            result=command(['docker','--context',self.args.context,'logs',row['container']],check=False)
            (self.root/(row['container'][:12]+'-before-erasure.log')).write_text(self.redact(result.stdout+result.stderr))
        proof=self.event('fence/delete',{'expected':expected,'inventory':inventory},
            lambda:self.launcher.fence(expected,self.docker))
        require(len(proof)==5 and all(row['container_absent'] and row['disk_absent'] for row in proof),
                'incomplete physical fence')
        reloaded=ManagedLauncher(self.root,self.run_id)
        for row in old:
            restart=command(['docker','--context',self.args.context,'start',row['container']],check=False)
            require(restart.returncode!=0,'old immutable container restarted')
            try:reloaded.restart(row['container'],lambda:(_ for _ in ()).throw(AssertionError('revoked launcher entered Docker')))
            except ValueError as error:require(str(error)=='deployment permanently revoked','different restart refusal')
            else:raise ValueError('revoked deployment restarted')
        payload=reloaded.certificate_payload(expected,self.docker)
        message=self.root/'managed-fence-message';signature=self.root/'managed-fence-signature'
        message.write_bytes(PURPOSE+payload)
        command(['openssl','pkeyutl','-sign','-inkey',str(self.key),'-rawin','-in',str(message),'-out',str(signature)])
        signed=signature.read_bytes();require(len(signed)==64,'invalid Ed25519 signature length')
        fenced=self.control('recovery-fence',{'step':'fence','selection':self.model['selection'],
            'certificate':list(payload+signed)})
        require(fenced['fence_digest']==expected and fenced['ranges']==1,'different persisted SDK fence')
        self.model['fenced']=fenced;self.report['fence']=proof
        self.report['checks'].update(all_source_disks_deleted=True,permanent_restart_refusal=True)
        self.save()

    def target_nodes(self):
        return [row for row in self.model['nodes'] if row['generation']==1]

    def recover(self):
        scope=self.model['bootstrap'];history=self.model['replacement']
        self.launcher.declare(1,scope['catalog_id'],scope['namespace_digest'],
            [{'group':history['group'],'incarnation':history['incarnation']}])
        for index in range(3):
            row=self.launch_node(index,1);row['member']=11+index;self.save();self.admit(row)
        rows=self.target_nodes()
        replacement={'history':{'group':history['group'],'incarnation':history['incarnation']},
            'creation':history['creation'],'epoch':self.model['fenced']['cuts'][0]['epoch']+1,
            'voters':[{'node':row['node'],'member':row['member']} for row in rows],'learners':[]}
        plan=self.control('recovery-stage',{'step':'stage','selection':self.model['selection'],'replacements':[replacement]})
        selected={'selection':self.model['selection'],'plan_digest':plan['plan_digest']}
        self.model['replacement_plan']=plan;self.save()
        selector={'group':history['group'],'incarnation':history['incarnation']}
        self.control('recovery-reserve',{'step':'reserve','basis':selected,'history':selector})
        commitments=[]
        for row in rows:
            payload={'action':'reopen_recovery','selection':self.model['selection'],'plan_digest':plan['plan_digest']}
            self.event('recovery/reopen/'+row['process'],payload,lambda:self.client(row).call(payload,timeout=20))
            self.event('recovery/prepare/'+row['process'],selector,
                lambda:self.client(row).prepare_seed(scope['catalog_id'],scope['namespace_digest'],history['group'],history['incarnation']))
            seeded=self.event('recovery/seed/'+row['process'],selector,
                lambda:self.client(row).seed(scope['catalog_id'],scope['namespace_digest'],history['group'],history['incarnation']))
            require(seeded['seeded'] and seeded['member']==row['member'],'wrong seed voter')
            commitments.append(seeded['commitment'])
        require(len(set(commitments))==1,'voters seeded different genesis commitments')
        seeds=self.control('recovery-select-seeds',{'step':'select_seeds','basis':selected})
        seed_digest=seeds['seeds']['seed_digest']
        request={'operation':self.model['recovery'],'plan_digest':plan['plan_digest'],'seed_digest':seed_digest}
        self.model['attach_request']=request;self.save()
        self.control('recovery-publish',{'step':'publish','basis':selected,'seed_digest':seed_digest})
        self.attach_targets('initial')
        leader=self.until(lambda:self.target_leader(False),60)
        self.activate_target(leader)
        activated=self.control('recovery-activation',{'step':'activation','request':request,'history':selector})
        require(activated['activation'] is not None,'missing committed activation')
        settled=self.control('recovery-settle',{'step':'settle','request':request})
        require(settled['promoted_ranges']==settled['total_ranges']==1,'incomplete promoted recovery range')
        complete=self.control('recovery-complete',{'step':'complete','request':request})
        self.control('recovery-release',{'step':'release','request':request,'completion':complete['completion']})
        self.model['completion']=complete['completion'];self.save()
        self.grant_origins(rows)
        self.until(self.target_leader,60)
        self.verify_targets(replay=True,old_tokens=True)
        self.fresh_writes()
        self.restart_targets()
        retired=self.control('recovery-retire',{'step':'retire','request':request,'completion':complete['completion']})
        require(retired['retired_histories']==retired['total_histories'] and retired['fleet_slot_released'],
                'recovery retirement remains incomplete')
        self.report['checks'].update(fresh_genesis_quorum=True,restored_sql_and_receipts=True,
            old_tokens_refused=True,fresh_s3_writes=True,recovered_retained_restart=True,recovery_retired=True)
        self.report.update(completed=True,full_dr_completed=True);self.save()

    def physical(self,method,row):
        scope=self.model['bootstrap'];history=self.model['replacement'];request=self.model['attach_request']
        return method(scope['catalog_id'],scope['namespace_digest'],history['group'],history['incarnation'],
            request['operation'],request['plan_digest'],request['seed_digest'])

    def activate_target(self,leader):
        # Accepted is a known proposal, not an applied activation. The SDK
        # explicitly permits reconciling this same immutable request. A lost or
        # refused call raises immediately; only a returned Accepted continues.
        deadline=time.monotonic()+20
        attempt=0
        while deadline-time.monotonic()>=5:
            result=self.event('recovery/activate/'+str(attempt),self.model['attach_request'],
                lambda:self.physical(self.client(leader).activate,leader))
            if result['applied'] is not None:return result
            attempt+=1
            time.sleep(.05)
        raise ValueError('activation proposal remains unconfirmed; reconcile the original request')

    def attach_targets(self,label):
        for row in self.target_nodes():
            self.event('recovery/attach/'+label+'/'+row['process'],self.model['attach_request'],
                lambda:self.physical(self.client(row).attach,row))

    def target_leader(self,ready=True):
        history=self.model['replacement']
        for row in self.target_nodes():
            try:
                value=self.client(row).call({'action':'status','group':list(bytes.fromhex(history['group'])),
                    'incarnation':list(bytes.fromhex(history['incarnation']))})
                if value['leader']==row['member'] and (value['ready'] or not ready):return row
            except VoterFailure:pass
        return None

    def query_target(self,row,cell,minimum=None):
        return self.client(row).call({'action':'read','cell':cell,
            'incarnation':list(bytes.fromhex(self.model['replacement']['incarnation'])),
            'invocation':list(bytes.fromhex(nonce())),'minimum':minimum})

    def verify_targets(self,replay=False,old_tokens=False):
        rows=self.target_nodes();before={}
        for row in rows:
            require(self.client(row).call({'action':'observe'})['groups']==1,'replacement must own exactly one shared group')
            for cell,record in self.model['cells'].items():
                observed=self.query_target(row,cell)
                require(int.from_bytes(bytes.fromhex(observed['result']),'big',signed=True)==self.model['recovered_values'].get(cell,1),
                    'restored SQL value changed')
                value=(observed['result'],observed['version'])
                require(before.setdefault(cell,value)==value,'voters disagree on recovered SQL/version')
                if old_tokens:
                    try:self.query_target(row,cell,record['receipt']['token'])
                    except VoterFailure as error:
                        require(error.status==503 and 'WrongHistory' in str(error.reply),'old token refused for a different reason')
                    else:raise ValueError('old-incarnation token was accepted')
        if replay:
            history=self.model['replacement'];leader=self.until(self.target_leader,60)
            for cell,record in self.model['cells'].items():
                for kind,original in (('s3',record),('quorum',record['quorum'])):
                    payload=dict(original['request'],incarnation=list(bytes.fromhex(history['incarnation'])))
                    result=self.event('recovery/retry/'+cell+'/'+kind,payload,lambda:self.client(leader).call(payload))
                    require(result['result']==original['receipt']['result']
                        and result['durability']==original['receipt']['durability']
                        and bytes(result['group']).hex()==history['group']
                        and bytes(result['incarnation']).hex()==history['incarnation'],'restored command receipt changed')
            for row in rows:
                for cell,value in before.items():
                    observed=self.query_target(row,cell)
                    require((observed['result'],observed['version'])==value,'retry executed an extra SQL mutation')
        return before

    def fresh_writes(self):
        leader=self.until(self.target_leader,60);history=self.model['replacement']
        for cell in self.model['cells']:
            payload={'action':'write','cell':cell,'incarnation':list(bytes.fromhex(history['incarnation'])),
                'session':list(bytes.fromhex(nonce())),'sequence':1,'retry_until_ms':int(time.time()*1000)+300000,'delta':1,'s3':True}
            result=self.event('recovery/fresh/'+cell,payload,lambda:self.client(leader).call(payload))
            expected=self.model['recovered_values'].get(cell,1)+1
            require(int.from_bytes(bytes.fromhex(result['result']),'big',signed=True)==expected
                    and result['durability']=='s3','fresh recovery write lacks SQL/S3 proof')
            self.model['recovered_values'][cell]=expected;self.save()
        self.verify_targets()

    def restart_targets(self):
        for row in self.target_nodes():
            self.event('recovery/stop/'+row['process'],{'action':'stop'},lambda:self.client(row).call({'action':'stop'}))
            self.until(lambda:not owned_container(self.docker,row['container'],self.run_id)['State']['Running'],30)
            self.joined_voter(row,1)
            row['process']=nonce();self.save()
            def invoke():
                self.copy_node(row,True)
                return self.docker('start',row['container'])
            self.launcher.restart(row['container'],invoke)
            self.until(lambda:self.client(row).call({'action':'observe'}),60);self.admit(row)
        self.attach_targets('retained');self.grant_origins(self.target_nodes())
        self.until(self.target_leader,60);self.verify_targets()

    def drill(self):
        self.source_workflow()
        self.transition();self.recover()

    def close(self):
        Qualification.close(self)
        if self.report['completed']:
            try:
                for row in self.target_nodes():self.joined_voter(row,1)
            except BaseException:
                self.report.update(completed=False,shutdown_verified=False);self.save();raise


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context',required=True);parser.add_argument('--image',required=True)
    parser.add_argument('--output',type=Path,required=True);parser.add_argument('--mode',choices=['materialized','extents'],required=True)
    parser.add_argument('--port-base',type=int,default=19800);parser.add_argument('--subnet',type=qualification_subnet)
    args=parser.parse_args();require(1024<=args.port_base<65530,'invalid loopback port range')
    q=TransitionQualification(args)
    try:q.setup();q.drill()
    except BaseException as error:
        q.report.update(completed=False,failure=type(error).__name__+': '+q.redact(str(error)));raise
    finally:q.close()
    require(q.report['completed'] and q.report['shutdown_verified'],'incomplete managed recovery')
    print('PASS managed unpublished-transition disaster recovery ('+args.mode+'); full CG-13 remains unqualified',flush=True)


if __name__=='__main__':main()

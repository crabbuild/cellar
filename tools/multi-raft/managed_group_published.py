# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Published split children with newer S3 tails and two-range managed recovery.

The three replacement voters share both groups within the existing two-group
profile. Preparations remain sequential under the one-operation recovery pool.
This does not qualify the full CG-13 external-effect and failure matrix.
"""
import time

from managed_group_drill import nonce
from managed_group_transition import TransitionQualification
from managed_group_voter import VoterFailure
from raft_run import require


class PublishedTransitionQualification(TransitionQualification):
    def __init__(self,args):
        super().__init__(args)
        self.report.update(suite='managed-published-disaster')
        self.report['limitations'][1]='Counter/one namespace; published split with two replacement ranges. External effects and all six CG-13 cases remain separate gates.'

    def child_rows(self,child):
        by_node={row['node']:row for row in self.model['nodes'] if row['generation']==0}
        return [(by_node[voter['node_id']],voter['member_id']) for voter in child['voters']]

    def released_preparation(self,row,history,plan,seeded):
        scope=self.model['bootstrap'];client=self.client(row)
        selector={'group':history['group'],'incarnation':history['incarnation'],
            'plan_digest':plan['plan_digest'],'commitment':seeded['commitment']}
        suffix=row['process']+'/'+history['group']
        repeated=self.event('recovery/release-replay/'+suffix,selector,
            lambda:client.release_seed(scope['catalog_id'],scope['namespace_digest'],history['group'],
                history['incarnation'],plan['plan_digest'],seeded['commitment']))
        require(repeated=={'released':True},'exact seed release did not reconcile')
        def refuse():
            try:client.prepare_seed(scope['catalog_id'],scope['namespace_digest'],history['group'],history['incarnation'])
            except VoterFailure as error:
                require(error.status==503 and 'range preparation already released' in str(error.reply),
                        'completed preparation was refused for a different reason')
                return {'status':error.status,'refused_before_preparation':True}
            raise ValueError('released range acquired another preparation')
        self.event('recovery/released-prepare-refusal/'+suffix,selector,refuse)
        self.report['checks'].update(seed_release_exact_replay=True,released_seed_reprepare_refused=True)
        self.save()

    def child_leader(self,child,ready=False):
        for row,member in self.child_rows(child):
            try:
                value=self.client(row).call({'action':'status',
                    'group':list(bytes.fromhex(child['group']['group_id'])),
                    'incarnation':list(bytes.fromhex(child['group']['incarnation_id']))})
                if value['leader']==member and (value['ready'] or not ready):return row
            except VoterFailure:pass
        return None

    def child_step(self,common,child,step,row):
        request=dict(common,request_id=nonce(),child=child['group'],step=step)
        scope=self.model['bootstrap']
        return self.event('transition/'+step+'/'+child['group']['group_id']+'/'+row['process'],request,
            lambda:self.client(row).setup_child(scope['catalog_id'],scope['namespace_digest'],request))

    def activate_child(self,common,child,row):
        request=dict(common,request_id=nonce(),**child['group'])
        scope=self.model['bootstrap'];deadline=time.monotonic()+20;attempt=0
        while deadline-time.monotonic()>=5:
            value=self.event('transition/activate/'+child['group']['group_id']+'/'+str(attempt),request,
                lambda:self.client(row).activate_child(scope['catalog_id'],scope['namespace_digest'],request))
            if value['applied'] is not None:return value
            # A returned provisional proposal permits reconciliation of exactly
            # this request. Refused/lost calls raise and never enter this loop.
            attempt+=1;time.sleep(.05)
        raise ValueError('child activation remains unconfirmed; reconcile the original request')

    def disaster_sources(self,common,children):
        for child in children:
            for step in ('install','attach'):
                for row,_member in self.child_rows(child):self.child_step(common,child,step,row)
            leader=self.until(lambda:self.child_leader(child),60)
            self.child_step(common,child,'quorum',leader)
        archive=self.control('transition-verify-archive',{'step':'transition_verify_archive',
            'request':dict(common,request_id=nonce())})
        publication=self.control('transition-publish',{'step':'transition_publish',
            'request':dict(common,request_id=nonce(),archive_digest=archive['archive_digest'])})
        self.model['transition_publication']=publication;self.save()
        for child in children:
            leader=self.until(lambda:self.child_leader(child),60)
            self.activate_child(common,child,leader)
        for child in children:
            leader=self.until(lambda:self.child_leader(child,True),60)
            self.child_step(common,child,'initialize_archive',leader)
        self.grant_origins([row for row in self.model['nodes'] if row['generation']==0],phase='published')
        for child in children:self.write_child_tail(child)
        basis=self.control('recovery-basis',{'step':'recovery_basis','operation':self.model['transition']})
        require(basis['publication_digest']==publication['publication_digest'] and len(basis['children'])==2,
                'published recovery must select both children')
        children_by_id={child['group']['group_id']:child for child in children}
        self.model['replacements']=[]
        for source in basis['children']:
            child=children_by_id[source['group_id']]
            require(source==child['group'],'different published source incarnation')
            self.model['replacements'].append({'group':nonce(),'incarnation':nonce(),'creation':nonce(),
                'source':source,'cells':self.child_cells(child)})
        require({cell for history in self.model['replacements'] for cell in history['cells']}==set(self.model['cells']),
                'published ranges lost a cell')
        self.report['checks'].update(published_split=True,newer_s3_child_tails=True,two_replacement_ranges=True)
        self.save();return basis

    def child_cells(self,child):
        children=self.model['transition_request']['children']
        pivot=self.model['transition_request']['partition']['split']['pivot_cell']
        left=child['group']==children[0]['group']
        return [cell for cell in self.model['cells'] if (cell<pivot)==left]

    def write_child_tail(self,child):
        leader=self.until(lambda:self.child_leader(child,True),60)
        for cell in self.child_cells(child):
            request={'action':'write','cell':cell,'incarnation':list(bytes.fromhex(self.model['incarnation'])),
                'session':list(bytes.fromhex(nonce())),'sequence':1,'retry_until_ms':int(time.time()*1000)+300000,
                'delta':10,'s3':True}
            receipt=self.event('transition/tail/'+cell,request,lambda:self.client(leader).call(request))
            require(receipt['durability']=='s3' and int.from_bytes(bytes.fromhex(receipt['result']),'big',signed=True)==11
                    and bytes(receipt['group']).hex()==child['group']['group_id']
                    and bytes(receipt['incarnation']).hex()==child['group']['incarnation_id'],
                    'newer S3 receipt does not belong to the selected child')
            self.model['cells'][cell]['tail']={'request':request,'receipt':receipt}
            self.model['recovered_values'][cell]=11;self.save()
        for row,_member in self.child_rows(child):
            for cell in self.child_cells(child):
                value=self.client(row).call({'action':'read','cell':cell,
                    'incarnation':list(bytes.fromhex(self.model['incarnation'])),
                    'invocation':list(bytes.fromhex(nonce())),'minimum':self.model['cells'][cell]['tail']['receipt']['token']})
                require(int.from_bytes(bytes.fromhex(value['result']),'big',signed=True)==11,
                        'published child does not serve its acknowledged S3 tail')

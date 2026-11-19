# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Missing families, lost state and weaker acknowledgements must fail the oracle."""

import copy
import json
from itertools import product
import unittest

from daemon_primitives import verify


def campaign(minimum):
    events = [{'phase': 'started', 'store': 'rustfs_catalog_archives_and_node_leases'}]

    def event(phase, **values):
        events.append(dict(phase=phase, **values))

    def voter(member, term=1, cells=2):
        leader = 2 if term == 1 else 1
        return {'hosted_groups': 2, 'groups': [dict(namespace=namespace, group=namespace, ready=member == leader,
                cells=cells, term=term, member=member, leader=leader,
                is_leader=member == leader) for namespace in ('sql', 'kv')]}

    def observed(members, term=1, cells=2):
        event('primitive_observed', voters=[voter(member, term, cells) for member in members])

    def call(adapter, family, cell, operation, value=True, status=200):
        route = f'/{adapter}/{family}/{cell}/{operation}'
        event('primitive_send', route=route)
        event('primitive_reply', route=route, status=status, body=json.dumps(value))

    def writes(stage):
        for cell, adapter in (('one', 'native'), ('two', 'axum')):
            for family in ('sql', 'kv'):
                call(adapter, family, cell, 'write')
                event('primitive_write_verified', stage=stage, adapter=adapter, family=family, cell=cell)

    def reads(stage, members):
        for member, adapter, cell, family in product(members, ('native', 'axum'), ('one', 'two'), ('sql', 'kv')):
            value = f'{stage}-{cell}'
            call(adapter, family, cell, 'read', [[1, value]] if family == 'sql' else value)
            event('primitive_read_verified', stage=stage, member=member, adapter=adapter, family=family, cell=cell)

    def stops(members=range(1, 4)):
        for member in members:
            event('primitive_shutdown_joined', member=member, report={'raft': {
                'owners_closed': True, 'startup_incomplete': False,
                'groups': 2, 'retain_replica_disks': True}})

    event('primitive_scope', families=['sql', 'kv'], groups=2, cells_per_group=2,
          minimum=minimum, image='materialized', hosting='native_daemon',
          application_adapters=['native', 'axum'], external_axum_hosting=False,
          same_id_transport_retry=False)
    observed(range(1, 4), cells=0)
    for cell, adapter in (('one', 'native'), ('two', 'axum')):
        call(adapter, 'sql', cell, 'setup')
    writes('initial')
    for cell, adapter in (('one', 'native'), ('two', 'axum')):
        call(adapter, 'sql', cell, 'rollback')
    reads('initial', range(1, 4))
    observed(range(1, 4))
    stops()
    reads('initial', range(1, 4))
    observed(range(1, 4))
    event('primitive_retained_restart_verified', old_pids=[101, 102, 103])
    event('primitive_before_leader_kill', member=2, pid=202, term=1)
    observed((1, 3), term=2)
    writes('after-kill')
    reads('after-kill', (1, 3))
    event('primitive_leader_loss_verified', killed_member=2, old_pid=202,
          survivors=[voter(1, 2), voter(3, 2)])
    observed(range(1, 4), term=2)
    reads('after-kill', range(1, 4))
    event('primitive_archive_blocked')
    for adapter, family in product(('native', 'axum'), ('sql', 'kv')):
        status = 200 if minimum == 'quorum' else 503
        call(adapter, family, 'one', 'write', True if status == 200 else {'error': 'uncertain'}, status)
        event('primitive_archive_boundary', adapter=adapter, family=family, status=status)
    event('primitive_archive_denied', members=[1, 3])
    event('primitive_archive_restored')
    writes('restored')
    reads('restored', range(1, 4))
    stops((1, 2))
    for adapter, family, operation in product(('native', 'axum'), ('sql', 'kv'), ('read', 'write')):
        call(adapter, family, 'one', operation, {'error': 'no quorum'}, 503)
        event('primitive_quorum_refused', adapter=adapter, family=family, operation=operation, status=503)
    stops((3,))
    event('primitive_daemon_passed', physical_groups=2, logical_cells=4)
    return events


class PrimitiveOracleTests(unittest.TestCase):
    def test_complete_scoped_campaigns(self):
        for minimum in ('quorum', 's3'):
            verify(campaign(minimum))

    def test_missing_or_duplicated_required_proof(self):
        for phase in ('primitive_write_verified', 'primitive_read_verified', 'primitive_reply',
                      'primitive_retained_restart_verified', 'primitive_before_leader_kill',
                      'primitive_archive_boundary', 'primitive_quorum_refused', 'primitive_observed',
                      'primitive_shutdown_joined', 'primitive_archive_denied'):
            for duplicate in (False, True):
                events = campaign('s3')
                index = next(i for i, event in enumerate(events) if event['phase'] == phase)
                if duplicate:
                    events.insert(index, copy.deepcopy(events[index]))
                else:
                    events.pop(index)
                with self.subTest(phase=phase, duplicate=duplicate), self.assertRaises(ValueError):
                    verify(events)

    def test_weaker_scope_lost_state_and_false_acknowledgement(self):
        for phase, field, value in [
            ('primitive_scope', 'families', ['sql']),
            ('primitive_scope', 'same_id_transport_retry', True),
            ('primitive_scope', 'external_axum_hosting', True),
            ('primitive_archive_boundary', 'status', 200),
            ('primitive_quorum_refused', 'status', 200),
            ('primitive_reply', 'body', 'false'),
        ]:
            events = campaign('s3')
            next(event for event in events if event['phase'] == phase)[field] = value
            with self.subTest(phase=phase, field=field), self.assertRaises(ValueError):
                verify(events)
        events = campaign('quorum')
        next(e for e in events if e['phase'] == 'primitive_reply' and e['route'].endswith('/read'))['body'] = '[]'
        with self.assertRaises(ValueError):
            verify(events)
        events = campaign('quorum')
        groups = next(e for e in events if e['phase'] == 'primitive_observed')['voters'][0]['groups']
        groups[0]['ready'] = True
        with self.assertRaises(ValueError):
            verify(events)
        events = campaign('quorum')
        groups = next(e for e in events if e['phase'] == 'primitive_observed')['voters'][0]['groups']
        groups[1]['group'] = groups[0]['group']
        with self.assertRaises(ValueError):
            verify(events)


if __name__ == '__main__':
    unittest.main()

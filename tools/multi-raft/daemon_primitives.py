# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Semantic checks for the public SQL/KV daemon campaign, not CG-17 acceptance."""

from itertools import product
import json


def verify(events):
    def require(condition, message):
        if not condition:
            raise ValueError(message)

    def phase(name):
        return [event for event in events if event.get('phase') == name]

    scopes = phase('primitive_scope')
    require(len(scopes) == 1, 'one primitive scope required')
    scope = scopes[0]
    require(scope['families'] == ['sql', 'kv'] and scope['groups'] == 2
            and scope['cells_per_group'] == 2 and scope['minimum'] in ('quorum', 's3')
            and scope['image'] in ('materialized', 'extents')
            and scope['hosting'] == 'native_daemon'
            and scope['application_adapters'] == ['native', 'axum']
            and scope['external_axum_hosting'] is False
            and scope['same_id_transport_retry'] is False, 'primitive scope differs')
    require(events[0]['store'] == 'rustfs_catalog_archives_and_node_leases',
            'real RustFS required')
    require(len(phase('primitive_before_leader_kill')) == 1
            and len(phase('primitive_leader_loss_verified')) == 1,
            'actual leader loss required')
    before = phase('primitive_before_leader_kill')[0]
    after = phase('primitive_leader_loss_verified')[0]
    require(before['member'] in (1, 2, 3) and before['pid'] > 0
            and after['killed_member'] == before['member']
            and after['old_pid'] == before['pid'] and len(after['survivors']) == 2,
            'leader kill identity differs')
    for voter in after['survivors']:
        require(voter['groups'][0]['term'] > before['term'], 'leader term did not advance')
    restarts = phase('primitive_retained_restart_verified')
    require(len(restarts) == 1 and len(set(restarts[0]['old_pids'])) == 3,
            'retained restart proof missing')
    writes = phase('primitive_write_verified')
    expected_writes = [(stage, adapter, family, cell)
                       for stage in ('initial', 'after-kill', 'restored')
                       for cell, adapter in (('one', 'native'), ('two', 'axum'))
                       for family in ('sql', 'kv')]
    require([(e['stage'], e['adapter'], e['family'], e['cell']) for e in writes]
            == expected_writes, 'primitive writes missing, duplicated or reordered')
    reads = phase('primitive_read_verified')
    expected_reads = []
    for stage, members in [('initial', (1, 2, 3)), ('initial', (1, 2, 3)),
                           ('after-kill', tuple(m for m in (1, 2, 3) if m != before['member'])),
                           ('after-kill', (1, 2, 3)), ('restored', (1, 2, 3))]:
        expected_reads.extend((stage, *items) for items in product(
            members, ('native', 'axum'), ('one', 'two'), ('sql', 'kv')))
    require([(e['stage'], e['member'], e['adapter'], e['cell'], e['family']) for e in reads]
            == expected_reads, 'all-responder primitive reads missing or duplicated')
    outages = phase('primitive_archive_boundary')
    require([(e['adapter'], e['family'], e['status']) for e in outages]
            == [(adapter, family, 200 if scope['minimum'] == 'quorum' else 503)
                for adapter, family in product(('native', 'axum'), ('sql', 'kv'))],
            'archive durability boundary differs')
    require(len(phase('primitive_archive_blocked')) == 1
            and len(phase('primitive_archive_restored')) == 1, 'archive outage incomplete')
    denied = phase('primitive_archive_denied')
    require(len(denied) == 1 and denied[0]['members']
            and set(denied[0]['members']) <= {1, 2, 3}, 'no actual archive write refused')
    refusals = phase('primitive_quorum_refused')
    require([(e['adapter'], e['family'], e['operation'], e['status']) for e in refusals]
            == [(*items, 503) for items in product(('native', 'axum'), ('sql', 'kv'), ('read', 'write'))],
            'minority write or strong-read refusal missing')
    stops = phase('primitive_shutdown_joined')
    require([e['member'] for e in stops] == [1, 2, 3, 1, 2, 3], 'joined stop inventory differs')
    require(events.index(stops[4]) < events.index(refusals[0])
            and events.index(refusals[-1]) < events.index(stops[5]),
            'minority requests did not occur between majority and final stop')
    for event in stops:
        raft = event['report']['raft']
        require(raft['owners_closed'] is True and raft['startup_incomplete'] is False
                and raft['groups'] == 2 and raft['retain_replica_disks'] is True,
                'primitive owners did not join')
    observations = phase('primitive_observed')
    require(len(observations) == 5, 'shared runtime observation missing')
    for index, event in enumerate(observations):
        require(len(event['voters']) == (2 if index == 3 else 3), 'responder observation missing')
        for voter in event['voters']:
            groups = voter['groups']
            require(voter['hosted_groups'] == len(groups) == 2
                    and len({g['namespace'] for g in groups}) == 2
                    and len({g['group'] for g in groups}) == 2
                    and all(g['cells'] == (0 if index == 0 else 2) for g in groups),
                    'shared group proof missing')
        for namespace in range(2):
            groups = [voter['groups'][namespace] for voter in event['voters']]
            leaders = [g for g in groups if g['is_leader']]
            require(len(leaders) == 1 and leaders[0]['ready'] is True
                    and all(g['ready'] == g['is_leader'] == (g['member'] == leaders[0]['member'])
                            and g['leader'] == leaders[0]['member'] and g['term'] == leaders[0]['term']
                            for g in groups), 'voters disagree on a ready leader')
    expected_calls = []

    def call(adapter, family, cell, operation, value, status=200):
        expected_calls.append((f'/{adapter}/{family}/{cell}/{operation}', status, value))

    def write_calls(stage):
        for cell, adapter in (('one', 'native'), ('two', 'axum')):
            for family in ('sql', 'kv'):
                call(adapter, family, cell, 'write', True)

    def read_calls(stage, members):
        for _, adapter, cell, family in product(members, ('native', 'axum'), ('one', 'two'), ('sql', 'kv')):
            value = f'{stage}-{cell}'
            call(adapter, family, cell, 'read', [[1, value]] if family == 'sql' else value)

    for cell, adapter in (('one', 'native'), ('two', 'axum')):
        call(adapter, 'sql', cell, 'setup', True)
    write_calls('initial')
    for cell, adapter in (('one', 'native'), ('two', 'axum')):
        call(adapter, 'sql', cell, 'rollback', True)
    read_calls('initial', range(3))
    read_calls('initial', range(3))
    write_calls('after-kill')
    read_calls('after-kill', range(2))
    read_calls('after-kill', range(3))
    for adapter, family in product(('native', 'axum'), ('sql', 'kv')):
        call(adapter, family, 'one', 'write', True if scope['minimum'] == 'quorum' else None,
             200 if scope['minimum'] == 'quorum' else 503)
    write_calls('restored')
    read_calls('restored', range(3))
    for adapter, family, operation in product(('native', 'axum'), ('sql', 'kv'), ('read', 'write')):
        call(adapter, family, 'one', operation, None, 503)
    replies = phase('primitive_reply')
    sends = phase('primitive_send')
    require(len(replies) == len(sends) == len(expected_calls), 'primitive HTTP calls missing or duplicated')
    for sent, reply, (route, status, value) in zip(sends, replies, expected_calls):
        require(sent['route'] == reply['route'] == route and reply['status'] == status,
                'primitive HTTP route/status differs')
        body = json.loads(reply['body'])
        require(body == value if status == 200 else isinstance(body.get('error'), str),
                'primitive HTTP result or rollback state differs')
    summaries = phase('primitive_daemon_passed')
    require(len(summaries) == 1 and summaries[0]['physical_groups'] == 2
            and summaries[0]['logical_cells'] == 4
            and events[-1] == summaries[0], 'primitive campaign summary differs')

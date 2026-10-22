"""Independent serialized reference-history checker for Fleet/bucket receipts.

These are application transaction receipts, never Raft positions or tokens.
Ambiguity is retained and refuses qualification, including non-200 mutations.
"""
import json
from raft_workload import input_digest, model


def require(condition, message):
    if not condition:
        raise ValueError(message)


def position(reply):
    sequence = reply['sequence']
    require(type(sequence) is int and 1 <= sequence <= 4096, 'invalid baseline sequence')
    return {'sequence': sequence}


def reconcile(path, run_id, durability):
    require(durability in ('fleet-or-bucket', 'bucket'), 'invalid baseline durability')
    states, seeds, pending, busy, seen = {}, {}, {}, set(), set()
    commands = queries = 0
    with open(path) as stream:
        for order, line in enumerate(stream, 1):
            event = json.loads(line)
            require(event['order'] == order, 'ledger order gap')
            cell, kind = event['cell'], event['event']
            if kind == 'seed_invoke':
                require(cell not in seeds and cell not in states and input_digest(event['input']) == event['input_sha256'], 'invalid seed')
                seeds[cell] = event
            elif kind == 'seed_complete':
                invocation = seeds.pop(cell)
                expected, changed = model(None, invocation['input'])
                reply = event['response']
                require(event['status'] == 200 and reply['state'] == expected and reply['changed'] == changed, 'seed result mismatch')
                check_command(reply, invocation, run_id, durability, 1)
                states[cell] = {'state': expected, 'sequence': 1, 'client': invocation['command_id'].split(':')[0], 'scope': reply['scope']}
                commands += 1
            elif kind in ('invoke', 'reject'):
                key = event['id']
                require(key not in seen and cell in states, 'duplicate offer or unseeded cell')
                seen.add(key)
                if kind == 'reject':
                    continue
                require(cell not in busy and input_digest(event['input']) == event['input_sha256'], 'overlap or altered input')
                require(event['operation'] in ('command', 'query'), 'unknown operation')
                busy.add(cell)
                pending[key] = event
            elif kind == 'complete':
                invocation = pending.pop(event['id'])
                require(invocation['cell'] == cell and invocation['operation'] == event['operation'] and not event.get('violation'), 'completion identity or violation')
                busy.remove(cell)
                require(event['outcome'] != 'ambiguous', 'ambiguous history requires a separate recovery drill')
                if event['status'] != 200:
                    require(event['operation'] == 'query' and event['outcome'] in ('failed', 'rejected'), 'mutation outcome is not proven')
                    continue
                require(event['outcome'] == 'completed', 'successful outcome mismatch')
                current, reply = states[cell], event['response']
                require(reply['scope'] == current['scope'] and isinstance(reply['owner'], str) and reply['owner'], 'wrong scope or owner')
                if event['operation'] == 'command':
                    expected, changed = model(current['state'], invocation['input'])
                    sequence = current['sequence'] + 1
                    require(invocation['command_id'] == f"{current['client']}:{sequence}", 'command identity mismatch')
                    check_command(reply, invocation, run_id, durability, sequence)
                    require(reply['state'] == expected and reply['changed'] == changed, 'command transition mismatch')
                    current.update(state=expected, sequence=sequence)
                    commands += 1
                else:
                    require(invocation['input'] == {'consistency': 'owner'} and reply['read_contract'] == 'owner', 'unsupported baseline read contract')
                    require(reply['state'] == current['state'] and reply['sequence'] == current['sequence'], 'owner read differs from preceding acknowledgement')
                    queries += 1
            else:
                raise ValueError('unknown ledger event')
    require(not seeds and not pending and not busy, 'incomplete ledger')
    return {'commands_including_seed': commands, 'queries': queries, 'cells': len(states), 'final': states}


def check_command(reply, invocation, run_id, durability, sequence):
    require(invocation['command_id'].endswith(f':{sequence}'), 'command sequence differs from identity')
    require(reply['scope'] == f"{run_id}:/cells/{invocation['cell']}" and reply['command_id'] == invocation['command_id'], 'wrong receipt identity')
    require(position(reply)['sequence'] == sequence and reply['durability'] == durability and reply['receipt_kind'] == 'application-transaction', 'wrong receipt or output contract')
    require(isinstance(reply['owner'], str) and bool(reply['owner']), 'missing owner')
    require(not {'index', 'term', 'group', 'incarnation', 'version_token'} & reply.keys(), 'baseline cannot claim a Raft proof')

#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Run and independently verify source-pinned shared-group SQL/RustFS campaigns."""
import argparse
import json
from pathlib import Path

import cell_group_campaign as campaign
from cell_group_resources import save, stop
from raft_run import read_json, require


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='action', required=True)
    check = commands.add_parser('preflight')
    check.add_argument('--config', type=Path, required=True)
    check.add_argument('--profile', required=True)
    run = commands.add_parser('run')
    run.add_argument('--config', type=Path, required=True)
    run.add_argument('--profile', required=True)
    run.add_argument('--output', type=Path, required=True)
    for action in ('verify', 'stop'):
        commands.add_parser(action).add_argument('--run', type=Path, required=True)
    arguments = parser.parse_args()
    if arguments.action == 'preflight':
        result = campaign.preflight(campaign.config(arguments.config), arguments.profile)
        result = {'ready': True, 'profile': arguments.profile, 'cohorts': len(result['cohorts']),
                  'build': result['build'], 'vm': result['vm']}
    elif arguments.action == 'run':
        result = campaign.run(campaign.config(arguments.config), arguments.profile, arguments.output)
    elif arguments.action == 'verify':
        result = campaign.verify(arguments.run)
    else:
        record = read_json(arguments.run / 'campaign.json')
        require(arguments.run.resolve() == Path(record['settings']['data_root']) / record['settings']['run_id'],
                'campaign stop directory differs from retained ownership')
        result = [stop(path.parent, record['settings']['run_id'])
                  for path in sorted(arguments.run.glob('cohort-*/resources.json'))]
        # Only ownership observations may change. Never re-sign a drifted ledger
        # or source/profile report simply because an operator needed to stop it.
        receipt = read_json(arguments.run / 'receipt.json')
        for path in arguments.run.glob('cohort-*/resources.json'):
            receipt['artifacts'][path.relative_to(arguments.run).as_posix()] = campaign.digest(path)
        save(arguments.run / 'receipt.json', receipt)
    print(json.dumps(result, indent=2, allow_nan=False))
    if isinstance(result, dict) and 'qualified' in result:
        require(result['qualified'], 'campaign retained but release profile gates remain failed or unmeasured')


if __name__ == '__main__':
    main()

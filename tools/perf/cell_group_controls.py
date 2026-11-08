# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Read-only import of exact, pinned historical per-cell control evidence.

The pinned standalone historical runner measures materialized images. Its
managed extent mode needs a separate matched adapter; an extents label cannot
be manufactured for a standalone run; the frozen release matrix keeps that gap visible.
The historical oracle runs from its own clean source, never through the new
shared-group topology parser.
"""
import hashlib
import json
from pathlib import Path
import subprocess

from raft_run import input_digest, read_json, require, source_manifest

PIN = '1be27d234d4674d4d014785c60cecbfe394bdab9'


def validate(control, profile, expected_workload):
    require(isinstance(control, dict) and set(control) == {'checkout', 'run', 'repetition', 'image_mode', 'artifacts_sha256'},
            'invalid historical control configuration')
    require(profile['historical_control'] is True and profile['historical_revision'] == PIN,
            'unsupported historical control revision')
    require(control['image_mode'] == 'materialized' and 'materialized' in profile['image_modes'],
            'pinned historical standalone adapter cannot establish extent image measurements')
    require(type(control['repetition']) is int and 0 <= control['repetition'] < profile['repetitions'], 'unmatched historical repetition')
    source, run = Path(control['checkout']), Path(control['run'])
    require(source.is_absolute() and run.is_absolute() and source.is_dir() and run.is_dir(), 'historical inputs must exist')
    revision = subprocess.check_output(['git', '-C', str(source), 'rev-parse', 'HEAD'], text=True).strip()
    require(revision == PIN and not subprocess.check_output(['git', '-C', str(source), 'diff', '--name-only', 'HEAD']),
            'historical checkout differs from its exact source pin')
    manifest = source_manifest(source)
    # Reject untracked executable/parser inputs as well as modified tracked inputs.
    names = subprocess.check_output(['git', '-C', str(source), 'ls-files', '-z', '--', 'Cargo.toml', 'Cargo.lock', 'crates', 'vendor', 'examples', 'tools']).decode().split('\0')
    require(set(manifest) == set(names) - {''}, 'historical source inventory differs from its Git tree')
    from cell_group_campaign import artifacts
    require(input_digest(artifacts(run)) == control['artifacts_sha256'], 'historical raw evidence drifted')
    report = read_json(run / 'report.json')
    require(report['build']['source_revision'] == PIN and report['build']['source_manifest_sha256'] == input_digest(manifest),
            'historical binary source differs from pinned inputs')
    for name, digest in report['runner_sha256'].items():
        require(manifest['tools/perf/' + name] == digest, 'historical oracle inputs differ from recorded runner')
    workload = dict(expected_workload)
    workload.pop('image_mode', None)
    workload['cells'] = {**workload['cells'], 'per_group': 1}
    require(report['profile'] == workload, 'historical population, phase, workload or durability differs')
    result = subprocess.run(['python3', '-B', str(source / 'tools/perf/raft_analyze.py'), str(run)],
                            capture_output=True, text=True, timeout=300)
    require(result.returncode == 0, 'pinned historical independent oracle refused control')
    return {'repetition': control['repetition'], 'image_mode': 'materialized', 'occupancy': 1,
            'variant': 'historical', 'source_revision': PIN, 'build': report['build'],
            'artifacts_sha256': control['artifacts_sha256'], 'analysis': json.loads(result.stdout)}

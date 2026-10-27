#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Run isolated, exact native cold-recovery trials and retain every outcome.

Build/list outside the timer. Native assertions produce the controller-clock
ledger. This embedded-node pilot does not qualify hard process crashes or RTO.
"""
import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
PROFILES = Path(__file__).with_name('recovery-profiles.json')


class InterruptedRun(Exception):
    def __init__(self, signum):
        super().__init__(f'recovery campaign interrupted by signal {signum}')
        self.signum = signum


@contextmanager
def termination_signals():
    """Unwind the CLI's process and worker-directory owners on termination."""
    watched = (signal.SIGINT, signal.SIGTERM)
    previous = {}

    def interrupt(signum, _frame):
        # A second signal must not interrupt child termination/join or erase
        # worker data before the child has stopped. Cleanup is bounded by the
        # existing TERM/KILL timeout in stop_owned().
        for item in watched:
            signal.signal(item, signal.SIG_IGN)
        raise InterruptedRun(signum)

    try:
        for signum in watched:
            previous[signum] = signal.signal(signum, interrupt)
        yield
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def sha256(path):
    result = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def source_identity():
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    files = subprocess.check_output(['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], cwd=ROOT)
    inputs = []
    for encoded in sorted(set(files.split(b'\0')) - {b''}):
        name = os.fsdecode(encoded)
        if not (name.endswith(('.rs', 'Cargo.toml', 'Cargo.lock')) or name.startswith(('crates/', 'tools/perf/', '.cargo/'))):
            continue
        path = ROOT / name
        if path.is_symlink():
            resolved = path.resolve()
            require(resolved.is_relative_to(ROOT) and resolved.is_file(),
                    f'build input symlink needs explicit source ownership: {name}')
            inputs.append({'path': name, 'symlink': os.readlink(path), 'sha256': sha256(resolved)})
        elif path.is_file():
            inputs.append({'path': name, 'sha256': sha256(path)})
    return {'commit': commit, 'inputs_sha256': hashlib.sha256(canonical(inputs)).hexdigest()}, inputs


def stop_owned(process):
    if process.poll() is not None:
        return
    try:
        if os.name == 'posix':
            os.killpg(process.pid, signal.SIGTERM)
        else:
            process.terminate()
    except ProcessLookupError:
        process.wait()
        return
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        if os.name == 'posix':
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
        process.wait()


def run_process(command, output, *, timeout, env=None):
    """Own and join one process group even on timeout, cancellation or signals."""
    with output.open('wb') as log:
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=os.name == 'posix')
        timed_out = False
        try:
            try:
                code = process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                stop_owned(process)
                code = process.returncode
            return code, timed_out
        finally:
            stop_owned(process)



@contextmanager
def worker_environment(parent, trial_dir, *, physical, keep=False):
    """Only this newly allocated directory set can be removed by this runner."""
    env, owned = dict(parent), []
    try:
        primary = Path(parent['CELLAR_LARGE_IMAGE_WORKDIR']) if physical else trial_dir
        primary = Path(tempfile.mkdtemp(prefix='cold-recovery-', dir=primary))
        owned.append(primary)
        env['CELLAR_LARGE_IMAGE_WORKDIR'] = str(primary)
        env['CELLAR_RECOVERY_WORKDIR'] = str(primary)
        if physical and parent.get('CELLAR_LARGE_IMAGE_SECONDARY_WORKDIR'):
            secondary = Path(tempfile.mkdtemp(prefix='cold-recovery-', dir=parent['CELLAR_LARGE_IMAGE_SECONDARY_WORKDIR']))
            owned.append(secondary)
            env['CELLAR_LARGE_IMAGE_SECONDARY_WORKDIR'] = str(secondary)
        yield env, owned
    finally:
        if not keep:
            for directory in reversed(owned):
                shutil.rmtree(directory)


def select_test(profile, backend):
    require(backend in profile['backends'], 'backend is not supported by this profile')
    return f"recovery::{profile['fixture']}_{backend}_recovers_with_complete_phase_ledger"


def selected_results(text):
    selected = re.findall(r'^running ([0-9]+) tests?$', text, re.M)
    results = re.findall(r'^test result: ok\. ([0-9]+) passed; 0 failed;', text, re.M)
    return (int(selected[0]) if len(selected) == 1 else 0,
            int(results[0]) if len(results) == 1 else 0)


def binary_for(args, directory, identity):
    if args.binary:
        require(args.binary_manifest is not None, '--binary requires its source-bound --binary-manifest')
        manifest = json.loads(args.binary_manifest.read_text())
        require(manifest.get('build_profile') == 'release', 'prebuilt binary must have a release build manifest')
        require(all(manifest.get(key) == identity[key] for key in ('commit', 'inputs_sha256')),
                'prebuilt binary source differs')
        require(manifest.get('binary_sha256') == sha256(args.binary), 'prebuilt binary bytes differ')
        return args.binary.resolve()
    require(os.environ.get('CARGO_TARGET_DIR'), 'set a checkout-specific CARGO_TARGET_DIR before building')
    cargo = shutil.which('cargo') or str(Path.home() / '.cargo/bin/cargo')
    command = [cargo]
    if args.toolchain:
        command.append('+' + args.toolchain)
    command += ['test', '--locked', '--release', '-p', 'cellar', '--test', 'native_raft_large_images',
                '--no-run', '--message-format=json']
    code, timed_out = run_process(command, directory / 'build.jsonl', timeout=args.build_timeout_seconds)
    require(code == 0 and not timed_out, 'native build failed; inspect build.jsonl')
    artifacts = []
    for line in (directory / 'build.jsonl').read_text(errors='replace').splitlines():
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if (value.get('reason') == 'compiler-artifact' and value.get('target', {}).get('name') == 'native_raft_large_images'
                and value.get('executable')):
            artifacts.append(Path(value['executable']))
    require(len(artifacts) == 1, 'expected one native recovery test executable')
    return artifacts[0]


def run(args):
    require(1 <= args.repetitions <= 1000, 'repetitions must be in 1..=1000')
    require(args.trial_timeout_seconds > 0 and args.build_timeout_seconds > 0, 'timeouts must be positive')
    profiles = json.loads(PROFILES.read_text())
    require(profiles['format'] == 1 and args.profile in profiles['profiles'], 'unknown recovery profile')
    profile = profiles['profiles'][args.profile]
    test = select_test(profile, args.backend)
    if profile['ignored']:
        for name in ('CELLAR_LARGE_IMAGE_WORKDIR', 'CELLAR_LARGE_IMAGE_ENDPOINT', 'CELLAR_LARGE_IMAGE_BUCKET'):
            require(os.environ.get(name), f'missing required variable {name}')
    directory = args.output.resolve()
    directory.mkdir(mode=0o700, parents=False, exist_ok=False)
    before, inputs = source_identity()
    binary = binary_for(args, directory, before)
    after, _ = source_identity()
    require(before == after, 'source changed during native build')
    source = {**before, 'binary_sha256': sha256(binary), 'build_profile': 'release'}
    (directory / 'binary-manifest.json').write_text(json.dumps(source, indent=2) + '\n')
    (directory / 'inputs.json').write_text(json.dumps(inputs, indent=2) + '\n')
    code, _ = run_process([str(binary), '--exact', test, '--list'], directory / 'test-list.txt', timeout=30)
    listed = (directory / 'test-list.txt').read_text().splitlines()
    require(code == 0 and listed.count(test + ': test') == 1, 'exact recovery test is absent')
    campaign = {
        'format': 1, 'backend': args.backend, 'storage': args.storage,
        'image_bytes': profile['image_bytes'], 'requested_repetitions': args.repetitions,
        'source': source, 'profile': profile, 'profile_sha256': hashlib.sha256(canonical(profile)).hexdigest(),
        'reference_profile_verified': False, 'fault_suite': {'complete': False}, 'trials': [],
    }
    report_path = directory / 'campaign.json'
    def persist():
        report_path.write_text(json.dumps(campaign, indent=2, allow_nan=False) + '\n')
    persist()
    for number in range(1, args.repetitions + 1):
        trial_dir = directory / f'trial-{number:04d}'
        trial_dir.mkdir(mode=0o700)
        evidence = trial_dir / 'evidence.json'
        command = [str(binary), '--exact', test, '--nocapture', '--test-threads=1']
        if profile['ignored']:
            command.append('--ignored')
        with worker_environment(os.environ, trial_dir, physical=profile['ignored'], keep=args.keep_node_state) as (env, owned):
            env['CELLAR_RECOVERY_EVIDENCE'] = str(evidence)
            worker_paths = [str(path) for path in owned]
            code, timed_out = run_process(command, trial_dir / 'native.log', timeout=args.trial_timeout_seconds, env=env)
        selected, passed = selected_results((trial_dir / 'native.log').read_text(errors='replace'))
        try:
            value = json.loads(evidence.read_text())
        except (OSError, ValueError):
            value = None
        current, _ = source_identity()
        campaign['trials'].append({
            'returncode': code, 'timed_out': timed_out, 'selected_tests': selected, 'passed_tests': passed,
            'node_directories': worker_paths, 'node_directories_retained': args.keep_node_state,
            'source_unchanged': current == before and sha256(binary) == source['binary_sha256'],
            'evidence': value,
        })
        persist()
        print(f'trial {number}/{args.repetitions}: exit={code} selected={selected} passed={passed}', flush=True)
    from analyze_recovery import trial_intervals
    try:
        for trial in campaign['trials']:
            trial_intervals(trial, campaign)
    except ValueError:
        return 1
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--backend', choices=('peer', 'rustfs'), required=True)
    parser.add_argument('--storage', choices=('materialized',), default='materialized')
    parser.add_argument('--profile', default='reference-10gb')
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--keep-node-state', action='store_true', help="retain this run's synthetic worker directories for diagnosis")
    parser.add_argument('--binary', type=Path)
    parser.add_argument('--binary-manifest', type=Path)
    parser.add_argument('--toolchain')
    parser.add_argument('--trial-timeout-seconds', type=int, default=3600)
    parser.add_argument('--build-timeout-seconds', type=int, default=1800)
    args = parser.parse_args()
    with termination_signals():
        try:
            return run(args)
        except InterruptedRun as error:
            print(f'ERROR: {error}', file=sys.stderr)
            return 128 + error.signum
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            print(f'ERROR: {error}', file=sys.stderr)
            return 1


if __name__ == '__main__':
    raise SystemExit(main())

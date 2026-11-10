# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Copy a private, immutable configuration snapshot through the Docker API.

Do not bind mutable host files: virtiofs may expose mixed file size/content across
atomic host replacements. No process starts until Docker returns exact bytes.
"""
import hashlib
import io
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile


FILES = ('config.json', 'ledger.json')


def read_copy(context, identifier, name):
    if re.fullmatch('[0-9a-f]{64}', identifier) is None or name not in FILES:
        raise ValueError('invalid configuration copy identity')
    result = subprocess.run(['docker', '--context', context, 'cp', identifier+':/config/'+name, '-'],
                            capture_output=True, timeout=240)
    if result.returncode:
        raise RuntimeError('Docker configuration readback failed')
    with tarfile.open(fileobj=io.BytesIO(result.stdout)) as archive:
        members = archive.getmembers()
        if len(members) != 1 or not members[0].isfile() or members[0].name != name or members[0].size > 4*1024*1024:
            raise ValueError('invalid Docker configuration readback')
        return archive.extractfile(members[0]).read()


def copy_configuration(context, identifier, directory):
    """Only configuration and verification tokens enter the container.

    The authority key and durable launcher ledger stay on the controller host.
    Caller owns the immutable container ID and keeps its process stopped.
    """
    if re.fullmatch('[0-9a-f]{64}', identifier) is None:
        raise ValueError('invalid configuration copy identity')
    contents = {name: (Path(directory)/name).read_bytes() for name in FILES}
    with tempfile.TemporaryDirectory(prefix='cellar-config-snapshot-') as temporary:
        snapshot = Path(temporary)/'config'
        snapshot.mkdir(mode=0o700)
        for name, data in contents.items():
            path = snapshot/name
            path.write_bytes(data)
            path.chmod(0o400)
        result = subprocess.run(['docker', '--context', context, 'cp', str(snapshot), identifier+':/'],
                                capture_output=True, timeout=240)
        if result.returncode:
            raise RuntimeError('Docker configuration copy failed')
    for name, expected in contents.items():
        if read_copy(context, identifier, name) != expected:
            raise ValueError('Docker configuration snapshot differs from controller bytes')
    return {name: hashlib.sha256(data).hexdigest() for name, data in contents.items()}

# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Run one exact outage case with four owner-checked RustFS transitions."""

import json
import os
from pathlib import Path
import re
import signal
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

from local import command

TRANSITIONS = (
    ("materialized", "stop"), ("materialized", "start"),
    ("extents", "stop"), ("extents", "start"),
)


def atomic_json(path, value):
    temporary = path.with_suffix(".pending")
    with temporary.open("x") as output:
        json.dump(value, output)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class BackendControl:
    def __init__(self, remote, directory):
        if (not re.fullmatch(r"cellar-multi-raft-[0-9a-f]{16}", remote["owner"])
                or remote["container"] != remote["owner"] + "-rustfs"
                or not re.fullmatch(r"colima-cellar-multi-raft(?:-[a-z0-9-]+)?", remote["context"])
                or not re.fullmatch(r"http://127\.0\.0\.1:[0-9]+", remote["endpoint"])):
            raise ValueError("requires the captured owned loopback backend")
        self.remote = remote
        self.directory = Path(directory).resolve()
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=False)
        self.docker = ["docker", "--context", remote["context"]]
        self.target = remote["container"]
        self.identity = self.inspect()
        self.verify(True)
        self.target = self.identity["id"]
        self.events = []
        self.status = "RUNNING"
        self.error = None
        self.cleanup = []
        self.save()

    def inspect(self):
        # Deliberately exclude Config.Env and all credential-bearing fields.
        return json.loads(command(self.docker + [
            "inspect", "--format",
            '{"id":{{json .Id}},"owner":{{json (index .Config.Labels "cellar.multi-raft.run")}},'
            '"image":{{json .Image}},"running":{{json .State.Running}},'
            '"cpus":{{json .HostConfig.NanoCpus}},"memory":{{json .HostConfig.Memory}},'
            '"swap":{{json .HostConfig.MemorySwap}},"ports":{{json .HostConfig.PortBindings}},'
            '"mounts":{{json .Mounts}}}', self.target,
        ]))

    def verify(self, running):
        actual = self.inspect()
        port = self.remote["endpoint"].rsplit(":", 1)[1]
        if (not re.fullmatch(r"[0-9a-f]{64}", actual["id"])
                or actual["id"] != self.identity["id"]
                or actual["owner"] != self.remote["owner"]
                or actual["image"] != self.remote["image_id"]
                or type(actual["running"]) is not bool or actual["running"] != running
                or (actual["cpus"], actual["memory"], actual["swap"])
                != (10**9, 2 * 1024**3, 2 * 1024**3)
                or actual["ports"].get("9000/tcp") != [{"HostIp": "127.0.0.1", "HostPort": port}]
                or actual["mounts"] != self.identity["mounts"]):
            raise ValueError("backend identity, owner, resource, mount or state changed")
        return actual

    def healthy(self):
        deadline = time.monotonic() + 20
        while True:
            try:
                with urllib.request.urlopen(self.remote["endpoint"] + "/health", timeout=1) as response:
                    if response.status == 200:
                        return
            except (urllib.error.URLError, TimeoutError, OSError):
                pass
            if time.monotonic() >= deadline:
                raise RuntimeError("owned RustFS restart health deadline expired")
            time.sleep(0.02)

    def transition(self, request):
        sequence = len(self.events)
        if sequence >= len(TRANSITIONS):
            raise ValueError("too many backend control operations")
        mode, action = TRANSITIONS[sequence]
        if (set(request) != {"format", "sequence", "mode", "action"}
                or type(request["format"]) is not int or request["format"] != 1
                or type(request["sequence"]) is not int or request["sequence"] != sequence
                or (request["mode"], request["action"]) != (mode, action)):
            raise ValueError("unexpected backend transition request")
        running = action == "start"
        before = self.verify(not running)
        started = time.monotonic()
        if running:
            command(self.docker + ["start", self.target])
            self.healthy()
        else:
            command(self.docker + ["stop", "--time", "5", self.target])
        after = self.verify(running)
        self.events.append({"sequence": sequence, "mode": mode, "action": action,
                            "before": before, "after": after,
                            "elapsed_seconds": round(time.monotonic() - started, 3)})
        self.save()
        atomic_json(self.directory / f"{sequence:02}-response.json",
                    {**request, "running": running})

    def save(self):
        path = self.directory / "record.json"
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps({"format": 1, "status": self.status,
                                        "container_id": self.identity["id"],
                                        "events": self.events, "cleanup": self.cleanup,
                                        "error": self.error},
                                       indent=2) + "\n")
        temporary.replace(path)

    def restore(self):
        actual = self.inspect()
        # Identity/resource checks also apply to cleanup; never start a replacement.
        self.verify(actual["running"])
        if not actual["running"]:
            command(self.docker + ["start", self.target])
            self.healthy()
            self.cleanup.append({"restarted_owned_backend": self.verify(True)})
        else:
            self.cleanup.append({"already_running": True})
        self.save()

    def run(self, argv, cwd, env):
        child_env = dict(env, CELLAR_CELL_GROUPS_RUSTFS_CONTROL=str(self.directory))
        with tempfile.TemporaryFile(mode="w+") as output:
            process = subprocess.Popen(argv, cwd=cwd, env=child_env, stdout=output,
                                       stderr=subprocess.STDOUT, start_new_session=True)
            deadline = None  # Build time precedes the infrastructure experiment.
            returncode = 125
            try:
                while process.poll() is None:
                    sequence = len(self.events)
                    request = self.directory / f"{sequence:02}-request.json"
                    for observed in self.directory.glob("*-request.json"):
                        if not re.fullmatch(r"[0-3][0-9]-request\.json", observed.name):
                            raise ValueError("unexpected backend control filename")
                        if int(observed.name[:2]) > sequence:
                            raise ValueError("out-of-order backend control request")
                    if request.exists():
                        with request.open("rb") as source:
                            encoded = source.read(4097)
                        if len(encoded) > 4096:
                            raise ValueError("backend control request exceeds limit")
                        if deadline is None:
                            deadline = time.monotonic() + 180
                        self.transition(json.loads(encoded))
                    if deadline is not None and time.monotonic() >= deadline:
                        raise RuntimeError("backend outage experiment deadline expired")
                    time.sleep(0.01)
                returncode = process.wait()
                if returncode or len(self.events) != len(TRANSITIONS):
                    raise RuntimeError("outage case failed or omitted a backend transition")
                self.verify(True)
                self.status = "PASSED"
            except BaseException as error:
                self.status = "FAILED"
                self.error = type(error).__name__ + ": " + str(error)
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
                returncode = process.wait() or 125
            finally:
                try:
                    self.restore()
                except BaseException as error:
                    self.status = "FAILED"
                    self.error = "cleanup " + type(error).__name__ + ": " + str(error)
                    returncode = returncode or 125
                    self.save()
            output.seek(0)
            return subprocess.CompletedProcess(argv, returncode, output.read())

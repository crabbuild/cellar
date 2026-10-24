#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Keep a private three-node FleetCare lab and its Cellar console running in Colima."""

import argparse
import fcntl
import hashlib
import json
import os
import pathlib
import secrets
import shlex
import socket
import subprocess
import time
import urllib.error
import urllib.request

from fleetcare import ROOT, RUSTFS
from run import create_bucket


def private_file(path, content):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        os.fchmod(stream.fileno(), 0o600)
        stream.write(content)


def console_config(port):
    return {
        "listen": "0.0.0.0:4174", "assets": "/opt/cellar/console",
        "audit": "/data/console-audit.sqlite",
        "origins": [f"http://127.0.0.1:{port}", f"http://localhost:{port}"],
        "fleets": [{"id": "fleetcare", "name": "FleetCare", "environment": "Local",
                    "bucket": "s3://fleetcare", "endpoint": "http://rustfs:9000",
                    "region": "us-east-1"}],
        "users": [{"id": "builder", "token_env": "CELLAR_DASHBOARD_TOKEN",
                   "grants": {"fleetcare": ["metadata_read", "data_read", "data_write",
                                            "operate", "configure"]}}],
    }


def available_port(preferred, fallback):
    if not 1024 <= preferred <= 65525:
        raise RuntimeError("Console port must leave room for the reserved node and store ports")
    for port in range(preferred, 65526, 100) if fallback else [preferred]:
        sockets = []
        try:
            for offset in (0, 1, 2, 3, 10):
                listener = socket.socket()
                sockets.append(listener)
                listener.bind(("127.0.0.1", port + offset))
            return port
        except OSError:
            pass
        finally:
            for listener in sockets:
                listener.close()
    raise RuntimeError("Console or node ports are occupied; choose another --port")


class Lab:
    def __init__(self, args):
        self.args = args
        checkout = hashlib.sha256(str(ROOT).encode()).hexdigest()[:8]
        self.profile = f"cellar-fleetcare-{ROOT.parent.name}" if ROOT.parent.parent.name == "worktrees" else f"cellar-fleetcare-{checkout}"
        self.context = f"colima-{self.profile}"
        self.image = f"cellar-fleetcare-console:{checkout}"
        self.directory = ROOT / ".cellar/fleetcare-console"
        self.directory.mkdir(parents=True, exist_ok=True)
        self.directory.chmod(0o700)
        self.state_path = self.directory / "lab.json"
        self.vm_root = f"/mnt/lima-colima-{self.profile}/fleetcare"

    def run(self, *args, **kwargs):
        result = subprocess.run(args, text=True, capture_output=True, **kwargs)
        if result.returncode:
            # Environment values are supplied via private files, never command arguments.
            raise RuntimeError(f"{' '.join(args[:4])}: {result.stderr or result.stdout}")
        return result.stdout.strip()

    def docker(self, *args, **kwargs):
        return self.run("docker", "--context", self.context, *args, **kwargs)

    def ssh(self, script):
        return self.run("colima", "--profile", self.profile, "ssh", "--", "sudo", "bash", "-s", input=script)

    def save(self, state):
        private_file(self.state_path, json.dumps(state, indent=2) + "\n")

    def state(self):
        if not self.state_path.exists():
            raise RuntimeError("No lab configured; run up first")
        return json.loads(self.state_path.read_text())

    def env_file(self, name, values):
        path = self.directory / f"{name}.env"
        private_file(path, "".join(f"{key}={value}\n" for key, value in values.items()))
        return str(path)

    def wait(self, url, headers=None, timeout=120):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(urllib.request.Request(url, headers=headers or {}), timeout=5) as response:
                    return response.read()
            except (OSError, urllib.error.URLError):
                time.sleep(1)
        raise RuntimeError(f"Readiness timeout: {url}; inspect docker logs in context {self.context}")

    def ensure_container(self, name, *arguments):
        names = self.docker("ps", "-a", "--format", "{{.Names}}").splitlines()
        image = self.image if self.image in arguments else RUSTFS
        if name in names:
            observed = self.docker("inspect", "--format", "{{.Image}}", name)
            desired = self.docker("image", "inspect", "--format", "{{.Id}}", image)
            if observed != desired:
                self.docker("stop", "--time", "150", name)
                self.docker("rm", name)
                names.remove(name)
        if name in names:
            self.docker("start", name)
        else:
            self.docker("run", "-d", "--name", name, "--restart", "unless-stopped", *arguments)

    def disks(self):
        root = shlex.quote(self.vm_root)
        # Existing disks are only mounted, never formatted again. Fail closed if a
        # retained image is not ext4, or a mount is backed by another image.
        script = f"""set -euo pipefail
mkdir -p {root}
for node in 1 2 3; do
  image={root}/node${{node}}.img
  directory={root}/node${{node}}
  if [ ! -e "$image" ]; then
    truncate -s 4G "$image"
    mkfs.ext4 -q -F -m 0 "$image"
  fi
  test "$(stat -c %s "$image")" = 4294967296
  test "$(blkid -p -s TYPE -o value "$image")" = ext4
  mkdir -p "$directory"
  if mountpoint -q "$directory"; then
    device=$(findmnt -n -o SOURCE --target "$directory")
    test "$(losetup -n -O BACK-FILE "$device")" = "$image"
  else
    mount -o loop,noatime "$image" "$directory"
  fi
  entry="$image $directory ext4 loop,noatime 0 0"
  grep -Fqx "$entry" /etc/fstab || echo "$entry" >> /etc/fstab
done
mkdir -p /etc/systemd/system/docker.service.d
echo '[Unit]' > /etc/systemd/system/docker.service.d/fleetcare-disks.conf
echo 'RequiresMountsFor={self.vm_root}/node1 {self.vm_root}/node2 {self.vm_root}/node3' >> /etc/systemd/system/docker.service.d/fleetcare-disks.conf
systemctl daemon-reload
df -B1 {root}/node{{1,2,3}}
"""
        private_file(self.directory / "disks.txt", self.ssh(script) + "\n")

    def up(self):
        print(f"Starting private Colima profile {self.profile}", flush=True)
        self.run("colima", "start", self.profile, "--activate=false", "--cpu", "6", "--memory", "10", "--disk", "40")
        if self.state_path.exists():
            state = self.state()
            if state["profile"] != self.args.profile or (self.args.port is not None and state["port"] != self.args.port):
                raise RuntimeError("Use the existing lab's --profile and --port; retained state cannot be reconfigured")
        else:
            state = {"profile": self.args.profile, "port": self.args.port or 4174,
                     "access": "fleetcare-" + secrets.token_hex(8), "secret": secrets.token_hex(32),
                     "app_token": secrets.token_hex(32), "console_token": secrets.token_hex(32),
                     "seeded": False}
            self.save(state)
        if not self.args.no_build:
            print(f"Building application and console; log: {self.directory / 'build.log'}", flush=True)
            with (self.directory / "build.log").open("w") as output:
                subprocess.run(["docker", "--context", self.context, "build", "--progress=plain",
                                "-f", "tools/perf/fleetcare-console.Dockerfile", "-t", self.image,
                                "--build-arg", f"TARGET_CACHE_ID=fleetcare-console-{self.profile}", "."],
                               cwd=ROOT, stdout=output, stderr=subprocess.STDOUT, check=True)
        image = self.docker("image", "inspect", "--format", "{{.Id}}", self.image)
        if state.get("image") != image:
            state["published"] = False
            state["image"] = image
            self.save(state)
        existing = set(self.docker("ps", "-a", "--format", "{{.Names}}").splitlines())
        if not existing.intersection({"fleetcare-console", "fleetcare-node1", "fleetcare-node2", "fleetcare-node3"}):
            state["port"] = available_port(state["port"], self.args.port is None)
            self.save(state)
        self.disks()
        networks = self.docker("network", "ls", "--format", "{{.Name}}").splitlines()
        if "fleetcare" not in networks:
            self.docker("network", "create", "fleetcare")
        store_env = self.env_file("store", {"RUSTFS_ACCESS_KEY": state["access"],
            "RUSTFS_SECRET_KEY": state["secret"], "RUSTFS_CONSOLE_ENABLE": "false",
            "RUSTFS_OBS_LOGGER_LEVEL": "warn"})
        self.ensure_container("fleetcare-store", "--network", "fleetcare", "--network-alias", "rustfs",
            "--cpus", "1", "--memory", "2g", "--memory-swap", "2g", "--env-file", store_env,
            "--publish", f"127.0.0.1:{state['port']+10}:9000",
            "--mount", "type=volume,src=fleetcare-store,dst=/data", RUSTFS, "/data")
        store_url = f"http://127.0.0.1:{state['port']+10}"
        self.wait(store_url + "/health")
        deadline = time.monotonic() + 90
        while True:
            try:
                create_bucket(store_url, "fleetcare", state["access"], state["secret"])
                break
            except urllib.error.HTTPError as error:
                body = error.read().decode()
                if error.code == 409 and "BucketAlreadyOwnedByYou" in body:
                    break
                if error.code != 503 or time.monotonic() >= deadline:
                    raise RuntimeError(f"Bucket creation failed: HTTP {error.code}") from error
                time.sleep(1)
        self.ensure_container("fleetcare-technician", "--network", "fleetcare", "--network-alias", "technician",
            "--cpus", "0.25", "--memory", "128m", "--memory-swap", "128m", "--entrypoint", "python3",
            self.image, "/usr/local/bin/fleetcare-simulator.py")
        aws = {"AWS_ACCESS_KEY_ID": state["access"], "AWS_SECRET_ACCESS_KEY": state["secret"]}
        if not state.get("published"):
            env = self.env_file("publisher", {**aws, "FLEETCARE_TOKEN": state["app_token"],
                "FLEETCARE_PROFILE": state["profile"], "FLEETCARE_BUCKET": "fleetcare",
                "FLEETCARE_ENDPOINT": "http://rustfs:9000"})
            self.docker("run", "--rm", "--network", "fleetcare", "--env-file", env,
                "--cpus", "1", "--memory", "1g", "--entrypoint", "fleetcare-publish", self.image)
            state["published"] = True
            self.save(state)
        for node in range(1, 4):
            env = self.env_file(f"node{node}", {**aws, "CELLAR_NODE": f"node{node}",
                "FLEETCARE_TOKEN": state["app_token"], "FLEETCARE_PROFILE": state["profile"],
                "FLEETCARE_ASYNC": "1", "FLEETCARE_SIMULATOR": "http://technician:8080",
                "CELLAR_TEST_DATA_DIR": "/data/state", "CELLAR_ASSET_CACHE_DIR": "/data/asset-cache",
                "CELLAR_NATIVE_FLEET": "1", "CELLAR_TOKIO_THREADS": "1",
                "CELLAR_PLACEMENT_WEIGHT": "1",
                "CELLAR_MAX_RSS_MB": "0", "CELLAR_MAX_RESIDENT_CELLS": "256",
                "CELLAR_MAX_CACHED_ROUTES": "2048", "CELLAR_MAX_REQUESTS": "128",
                "CELLAR_DURABILITY": "bucket", "CELLAR_SHUTDOWN_TOTAL_MS": "120000",
                "CELLAR_SHUTDOWN_REPORT_JSON": "1", "RUST_LOG": "warn"})
            self.ensure_container(f"fleetcare-node{node}", "--network", "fleetcare", "--network-alias", f"node{node}",
                "--cpus", "1", "--memory", "2g", "--memory-swap", "2g", "--read-only",
                "--tmpfs", "/tmp:rw,size=64m", "--ulimit", "nofile=65536:65536", "--env-file", env,
                "--publish", f"127.0.0.1:{state['port']+node}:8080",
                "--mount", f"type=bind,src={self.vm_root}/node{node},dst=/data",
                self.image, "--bucket", "fleetcare", "--endpoint", "http://rustfs:9000", "--region", "us-east-1",
                "--listen", "0.0.0.0:8080", "--internal-listen", "0.0.0.0:8081", "--advertise", f"node{node}:8081")
        for node in range(1, 4):
            self.wait(f"http://127.0.0.1:{state['port']+node}/.well-known/cellar/health")
        config = self.directory / "console.json"
        private_file(config, json.dumps(console_config(state["port"]), indent=2) + "\n")
        env = self.env_file("console", {**aws, "CELLAR_DASHBOARD_TOKEN": state["console_token"]})
        private_file(self.directory / "dashboard-token.txt", state["console_token"] + "\n")
        self.ensure_container("fleetcare-console", "--network", "fleetcare", "--cpus", "0.5",
            "--memory", "512m", "--memory-swap", "512m", "--env-file", env,
            "--publish", f"127.0.0.1:{state['port']}:4174",
            "--mount", f"type=bind,src={config},dst=/config/console.json,readonly",
            "--mount", "type=volume,src=fleetcare-console-audit,dst=/data",
            "--entrypoint", "cellar-dashboard", self.image, "--config", "/config/console.json")
        self.wait(f"http://127.0.0.1:{state['port']}/api/v1/session",
                  {"Authorization": f"Bearer {state['console_token']}"})
        if not state["seeded"]:
            print(f"Seeding {state['profile']} sample inventory", flush=True)
            self.client(state, "seed")
            state["seeded"] = True
            self.save(state)
        self.status()

    def client(self, state, action):
        config = {"action": action, "urls": [f"http://node{node}:8080" for node in range(1, 4)],
                  "token": state["app_token"], "manifest": "/results/manifest.json",
                  "ledger": "/results/seed.jsonl", "report": f"/results/{action}.json", "concurrency": 4}
        file = self.directory / "client.json"
        private_file(file, json.dumps(config))
        # Remove only a completed previous client, retaining failed seed evidence.
        name = "fleetcare-client"
        names = self.docker("ps", "-a", "--format", "{{.Names}}").splitlines()
        if name in names:
            running = self.docker("inspect", "--format", "{{.State.Running}}", name)
            if running == "true":
                raise RuntimeError("A seed/audit client is already running")
            self.docker("rm", name)
        self.docker("run", "-d", "--name", name, "--network", "fleetcare", "--cpus", "1", "--memory", "1g",
            "--mount", f"type=bind,src={file},dst=/config/client.json,readonly",
            "--mount", "type=volume,src=fleetcare-results,dst=/results", "--entrypoint", "fleetcare-load",
            self.image, "/config/client.json")
        code = self.docker("wait", name)
        logs = subprocess.run(["docker", "--context", self.context, "logs", name],
                              text=True, capture_output=True, check=True)
        private_file(self.directory / f"{action}.log", logs.stdout + logs.stderr)
        if code != "0":
            raise RuntimeError(f"{action} failed; inspect docker logs {name} in context {self.context}")
        self.docker("cp", f"{name}:/results/{action}.json", str(self.directory / f"{action}.json"))

    def status(self):
        state = self.state()
        print(self.docker("ps", "-a", "--format", "table {{.Names}}\t{{.Status}}\t{{.Ports}}"))
        print(f"\nConsole: http://127.0.0.1:{state['port']}\nDocker context: {self.context}")
        print(f"Login token: {self.directory / 'dashboard-token.txt'}")
        print("Nodes: 3 × 1 vCPU / 2 GiB RAM / 4 GiB private ext4 disk")

    def stop(self):
        # Stopping retains all disks, bucket objects, configuration and console audit.
        for name in ["fleetcare-console", "fleetcare-client", *[f"fleetcare-node{node}" for node in range(1, 4)],
                     "fleetcare-technician", "fleetcare-store"]:
            names = self.docker("ps", "--format", "{{.Names}}").splitlines()
            if name in names:
                timeout = "150" if name.startswith("fleetcare-node") else "30"
                self.docker("stop", "--time", timeout, name)
        print("Lab stopped; run up --no-build to resume retained data")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["up", "status", "stop", "audit"])
    parser.add_argument("--profile", choices=["smoke", "full"], default="smoke")
    parser.add_argument("--port", type=int)
    parser.add_argument("--no-build", action="store_true")
    args = parser.parse_args()
    if args.port is not None and not 1024 <= args.port <= 65525:
        parser.error("port must be in 1024..65525 (reserves the next ten ports)")
    lab = Lab(args)
    if args.action == "status":
        lab.status()
        return
    with (lab.directory / "lock").open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("Another lab operation is running") from error
        if args.action == "up":
            lab.up()
        elif args.action == "stop":
            lab.stop()
        else:
            lab.client(lab.state(), "audit")
            print(f"Audit saved to {lab.directory / 'audit.json'}")


if __name__ == "__main__":
    main()

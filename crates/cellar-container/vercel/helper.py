# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Binary-safe RPC helper executed inside the host-selected Vercel image.

Input arrives on stdin, never interpolated into code or a shell command.
Only ASCII JSON frames reach Vercel's text-log transport.
"""
import base64
import json
import os
import selectors
import signal
import stat
import subprocess
import sys
import time


def emit(value):
    print(json.dumps(value, ensure_ascii=True, separators=(",", ":")), flush=True)


def metadata(path):
    item = os.lstat(path)
    kind = ("file" if stat.S_ISREG(item.st_mode) else
            "directory" if stat.S_ISDIR(item.st_mode) else
            "symlink" if stat.S_ISLNK(item.st_mode) else "other")
    return {"name": os.path.basename(path.rstrip("/")) or "/",
            "kind": kind, "size": item.st_size, "mode": stat.S_IMODE(item.st_mode) & 0o777,
            "modified_ms": item.st_mtime_ns // 1_000_000,
            "link_target": os.readlink(path) if kind == "symlink" else None}


def command(options, budget, deadline_ms):
    env = dict(os.environ)
    env.update(options.get("env", {}))
    process = subprocess.Popen(options["cmd"], cwd=options.get("cwd"), env=env,
                               stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, start_new_session=True)
    deadline = time.monotonic() + deadline_ms / 1000
    try:
        with selectors.DefaultSelector() as selector:
            for stream, name in ((process.stdout, "stdout"), (process.stderr, "stderr")):
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ, name)
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("command deadline exceeded")
                for key, _ in selector.select(min(remaining, 1)):
                    data = os.read(key.fileobj.fileno(), 16 * 1024)
                    if not data:
                        selector.unregister(key.fileobj)
                        continue
                    if len(data) > budget:
                        raise ValueError("combined command output exceeds budget")
                    budget -= len(data)
                    emit({"stream": key.data, "data": base64.b64encode(data).decode("ascii")})
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("command deadline exceeded")
        return process.wait(timeout=remaining)
    finally:
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
        process.stdout.close()
        process.stderr.close()


def main():
    raw = sys.stdin.buffer.read(64 * 1024 * 1024 + 1)
    if len(raw) > 64 * 1024 * 1024:
        raise ValueError("helper input exceeds limit")
    request = json.loads(raw)
    if request["format"] != 1:
        raise ValueError("unsupported helper format")
    operation = request["operation"]
    if operation == "exec":
        code = command(request["options"], request["max_bytes"], request["deadline_ms"])
    else:
        path = request["path"]
        if operation == "write":
            data = base64.b64decode(request["data"], validate=True)
            # Existing parents only. No mkdir, shell, or host-side extraction.
            with open(path, "wb") as output:
                output.write(data)
                output.flush()
                os.fchmod(output.fileno(), request["mode"])
            result = None
        elif operation == "stat":
            result = metadata(path)
        elif operation == "list":
            result = []
            with os.scandir(path) as entries:
                for entry in entries:
                    if len(result) >= request["max_entries"]:
                        raise ValueError("directory exceeds entry limit")
                    result.append(metadata(entry.path))
                    if len(json.dumps(result, ensure_ascii=False).encode("utf-8")) > request["max_bytes"]:
                        raise ValueError("directory exceeds byte limit")
            result.sort(key=lambda entry: entry["name"])
        else:
            raise ValueError("unsupported helper operation")
        emit({"result": result})
        code = 0
    emit({"end": code})


try:
    main()
except Exception:
    # Native failures preserve uncertainty; no paths, env values or secrets
    # belong in the transport's error diagnostics.
    emit({"error": "Vercel helper failed; submitted effects may have occurred"})
    sys.exit(1)

"""Private Linux diagnostic: correlate lease failures with pressure/scheduling."""
import datetime
import json
from pathlib import Path
import time


def read(path):
    try:
        return path.read_text()
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return None


while True:
    row = dict(wall_time=datetime.datetime.now(datetime.timezone.utc).isoformat(),
               monotonic_ns=time.monotonic_ns())
    row["system"] = {name: read(Path("/proc") / name) for name in
                     ("stat", "loadavg", "pressure/cpu", "pressure/io", "pressure/memory", "diskstats")}
    row["hosts"] = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdecimal():
            continue
        name = read(entry / "comm")
        if name is None or name.strip() not in ("load-native", "load-python"):
            continue
        host = {name: read(entry / name) for name in ("stat", "schedstat", "io", "status")}
        try:
            host["threads"] = {task.name: read(task / "schedstat") for task in (entry / "task").iterdir()}
        except FileNotFoundError:
            host["threads"] = None
        row["hosts"][entry.name] = host
    print(json.dumps(row), flush=True)
    time.sleep(1)

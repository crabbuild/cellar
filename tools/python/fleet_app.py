"""Fleet counter counterpart to tools/perf/cells.rs; configuration comes from main."""
import json
import hashlib
import sys
from cellar import Response

print(json.dumps({"event": "embedded_python_started", "version": sys.version,
                  "free_threaded": "t" in sys.abiflags,
                  "gil_enabled": sys._is_gil_enabled(), "limits": LIMITS,
                  "path_mode": PATH_MODE}), flush=True)


async def fetch(request, ctx):
    name = request.path.removeprefix("/cells/")
    if not request.path.startswith("/cells/") or not name:
        raise ValueError("expected /cells/NAME")
    return await ctx.namespace("ENTITIES").get_by_name(name).fetch(request)


class Entity:
    async def fetch(self, request, ctx):
        previous = (ctx.initial_storage["state"] if PATH_MODE == "direct-initial"
                    else await ctx.storage.read_json("state"))
        state = previous.value if previous.present else {"writes": 0, "payload": ""}
        if request.method == "POST":
            writes = state["writes"] + 1
            if writes >= 2**64:
                raise OverflowError("counter overflow")
            state = {"writes": writes, "payload": request.body.decode("utf-8")}
            # Match the native transaction's atomic increment and overwrite.
            # A checked batch refuses conflicting native writes; never replay an
            # ambiguous mutation or acknowledge a value whose batch failed.
            await ctx.storage.apply(checks={"state": previous}, puts={"state": state})
        elif request.method != "GET":
            raise ValueError("expected GET or POST")
        payload = state["payload"].encode("utf-8")
        result = {"writes": state["writes"], "payload_bytes": len(payload), "node": NODE}
        if request.method == "GET" and any(
                key.lower() == "x-cellar-verify-payload" and value == "sha256"
                for key, value in request.headers):
            # Hash the recovered bytes, never a stored or caller-supplied digest.
            # Only recovery audits request this extra work.
            result["payload_sha256"] = hashlib.sha256(payload).hexdigest()
        response = Response.json(result)
        response.headers.append(("x-cellar-owner", NODE))
        return response

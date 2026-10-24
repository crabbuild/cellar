"""Fleet counter counterpart to tools/perf/cells.rs; configuration comes from main."""
import json
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
        response = Response.json({"writes": state["writes"],
                                  "payload_bytes": len(state["payload"].encode("utf-8")),
                                  "node": NODE})
        response.headers.append(("x-cellar-owner", NODE))
        return response

"""Deterministic admission, task, resident-object and bridge saturation."""
import asyncio
import contextvars
import json
import threading
import weakref
from cellar import Request, Response

_lock = threading.RLock()
_active = {}
_references = {}
_visits = 0
_created = 0
_background = []


def snapshot():
    with _lock:
        return {"active": sorted(_active), "live": sum(ref() is not None for ref in _references.values()),
                "visits": _visits, "created": _created, "background": len(_background)}


def release(key):
    with _lock:
        event, loop = _active[key]
    if loop is None:
        event.set()
    else:
        loop.call_soon_threadsafe(event.set)


async def hold(key, blocking=False):
    event = threading.Event() if blocking else asyncio.Event()
    with _lock:
        _active[key] = (event, None if blocking else asyncio.get_running_loop())
    try:
        if blocking:
            assert event.wait(10), "supervisor did not release the bounded loop barrier"
        else:
            await event.wait()
    finally:
        with _lock:
            del _active[key]


async def background(loud, stubborn):
    try:
        while True:
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                if not stubborn:
                    raise
    finally:
        if loud:
            raise RuntimeError("PRIVATE-UNMANAGED-EXCEPTION-SENTINEL")


async def fetch(request, ctx):
    global _visits
    with _lock:
        _visits += 1
    if request.path.startswith("/background/"):
        loud = request.path.endswith("error")
        stubborn = not loud
        def create():
            task = asyncio.create_task(background(loud, stubborn))
            with _lock:
                _background.append(task)
        create()
        create()
        # The root finishes before this callback fills the last task slot.
        asyncio.get_running_loop().call_soon(create, context=contextvars.Context())
    elif request.path.startswith("/held/"):
        await hold(request.path.rsplit("/", 1)[-1])
    elif request.path.startswith("/block/"):
        await hold(request.path.rsplit("/", 1)[-1], blocking=True)
    elif request.path in ("/tasks/check", "/tasks/hold"):
        children = []
        try:
            children = [asyncio.create_task(asyncio.sleep(60)) for _ in range(2)]
            if request.path.endswith("check"):
                scoped = False
                try:
                    children.append(asyncio.create_task(asyncio.sleep(60)))
                except RuntimeError as error:
                    assert str(error) == "Cellar Python task capacity exhausted"
                    scoped = True
                checked = asyncio.get_running_loop().create_future()
                def callback():
                    try:
                        task = asyncio.create_task(asyncio.sleep(60))
                    except RuntimeError as error:
                        assert str(error) == "Cellar Python task capacity exhausted"
                        checked.set_result(True)
                    else:
                        children.append(task)
                        checked.set_result(False)
                asyncio.get_running_loop().call_soon(callback, context=contextvars.Context())
                unscoped = await checked
                return Response.json({"scoped": scoped, "unscoped": unscoped})
            await hold("tasks")
        finally:
            for task in children:
                task.cancel()
            await asyncio.gather(*children, return_exceptions=True)
    elif request.path == "/replies":
        # Two replies fit; the third must fail while this loop cannot drain.
        # No fourth submission races the native driver's resulting retirement.
        for index in range(3):
            ctx._bridge.submit(1000 + index, "var", json.dumps({"name": "DOMAIN_MARKER"}))
        await hold("replies", blocking=True)
    elif request.path.startswith("/cell/"):
        return await ctx.namespace("COUNTERS").get_by_name(request.path.rsplit("/", 1)[-1]).fetch(request)
    return Response.json(True)


class Counter:
    def __init__(self):
        global _created
        with _lock:
            _created += 1
            self.serial = _created
            _references[self.serial] = weakref.ref(self)

    async def fetch(self, request, ctx):
        return Response.json({"serial": self.serial, "scope": ctx.identity.scope, "epoch": ctx.identity.epoch})

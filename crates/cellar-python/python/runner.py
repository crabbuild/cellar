"""One interpreter and asyncio loop; no Cellar operations run on this thread."""
import asyncio
import inspect
import json
import threading
from cellar import Batch, Context, Request, Response, Scheduled, _encode, _current_invocation


def validate(module, descriptor):
    """Cold validation only; constructors/handlers are never run here."""
    descriptor = json.loads(descriptor)
    def handlers(target, events, arguments):
        for event in events:
            handler = getattr(target, event, None)
            if not inspect.iscoroutinefunction(handler):
                raise TypeError("Registered Cellar handlers must be async functions")
            inspect.signature(handler).bind(*([None] * arguments))
    handlers(module, descriptor["events"], 2)
    for name, events in descriptor["cells"].items():
        cell = getattr(module, name, None)
        if not inspect.isclass(cell):
            raise TypeError("Registered Cellar cells must be classes")
        inspect.signature(cell).bind()
        for constructor in (cell.__new__, cell.__init__):
            if inspect.iscoroutinefunction(constructor) or inspect.isasyncgenfunction(constructor):
                raise TypeError("Cellar constructors must be synchronous")
        handlers(cell, events, 3)


async def _invoke(module, objects, payload, bridge, context, stats, maximum):
    token = _current_invocation.set(bridge)
    try:
        if not bridge.active():
            return
        target = module
        if payload["object"] is not None:
            key = payload["object"]
            if key not in objects:
                if len(objects) >= maximum:
                    stats["budget_exhaustions"] += 1
                    raise RuntimeError("Cellar Python object capacity exhausted")
                objects[key] = (getattr(module, payload["class"])(), bridge)
                stats["objects_created"] += 1
            target = objects[key][0]
        kind, value = payload["kind"], payload["event"]
        handler = getattr(target, kind)
        argument = {"fetch": lambda: Request._from_wire(value),
                    "queue": lambda: Batch(context, value),
                    "scheduled": lambda: Scheduled(context, value),
                    "alarm": lambda: value["scheduled_ms"]}[kind]()
        result = handler(argument, context)
        if not inspect.isawaitable(result):
            raise TypeError("Cellar handlers must return an awaitable")
        result = await result
        if kind == "fetch" and not isinstance(result, Response):
            raise TypeError("fetch must return cellar.Response")
        encoded = _encode(result._wire() if kind == "fetch" else None)
        bridge.complete(encoded, False)
    except asyncio.CancelledError:
        raise
    except BaseException:
        # Exception messages, class names and tracebacks can contain request data
        # or credentials. The public failure carries no application-supplied text.
        if bridge.active():
            bridge.complete('"Python handler failed"', True)
    finally:
        context._close()
        _current_invocation.reset(token)


async def _serve(module, inbox, interval, shutdown, stats, maximum, max_tasks):
    invocations, objects = {}, {}
    try:
        while not inbox.done():
            for invocation, operation, result in inbox.replies():
                entry = invocations.get(invocation)
                if entry:
                    entry[1]._reply(operation, result)
            while (start := inbox.take()) is not None:
                invocation, payload, bridge = start
                context = Context(bridge, json.loads(payload)["identity"])
                if len(asyncio.all_tasks()) >= max_tasks:
                    stats["budget_exhaustions"] += 1
                    token = _current_invocation.set(bridge)
                    try:
                        bridge.complete('"Python task capacity exhausted"', True)
                    finally:
                        context._close()
                        _current_invocation.reset(token)
                    continue
                task = asyncio.create_task(_invoke(module, objects, json.loads(payload), bridge, context, stats, maximum))
                invocations[invocation] = (task, context, bridge)
            for invocation, (task, context, bridge) in tuple(invocations.items()):
                if not bridge.active() and not task.done():
                    task.cancel()
                if task.done():
                    # Retrieve exceptions, including failures while delivering a completion.
                    if not task.cancelled():
                        error = task.exception()
                        if error:
                            raise error
                    invocations.pop(invocation)
            for key, (_, bridge) in tuple(objects.items()):
                if not bridge.owner_active():
                    del objects[key]
                    stats["objects_retired"] += 1
            await asyncio.sleep(interval)
    finally:
        # Includes child tasks created directly by application code. Awaiting is
        # bounded; Python that blocks the interpreter requires a process watchdog.
        inbox.stop()
        for _, context, _ in invocations.values():
            context._close()
        remaining = asyncio.all_tasks() - {asyncio.current_task()}
        for task in remaining:
            task.cancel()
        pending = set()
        if remaining:
            finished, pending = await asyncio.wait(remaining, timeout=shutdown)
            for task in finished:
                if not task.cancelled():
                    task.exception()
        stats["objects_retired"] += len(objects)
        objects.clear()
    return len(pending), len(objects)


def run(module, inbox, interval, shutdown, max_tasks, max_objects):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    stats = dict(pending_tasks=0, pending_objects=0, pending_threads=0,
                 objects_created=0, objects_retired=0, peak_tasks=0,
                 budget_exhaustions=0, runtime_failed=False)

    def exception_handler(loop, details):
        # Never log application exception repr, task locals or traceback paths.
        stats["runtime_failed"] = True
        inbox.stop()

    def task_factory(loop, coroutine, **kwargs):
        count = len(asyncio.all_tasks(loop))
        if _current_invocation.get() is not None and count >= max_tasks:
            stats["budget_exhaustions"] += 1
            coroutine.close()
            raise RuntimeError("Cellar Python task capacity exhausted")
        stats["peak_tasks"] = max(stats["peak_tasks"], count + 1)
        return asyncio.Task(coroutine, loop=loop, **kwargs)

    loop.set_exception_handler(exception_handler)
    loop.set_task_factory(task_factory)
    serving = loop.create_task(_serve(module, inbox, interval, shutdown, stats, max_objects, max_tasks))
    try:
        try:
            stats["pending_tasks"], stats["pending_objects"] = loop.run_until_complete(serving)
        except BaseException:
            # Unmanaged SystemExit/KeyboardInterrupt can escape Task dispatch.
            # Preserve failure as a flag, never application-provided error text.
            stats["runtime_failed"] = True
            inbox.stop()
            serving.cancel()
            try:
                loop.run_until_complete(asyncio.wait({serving}, timeout=shutdown + interval))
                if serving.done() and not serving.cancelled():
                    serving.exception()
            except BaseException:
                pass

        # Generators can own resources even after their creating task completed.
        # Keep the loop alive while finally blocks run and bound noncooperation.
        generators = loop.create_task(loop.shutdown_asyncgens())
        _, remaining = loop.run_until_complete(asyncio.wait({generators}, timeout=shutdown))
        if not remaining:
            generators.result()
        else:
            generators.cancel()
        loop.run_until_complete(loop.shutdown_default_executor(timeout=shutdown))
        stats["pending_tasks"] = max(stats["pending_tasks"], len(asyncio.all_tasks(loop)))
        stats["pending_threads"] = sum(thread is not threading.main_thread() for thread in threading.enumerate())
        return stats
    except BaseException:
        stats["runtime_failed"] = True
        stats["pending_tasks"] = max(stats["pending_tasks"], len(asyncio.all_tasks(loop)))
        stats["pending_threads"] = sum(thread is not threading.main_thread() for thread in threading.enumerate())
        return stats
    finally:
        inbox.stop()
        asyncio.set_event_loop(None)
        loop.close()

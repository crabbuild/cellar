"""One interpreter and asyncio loop; no Cellar operations run on this thread."""
import asyncio
import inspect
import json
import resource
import sys
import threading
import time
from cellar import Batch, Context, Overloaded, Request, Response, Scheduled, _encode_response, _response_metadata, _current_invocation

_failures = dict(unraisable_errors=0, thread_errors=0)
# Collection while taking a snapshot can invoke another destructor on the same
# thread. Serialize other threads without deadlocking that reentrant error hook.
_failure_lock = threading.RLock()
_memory_tracer = None


class _TaskCapacity(RuntimeError):
    """The managed factory refused a coroutine before constructing its task."""


def configure_measurement(trace_allocations):
    global _memory_tracer
    if trace_allocations:
        import tracemalloc
        tracemalloc.start(1)
        _memory_tracer = tracemalloc


def stop_measurement():
    global _memory_tracer
    if _memory_tracer is not None:
        _memory_tracer.stop()
        _memory_tracer = None


def install_error_hooks():
    # Default hooks print arbitrary exception text, traceback paths and object
    # reprs to stderr. These hooks retain only failure counts, including during
    # application imports before the managed loop starts.
    def unraisable(details):
        with _failure_lock:
            _failures["unraisable_errors"] += 1
    def thread_failure(details):
        with _failure_lock:
            _failures["thread_errors"] += 1
    sys.unraisablehook = unraisable
    threading.excepthook = thread_failure


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


def _complete_failure(bridge, payload):
    if bridge.active():
        try:
            bridge.complete(payload, True)
        except (RuntimeError, ValueError, BlockingIOError):
            # A full mailbox/byte budget or concurrent native retirement can
            # refuse even a fixed failure. Context cleanup wakes the driver
            # without needing another queue slot; the runtime remains usable.
            pass


async def _invoke(module, objects, payload, body, bridge, context, stats, maximum):
    token = _current_invocation.set(bridge)
    started = time.perf_counter_ns()
    metrics = stats["interpreter"]
    metrics["handlers_started"] += 1
    try:
        if not bridge.active():
            metrics["handlers_cancelled"] += 1
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
        argument = {"fetch": lambda: Request._from_wire(value, body),
                    "queue": lambda: Batch(context, value),
                    "scheduled": lambda: Scheduled(context, value),
                    "alarm": lambda: value["scheduled_ms"]}[kind]()
        result = handler(argument, context)
        if not inspect.isawaitable(result):
            raise TypeError("Cellar handlers must return an awaitable")
        result = await result
        if kind == "fetch" and not isinstance(result, Response):
            raise TypeError("fetch must return cellar.Response")
        value, output_body = result._completion() if kind == "fetch" else (None, None)
        metadata = _response_metadata(value) if kind == "fetch" else None
        if (metadata is not None and metadata["body"] == "" and output_body is not None
                and 200 <= metadata["status"] <= 599):
            bridge.complete_response(metadata["status"], metadata["headers"], output_body)
        else:
            bridge.complete(_encode_response(value) if kind == "fetch" else "null", False, output_body)
        metrics["handlers_completed"] += 1
    except asyncio.CancelledError:
        metrics["handlers_cancelled"] += 1
        raise
    except Overloaded:
        metrics["handlers_failed"] += 1
        # Preserve the category without reading exception text or its claimed
        # outcome. A parent can commit before a nested admission refusal.
        _complete_failure(bridge, '{"error":"Overloaded"}')
    except BaseException:
        metrics["handlers_failed"] += 1
        # Exception messages, class names and tracebacks can contain request data
        # or credentials. The public failure carries no application-supplied text.
        _complete_failure(bridge, '"Python handler failed"')
    finally:
        elapsed = time.perf_counter_ns() - started
        metrics["handler_wall_ns_sum"] += elapsed
        metrics["handler_wall_ns_max"] = max(metrics["handler_wall_ns_max"], elapsed)
        context._close()
        _current_invocation.reset(token)


def _observe_delay(histogram, ns):
    bounds = (1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000, 10_000_000_000)
    index = next((index for index, bound in enumerate(bounds) if ns <= bound), 8)
    histogram["samples"] += 1
    histogram["total_ns"] = min(2**64 - 1, histogram["total_ns"] + ns)
    histogram["maximum_ns"] = max(histogram["maximum_ns"], ns)
    histogram["bucket_counts"][index] += 1


async def _serve(module, inbox, interval, shutdown, stats, maximum, telemetry, sample):
    invocations, objects = {}, {}
    loop = asyncio.get_running_loop()
    ready = asyncio.Event()
    descriptor = inbox.wakeup_fd()

    def readable():
        try:
            inbox.drain_wakeup()
        finally:
            ready.set()

    def completed(task):
        ready.set()

    def observe_ready(expected):
        _observe_delay(stats["interpreter"]["loop_delay"], max(0, time.perf_counter_ns() - expected))

    def deadline_ready(expected):
        observe_ready(expected)
        ready.set()

    loop.add_reader(descriptor, readable)
    next_sample = 0

    def publish_work(retiring=0):
        stats["work"].update(
            active_tasks=len(asyncio.all_tasks() - {asyncio.current_task()}),
            active_invocations=sum(not entry[0].done() for entry in invocations.values()),
            resident_objects=len(objects), pending_retirements=retiring)
        sample()
    try:
        while not inbox.done():
            ready.clear()
            now = time.perf_counter_ns()
            if any(_failures.values()):
                stats["runtime_failed"] = True
                inbox.stop()
                break
            for invocation, operation, result in inbox.replies():
                entry = invocations.get(invocation)
                if entry:
                    entry[1]._reply(operation, result)
                # A cancelled task can retain its cell through its traceback.
                # An idle pump may receive no later reply to overwrite entry.
                del entry, result
            # Rejections free native admission immediately. A continuously
            # replenished inbox must still yield to replies, timers and cleanup.
            for _ in range(64):
                start = inbox.take()
                if start is None:
                    break
                invocation, payload, body, bridge = start
                del start
                payload = json.loads(payload)
                context = Context(bridge, payload["identity"])
                try:
                    # The managed factory checks all live tasks, including
                    # application-created ones, immediately before construction.
                    # Do not scan the same task registry twice per admission.
                    task = asyncio.create_task(_invoke(module, objects, payload, body, bridge, context, stats, maximum),
                                               eager_start=True)
                except _TaskCapacity:
                    token = _current_invocation.set(bridge)
                    try:
                        bridge.complete('"Python task capacity exhausted"', True)
                    finally:
                        context._close()
                        _current_invocation.reset(token)
                    del context, bridge, payload, body
                    continue
                if task.done() and not task.cancelled() and task.exception() is None:
                    # Eager completion already ran _invoke's context cleanup.
                    # Release it before admitting another request. Failed
                    # cleanup stays registered for the supervisor's final pass.
                    del task, context, bridge, payload, body
                    continue
                if not task.done():
                    task.add_done_callback(completed)
                invocations[invocation] = (task, context, bridge)
                del task, context, bridge, payload, body
            else:
                # Defer the continuation so ready.wait() actually yields. No
                # additional native notification is required for queued starts.
                loop.call_soon(ready.set)
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
                # Registry removal must also release this iteration's strong
                # references; an empty next iteration cannot overwrite them.
                del task, context, bridge
            # Native owner drops publish a bounded dirty hint after revoking
            # authority. Consume it before scanning so a concurrent drop is
            # retained for another pass. _invoke checks authority before object
            # construction and does not yield before registering the object;
            # a queued invocation cannot register after consuming its own
            # retirement hint. SDK calls still check authority individually.
            retiring = ([key for key, (_, bridge) in objects.items() if not bridge.owner_active()]
                        if inbox.take_retirements() else [])
            if retiring:
                # Publish before destructors can block. Rust can observe a stale
                # snapshot with pending cleanup without acquiring the GIL.
                publish_work(len(retiring))
            for key in retiring:
                del objects[key]
                stats["objects_retired"] += 1
            if now >= next_sample:
                publish_work()
                next_sample = now + int(telemetry * 1_000_000_000)
            delay = min(interval, max(0, (next_sample - time.perf_counter_ns()) / 1_000_000_000))
            # Queue the checkpoint after resumed tasks, using the start of this
            # pump turn to include inline handler work. Host I/O awaits yield
            # the loop and do not contribute their elapsed wait to this sample.
            loop.call_soon(observe_ready, now)
            now = time.perf_counter_ns()
            timer = loop.call_later(delay, deadline_ready, now + int(delay * 1_000_000_000))
            try:
                await ready.wait()
            finally:
                timer.cancel()
    finally:
        # The runtime retains the descriptor until the loop has removed it.
        loop.remove_reader(descriptor)
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
                if not task.cancelled() and task.exception() is not None:
                    stats["runtime_failed"] = True
                    stats["runtime_failure_stage"] = stats["runtime_failure_stage"] or "task_cleanup"
        publish_work(len(objects))
        stats["objects_retired"] += len(objects)
        objects.clear()
        publish_work()
    return len(pending), len(objects)


def run(module, inbox, interval, shutdown, max_tasks, max_objects, telemetry):
    wall_started, cpu_started = time.perf_counter_ns(), time.thread_time_ns()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    stats = dict(pending_tasks=0, pending_objects=0, pending_threads=0,
                 objects_created=0, objects_retired=0, peak_tasks=0,
                 budget_exhaustions=0, runtime_failed=False, runtime_failure_stage=None)
    stats["interpreter"] = dict(thread_cpu_ns=0, wall_ns=0, process_peak_rss_bytes=0,
        allocated_blocks_initial=sys.getallocatedblocks(), allocated_blocks_final=0,
        allocated_blocks_current=0,
        traced_current_bytes=None, traced_peak_bytes=None, trace_bookkeeping_bytes=None,
        loop_delay=dict(samples=0, total_ns=0, maximum_ns=0, bucket_counts=[0] * 9),
        handlers_started=0, handlers_completed=0, handlers_failed=0,
        handlers_cancelled=0, handler_wall_ns_sum=0, handler_wall_ns_max=0)

    stats["work"] = dict(active_tasks=0, active_invocations=0, resident_objects=0, pending_retirements=0)

    def sample(final=False):
        metrics = stats["interpreter"]
        metrics.update(thread_cpu_ns=time.thread_time_ns() - cpu_started,
            wall_ns=time.perf_counter_ns() - wall_started,
            process_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == "darwin" else 1024),
            allocated_blocks_current=sys.getallocatedblocks())
        if final:
            metrics["allocated_blocks_final"] = metrics["allocated_blocks_current"]
        if _memory_tracer is not None and _memory_tracer.is_tracing():
            metrics["traced_current_bytes"], metrics["traced_peak_bytes"] = _memory_tracer.get_traced_memory()
            metrics["trace_bookkeeping_bytes"] = _memory_tracer.get_tracemalloc_memory()
        else:
            metrics.update(traced_current_bytes=None, traced_peak_bytes=None, trace_bookkeeping_bytes=None)
        inbox.sample(json.dumps({"interpreter": metrics, "work": stats["work"]}))

    def exception_handler(loop, details):
        # Never log application exception repr, task locals or traceback paths.
        stats["runtime_failed"] = True
        stats["runtime_failure_stage"] = "background_callback"
        inbox.stop()

    def task_factory(loop, coroutine, **kwargs):
        count = len(asyncio.all_tasks(loop))
        if count >= max_tasks:
            stats["budget_exhaustions"] += 1
            coroutine.close()
            raise _TaskCapacity("Cellar Python task capacity exhausted")
        stats["peak_tasks"] = max(stats["peak_tasks"], count + 1)
        return asyncio.Task(coroutine, loop=loop, **kwargs)

    def cleanup_task(coroutine):
        # Bounded supervisor tasks can exceed the application budget while
        # joining existing work. Ordinary callbacks, including empty-context
        # callbacks during shutdown, still use the bounded factory above.
        stats["peak_tasks"] = max(stats["peak_tasks"], len(asyncio.all_tasks(loop)) + 1)
        return asyncio.Task(coroutine, loop=loop)

    loop.set_exception_handler(exception_handler)
    loop.set_task_factory(task_factory)
    serving = loop.create_task(_serve(module, inbox, interval, shutdown, stats, max_objects, telemetry, sample))
    cleanup_stage = "serve"
    try:
        try:
            stats["pending_tasks"], stats["pending_objects"] = loop.run_until_complete(serving)
        except BaseException as error:
            # Unmanaged SystemExit/KeyboardInterrupt can escape Task dispatch.
            # Preserve failure as a flag, never application-provided error text.
            stats["runtime_failed"] = True
            stats["runtime_failure_stage"] = "serve_interrupt" if isinstance(error, KeyboardInterrupt) else "serve_failure"
            inbox.stop()
            serving.cancel()
            try:
                loop.run_until_complete(cleanup_task(asyncio.wait({serving}, timeout=shutdown + interval)))
                if serving.done() and not serving.cancelled():
                    serving.exception()
            except BaseException:
                pass

        # Generators can own resources even after their creating task completed.
        # Keep the loop alive while finally blocks run and bound noncooperation.
        cleanup_stage = "generators"
        generators = cleanup_task(loop.shutdown_asyncgens())
        _, remaining = loop.run_until_complete(cleanup_task(asyncio.wait({generators}, timeout=shutdown)))
        if not remaining:
            generators.result()
        else:
            generators.cancel()
        cleanup_stage = "executor"
        loop.run_until_complete(cleanup_task(loop.shutdown_default_executor(timeout=shutdown)))
        stats["pending_tasks"] = max(stats["pending_tasks"], len(asyncio.all_tasks(loop)))
        stats["work"]["active_tasks"] = len(asyncio.all_tasks(loop))
        stats["pending_threads"] = sum(thread is not threading.main_thread() for thread in threading.enumerate())
        return stats
    except BaseException as error:
        stats["runtime_failed"] = True
        stats["runtime_failure_stage"] = cleanup_stage + ("_interrupt" if isinstance(error, KeyboardInterrupt) else "_failure")
        stats["pending_tasks"] = max(stats["pending_tasks"], len(asyncio.all_tasks(loop)))
        stats["work"]["active_tasks"] = len(asyncio.all_tasks(loop))
        stats["pending_threads"] = sum(thread is not threading.main_thread() for thread in threading.enumerate())
        return stats
    finally:
        inbox.stop()
        asyncio.set_event_loop(None)
        loop.close()
        with _failure_lock:
            failures = _failures.copy()
        stats.update(failures)
        stats["runtime_failed"] |= any(failures.values())
        sample(final=True)

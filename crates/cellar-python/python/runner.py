"""One interpreter and asyncio loop; no Cellar operations run on this thread."""
import asyncio
from collections import deque
from functools import partial
import inspect
import json
import resource
import sys
import threading
import time
from cellar import Batch, Context, Overloaded, Request, Response, Scheduled, _encode_response, _current_invocation

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
    from cellar_rpc_shapes import CodecCatalog
    codecs = CodecCatalog(descriptor.get("rpc_contracts", {}))
    module.__cellar_rpc_codecs__ = codecs
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
    declared = descriptor.get("rpc", {})
    registrations = getattr(module, "__cellar_rpc__", {})
    if set(registrations) != set(declared):
        raise TypeError("Python RPC classes differ from native registrations")
    dispatch = {}
    if declared:
        from dataclasses import replace
        from types import MappingProxyType
        from cellar_rpc import RpcRegistration
        for name, methods in declared.items():
            registration = registrations[name]
            if not isinstance(registration, RpcRegistration) or set(registration.methods) != set(methods):
                raise TypeError("Python RPC methods differ from native registrations")
            cell = getattr(module, name)
            table = {}
            for wire, native in methods.items():
                rule = registration.methods[wire]
                if (rule.digest != native["digest"] or rule.name != wire
                        or rule.input.DESCRIPTOR.full_name != native["input"]
                        or rule.output.DESCRIPTOR.full_name != native["output"]):
                    raise TypeError("Python RPC contract differs from native registration")
                codecs.verify(rule)
                # A generated name selects one statically inspected function;
                # wire input never becomes an arbitrary attribute lookup.
                handler = inspect.getattr_static(cell, rule.handler)
                if not inspect.iscoroutinefunction(handler):
                    raise TypeError("Python RPC handlers must be async instance methods")
                inspect.signature(handler).bind(None, None, None)
                limits = native["limits"]
                rule = replace(rule, max_message_bytes=limits["max_message_bytes"],
                               max_depth=limits["max_depth"], max_elements=limits["max_elements"],
                               max_decoded_slots=limits["max_decoded_slots"])
                table[wire] = (handler, rule)
            dispatch[name] = MappingProxyType(table)
        module.__cellar_rpc_dispatch__ = MappingProxyType(dispatch)


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
        kind, value = payload["kind"], payload["event"]
        rpc_entry = None
        if kind == "rpc":
            rpc_entry = module.__cellar_rpc_dispatch__[payload["class"]][value["method"]]
        target = module
        if payload["object"] is not None:
            key = payload["object"]
            if key not in objects:
                if len(objects) >= maximum:
                    stats["budget_exhaustions"] += 1
                    raise Overloaded("Cellar Python object capacity exhausted")
                objects[key] = (getattr(module, payload["class"])(), bridge.owner_lifetime())
                stats["objects_created"] += 1
            target = objects[key][0]
        if rpc_entry is not None:
            function, rule = rpc_entry
            argument = rule.input.FromString(body)
            result = function(target, argument, context)
        else:
            handler = getattr(target, kind)
            if kind == "fetch":
                argument = Request._from_wire(value, body)
            elif kind == "queue":
                argument = Batch(context, value)
            elif kind == "scheduled":
                argument = Scheduled(context, value)
            elif kind == "alarm":
                argument = value["scheduled_ms"]
            else:
                raise KeyError(kind)
            result = handler(argument, context)
        if not inspect.isawaitable(result):
            raise TypeError("Cellar handlers must return an awaitable")
        result = await result
        if rpc_entry is not None:
            from cellar_rpc import encode_message
            try:
                if not isinstance(result, rule.output):
                    raise TypeError("RPC response type differs from registration")
                output_body = encode_message(result, rule)
            except Exception:
                metrics["handlers_failed"] += 1
                _complete_failure(bridge, '{"error":"InvalidResponse"}')
                return
            bridge.complete("null", False, output_body)
            metrics["handlers_completed"] += 1
            return
        if kind == "fetch" and not isinstance(result, Response):
            raise TypeError("fetch must return cellar.Response")
        value, output_body = result._completion() if kind == "fetch" else (None, None)
        bridge.complete(_encode_response(value) if kind == "fetch" else "null", False, output_body)
        metrics["handlers_completed"] += 1
    except asyncio.CancelledError:
        metrics["handlers_cancelled"] += 1
        raise
    except (Overloaded, _TaskCapacity):
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
        context._close(finished=True)
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
    completed_invocations = deque()
    loop = asyncio.get_running_loop()
    ready = asyncio.Event()
    descriptor = inbox.wakeup_fd()

    def deliver_replies():
        # Keep delivery locals out of the long-lived pump frame so a completed
        # or cancelled task cannot retain its cell until the next native reply.
        for invocation, operation, result, body in inbox.replies():
            entry = invocations.get(invocation)
            if entry:
                if entry[2].active():
                    entry[1]._reply(operation, result, body)
                else:
                    entry[0].cancel()

    def readable():
        try:
            inbox.drain_wakeup()
            # Queue SDK continuations before waking the admission pump. Host
            # operations still run in their original native invocation futures.
            done = inbox.done()
            if not done and not any(_failures.values()):
                deliver_replies()
            # Replies resume their own tasks. Completion callbacks release
            # those tasks; admissions and revocations have native dirty hints.
            # Avoid rebuilding the admission pump's timer for every SDK reply.
            if done or any(_failures.values()) or inbox.pump_needed():
                ready.set()
        except BaseException:
            ready.set()
            raise

    def completed(invocation, task):
        completed_invocations.append(invocation)
        ready.set()

    def release_completed():
        # A short-lived frame releases task/context references before waiting.
        while completed_invocations:
            invocation = completed_invocations.popleft()
            entry = invocations.get(invocation)
            if entry and not entry[0].cancelled():
                error = entry[0].exception()
                if error is not None:
                    # Keep failed cleanup registered for the final close pass.
                    # An exception may define a false truth value; that must
                    # never turn failed cleanup into a successful drain.
                    raise error
            invocations.pop(invocation, None)

    def cancel_invalidated():
        for task, _, bridge in invocations.values():
            if not task.done() and not bridge.active():
                task.cancel()

    def observe_ready(expected):
        _observe_delay(stats["interpreter"]["loop_delay"], max(0, time.perf_counter_ns() - expected))

    timer = None

    def deadline_ready(expected):
        nonlocal timer
        timer = None
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
            # Consume hints before the queues: publication during admission
            # must survive for the next readiness callback, including when a
            # producer enqueues just after take() observes an empty queue.
            lifecycle = inbox.take_lifecycle()
            now = time.perf_counter_ns()
            if any(_failures.values()):
                stats["runtime_failed"] = True
                inbox.stop()
                break
            deliver_replies()
            # Rejections free native admission immediately. A continuously
            # replenished inbox must still yield to replies, timers and cleanup.
            for _ in range(64):
                start = inbox.take()
                if start is None:
                    break
                invocation, payload, body, bridge = start
                del start
                payload = json.loads(payload)
                context = Context(bridge, payload["identity"], payload.get("initial_storage"))
                context._rpc_codecs = getattr(module, "__cellar_rpc_codecs__", None)
                try:
                    # The managed factory checks all live tasks, including
                    # application-created ones, immediately before construction.
                    # Do not scan the same task registry twice per admission.
                    task = asyncio.create_task(_invoke(module, objects, payload, body, bridge, context, stats, maximum),
                                               eager_start=True)
                except _TaskCapacity:
                    token = _current_invocation.set(bridge)
                    try:
                        bridge.complete('{"error":"Overloaded"}', True)
                    finally:
                        context._close(finished=True)
                        _current_invocation.reset(token)
                    del context, bridge, payload, body
                    continue
                if task.done() and not task.cancelled() and task.exception() is None:
                    # Eager completion already ran _invoke's context cleanup.
                    # Release it before admitting another request. Failed
                    # cleanup stays registered for the supervisor's final pass.
                    del task, context, bridge, payload, body
                    continue
                invocations[invocation] = (task, context, bridge)
                if task.done():
                    completed_invocations.append(invocation)
                else:
                    task.add_done_callback(partial(completed, invocation))
                del task, context, bridge, payload, body
            else:
                # Defer the continuation so ready.wait() actually yields. No
                # additional native notification is required for queued starts.
                loop.call_soon(ready.set)
            release_completed()
            # Ordinary admissions and replies do not revoke authority. Consume
            # native invalidation hints before scanning so concurrent revocation
            # remains visible on the next pass. Every SDK call and reply still
            # checks its own invocation authority.
            if lifecycle & 1:
                cancel_invalidated()
            # Native owner drops publish a bounded dirty hint after revoking
            # authority. Consume it before scanning so a concurrent drop is
            # retained for another pass. _invoke checks authority before object
            # construction and does not yield before registering the object;
            # a queued invocation cannot register after consuming its own
            # retirement hint. SDK calls still check authority individually.
            retiring = ([key for key, (_, owner) in objects.items() if not owner.active()]
                        if lifecycle & 2 else [])
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
            # Queue the checkpoint after resumed tasks, using the start of this
            # pump turn to include inline handler work. Host I/O awaits yield
            # the loop and do not contribute their elapsed wait to this sample.
            loop.call_soon(observe_ready, now)
            # Native readiness does not extend housekeeping's deadline. Retain
            # its one handle until it fires instead of cancelling/reallocating
            # a timer for every admission, completion and SDK reply.
            if timer is None:
                now = time.perf_counter_ns()
                delay = min(interval, max(0, (next_sample - now) / 1_000_000_000))
                timer = loop.call_later(delay, deadline_ready, now + int(delay * 1_000_000_000))
            await ready.wait()
    finally:
        if timer is not None:
            timer.cancel()
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

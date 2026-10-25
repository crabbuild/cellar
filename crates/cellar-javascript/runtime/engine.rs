// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Embedded V8 pump. Isolate handles and promise resolvers stay on this thread;
//! native SDK work crosses bounded channels back to its owning invocation.
use super::*;
use anyhow::Context as _;
use std::{
    cell::RefCell,
    collections::HashMap,
    rc::Rc,
    sync::{Mutex, Once},
    time::Instant,
};

struct InvocationState {
    start: Start,
    promise: Option<v8::Global<v8::Promise>>,
    operations: usize,
    pending: usize,
    mutation: bool,
}

struct Operation {
    invocation: u64,
    mutation: bool,
    resolver: v8::Global<v8::PromiseResolver>,
    cancellation: Option<cellar::sdk::web::AbortController>,
}

/// A private function created during checked admission can cancel only that
/// operation. An AbortSignal may invoke it from a different continuation.
fn cancel_operation(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue,
) {
    let Some(state) = scope.get_slot::<Shared>().cloned() else {
        return;
    };
    let Some(id) = args.data().number_value(scope) else {
        return;
    };
    let controller = {
        let state = state.borrow();
        state.operations.get(&(id as u64)).and_then(|operation| {
            state
                .invocations
                .get(&operation.invocation)
                .filter(|invocation| {
                    !state.runtime.closed.load(Ordering::Acquire)
                        && invocation.start.active.load(Ordering::Acquire)
                        && invocation.start.owner.load(Ordering::Acquire)
                })
                .and(operation.cancellation.clone())
        })
    };
    if let Some(controller) = controller {
        if controller.abort(None).is_err() {
            refuse(scope, "RuntimeFailed");
        }
    }
}

fn expire(scope: &mut v8::PinScope, operation: Operation) -> Result<()> {
    // Settle the same private reply shape as Node revocation. The facade turns
    // it into a typed rejection so application catch/finally blocks can run.
    // Already submitted work may have committed; cancellation cannot prove it
    // did not. Any new SDK call from cleanup is refused by invocation liveness.
    let id = v8::Number::new(scope, operation.invocation as f64);
    scope.set_continuation_preserved_embedder_data(id.into());
    let resolver = v8::Local::new(scope, &operation.resolver);
    let payload = v8::String::new(
        scope,
        r#"{"ok":false,"code":"ContextExpired","outcome":"unknown"}"#,
    )
    .context("JavaScript cancellation allocation failed")?;
    let undefined = v8::undefined(scope);
    let result = v8::Array::new_with_elements(scope, &[payload.into(), undefined.into()]);
    resolver.resolve(scope, result.into());
    Ok(())
}

struct State {
    runtime: Arc<Runtime>,
    invocations: HashMap<u64, InvocationState>,
    operations: HashMap<u64, Operation>,
    budgets: super::budgets::Budgets,
    objects: HashMap<u64, Life>,
}

// V8 callbacks share state only within the isolate thread. Never hold a borrow
// across application JavaScript, including property access or value conversion.
type Shared = Rc<RefCell<State>>;
#[path = "v8_budgets.rs"]
mod budget_controls;

fn refuse(scope: &v8::PinScope, message: &str) {
    if let Some(text) = v8::String::new(scope, message) {
        let exception = v8::Exception::error(scope, text);
        scope.throw_exception(exception);
    }
}

fn operation(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue,
) {
    let Some(state) = scope.get_slot::<Shared>().cloned() else {
        return;
    };
    // Promise continuations carry their invocation ID. A facade saved by another
    // invocation must not borrow the currently executing call's authority.
    let current = scope.get_continuation_preserved_embedder_data();
    let Some(current) = current.number_value(scope) else {
        return refuse(scope, "ContextExpired");
    };
    let Some(requested) = args.get(0).number_value(scope) else {
        return refuse(scope, "ContextMismatch");
    };
    if current != requested {
        return refuse(scope, "ContextMismatch");
    }

    let invocation = current as u64;
    // Require primitive strings: no user conversion while native state is borrowed.
    let Ok(name) = v8::Local::<v8::String>::try_from(args.get(1)) else {
        return refuse(scope, "ValidationError");
    };
    let Ok(payload) = v8::Local::<v8::String>::try_from(args.get(2)) else {
        return refuse(scope, "ValidationError");
    };
    let limit = state.borrow().runtime.options.max_value_bytes;
    if name.length() > 64 || payload.length() > limit {
        return refuse(scope, "ValueLimit");
    }
    let name = name.to_rust_string_lossy(scope);
    let payload = payload.to_rust_string_lossy(scope);
    let body = match super::binary::copy(scope, args.get(3), limit) {
        Ok(body) => body,
        Err(_) => return refuse(scope, "ValidationError"),
    };
    if (name == "cell.call") != body.is_some() {
        return refuse(scope, "ValidationError");
    }
    let mut state = state.borrow_mut();
    let runtime = state.runtime.clone();
    let budget = if !args.get(4).is_undefined() {
        if name != "cell.call" || !args.get(4).is_number() {
            return refuse(scope, "ContextMismatch");
        }
        let Some(id) = args.get(4).number_value(scope).filter(|id| {
            id.is_finite() && *id > 0.0 && id.fract() == 0.0 && *id <= 9_007_199_254_740_991.0
        }) else {
            return refuse(scope, "ContextMismatch");
        };
        match state.budgets.get(invocation, id as u64) {
            Ok(budget) => Some(budget),
            Err(error) => return refuse(scope, error),
        }
    } else {
        None
    };
    let Some(active) = state.invocations.get_mut(&invocation) else {
        return refuse(scope, "ContextExpired");
    };
    if runtime.closed.load(Ordering::Acquire)
        || !active.start.active.load(Ordering::Acquire)
        || !active.start.owner.load(Ordering::Acquire)
    {
        return refuse(scope, "ContextExpired");
    }
    if name == "$check" {
        return;
    }
    if name == "$rpc_budget" {
        drop(state);
        return budget_controls::begin(scope, invocation, rv);
    }

    let mutation = !cellar_guest::operations::is_read(&name);
    if active.pending >= runtime.options.max_pending_operations
        || active.operations >= runtime.options.max_operations
    {
        return refuse(scope, "OperationLimit");
    }
    if mutation && active.mutation {
        return refuse(scope, "ConcurrentMutation");
    }

    let Ok(size) = value_size(&payload, body.as_deref()) else {
        return refuse(scope, "ValueLimit");
    };
    let Ok(bytes) = runtime.reserve(size) else {
        return refuse(scope, "BridgeLimit");
    };
    let Ok(id) = runtime.id() else {
        return refuse(scope, "OperationLimit");
    };
    let Some(resolver) = v8::PromiseResolver::new(scope) else {
        return refuse(scope, "RuntimeFailed");
    };
    let cancellation = (name == "cell.call").then(cellar::sdk::web::AbortController::new);
    let cancel = if cancellation.is_some() {
        let data = v8::Number::new(scope, id as f64);
        let Some(cancel) = v8::Function::builder(cancel_operation)
            .data(data.into())
            .build(scope)
        else {
            return refuse(scope, "RuntimeFailed");
        };
        Some(cancel)
    } else {
        None
    };
    if active
        .start
        .sender
        .try_send(Event::Operation {
            id,
            name,
            payload,
            body,
            signal: cancellation.as_ref().map(|controller| controller.signal()),
            budget,
            _bytes: bytes,
        })
        .is_err()
    {
        return refuse(scope, "OperationLimit");
    }

    active.operations += 1;
    active.pending += 1;
    active.mutation |= mutation;
    let promise = resolver.get_promise(scope);
    state.operations.insert(
        id,
        Operation {
            invocation,
            mutation,
            resolver: v8::Global::new(scope, resolver),
            cancellation,
        },
    );
    if let Some(cancel) = cancel {
        let admitted = v8::Array::new_with_elements(scope, &[promise.into(), cancel.into()]);
        rv.set(admitted.into());
    } else {
        rv.set(promise.into());
    }
}

fn script<'s>(scope: &v8::PinScope<'s, '_>, source: &str) -> Result<v8::Local<'s, v8::Value>> {
    let code = v8::String::new(scope, source).context("JavaScript source allocation failed")?;
    v8::Script::compile(scope, code, None)
        .and_then(|script| script.run(scope))
        .context("JavaScript compilation or execution failed")
}

fn function<'s>(
    scope: &v8::PinScope<'s, '_>,
    value: v8::Local<v8::Object>,
    name: &str,
) -> Result<v8::Local<'s, v8::Function>> {
    let key = v8::String::new(scope, name).context("JavaScript key allocation failed")?;
    value
        .get(scope, key.into())
        .and_then(|v| v8::Local::<v8::Function>::try_from(v).ok())
        .context("JavaScript runtime function missing")
}

pub(super) fn run(
    runtime: Arc<Runtime>,
    source: Source,
    starts: mpsc::Receiver<Start>,
    replies: mpsc::Receiver<Reply>,
    ready: mpsc::SyncSender<Result<()>>,
) -> Result<()> {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        v8::V8::initialize_platform(v8::new_default_platform(0, false).make_shared());
        v8::V8::initialize();
    });
    let mut isolate =
        v8::Isolate::new(v8::CreateParams::default().heap_limits(0, runtime.options.heap_bytes));
    isolate.set_microtasks_policy(v8::MicrotasksPolicy::Explicit);
    let handle = isolate.thread_safe_handle();
    // The watchdog holds this mutex only while observing/terminating the isolate;
    // clearing it synchronizes against termination of a subsequent idle turn.
    let deadline = Arc::new(Mutex::new(None::<Instant>));
    let (end, watchdog_end) = mpsc::channel::<()>();
    let watch_deadline = deadline.clone();
    let watchdog_runtime = runtime.clone();
    let watchdog = std::thread::spawn(move || {
        while watchdog_end.recv_timeout(Duration::from_millis(2)).is_err() {
            let deadline = watch_deadline.lock().unwrap();
            if deadline.is_some_and(|end| {
                watchdog_runtime.closed.load(Ordering::Acquire) || Instant::now() >= end
            }) {
                handle.terminate_execution();
                if deadline.is_some() {
                    watchdog_runtime.close();
                }
            }
        }
    });

    // Always stop/join the watchdog before dropping its isolate, including errors.
    struct Watchdog(mpsc::Sender<()>, Option<std::thread::JoinHandle<()>>);

    impl Drop for Watchdog {
        fn drop(&mut self) {
            let _ = self.0.send(());
            if let Some(thread) = self.1.take() {
                let _ = thread.join();
            }
        }
    }
    let _watchdog = Watchdog(end, Some(watchdog));
    let state = Rc::new(RefCell::new(State {
        runtime: runtime.clone(),
        invocations: HashMap::new(),
        operations: HashMap::new(),
        budgets: super::budgets::Budgets::default(),
        objects: HashMap::new(),
    }));
    isolate.set_slot(state.clone());
    *deadline.lock().unwrap() = Some(Instant::now() + runtime.options.startup_timeout);
    v8::scope!(let scope, &mut isolate);
    let context = v8::Context::new(scope, Default::default());
    let scope = &mut v8::ContextScope::new(scope, context);
    let scope = std::pin::pin!(v8::TryCatch::new(scope));
    let scope = &mut scope.init();
    super::encoding::install(
        scope,
        runtime.options.max_value_bytes,
        runtime.options.max_text_decoders,
    )?;
    let host =
        v8::Function::new(scope, operation).context("JavaScript bridge initialization failed")?;
    let factory = script(scope, include_str!("runner.js"))?;
    let factory = v8::Local::<v8::Function>::try_from(factory)?;
    let undefined = v8::undefined(scope);
    // Capture pristine builtins before application initialization can change them.
    let factory = factory
        .call(scope, undefined.into(), &[host.into()])
        .context("JavaScript facade initialization failed")?;
    let factory = v8::Local::<v8::Function>::try_from(factory)?;
    script(scope, &source.code)?;
    let app = script(scope, "CellarApp")?;
    let descriptor = v8::String::new(scope, &source.descriptor().to_string())
        .context("JavaScript descriptor failed")?;
    let maximum = v8::Number::new(scope, runtime.options.max_objects as f64);
    let runner = factory
        .call(
            scope,
            undefined.into(),
            &[app, descriptor.into(), maximum.into()],
        )
        .context("JavaScript registrations are invalid")?;
    let runner = v8::Local::<v8::Object>::try_from(runner)?;
    let dispatch = v8::Global::new(scope, function(scope, runner, "dispatch")?);
    let retire = v8::Global::new(scope, function(scope, runner, "retire")?);
    *deadline.lock().unwrap() = None;
    let _ = ready.send(Ok(()));
    loop {
        if runtime.closed.load(Ordering::Acquire) {
            break;
        }
        // One fresh handle scope per pump iteration: no retained transient handles.
        let parent: &mut v8::PinScope = scope;
        v8::scope!(let turn, parent);
        // One budget covers the entire pump pass, including all admitted calls
        // and the microtask checkpoint below. It is not a per-handler I/O timeout.
        *deadline.lock().unwrap() = Some(Instant::now() + runtime.options.turn_timeout);
        // Service replies before admission. Never wait for one invocation to finish.
        for reply in replies
            .try_iter()
            .take(runtime.options.max_invocations * runtime.options.max_pending_operations)
        {
            let operation = state.borrow_mut().operations.remove(&reply.id);
            if let Some(operation) = operation {
                if operation.invocation != reply.invocation {
                    anyhow::bail!("JavaScript reply identity mismatch")
                }
                let live = {
                    let mut state = state.borrow_mut();
                    state
                        .invocations
                        .get_mut(&reply.invocation)
                        .is_some_and(|inv| {
                            inv.pending -= 1;
                            if operation.mutation {
                                inv.mutation = false;
                            }
                            inv.start.active.load(Ordering::Acquire)
                                && inv.start.owner.load(Ordering::Acquire)
                        })
                };
                if live {
                    let id = v8::Number::new(turn, reply.invocation as f64);
                    turn.set_continuation_preserved_embedder_data(id.into());
                    let resolver = v8::Local::new(turn, &operation.resolver);
                    let payload = v8::String::new(turn, &reply.payload)
                        .context("JavaScript reply allocation failed")?;
                    let body = super::binary::value(turn, reply.body)?;
                    let result = v8::Array::new_with_elements(turn, &[payload.into(), body]);
                    resolver.resolve(turn, result.into());
                } else {
                    expire(turn, operation)?;
                }
            }
        }

        for mut start in starts.try_iter().take(runtime.options.max_invocations) {
            if !start.active.load(Ordering::Acquire) || !start.owner.load(Ordering::Acquire) {
                continue;
            }
            let payload: serde_json::Value = serde_json::from_str(&start.payload)?;
            if let Some(object) = payload["object"].as_u64() {
                let mut state = state.borrow_mut();
                if !state.objects.contains_key(&object)
                    && state.objects.len() >= runtime.options.max_objects
                {
                    start.active.store(false, Ordering::Release);
                    let _ = start.sender.try_send(Event::Complete {
                        payload: "null".into(),
                        body: None,
                        failed: true,
                        _bytes: runtime.reserve(4)?,
                    });
                    continue;
                }
                state.objects.insert(object, start.owner.clone());
            }
            let id = start.id;
            let value = v8::String::new(turn, &start.payload)
                .context("JavaScript invocation allocation failed")?;
            let body = super::binary::value(turn, start.body.take())?;
            state.borrow_mut().invocations.insert(
                id,
                InvocationState {
                    start,
                    promise: None,
                    operations: 0,
                    pending: 0,
                    mutation: false,
                },
            );
            let token = v8::Number::new(turn, id as f64);
            turn.set_continuation_preserved_embedder_data(token.into());
            let dispatch = v8::Local::new(turn, &dispatch);
            let result = dispatch
                .call(turn, undefined.into(), &[token.into(), value.into(), body])
                .context("JavaScript dispatch failed")?;
            let promise = v8::Local::<v8::Promise>::try_from(result)?;
            state.borrow_mut().invocations.get_mut(&id).unwrap().promise =
                Some(v8::Global::new(turn, promise));
        }

        // Clear ambient authority; queued continuations restore their own IDs.
        turn.set_continuation_preserved_embedder_data(undefined.into());
        turn.perform_microtask_checkpoint();
        ensure!(
            !turn.is_execution_terminating(),
            "JavaScript engine turn exceeded execution budget"
        );

        let mut finished = Vec::new();
        for (&id, invocation) in &state.borrow().invocations {
            if !invocation.start.active.load(Ordering::Acquire)
                || !invocation.start.owner.load(Ordering::Acquire)
            {
                finished.push(id);
                continue;
            }
            let promise = v8::Local::new(
                turn,
                invocation
                    .promise
                    .as_ref()
                    .context("JavaScript invocation promise missing")?,
            );
            if promise.state() == v8::PromiseState::Pending {
                continue;
            }
            // A handler must await its SDK operations before reporting completion.
            let rejected = promise.state() == v8::PromiseState::Rejected;
            let unsettled = invocation.pending != 0 || state.borrow().budgets.has(id);
            let failed = rejected || unsettled;
            invocation.start.active.store(false, Ordering::Release);
            let result = promise.result(turn);
            let (payload, body) = if failed {
                // Only a fixed category may leave an entered RPC handler.
                // Never stringify exceptions or inspect application objects.
                let payload = if rejected && !unsettled && result.is_string() {
                    let value = v8::Local::<v8::String>::try_from(result)
                        .context("RPC failure category must be a string")?;
                    if value.length() <= 64 {
                        cellar_guest::rpc_failure::payload(&value.to_rust_string_lossy(turn))
                            .unwrap_or("null")
                    } else {
                        "null"
                    }
                } else {
                    "null"
                };
                (payload.into(), None)
            } else if result.is_uint8_array() {
                // A malformed application reply is a call failure, not an
                // engine failure. Missing bytes become InvalidResponse/unknown
                // at the native RPC boundary, after any committed writes.
                (
                    "null".into(),
                    super::binary::copy(turn, result, runtime.options.max_value_bytes)
                        .unwrap_or(None),
                )
            } else {
                let result = v8::Local::<v8::String>::try_from(result)
                    .context("JavaScript completion must be encoded")?;
                ensure!(
                    result.length() <= runtime.options.max_value_bytes,
                    "JavaScript completion exceeds limit"
                );
                (result.to_rust_string_lossy(turn), None)
            };
            let bytes = runtime.reserve(value_size(&payload, body.as_deref())?)?;
            // +1 completion slot is reserved independently of operation capacity.
            let _ = invocation.start.sender.try_send(Event::Complete {
                payload,
                body,
                failed,
                _bytes: bytes,
            });
            finished.push(id);
        }
        let abandoned = {
            let mut state = state.borrow_mut();
            for id in &finished {
                state.invocations.remove(id);
                state.budgets.retire(*id);
            }
            state
                .operations
                .extract_if(|_, operation| finished.contains(&operation.invocation))
                .map(|(_, operation)| operation)
                .collect::<Vec<_>>()
        };
        // Dropping a PromiseResolver leaves awaiters suspended forever. Remove
        // native authority first, then settle every abandoned operation. Its
        // continuations run at the next bounded microtask checkpoint, including
        // cleanup that awaits a separate application promise before resuming.
        for operation in abandoned {
            expire(turn, operation)?;
        }

        let retired: Vec<_> = state
            .borrow()
            .objects
            .iter()
            .filter(|(_, life)| !life.load(Ordering::Acquire))
            .map(|(&id, _)| id)
            .collect();
        for id in retired {
            state.borrow_mut().objects.remove(&id);
            let id = v8::Number::new(turn, id as f64);
            let retire = v8::Local::new(turn, &retire);
            retire
                .call(turn, undefined.into(), &[id.into()])
                .context("JavaScript object retirement failed")?;
        }
        *deadline.lock().unwrap() = None;
        std::thread::sleep(Duration::from_millis(1));
    }
    ensure!(
        !scope.is_execution_terminating(),
        "JavaScript runtime terminated"
    );
    Ok(())
}

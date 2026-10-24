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
}

struct State {
    runtime: Arc<Runtime>,
    invocations: HashMap<u64, InvocationState>,
    operations: HashMap<u64, Operation>,
    objects: HashMap<u64, Life>,
}

// V8 callbacks share state only within the isolate thread. Never hold a borrow
// across application JavaScript, including property access or value conversion.
type Shared = Rc<RefCell<State>>;

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
    let mut state = state.borrow_mut();
    let runtime = state.runtime.clone();
    let Some(active) = state.invocations.get_mut(&invocation) else {
        return refuse(scope, "ContextExpired");
    };
    if runtime.closed.load(Ordering::Acquire)
        || !active.start.active.load(Ordering::Acquire)
        || !active.start.owner.load(Ordering::Acquire)
    {
        return refuse(scope, "ContextExpired");
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

    let Ok(bytes) = runtime.reserve(payload.len()) else {
        return refuse(scope, "BridgeLimit");
    };
    let Ok(id) = runtime.id() else {
        return refuse(scope, "OperationLimit");
    };
    let Some(resolver) = v8::PromiseResolver::new(scope) else {
        return refuse(scope, "RuntimeFailed");
    };
    if active
        .start
        .sender
        .try_send(Event::Operation {
            id,
            name,
            payload,
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
        },
    );
    rv.set(promise.into());
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
        objects: HashMap::new(),
    }));
    isolate.set_slot(state.clone());
    *deadline.lock().unwrap() = Some(Instant::now() + runtime.options.startup_timeout);
    v8::scope!(let scope, &mut isolate);
    let context = v8::Context::new(scope, Default::default());
    let scope = &mut v8::ContextScope::new(scope, context);
    let scope = std::pin::pin!(v8::TryCatch::new(scope));
    let scope = &mut scope.init();
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
                    resolver.resolve(turn, payload.into());
                }
            }
        }

        for start in starts.try_iter().take(runtime.options.max_invocations) {
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
                .call(turn, undefined.into(), &[token.into(), value.into()])
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
            let failed = promise.state() == v8::PromiseState::Rejected || invocation.pending != 0;
            invocation.start.active.store(false, Ordering::Release);
            let result = promise.result(turn);
            let payload = if failed {
                "null".into()
            } else {
                let result = v8::Local::<v8::String>::try_from(result)
                    .context("JavaScript completion must be encoded")?;
                ensure!(
                    result.length() <= runtime.options.max_value_bytes,
                    "JavaScript completion exceeds limit"
                );
                result.to_rust_string_lossy(turn)
            };
            let bytes = runtime.reserve(payload.len())?;
            // +1 completion slot is reserved independently of operation capacity.
            let _ = invocation.start.sender.try_send(Event::Complete {
                payload,
                failed,
                _bytes: bytes,
            });
            finished.push(id);
        }
        {
            let mut state = state.borrow_mut();
            for id in finished {
                state.invocations.remove(&id);
                state.operations.retain(|_, op| op.invocation != id);
            }
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

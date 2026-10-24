use super::*;
use crate::EventKind;
use serde_json::json;
use std::time::Instant;

fn start(runtime: &Runtime, id: u64, active: Life) -> channel::Receiver<Event> {
    let (sender, receiver) = channel::channel(runtime.options.max_pending_operations + 1);
    let payload = json!({"kind":"fetch","event":{"url":"http://test/","method":"GET","headers":[],"body":""},"identity":null,"object":null,"class":null}).to_string();
    runtime
        .start(Start {
            id,
            _bytes: runtime.reserve(payload.len()).unwrap(),
            payload,
            body: None,
            active,
            owner: Arc::new(AtomicBool::new(true)),
            sender,
        })
        .unwrap();
    receiver
}

fn event(receiver: &mut channel::Receiver<Event>) -> Event {
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        match receiver.try_recv() {
            Ok(value) => return value,
            Err(channel::error::TryRecvError::Disconnected) => panic!("engine closed prematurely"),
            Err(_) => assert!(Instant::now() < until, "engine stalled"),
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn source(body: &str) -> Source {
    Source::new(
        format!("var CellarApp = {{fetch: async function(request, ctx) {{ {body} }} }};"),
        "test.js",
    )
    .events([EventKind::Fetch])
}

#[test]
fn interleaved_awaits_keep_scope_and_reject_saved_context() {
    let code = r#"
      if (!globalThis.saved) globalThis.saved = ctx;
      const first = await ctx.variable('first');
      const second = await ctx.variable('second');
      let mismatch = false;
      if (ctx !== globalThis.saved) {
        try { await globalThis.saved.variable('forbidden'); } catch { mismatch = true; }
      }
      return {status:200,headers:[],body:'',first,second,mismatch};
    "#;
    JavaScriptRuntime::run(Options::default(), source(code), |app| {
        let runtime = &app.runtime;
        let a = runtime.id()?;
        let b = runtime.id()?;
        let mut ra = start(runtime, a, Arc::new(AtomicBool::new(true)));
        let mut rb = start(runtime, b, Arc::new(AtomicBool::new(true)));
        let Event::Operation { id: oa, name, .. } = event(&mut ra) else {
            panic!()
        };
        assert_eq!(name, "var");
        let Event::Operation { id: ob, .. } = event(&mut rb) else {
            panic!()
        };
        runtime.reply(b, ob, json!({"ok":true,"value":"b1"}).to_string())?;
        let Event::Operation { id: ob, .. } = event(&mut rb) else {
            panic!()
        };
        runtime.reply(a, oa, json!({"ok":true,"value":"a1"}).to_string())?;
        let Event::Operation { id: oa, .. } = event(&mut ra) else {
            panic!()
        };
        runtime.reply(b, ob, json!({"ok":true,"value":"b2"}).to_string())?;
        let Event::Complete {
            payload, failed, ..
        } = event(&mut rb)
        else {
            panic!()
        };
        assert!(!failed);
        let value: serde_json::Value = serde_json::from_str(&payload)?;
        assert_eq!(value["first"], "b1");
        assert_eq!(value["second"], "b2");
        assert_eq!(value["mismatch"], true);
        runtime.reply(a, oa, json!({"ok":true,"value":"a2"}).to_string())?;
        let Event::Complete {
            payload, failed, ..
        } = event(&mut ra)
        else {
            panic!()
        };
        assert!(!failed);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&payload)?["first"],
            "a1"
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn cancellation_revokes_continuations_and_releases_capacity() {
    JavaScriptRuntime::run(
        Options::default(),
        source("await ctx.variable('one'); await ctx.variable('forbidden'); return null;"),
        |app| {
            let runtime = &app.runtime;
            let active = Arc::new(AtomicBool::new(true));
            let id = runtime.id()?;
            let mut receive = start(runtime, id, active.clone());
            let Event::Operation { id: operation, .. } = event(&mut receive) else {
                panic!()
            };
            active.store(false, Ordering::Release);
            runtime.reply(id, operation, "{\"ok\":true,\"value\":null}".into())?;
            let until = Instant::now() + Duration::from_secs(5);
            while !receive.is_closed() {
                assert!(Instant::now() < until);
                std::thread::sleep(Duration::from_millis(1));
            }
            assert!(receive.try_recv().is_err());
            Ok(())
        },
    )
    .unwrap();
}

#[test]
fn cancellation_settles_pending_operations_before_cleanup_loses_authority() -> Result<()> {
    // Observe cleanup through a separate live invocation. The cancelled handler
    // deliberately keeps running JavaScript after its native receiver is gone.
    let code = r#"
      const state = globalThis.cancellation ??= {started:false};
      if (!state.started) {
        state.started = true;
        try {
          await ctx.storage.sync();
          state.unexpectedSuccess = true;
        } catch (error) {
          state.cancelled = true;
          state.code = error.code;
          state.outcome = error.outcome;
          await new Promise(resolve => {state.release = resolve});
          for (const [name, operation] of [
            ['read', () => ctx.storage.read('count')],
            ['write', () => ctx.storage.apply({puts:{late:true}})],
            ['variable', () => ctx.variable('forbidden')],
          ]) {
            try { await operation(); state[name] = false; }
            catch (expired) {
              state[name] = expired.code === 'ContextExpired'
                && expired.outcome === 'not_committed';
            }
          }
          state.done = true;
        }
        return null;
      }
      if (state.cancelled && !state.released) {
        state.released = true;
        state.release();
      }
      return {cancelled:!!state.cancelled, code:state.code ?? null,
        outcome:state.outcome ?? null, done:!!state.done,
        read:!!state.read, write:!!state.write, variable:!!state.variable,
        unexpectedSuccess:!!state.unexpectedSuccess};
    "#;
    for cancel_owner in [false, true] {
        for queued_reply in [false, true] {
            JavaScriptRuntime::run(Options::default(), source(code), |app| {
                let runtime = &app.runtime;
                let active = Arc::new(AtomicBool::new(true));
                let owner = Arc::new(AtomicBool::new(true));
                let id = runtime.id()?;
                let (sender, mut receiver) = channel::channel(2);
                let payload = json!({"kind":"fetch","event":{"url":"http://test/","method":"GET","headers":[],"body":""},"identity":null,"object":null,"class":null}).to_string();
                runtime.start(Start {
                    id,
                    _bytes: runtime.reserve(payload.len())?,
                    payload,
                    body: None,
                    active: active.clone(),
                    owner: owner.clone(),
                    sender,
                })?;
                let Event::Operation {
                    id: operation,
                    name,
                    ..
                } = event(&mut receiver)
                else {
                    panic!("handler did not await its original operation")
                };
                assert_eq!(name, "storage.sync");
                if cancel_owner {
                    owner.store(false, Ordering::Release);
                } else {
                    active.store(false, Ordering::Release);
                }
                if queued_reply {
                    runtime.reply(id, operation, "{\"ok\":true,\"value\":null}".into())?;
                }
                let until = Instant::now() + Duration::from_secs(3);
                loop {
                    let mut probe = start(runtime, runtime.id()?, Arc::new(AtomicBool::new(true)));
                    let Event::Complete {
                        payload, failed, ..
                    } = event(&mut probe)
                    else {
                        panic!("probe admitted an unexpected SDK operation")
                    };
                    assert!(!failed);
                    let value: serde_json::Value = serde_json::from_str(&payload)?;
                    assert_eq!(value["unexpectedSuccess"], false);
                    if value["done"] == true {
                        assert_eq!(value["code"], "ContextExpired");
                        assert_eq!(value["outcome"], "unknown");
                        for field in ["cancelled", "read", "write", "variable"] {
                            assert_eq!(value[field], true, "cleanup did not refuse {field}");
                        }
                        break;
                    }
                    assert!(Instant::now() < until, "pending operation never cancelled: owner={cancel_owner}, queued_reply={queued_reply}, probe={value}");
                    std::thread::sleep(Duration::from_millis(1));
                }
                assert!(receiver.is_closed());
                assert!(
                    receiver.try_recv().is_err(),
                    "cancelled invocation emitted native work"
                );
                // A reply arriving after revocation must not revive its handler.
                if !queued_reply {
                    runtime.reply(id, operation, "{\"ok\":true,\"value\":null}".into())?;
                }
                let mut probe = start(runtime, runtime.id()?, Arc::new(AtomicBool::new(true)));
                let Event::Complete {
                    payload, failed, ..
                } = event(&mut probe)
                else {
                    panic!("late reply revived native work")
                };
                assert!(!failed);
                let value: serde_json::Value = serde_json::from_str(&payload)?;
                assert_eq!(value["done"], true);
                assert_eq!(value["unexpectedSuccess"], false);
                let until = Instant::now() + Duration::from_secs(2);
                while runtime.bytes.available_permits() != runtime.options.max_bridge_bytes {
                    assert!(
                        Instant::now() < until,
                        "cancelled bridge reservations leaked"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
                Ok(())
            })?;
        }
    }
    Ok(())
}

#[test]
fn concurrent_mutations_are_refused_before_second_effect() {
    JavaScriptRuntime::run(Options::default(), source("const first = ctx.storage.apply({puts:{x:1}}); let refused=false; try {await ctx.storage.apply({puts:{x:2}})} catch {refused=true} await first; return {refused};"), |app| {
        let id = app.runtime.id()?;
        let mut receive = start(&app.runtime, id, Arc::new(AtomicBool::new(true)));
        let Event::Operation {id: operation, ..} = event(&mut receive) else { panic!() };
        app.runtime.reply(id, operation, "{\"ok\":true,\"value\":null}".into())?;
        let Event::Complete {payload, failed, ..} = event(&mut receive) else { panic!("second mutation entered Rust") };
        assert!(!failed); assert_eq!(serde_json::from_str::<serde_json::Value>(&payload)?["refused"], true);
        Ok(())
    }).unwrap();
}

#[test]
fn runaway_javascript_terminates_and_joins() {
    let started = Instant::now();
    let entered = AtomicBool::new(false);
    let result = JavaScriptRuntime::run(
        Options {
            turn_timeout: Duration::from_millis(40),
            ..Default::default()
        },
        source("while(true) {}"),
        |app| {
            entered.store(true, Ordering::Release);
            let mut receive = start(
                &app.runtime,
                app.runtime.id()?,
                Arc::new(AtomicBool::new(true)),
            );
            assert!(receive.blocking_recv().is_none());
            Ok(())
        },
    );
    assert!(
        entered.load(Ordering::Acquire),
        "must test a running handler, not startup refusal"
    );
    assert!(result.is_err());
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[test]
fn missing_registration_refuses_startup() {
    assert!(JavaScriptRuntime::run(
        Options::default(),
        Source::new("var CellarApp = {};", "test.js").events([EventKind::Fetch]),
        |_| -> Result<()> { panic!("host must not start") }
    )
    .is_err());
}

#[test]
fn activation_objects_are_bounded_and_retired() {
    let source = Source::new("var CellarApp = {cells:{Counter: class {constructor(){this.count=0} async fetch(){return {count:++this.count}}}}};", "test.js").cell("Counter");
    JavaScriptRuntime::run(Options {max_objects: 1, ..Default::default()}, source, |app| {
        let runtime = &app.runtime;
        let invoke = |object, owner: Life| -> Result<(bool, serde_json::Value)> {
            let payload = json!({"kind":"fetch","event":{"url":"http://test/","method":"GET","headers":[],"body":""},"identity":null,"object":object,"class":"Counter"}).to_string();
            let (sender, mut receiver) = channel::channel(2);
            runtime.start(Start {id:runtime.id()?, _bytes:runtime.reserve(payload.len())?, payload, body:None, active:Arc::new(AtomicBool::new(true)), owner, sender})?;
            let Event::Complete {payload, failed, ..} = event(&mut receiver) else {panic!()};
            Ok((failed, serde_json::from_str(&payload)?))
        };
        let owner = Arc::new(AtomicBool::new(true));
        assert_eq!(invoke(100, owner.clone())?.1["count"], 1);
        assert_eq!(invoke(100, owner.clone())?.1["count"], 2);
        let next = Arc::new(AtomicBool::new(true));
        assert!(invoke(101, next.clone())?.0);
        owner.store(false, Ordering::Release);
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            let (failed, value) = invoke(101, next.clone())?;
            if !failed { assert_eq!(value["count"], 1); break; }
            assert!(Instant::now() < until, "activation was not retired");
        }
        Ok(())
    }).unwrap();
}

#[test]
fn result_conversion_failure_retains_ambiguous_mutation_outcome() {
    JavaScriptRuntime::run(Options::default(), source("try {await ctx.storage.sql('select 1')} catch(error) {return {code:error.code,outcome:error.outcome}}"), |app| {
        let id = app.runtime.id()?;
        let mut receive = start(&app.runtime, id, Arc::new(AtomicBool::new(true)));
        let Event::Operation {id: operation, ..} = event(&mut receive) else {panic!()};
        app.runtime.reply(id, operation, "{\"ok\":true,\"value\":9007199254740992}".into())?;
        let Event::Complete {payload,failed,..} = event(&mut receive) else {panic!()};
        assert!(!failed);
        let value: serde_json::Value = serde_json::from_str(&payload)?;
        assert_eq!(value["code"],"Ambiguous"); assert_eq!(value["outcome"],"unknown");
        Ok(())
    }).unwrap();
}

#[test]
fn unsafe_queue_integer_is_rejected_before_native_effects() {
    let source = Source::new(
        "var CellarApp = {queue: async function(event,ctx){await ctx.variable('forbidden')}};",
        "queue.js",
    )
    .events([EventKind::Queue]);
    JavaScriptRuntime::run(Options::default(), source, |app| {
        let runtime = &app.runtime;
        let payload = json!({"kind":"queue","event":{"queue":"jobs","messages":[{"body":9_007_199_254_740_992_u64}]},"identity":null,"object":null,"class":null}).to_string();
        let (sender, mut receiver) = channel::channel(2);
        runtime.start(Start {id:runtime.id()?, _bytes:runtime.reserve(payload.len())?, payload, body:None, active:Arc::new(AtomicBool::new(true)), owner:Arc::new(AtomicBool::new(true)), sender})?;
        let Event::Complete {failed,..} = event(&mut receiver) else {panic!("unsafe queue value entered native operation")};
        assert!(failed);
        Ok(())
    }).unwrap();
}

#[test]
fn runaway_module_initialization_is_interrupted() {
    let started = Instant::now();
    let result = JavaScriptRuntime::run(
        Options {
            startup_timeout: Duration::from_millis(40),
            ..Default::default()
        },
        Source::new("while(true) {}", "startup.js"),
        |_| -> Result<()> { panic!("runaway initialization must not admit the host") },
    );
    assert!(result.is_err());
    assert!(started.elapsed() < Duration::from_secs(5));
}

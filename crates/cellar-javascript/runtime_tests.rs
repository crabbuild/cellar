// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::EventKind;
use serde_json::json;
use std::time::Instant;

fn start(runtime: &Runtime, id: u64, active: Life) -> channel::Receiver<Event> {
    let (start, receive) = invocation(runtime, id, active).unwrap();
    runtime.start(start).unwrap();
    receive
}

fn invocation(
    runtime: &Runtime,
    id: u64,
    active: Life,
) -> Result<(Start, channel::Receiver<Event>)> {
    let (sender, receiver) = channel::channel(runtime.options.max_pending_operations + 1);
    let payload = json!({"kind":"fetch","event":{"url":"http://test/","method":"GET","headers":[],"body":""},"identity":null,"object":null,"class":null}).to_string();
    Ok((
        Start {
            _completion: Default::default(),
            budget_source: None,
            id,
            _bytes: runtime.reserve(payload.len())?,
            payload,
            body: None,
            active,
            owner: Arc::new(AtomicBool::new(true)),
            sender,
        },
        receiver,
    ))
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
fn filesystem_helpers_preserve_preflight_failure_outcomes() -> Result<()> {
    let code = r#"
      const file = await ctx.fs.open('/note', {access:'read_write'});
      const results = [];
      for (let round = 0; round < 2; round++) {
        for (const call of [
          () => file.readExact(0), () => file.readExact(1),
          () => file.writeAll(new Uint8Array()), () => file.writeAll(new Uint8Array([1])),
        ]) {
          try { await call(); throw new Error('preflight unexpectedly succeeded'); }
          catch(error) {
            results.push({code:error.code, errno:error.errno, operation:error.operation,
              outcome:error.outcome, completed_bytes:error.completed_bytes});
          }
        }
      }
      return results;
    "#;
    JavaScriptRuntime::run(Options::default(), source(code), |app| {
        let runtime = &app.runtime;
        let id = runtime.id()?;
        let mut receive = start(runtime, id, Arc::new(AtomicBool::new(true)));
        let Event::Operation {
            id: operation,
            name,
            ..
        } = event(&mut receive)
        else {
            panic!("helper did not open its protocol descriptor")
        };
        assert_eq!(name, "filesystem.open");
        runtime.reply(
            id,
            operation,
            json!({"ok":true,"value":{"handle":"test"}}).to_string(),
        )?;
        let mut expected = Vec::new();
        for outcome in ["not_committed", "unknown"] {
            for method in ["read_exact", "read_exact", "write_all", "write_all"] {
                let Event::Operation {
                    id: operation,
                    name,
                    ..
                } = event(&mut receive)
                else {
                    panic!("helper did not await its capability preflight")
                };
                // No read/write operation can be submitted after this refusal.
                assert_eq!(name, "filesystem.capabilities");
                runtime.reply(
                    id,
                    operation,
                    json!({"ok":false,"code":"Overloaded",
                    "errno":"EIO","operation":"filesystem.capabilities", "outcome":outcome,
                    "completed_bytes":0})
                    .to_string(),
                )?;
                expected.push(json!({"code":"Overloaded","errno":"EIO",
                    "operation":format!("filesystem.{method}"), "outcome":outcome,
                    "completed_bytes":0}));
            }
        }
        let Event::Complete {
            payload, failed, ..
        } = event(&mut receive)
        else {
            panic!("preflight refusal admitted another byte operation")
        };
        assert!(!failed);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&payload)?,
            json!(expected)
        );
        Ok(())
    })
}

#[test]
fn named_container_handles_keep_their_slot_on_every_followup() -> Result<()> {
    let code = r#"
      const c = ctx.containers.get('backup');
      const p = await c.exec({cmd:['tool']});
      await p.readStdout(); await p.readStderr(); await p.write(new Uint8Array([1]));
      await p.closeStdin(); await p.wait(); await p.kill(); await p.release();
      const port = c.port(80);
      const portRequest = {url:'http://container/',method:'GET',headers:[],body:''};
      await port.waitReady(); await port.fetch(portRequest);
      const response = await port.fetchStream(portRequest);
      await response.read(); await response.cancel();
      const socket = await port.connect();
      await socket.read(); await socket.write(new Uint8Array([1]));
      await socket.closeWrite(); await socket.close(); await socket.release();
      const upgrade = await port.connectWebSocket('ws://container/');
      await upgrade.socket.receive(); await upgrade.socket.sendText('hello');
      await upgrade.socket.sendBinary(new Uint8Array([1]));
      await upgrade.socket.close(); await upgrade.socket.release();
      return null;
    "#;
    JavaScriptRuntime::run(Options::default(), source(code), |app| {
        let runtime = &app.runtime;
        let invocation = runtime.id()?;
        let mut receive = start(runtime, invocation, Arc::new(AtomicBool::new(true)));
        let head = json!({"status":200,"headers":[],"body":""});
        for (name, value) in [
            ("container.exec", json!("1")),
            ("container.exec.read_stdout", json!("")),
            ("container.exec.read_stderr", json!("")),
            ("container.exec.write", json!(null)),
            ("container.exec.close_stdin", json!(null)),
            ("container.exec.wait", json!(0)),
            ("container.exec.kill", json!(null)),
            ("container.release", json!(null)),
            ("container.port.wait_ready", json!(null)),
            ("container.port.fetch", head.clone()),
            (
                "container.port.fetch_stream",
                json!({"status":200,"headers":[],"body":"","stream":"1"}),
            ),
            (
                "container.response.read",
                json!({"done":false,"body":"AQ=="}),
            ),
            ("container.response.cancel", json!(null)),
            ("container.port.connect", json!("1")),
            ("container.socket.read", json!("")),
            ("container.socket.write", json!(null)),
            ("container.socket.close_write", json!(null)),
            ("container.socket.close", json!(null)),
            ("container.release", json!(null)),
            (
                "container.port.connect_websocket",
                json!({"upgraded":true,"id":"1","protocol":"","headers":[]}),
            ),
            (
                "container.websocket.receive",
                json!({"type":"binary","data":"AQ=="}),
            ),
            ("container.websocket.send_text", json!(null)),
            ("container.websocket.send_binary", json!(null)),
            ("container.websocket.close", json!(null)),
            ("container.release", json!(null)),
        ] {
            let Event::Operation {
                id,
                name: actual,
                payload,
                ..
            } = event(&mut receive)
            else {
                anyhow::bail!("named facade failed before {name}");
            };
            assert_eq!(actual, name);
            let input: serde_json::Value = serde_json::from_str(&payload)?;
            assert_eq!(input["binding"], "backup", "{name} lost its slot");
            runtime.reply(invocation, id, json!({"ok":true,"value":value}).to_string())?;
        }
        assert!(matches!(
            event(&mut receive),
            Event::Complete { failed: false, .. }
        ));
        Ok(())
    })
}

#[test]
fn v8_completion_reservation_requires_startup_headroom() {
    let mut entered = false;
    let error = JavaScriptRuntime::run(
        Options {
            max_value_bytes: 128,
            max_bridge_bytes: 128,
            ..Options::default()
        },
        Source::new("var CellarApp = {};", "completion-headroom.js"),
        |_| {
            entered = true;
            Ok(())
        },
    )
    .expect_err("V8 accepted a bridge with no completion headroom");
    assert!(!entered);
    assert!(error.to_string().contains("completion"), "{error:#}");
}

#[test]
fn v8_completion_reservation_releases_on_admission_refusal_and_queue_drop() -> Result<()> {
    for byte_exhaustion in [false, true] {
        let options = Options {
            max_invocations: 1,
            max_value_bytes: 1024,
            // After the 1 KiB hold and 128-byte input, only 127 bytes remain.
            max_bridge_bytes: if byte_exhaustion { 1279 } else { 4096 },
            ..Options::default()
        };
        // Hold the real native start queue without consuming it. This tests
        // admission ownership, not language execution or durability.
        let runtime = run_with(
            options,
            source("return null;"),
            EngineBackend::Embedded,
            |app| {
                let runtime = &app.runtime;
                let held = if byte_exhaustion {
                    Some(runtime.reserve(1024)?)
                } else {
                    let _receive = start(runtime, runtime.id()?, Arc::new(AtomicBool::new(true)));
                    None
                };
                let before = runtime.usage().retained_bytes;
                let (start, _receive) =
                    invocation(runtime, runtime.id()?, Arc::new(AtomicBool::new(true)))?;
                if byte_exhaustion {
                    ensure!(runtime.bytes.available_permits() < COMPLETION_BYTES);
                }
                let error = runtime
                    .start(start)
                    .expect_err("saturated admission accepted work");
                if !byte_exhaustion {
                    ensure!(error.to_string().contains("admission exhausted"));
                }
                ensure!(
                    runtime.usage().retained_bytes == before,
                    "refused start leaked bytes"
                );
                drop(held);
                Ok(runtime.clone())
            },
            move |runtime, _source, starts, _replies, ready| {
                ready
                    .send(Ok(()))
                    .map_err(|_| anyhow::anyhow!("host left before startup"))?;
                while !runtime.closed.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(1));
                }
                let queued = starts.try_iter().count();
                ensure!(
                    queued == usize::from(!byte_exhaustion),
                    "unexpected queued admission"
                );
                Ok(())
            },
        )?;
        ensure!(
            runtime.usage().stopped && runtime.usage().retained_bytes == 0,
            "queued completion reservations survived joined shutdown"
        );
    }
    Ok(())
}

#[test]
fn v8_completion_reservation_enforces_utf8_limit_without_stopping_engine() -> Result<()> {
    let code = r#"
      globalThis.calls = (globalThis.calls ?? 0) + 1;
      return {value:'é'.repeat(506) + (globalThis.calls === 1 ? 'a' : '')};
    "#;
    JavaScriptRuntime::run(
        Options {
            max_value_bytes: 1024,
            ..Options::default()
        },
        source(code),
        |app| {
            let runtime = &app.runtime;
            for oversize in [true, false] {
                let mut receive = start(runtime, runtime.id()?, Arc::new(AtomicBool::new(true)));
                let Event::Complete {
                    payload,
                    body,
                    failed,
                    ..
                } = event(&mut receive)
                else {
                    anyhow::bail!("UTF-8 reply fixture unexpectedly submitted SDK work");
                };
                ensure!(body.is_none());
                if oversize {
                    ensure!(failed);
                    let failure = cellar_guest::rpc_failure::failure(&payload);
                    ensure!(failure.code() == cellar::sdk::rpc::Code::InvalidResponse);
                    ensure!(failure.outcome() == cellar::sdk::rpc::EffectOutcome::Unknown);
                } else {
                    ensure!(!failed && payload.len() == 1024);
                    ensure!(
                        serde_json::from_str::<serde_json::Value>(&payload)?
                            == json!({"value":"é".repeat(506)})
                    );
                }
                ensure!(!runtime.usage().stopped);
            }
            let until = Instant::now() + Duration::from_secs(2);
            while runtime.usage().retained_bytes != 0 {
                ensure!(Instant::now() < until, "UTF-8 completion retained bytes");
                std::thread::sleep(Duration::from_millis(1));
            }
            Ok(())
        },
    )
}

#[test]
fn operation_value_limit_counts_utf8_bytes_at_the_exact_boundary() -> Result<()> {
    for extra in [1, 2] {
        let options = Options {
            max_value_bytes: 1024,
            ..Options::default()
        };
        // JSON's {"name":""} costs 11 bytes; each é costs two UTF-8 bytes.
        let code = format!(
            r#"
          try {{ return {{value:await ctx.variable('é'.repeat(506) + 'a'.repeat({extra}))}}; }}
          catch (error) {{ return {{code:error.code, outcome:error.outcome}}; }}
        "#
        );
        JavaScriptRuntime::run(options, source(&code), |app| {
            let runtime = &app.runtime;
            let id = runtime.id()?;
            let mut receive = start(runtime, id, Arc::new(AtomicBool::new(true)));
            if extra == 1 {
                let Event::Operation {
                    id: operation,
                    name,
                    payload,
                    ..
                } = event(&mut receive)
                else {
                    anyhow::bail!("exact-limit UTF-8 value was refused");
                };
                assert_eq!(name, "var");
                assert_eq!(payload.len(), 1024);
                runtime.reply(
                    id,
                    operation,
                    json!({"ok":true,"value":"accepted"}).to_string(),
                )?;
            }
            let Event::Complete {
                payload, failed, ..
            } = event(&mut receive)
            else {
                anyhow::bail!("oversize UTF-8 value reached native work");
            };
            assert!(!failed);
            let expected = if extra == 1 {
                json!({"value":"accepted"})
            } else {
                json!({"code":"ValueLimit", "outcome":"not_committed"})
            };
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&payload)?,
                expected
            );
            Ok(())
        })?;
    }
    Ok(())
}

#[test]
fn exhausted_bridge_refuses_rpc_before_copying_its_attachment() -> Result<()> {
    let options = Options {
        max_bridge_bytes: 4 * 1024 * 1024,
        ..Options::default()
    };
    let code = r#"
      try {
        await ctx[Symbol.for('cellar.rpc')].call({binding:'COUNTERS', name:'alice',
          method:'example.counter.v1.Counter/GetCount', contract_digest:'a'.repeat(64)},
          new Uint8Array(8192));
        return {unexpected:true};
      } catch (error) {
        return {code:error.code, outcome:error.outcome};
      }
    "#;
    JavaScriptRuntime::run(options, source(code), |app| {
        let runtime = &app.runtime;
        // Keep room for the small invocation and its refusal, but not the 8 KiB
        // attachment. Saturation must not prevent completion or host shutdown.
        let first = runtime.reserve(runtime.options.max_value_bytes)?;
        let second = runtime
            .reserve(runtime.options.max_bridge_bytes - runtime.options.max_value_bytes - 512)?;
        let mut receive = start(runtime, runtime.id()?, Arc::new(AtomicBool::new(true)));
        let Event::Complete {
            payload, failed, ..
        } = event(&mut receive)
        else {
            anyhow::bail!("exhausted bridge submitted native RPC work");
        };
        assert!(!failed);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&payload)?,
            json!({"code":"BridgeLimit", "outcome":"not_committed"})
        );
        assert_eq!(runtime.attachment_copied.load(Ordering::Relaxed), 0);
        drop((first, second));
        let until = Instant::now() + Duration::from_secs(2);
        while runtime.usage().retained_bytes != 0 {
            assert!(Instant::now() < until, "refused RPC retained a reservation");
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    })
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
                    _completion: Default::default(),
                    budget_source: None,
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
fn requested_shutdown_interrupts_dispatch_without_a_runtime_failure() {
    requested_shutdown(false);
}

#[test]
fn requested_shutdown_interrupts_microtasks_without_a_budget_failure() {
    requested_shutdown(true);
}

fn requested_shutdown(in_microtasks: bool) {
    for fail_host in [false, true] {
        let started = Instant::now();
        let mut retained = None;
        let result = JavaScriptRuntime::run(
            Options::default(),
            // The operation proves dispatch entered the actual isolate. It is
            // deliberately not awaited, so JavaScript stays on this CPU turn
            // until the watchdog observes the host's shutdown request.
            source(if in_microtasks {
                "await ctx.now(); void ctx.now(); while (true) {}"
            } else {
                "void ctx.now(); while (true) {}"
            }),
            |app| {
                retained = Some(app.runtime.clone());
                let invocation = app.runtime.id()?;
                let mut receive = start(&app.runtime, invocation, Arc::new(AtomicBool::new(true)));
                if in_microtasks {
                    let Event::Operation { id, .. } = event(&mut receive) else {
                        anyhow::bail!("shutdown fixture did not suspend dispatch")
                    };
                    app.runtime
                        .reply(invocation, id, r#"{"ok":true,"value":0}"#.into())?;
                }
                let Event::Operation { name, .. } = event(&mut receive) else {
                    anyhow::bail!("shutdown fixture did not enter JavaScript")
                };
                ensure!(name == "clock.now");
                if fail_host {
                    anyhow::bail!("original host failure")
                }
                Ok(42)
            },
        );
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(retained.unwrap().usage().retained_bytes, 0);
        if fail_host {
            assert_eq!(result.unwrap_err().to_string(), "original host failure");
        } else {
            assert_eq!(result.unwrap(), 42);
        }
    }
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
    assert_eq!(
        result.unwrap_err().root_cause().to_string(),
        "JavaScript engine turn exceeded execution budget"
    );
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
            runtime.start(Start { _completion: Default::default(), budget_source: None,id:runtime.id()?, _bytes:runtime.reserve(payload.len())?, payload, body:None, active:Arc::new(AtomicBool::new(true)), owner, sender})?;
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
        runtime.start(Start { _completion: Default::default(), budget_source: None,id:runtime.id()?, _bytes:runtime.reserve(payload.len())?, payload, body:None, active:Arc::new(AtomicBool::new(true)), owner:Arc::new(AtomicBool::new(true)), sender})?;
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
    let error = format!("{:#}", result.expect_err("runaway startup succeeded"));
    assert!(
        error.contains("startup application execution") && error.contains("execution budget"),
        "startup lost its watchdog cause: {error}"
    );
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[test]
fn startup_syntax_failure_has_a_fixed_phase_without_source_contents() {
    let result = JavaScriptRuntime::run(
        Options::default(),
        Source::new("const private_marker = ;", "private-input.js"),
        |_| -> Result<()> { panic!("invalid startup must not admit the host") },
    );
    let error = format!("{:#}", result.expect_err("invalid startup succeeded"));
    assert!(
        error.contains("startup application compilation"),
        "startup lost its compilation phase: {error}"
    );
    assert!(!error.contains("private_marker") && !error.contains("private-input.js"));
}

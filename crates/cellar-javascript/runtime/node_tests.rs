// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::EventKind;

fn with_module<T>(
    code: &str,
    host: impl FnOnce(Arc<JavaScriptApplication>) -> Result<T>,
) -> Result<T> {
    let directory = tempfile::tempdir()?;
    let entry = directory.path().join("app.mjs");
    std::fs::write(&entry, code)?;
    NodeRuntime::run(
        Options::default(),
        NodeOptions::new(entry),
        Source::module().events([EventKind::Fetch]),
        host,
    )
}

fn start(runtime: &Runtime, active: Life) -> (u64, channel::Receiver<Event>) {
    let id = runtime.id().unwrap();
    let (sender, receiver) = channel::channel(runtime.options.max_pending_operations + 1);
    let payload = json!({"kind":"fetch","event":{"url":"http://test/","method":"GET","headers":[],"body":""},"identity":null,"object":null,"class":null}).to_string();
    runtime
        .start(Start {
            _completion: Default::default(),
            budget_source: None,
            id,
            _bytes: runtime.reserve(payload.len()).unwrap(),
            payload,
            body: None,
            active,
            owner: Arc::new(AtomicBool::new(true)),
            sender,
        })
        .unwrap();
    (id, receiver)
}

fn event(receiver: &mut channel::Receiver<Event>) -> Event {
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        match receiver.try_recv() {
            Ok(value) => return value,
            Err(channel::error::TryRecvError::Disconnected) => panic!("Node closed prematurely"),
            Err(_) => assert!(Instant::now() < until, "Node stalled"),
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn node_async_contexts_survive_timers_and_reject_foreign_context() -> Result<()> {
    with_module(
        r#"
        import {setTimeout} from 'node:timers/promises';
        let saved;
        export async function fetch(req, ctx) {
            saved ??= ctx;
            await setTimeout(5);
            const value = await ctx.variable('value');
            let mismatch = false;
            if (saved !== ctx) { try {await saved.now()} catch(e) {mismatch = e.code === 'ContextMismatch'} }
            return {value, mismatch};
        }
    "#,
        |app| {
            let (a, mut ra) = start(&app.runtime, Arc::new(AtomicBool::new(true)));
            let (b, mut rb) = start(&app.runtime, Arc::new(AtomicBool::new(true)));
            let Event::Operation { id: oa, .. } = event(&mut ra) else {
                panic!()
            };
            let Event::Operation { id: ob, .. } = event(&mut rb) else {
                panic!()
            };
            app.runtime
                .reply(b, ob, json!({"ok":true,"value":"b"}).to_string())?;
            let Event::Complete {
                payload, failed, ..
            } = event(&mut rb)
            else {
                panic!()
            };
            assert!(!failed);
            assert_eq!(
                serde_json::from_str::<Value>(&payload)?,
                json!({"value":"b","mismatch":true})
            );
            app.runtime
                .reply(a, oa, json!({"ok":true,"value":"a"}).to_string())?;
            let Event::Complete {
                payload, failed, ..
            } = event(&mut ra)
            else {
                panic!()
            };
            assert!(!failed);
            assert_eq!(serde_json::from_str::<Value>(&payload)?["value"], "a");
            Ok(())
        },
    )
}

#[test]
fn node_cancellation_revokes_delayed_operations() -> Result<()> {
    with_module("export async function fetch(req,ctx) {await ctx.now(); await ctx.variable('forbidden'); return null}", |app| {
        let active = Arc::new(AtomicBool::new(true));
        let (id, mut receiver) = start(&app.runtime, active.clone());
        let Event::Operation {id: operation,..} = event(&mut receiver) else {panic!()};
        active.store(false,Ordering::Release);
        app.runtime.reply(id,operation,"{\"ok\":true,\"value\":null}".into())?;
        let deadline = Instant::now() + Duration::from_secs(5);
        while !receiver.is_closed() {assert!(Instant::now()<deadline); std::thread::sleep(Duration::from_millis(1));}
        assert!(receiver.try_recv().is_err());
        Ok(())
    })
}

#[test]
fn node_refuses_overlapping_mutations() -> Result<()> {
    with_module("export async function fetch(req,ctx) {const first=ctx.storage.apply({puts:{x:1}}); let refused=false; try {await ctx.storage.apply({puts:{x:2}})} catch(e) {refused=e.code==='ConcurrentMutation' && e.outcome==='not_committed'} await first; return {refused}}", |app| {
        let (id, mut receiver) = start(&app.runtime,Arc::new(AtomicBool::new(true)));
        let Event::Operation {id: operation,..}=event(&mut receiver) else {panic!()};
        app.runtime.reply(id,operation,"{\"ok\":true,\"value\":null}".into())?;
        let Event::Complete {payload,failed,..}=event(&mut receiver) else {panic!("second mutation admitted")};
        assert!(!failed); assert_eq!(serde_json::from_str::<Value>(&payload)?["refused"],true);
        Ok(())
    })
}

#[test]
fn node_process_io_and_cleanup_do_not_block_storage_mutations() -> Result<()> {
    with_module(
        r#"
        export async function fetch(req, ctx) {
            for (const name of ['container.exec.write', 'container.exec.close_stdin',
                                'container.exec.kill', 'container.release']) {
                const pending = ctx._call(name, {id:'1', data:'AQ==', signal:9});
                try { await ctx.storage.apply({puts:{last:name}}); }
                catch (error) { return {blocked:name, code:error.code}; }
                await pending;
            }
            return {joined:true};
        }
        "#,
        |app| {
            let (id, mut receiver) = start(&app.runtime, Arc::new(AtomicBool::new(true)));
            for expected in [
                "container.exec.write",
                "container.exec.close_stdin",
                "container.exec.kill",
                "container.release",
            ] {
                let Event::Operation { id: io, name, .. } = event(&mut receiver) else {
                    panic!("process control missing: {expected}")
                };
                assert_eq!(name, expected);
                // Hold the process acknowledgement until storage has been
                // admitted and acknowledged. A shared mutation slot deadlocks
                // this ordering or rejects the legitimate storage mutation.
                let Event::Operation {
                    id: storage, name, ..
                } = event(&mut receiver)
                else {
                    panic!("pending {expected} prevented independent storage admission")
                };
                assert_eq!(name, "storage.apply");
                app.runtime
                    .reply(id, storage, r#"{"ok":true,"value":null}"#.into())?;
                app.runtime
                    .reply(id, io, r#"{"ok":true,"value":null}"#.into())?;
            }
            let Event::Complete {
                payload, failed, ..
            } = event(&mut receiver)
            else {
                panic!("process I/O did not join")
            };
            assert!(!failed);
            assert_eq!(
                serde_json::from_str::<Value>(&payload)?,
                json!({"joined":true})
            );
            Ok(())
        },
    )
}

#[test]
fn node_process_survives_native_observer_pause() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let entry = directory.path().join("app.mjs");
    std::fs::write(&entry, "export function fetch(){return {alive:true}}")?;
    let resumed = Arc::new(AtomicBool::new(false));
    let mut node = NodeOptions::new(entry);
    node.heartbeat_timeout = Duration::from_millis(100);
    node.observer_pause = Some((Duration::from_millis(300), resumed.clone()));
    NodeRuntime::run(
        Options::default(),
        node,
        Source::module().events([EventKind::Fetch]),
        |app| {
            // Node remains a live, independently scheduled process during the
            // native pause. Its ordinary 25 ms heartbeats accumulate on IPC.
            let until = Instant::now() + Duration::from_secs(2);
            while !resumed.load(Ordering::Acquire) {
                assert!(Instant::now() < until, "native observer did not resume");
                std::thread::sleep(Duration::from_millis(1));
            }
            let (_, mut receiver) = start(&app.runtime, Arc::new(AtomicBool::new(true)));
            let Event::Complete {
                payload, failed, ..
            } = event(&mut receiver)
            else {
                panic!("healthy Node worker did not complete");
            };
            assert!(!failed);
            assert_eq!(
                serde_json::from_str::<Value>(&payload)?,
                json!({"alive":true})
            );
            Ok(())
        },
    )
}

#[test]
fn node_queued_large_controls_do_not_hide_a_healthy_heartbeat() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let entry = directory.path().join("app.mjs");
    std::fs::write(
        &entry,
        r#"
        export async function fetch(req, ctx) {
            return Promise.all(Array.from({length:8}, () => ctx.variable('x'.repeat(512 << 10))));
        }
    "#,
    )?;
    let resumed = Arc::new(AtomicBool::new(false));
    let mut node = NodeOptions::new(entry);
    node.heartbeat_timeout = Duration::from_millis(100);
    node.observer_pause = Some((Duration::from_millis(300), resumed.clone()));
    node.observer_pause_after_start = true;
    NodeRuntime::run(
        Options::default(),
        node,
        Source::module().events([EventKind::Fetch]),
        |app| {
            let (id, mut receiver) = start(&app.runtime, Arc::new(AtomicBool::new(true)));
            let until = Instant::now() + Duration::from_secs(2);
            while !resumed.load(Ordering::Acquire) {
                assert!(Instant::now() < until, "active observer did not resume");
                std::thread::sleep(Duration::from_millis(1));
            }
            for _ in 0..8 {
                let Event::Operation {
                    id: operation,
                    name,
                    payload,
                    ..
                } = event(&mut receiver)
                else {
                    panic!("large control was lost")
                };
                assert_eq!(name, "var");
                assert!(payload.len() >= 512 << 10);
                app.runtime
                    .reply(id, operation, r#"{"ok":true,"value":null}"#.into())?;
            }
            let Event::Complete {
                payload, failed, ..
            } = event(&mut receiver)
            else {
                panic!("extra operation")
            };
            assert!(!failed);
            assert_eq!(
                serde_json::from_str::<Value>(&payload)?,
                json!([null, null, null, null, null, null, null, null])
            );
            Ok(())
        },
    )
}

#[test]
fn node_crash_and_runaway_revoke_and_join() {
    for body in ["process.exit(9)", "while(true){}"] {
        let before = Instant::now();
        let entered = AtomicBool::new(false);
        let result = with_module(&format!("export function fetch(){{{body}}}"), |app| {
            entered.store(true, Ordering::Release);
            let (_, mut receiver) = start(&app.runtime, Arc::new(AtomicBool::new(true)));
            assert!(receiver.blocking_recv().is_none());
            Ok(())
        });
        assert!(entered.load(Ordering::Acquire));
        assert!(result.is_err());
        assert!(before.elapsed() < Duration::from_secs(5));
    }
}

#[test]
fn node_missing_export_and_startup_loop_refuse_host() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let entry = directory.path().join("app.mjs");
    for code in ["export const other=1", "while(true){}"] {
        std::fs::write(&entry, code)?;
        let before = Instant::now();
        assert!(NodeRuntime::run(
            Options {
                startup_timeout: Duration::from_millis(500),
                ..Default::default()
            },
            NodeOptions::new(&entry),
            Source::module().events([EventKind::Fetch]),
            |_| -> Result<()> { panic!("invalid application admitted") }
        )
        .is_err());
        assert!(before.elapsed() < Duration::from_secs(3));
    }
    Ok(())
}

#[test]
fn node_commonjs_entry_and_stdout_do_not_corrupt_bridge() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let entry = directory.path().join("app.cjs");
    std::fs::write(&entry, "const {Buffer}=require('node:buffer'); module.exports={async fetch(req,ctx){process.stdout.write('Node package stdout test\\n'); return {value:Buffer.from(await ctx.variable('value')).toString('hex')}}}")?;
    NodeRuntime::run(
        Options::default(),
        NodeOptions::new(entry),
        Source::module().events([EventKind::Fetch]),
        |app| {
            let (id, mut receiver) = start(&app.runtime, Arc::new(AtomicBool::new(true)));
            let Event::Operation { id: operation, .. } = event(&mut receiver) else {
                panic!()
            };
            app.runtime
                .reply(id, operation, json!({"ok":true,"value":"node"}).to_string())?;
            let Event::Complete {
                payload, failed, ..
            } = event(&mut receiver)
            else {
                panic!()
            };
            assert!(!failed);
            assert_eq!(
                serde_json::from_str::<Value>(&payload)?["value"],
                "6e6f6465"
            );
            Ok(())
        },
    )
}

#[test]
fn node_shutdown_reaps_child_with_live_timers() -> Result<()> {
    let mut pid = 0;
    with_module("setInterval(()=>{},100); export async function fetch(req,ctx){await ctx.variable(String(process.pid)); return null}", |app| {
        let (_,mut receiver)=start(&app.runtime,Arc::new(AtomicBool::new(true)));
        let Event::Operation {payload,..}=event(&mut receiver) else {panic!()};
        pid=serde_json::from_str::<Value>(&payload)?["name"].as_str().unwrap().parse::<i32>()?;
        // Leave both the handler and its timer live when bootstrap closes.
        Ok(())
    })?;
    #[cfg(unix)]
    {
        // SAFETY: signal zero only queries the recorded child process ID.
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
    }
    Ok(())
}

#[test]
fn node_worker_loss_after_submission_never_replays_mutation() {
    let mut submitted = 0;
    let result=with_module("import {setTimeout} from 'node:timers/promises'; export async function fetch(req,ctx){const pending=ctx.storage.apply({puts:{x:1}}); await setTimeout(50); process.exit(9); await pending}", |app| {
        let (_,mut receiver)=start(&app.runtime,Arc::new(AtomicBool::new(true)));
        let Event::Operation {name,..}=event(&mut receiver) else {panic!()};
        assert_eq!(name,"storage.apply"); submitted+=1;
        // Simulate a native mutation whose outcome has not reached the guest.
        assert!(receiver.blocking_recv().is_none());
        Ok(())
    });
    assert!(result.is_err());
    assert_eq!(submitted, 1);
}

#[cfg(unix)]
#[test]
fn node_bootstrap_keeps_compiled_source_out_of_exec_arguments() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir()?;
    let entry = directory.path().join("app.mjs");
    std::fs::write(
        &entry,
        r#"
export function fetch() {
    const stack = new Error("trace probe").stack;
    return {booted: true, stackBytes: Buffer.byteLength(stack),
        fixedSource: stack.includes("cellar:node-bootstrap")};
}
"#,
    )?;
    let executable = directory.path().join("bounded-node");
    // Linux with 4 KiB pages permits at most 131,071 bytes before an argument's
    // terminating NUL. Exercise that boundary on every supported Unix host,
    // then delegate to the actual Node executable and run a real invocation.
    std::fs::write(
        &executable,
        r#"#!/bin/sh
LC_ALL=C
export LC_ALL
for argument do
  if [ "${#argument}" -ge 131072 ]; then
    echo "Node bootstrap exceeded the exec argument limit" >&2
    exit 77
  fi
done
exec node "$@"
"#,
    )?;
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755))?;
    let mut node = NodeOptions::new(entry);
    node.executable = executable;
    NodeRuntime::run(
        Options::default(),
        node,
        Source::module().events([EventKind::Fetch]),
        |application| {
            let (_, mut receiver) = start(&application.runtime, Arc::new(AtomicBool::new(true)));
            let Event::Complete {
                payload, failed, ..
            } = event(&mut receiver)
            else {
                panic!("expected completed invocation")
            };
            assert!(!failed);
            let value: Value = serde_json::from_str(&payload)?;
            assert_eq!(value["booted"], true);
            assert!(
                value["stackBytes"].as_u64().unwrap() < 4096,
                "the compiled source became an unbounded stack-frame URL"
            );
            assert_eq!(value["fixedSource"], true);
            Ok(())
        },
    )
}

#[cfg(unix)]
#[test]
fn node_large_bootstrap_stays_out_of_process_arguments() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let executable = Command::new("node")
        .args(["-p", "process.execPath"])
        .output()?;
    ensure!(executable.status.success(), "Node executable lookup failed");
    let executable = String::from_utf8(executable.stdout)?.trim().to_owned();
    let directory = tempfile::tempdir()?;
    let entry = directory.path().join("app.mjs");
    std::fs::write(&entry, "export const fetch = () => null;")?;
    let wrapper = directory.path().join("bounded-node");
    // Model Linux's per-argument bound on every Unix test host. The padded
    // real bootstrap must still initialize the worker through its private pipe.
    std::fs::write(&wrapper, "#!/bin/sh\nexport LC_ALL=C\nfor cellar_boot_arg do\n  if [ \"${#cellar_boot_arg}\" -ge 131072 ]; then\n    echo 'bootstrap argument exceeds Linux bound' >&2\n    exit 86\n  fi\ndone\nexec \"$CELLAR_TEST_BOOTSTRAP_NODE\" \"$@\"\n")?;
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755))?;
    let mut options = NodeOptions::new(entry);
    options.executable = wrapper;
    options.bootstrap_padding = 128 * 1024;
    options
        .environment
        .insert("CELLAR_TEST_BOOTSTRAP_NODE".into(), executable);
    let mut entered = false;
    NodeRuntime::run(
        Options::default(),
        options,
        Source::module().events([EventKind::Fetch]),
        |_| {
            entered = true;
            Ok(())
        },
    )?;
    ensure!(entered, "large bootstrap did not reach host admission");
    Ok(())
}

#[cfg(unix)]
#[test]
fn node_bootstrap_pipe_is_covered_by_startup_deadline() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir()?;
    let executable = directory.path().join("blocked-node");
    std::fs::write(&executable, "#!/bin/sh\nsleep 30\n")?;
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755))?;
    let mut options = NodeOptions::new(directory.path().join("app.mjs"));
    options.executable = executable;
    let mut source = Source::module();
    // Exceed pipe capacity while the fake executable never consumes stdin.
    for n in 0..2000 {
        source = source.cell(format!("Counter{n:0100}"));
    }
    let before = Instant::now();
    assert!(NodeRuntime::run(
        Options {
            startup_timeout: Duration::from_millis(100),
            ..Default::default()
        },
        options,
        source,
        |_| -> Result<()> { panic!("blocked worker admitted") }
    )
    .is_err());
    assert!(before.elapsed() < Duration::from_secs(2));
    Ok(())
}

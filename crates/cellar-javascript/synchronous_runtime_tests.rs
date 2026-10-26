// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::EventKind;
use serde_json::{json, Value};
use std::time::Instant;

#[derive(Clone, Copy)]
enum Backend {
    #[cfg(feature = "embed")]
    Embedded,
    #[cfg(feature = "node")]
    Node,
}

fn modes() -> Vec<Backend> {
    vec![
        #[cfg(feature = "embed")]
        Backend::Embedded,
        #[cfg(feature = "node")]
        Backend::Node,
    ]
}

fn run<T>(
    backend: Backend,
    options: Options,
    body: &str,
    host: impl FnOnce(Arc<JavaScriptApplication>) -> Result<T>,
) -> Result<T> {
    match backend {
        #[cfg(feature = "node")]
        Backend::Node => {
            let directory = tempfile::tempdir()?;
            let entry = directory.path().join("sync.mjs");
            std::fs::write(
                &entry,
                format!("export function fetch(req, ctx) {{ {body} }}; export const cells = {{ Counter: class {{ fetch(req, ctx) {{ {body} }} }} }};"),
            )?;
            node::NodeRuntime::run(
                options,
                node::NodeOptions::new(entry),
                Source::module().events([EventKind::Fetch]).cell("Counter"),
                host,
            )
        }
        #[cfg(feature = "embed")]
        Backend::Embedded => JavaScriptRuntime::run(
            options,
            Source::new(
                format!("var CellarApp = {{fetch(req, ctx) {{ {body} }}, cells: {{Counter: class {{fetch(req, ctx) {{ {body} }} }} }} }}"),
                "sync.js",
            )
            .events([EventKind::Fetch]).cell("Counter"),
            host,
        ),
    }
}

fn start(runtime: &Runtime, active: Life) -> (u64, channel::Receiver<Event>) {
    start_scope(runtime, active, Arc::new(AtomicBool::new(true)), None)
}

fn start_scope(
    runtime: &Runtime,
    active: Life,
    owner: Life,
    object: Option<u64>,
) -> (u64, channel::Receiver<Event>) {
    let id = runtime.id().unwrap();
    let (sender, receiver) = channel::channel(runtime.options.max_pending_operations + 1);
    let payload = json!({"kind":"fetch","event":{"url":"http://test/","method":"GET","headers":[],"body":""},
        "identity":object.map(|_| json!({"scope":"Counter:test","name":"test","class":"Counter","epoch":"7"})),
        "object":object,"class":object.map(|_| "Counter")}).to_string();
    runtime
        .start(Start {
            _completion: Default::default(),
            body: None,
            budget_source: None,
            id,
            _bytes: runtime.reserve(payload.len()).unwrap(),
            payload,
            active,
            owner,
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
            Err(channel::error::TryRecvError::Disconnected) => panic!("runtime closed prematurely"),
            Err(_) => assert!(Instant::now() < until, "synchronous operation stalled"),
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn synchronous_limits_and_uncertain_reply_preserve_provenance() -> Result<()> {
    let body = r#"
      let unsafe = false, limit = false, unsupported = false;
      try { ctx._sync('cell.fetch', {}); } catch (e) { unsupported = e.code === 'UnsupportedOperation' && e.outcome === 'not_committed'; }
      try { ctx._sync('storage.sql', { sql: 'update example set value = 1', params: [] }); } catch (e) { unsafe = e.code === 'Ambiguous' && e.outcome === 'unknown'; }
      try { ctx._sync('storage.read', {key: 'second'}); } catch(e) { limit = e.code === 'OperationLimit' && e.outcome === 'not_committed'; }
      return {unsafe, limit, unsupported};
    "#;
    for backend in modes() {
        run(
            backend,
            Options {
                max_operations: 1,
                turn_timeout: Duration::from_secs(2),
                ..Default::default()
            },
            body,
            |app| {
                let runtime = &app.runtime;
                let (invocation, mut receive) = start(runtime, Arc::new(AtomicBool::new(true)));
                let Event::Operation {
                    id,
                    name,
                    synchronous,
                    ..
                } = event(&mut receive)
                else {
                    panic!("operation missing")
                };
                assert_eq!(name, "storage.sql");
                runtime.respond(
                    invocation,
                    id,
                    r#"{"ok":true,"value":9007199254740992}"#.into(),
                    synchronous,
                )?;
                let Event::Complete {
                    payload, failed, ..
                } = event(&mut receive)
                else {
                    panic!("second effect admitted")
                };
                assert!(!failed);
                assert_eq!(
                    serde_json::from_str::<Value>(&payload)?,
                    json!({"unsafe":true,"limit":true,"unsupported":true})
                );
                Ok(())
            },
        )?;
    }
    Ok(())
}

#[test]
fn synchronous_cancellation_releases_waiter_and_keeps_runtime_usable() -> Result<()> {
    for backend in modes() {
        run(
            backend,
            Options {
                turn_timeout: Duration::from_secs(2),
                ..Default::default()
            },
            "return {value: ctx._sync('storage.read', {key:'value'})};",
            |app| {
                let runtime = &app.runtime;
                let active = Arc::new(AtomicBool::new(true));
                let (_, mut receive) = start(runtime, active.clone());
                let cancelled = event(&mut receive);
                active.store(false, Ordering::Release);
                drop(cancelled);
                let until = Instant::now() + Duration::from_secs(5);
                while !receive.is_closed() {
                    assert!(
                        Instant::now() < until,
                        "cancelled waiter retained authority"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
                assert!(receive.try_recv().is_err());
                let (invocation, mut receive) = start(runtime, Arc::new(AtomicBool::new(true)));
                let Event::Operation {
                    id, synchronous, ..
                } = event(&mut receive)
                else {
                    panic!("new invocation refused")
                };
                runtime.respond(
                    invocation,
                    id,
                    json!({"ok":true,"value":42}).to_string(),
                    synchronous,
                )?;
                let Event::Complete {
                    payload, failed, ..
                } = event(&mut receive)
                else {
                    panic!("completion missing")
                };
                assert!(!failed);
                assert_eq!(serde_json::from_str::<Value>(&payload)?["value"], 42);
                Ok(())
            },
        )?;
    }
    Ok(())
}

#[test]
fn synchronous_shutdown_joins_without_a_host_reply() -> Result<()> {
    for backend in modes() {
        let before = Instant::now();
        run(
            backend,
            Options {
                turn_timeout: Duration::from_secs(2),
                ..Default::default()
            },
            "ctx._sync('storage.apply', {puts:{x:1}}); return null;",
            |app| {
                let (_, mut receive) = start(&app.runtime, Arc::new(AtomicBool::new(true)));
                let Event::Operation { name, .. } = event(&mut receive) else {
                    panic!("submission missing")
                };
                assert_eq!(name, "storage.apply");
                // The native mutation may have committed. Bootstrap shutdown must
                // revoke/join the blocked language callback without replaying it.
                Ok(())
            },
        )?;
        assert!(before.elapsed() < Duration::from_secs(5));
    }
    Ok(())
}

#[test]
fn activation_facades_cannot_rebind_after_same_epoch_instance_reset() -> Result<()> {
    for backend in modes() {
        run(
            backend,
            Options {
                turn_timeout: Duration::from_secs(2),
                ..Default::default()
            },
            r#"
            if (!globalThis.savedActivation) { globalThis.savedActivation = ctx._activation(); return {first: true}; }
            let refused = false;
            try { globalThis.savedActivation(); } catch(error) { refused = error.code === 'ContextMismatch' && error.outcome === 'not_committed'; }
            const value = ctx._activation()()._sync('storage.read', {key:'value'});
            return {refused, value};
        "#,
            |app| {
                let runtime = &app.runtime;
                let first_owner = Arc::new(AtomicBool::new(true));
                let (_, mut receive) = start_scope(
                    runtime,
                    Arc::new(AtomicBool::new(true)),
                    first_owner.clone(),
                    Some(100),
                );
                let Event::Complete {
                    payload, failed, ..
                } = event(&mut receive)
                else {
                    panic!("initial facade made an effect")
                };
                assert!(!failed);
                assert_eq!(serde_json::from_str::<Value>(&payload)?["first"], true);
                first_owner.store(false, Ordering::Release);
                // A native reset can replace an instance on the same resident DB
                // and ownership epoch. Scope/epoch equality cannot authorize reuse.
                let (invocation, mut receive) = start_scope(
                    runtime,
                    Arc::new(AtomicBool::new(true)),
                    Arc::new(AtomicBool::new(true)),
                    Some(101),
                );
                let Event::Operation {
                    id,
                    name,
                    synchronous,
                    ..
                } = event(&mut receive)
                else {
                    panic!("fresh activation did not run")
                };
                assert_eq!(name, "storage.read");
                runtime.respond(
                    invocation,
                    id,
                    json!({"ok":true,"value":42}).to_string(),
                    synchronous,
                )?;
                let Event::Complete {
                    payload, failed, ..
                } = event(&mut receive)
                else {
                    panic!("retired facade made an effect")
                };
                assert!(!failed);
                assert_eq!(
                    serde_json::from_str::<Value>(&payload)?,
                    json!({"refused":true,"value":42})
                );
                Ok(())
            },
        )?;
    }
    Ok(())
}

#[test]
fn output_barriers_overlap_writes_without_admitting_another_mutation() -> Result<()> {
    let body = r#"
      const write = ctx.storage.apply({puts:{x:1}});
      const proofs = [ctx.storage.sync(), ctx.storage.sync()];
      const refused = ctx.storage.apply({puts:{y:2}}).then(
        () => false,
        error => error.code === 'ConcurrentMutation' && error.outcome === 'not_committed'
      );
      return Promise.all([write, ...proofs, refused]).then(values => ({refused:values[3]}));
    "#;
    for backend in modes() {
        run(backend, Options::default(), body, |app| {
            let (invocation, mut receive) = start(&app.runtime, Arc::new(AtomicBool::new(true)));
            let mut pending = Vec::new();
            for expected in ["storage.apply", "storage.sync", "storage.sync"] {
                let Event::Operation {
                    id,
                    name,
                    synchronous,
                    ..
                } = event(&mut receive)
                else {
                    anyhow::bail!("output barrier refused while a native write was pending");
                };
                assert_eq!(name, expected);
                pending.push((id, synchronous));
            }
            // Do not acknowledge either proof before the owning native write.
            for (id, synchronous) in pending {
                app.runtime.respond(
                    invocation,
                    id,
                    r#"{"ok":true,"value":null}"#.into(),
                    synchronous,
                )?;
            }
            let Event::Complete {
                payload, failed, ..
            } = event(&mut receive)
            else {
                anyhow::bail!("another mutation was admitted");
            };
            assert!(!failed);
            assert_eq!(
                serde_json::from_str::<Value>(&payload)?,
                json!({"refused":true})
            );
            Ok(())
        })?;
    }
    Ok(())
}

#[test]
fn pending_socket_output_allows_storage_but_not_overlapping_mutations() -> Result<()> {
    let body = r#"
      const output = [
        ctx._call('container.websocket.send_text', {id:'text',text:'held'}),
        ctx._call('container.websocket.send_binary', {id:'binary',data:'AA=='}),
        ctx._call('container.websocket.close', {id:'close',code:1000,reason:''})
      ];
      const write = ctx.storage.apply({puts:{received:1}});
      const refused = ctx.storage.apply({puts:{received:2}}).then(
        () => false,
        error => error.code === 'ConcurrentMutation' && error.outcome === 'not_committed'
      );
      return Promise.all([...output, write, refused]).then(values => ({refused:values[4]}));
    "#;
    for backend in modes() {
        run(backend, Options::default(), body, |app| {
            let (invocation, mut receive) = start(&app.runtime, Arc::new(AtomicBool::new(true)));
            let mut pending = Vec::new();
            for expected in [
                "container.websocket.send_text",
                "container.websocket.send_binary",
                "container.websocket.close",
                "storage.apply",
            ] {
                let Event::Operation {
                    id,
                    name,
                    synchronous,
                    ..
                } = event(&mut receive)
                else {
                    anyhow::bail!("storage refused while socket output was pending");
                };
                assert_eq!(name, expected);
                pending.push((id, synchronous));
            }
            for (id, synchronous) in pending {
                app.runtime.respond(
                    invocation,
                    id,
                    r#"{"ok":true,"value":null}"#.into(),
                    synchronous,
                )?;
            }
            let Event::Complete {
                payload, failed, ..
            } = event(&mut receive)
            else {
                anyhow::bail!("overlapping storage mutation was admitted");
            };
            assert!(!failed);
            assert_eq!(
                serde_json::from_str::<Value>(&payload)?,
                json!({"refused":true})
            );
            Ok(())
        })?;
    }
    Ok(())
}

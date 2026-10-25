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
                format!("export function fetch(req, ctx) {{ {body} }}"),
            )?;
            node::NodeRuntime::run(
                options,
                node::NodeOptions::new(entry),
                Source::module().events([EventKind::Fetch]),
                host,
            )
        }
        #[cfg(feature = "embed")]
        Backend::Embedded => JavaScriptRuntime::run(
            options,
            Source::new(
                format!("var CellarApp = {{fetch(req, ctx) {{ {body} }} }}"),
                "sync.js",
            )
            .events([EventKind::Fetch]),
            host,
        ),
    }
}

fn start(runtime: &Runtime, active: Life) -> (u64, channel::Receiver<Event>) {
    let id = runtime.id().unwrap();
    let (sender, receiver) = channel::channel(runtime.options.max_pending_operations + 1);
    let payload = json!({"kind":"fetch","event":{"url":"http://test/","method":"GET","headers":[],"body":""},"identity":null,"object":null,"class":null}).to_string();
    runtime
        .start(Start {
            id,
            _bytes: runtime.reserve(payload.len()).unwrap(),
            payload,
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

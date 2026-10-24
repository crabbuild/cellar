// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Runtime-only controls and ownership. Fleet tests separately execute the
//! generated business calls and verify late storage/commit refusal.
use super::*;
use serde_json::{json, Value};
use std::time::Instant;

const CODE: &str = r#"
let probe;
async function fetch(request, ctx) {
  if (request.url.endsWith('/begin')) {
    const signal = {
      aborted:false,
      addEventListener(_name, listener) { this.listener = listener; },
      removeEventListener(_name, listener) { if (this.listener === listener) this.listener = undefined; },
    };
    probe = {context:ctx, signal};
    try {
      await ctx[Symbol.for('cellar.rpc')].call({probe:'original'}, Uint8Array.of(42), signal);
      throw Error('unexpected success');
    } catch (error) {
      return {code:error.code, outcome:error.outcome, unsubscribed:!signal.listener};
    }
  }
  if (request.url.endsWith('/fire')) {
    let foreignRefused = false;
    try {
      await probe.context[Symbol.for('cellar.rpc')].call({probe:'forbidden'}, Uint8Array.of(43));
    } catch (error) {
      foreignRefused = error.code === 'ContextMismatch' && error.outcome === 'not_committed';
    }
    probe.signal.aborted = true;
    probe.signal.listener?.();
    probe.signal.listener?.();
    return {foreignRefused};
  }
  return {unsubscribed:!probe.signal.listener};
}
"#;

fn start(runtime: &Runtime, path: &str, active: Life) -> Result<(u64, channel::Receiver<Event>)> {
    let id = runtime.id()?;
    let (sender, receive) = channel::channel(runtime.options.max_pending_operations + 1);
    let payload = json!({"kind":"fetch","event":{"url":format!("http://test/{path}"),"method":"GET","headers":[],"body":""},"identity":null,"object":null,"class":null}).to_string();
    runtime.start(Start {
        budget_source: None,
        id,
        _bytes: runtime.reserve(payload.len())?,
        payload,
        body: None,
        active,
        owner: Arc::new(AtomicBool::new(true)),
        sender,
    })?;
    Ok((id, receive))
}

fn event(receive: &mut channel::Receiver<Event>) -> Result<Event> {
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        match receive.try_recv() {
            Ok(event) => return Ok(event),
            Err(channel::error::TryRecvError::Disconnected) => {
                anyhow::bail!("cancellation invocation closed")
            }
            Err(_) => ensure!(Instant::now() < until, "cancellation event stalled"),
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn completion(receive: &mut channel::Receiver<Event>) -> Result<Value> {
    let Event::Complete {
        payload, failed, ..
    } = event(receive)?
    else {
        anyhow::bail!("unexpected SDK operation from cancellation control");
    };
    ensure!(!failed, "cancellation control handler failed");
    Ok(serde_json::from_str(&payload)?)
}

fn drained(runtime: &Runtime) -> Result<()> {
    let until = Instant::now() + Duration::from_secs(2);
    while runtime.bytes.available_permits() != runtime.options.max_bridge_bytes {
        ensure!(
            Instant::now() < until,
            "cancellation retained a byte reservation"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}

fn controls(app: Arc<JavaScriptApplication>) -> Result<()> {
    let (id, mut receive) = start(&app.runtime, "begin", Arc::new(AtomicBool::new(true)))?;
    let Event::Operation {
        id: operation,
        signal: Some(signal),
        _bytes: reservation,
        ..
    } = event(&mut receive)?
    else {
        anyhow::bail!("RPC admission has no native signal");
    };
    ensure!(!signal.aborted());
    let (_, mut control) = start(&app.runtime, "fire", Arc::new(AtomicBool::new(true)))?;
    ensure!(completion(&mut control)?["foreignRefused"] == true);
    ensure!(
        signal.aborted(),
        "private control did not cancel its original native signal"
    );
    ensure!(
        receive.try_recv().is_err(),
        "cancellation bypassed the native acknowledgement"
    );
    ensure!(app.runtime.bytes.available_permits() < app.runtime.options.max_bridge_bytes);
    // The actual native owner retains this reservation until settling the call.
    drop(reservation);
    app.runtime.reply(
        id,
        operation,
        json!({"ok":false,"code":"Cancelled","outcome":"unknown"}).to_string(),
    )?;
    ensure!(
        completion(&mut receive)?
            == json!({"code":"Cancelled","outcome":"unknown","unsubscribed":true})
    );
    let (_, mut inspected) = start(&app.runtime, "inspect", Arc::new(AtomicBool::new(true)))?;
    ensure!(completion(&mut inspected)?["unsubscribed"] == true);
    drained(&app.runtime)
}

fn retirement(app: Arc<JavaScriptApplication>) -> Result<()> {
    let active = Arc::new(AtomicBool::new(true));
    let (_, mut receive) = start(&app.runtime, "begin", active.clone())?;
    let Event::Operation {
        _bytes: reservation,
        ..
    } = event(&mut receive)?
    else {
        anyhow::bail!("RPC did not begin before retirement");
    };
    active.store(false, Ordering::Release);
    drop(reservation);
    drained(&app.runtime)?;
    // Revocation removes native ownership first. Its queued JS catch/finally
    // continuation must also release the subscription within the pump bound.
    let until = Instant::now() + Duration::from_secs(2);
    loop {
        let (_, mut inspected) = start(&app.runtime, "inspect", Arc::new(AtomicBool::new(true)))?;
        if completion(&mut inspected)?["unsubscribed"] == true {
            break;
        }
        ensure!(
            Instant::now() < until,
            "retirement did not unsubscribe cancellation"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    drained(&app.runtime)
}

fn options() -> Options {
    Options {
        max_pending_operations: 1,
        max_operations: 1,
        ..Options::default()
    }
}

#[cfg(feature = "embed")]
#[test]
fn v8_cancellation_controls_bypass_full_operation_limits_and_wait_for_native_cleanup() -> Result<()>
{
    let source = Source::new(
        format!("var CellarApp = (() => {{ {CODE} return {{fetch}}; }})();"),
        "cancel.js",
    )
    .events([crate::EventKind::Fetch]);
    JavaScriptRuntime::run(options(), source.clone(), controls)?;
    JavaScriptRuntime::run(options(), source, retirement)
}

#[cfg(feature = "node")]
#[test]
fn node_cancellation_controls_bypass_full_operation_limits_and_wait_for_native_cleanup(
) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let entry = directory.path().join("cancel.mjs");
    std::fs::write(&entry, format!("{CODE}\nexport {{fetch}};"))?;
    let source = Source::module().events([crate::EventKind::Fetch]);
    node::NodeRuntime::run(
        options(),
        node::NodeOptions::new(&entry),
        source.clone(),
        controls,
    )?;
    node::NodeRuntime::run(
        options(),
        node::NodeOptions::new(&entry),
        source,
        retirement,
    )
}

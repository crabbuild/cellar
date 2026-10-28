// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Runtime-only attachment qualification. These identity codecs intentionally
//! isolate byte ownership/framing; real Protobuf/cell durability has separate tests.
use super::*;
#[cfg(feature = "embed")]
use anyhow::Context as _;
use cellar::sdk::rpc;
use serde_json::json;
use std::time::Instant;

fn source() -> Result<Source> {
    source_with_body(
        r#"
        const rpc = ctx[Symbol.for('cellar.rpc')];
        rpc.check();
        const bytes = Uint8Array.of(42, 0, 255, 128, 43);
        const pending = rpc.call({binding:'COUNTERS', name:'alice',
          method:'example.counter.v1.Counter/GetCount', contract_digest:'DIGEST'}, bytes.subarray(1,4));
        bytes.fill(99);
        const reply = await pending;
        return reply.subarray(1, reply.length - 1);
    "#,
    )
}

fn source_with_body(body: &str) -> Result<Source> {
    let contract = Arc::new(rpc::Contract::from_descriptor(
        include_bytes!("../../../tools/rpc/fixtures/counter.binpb"),
        rpc::Limits::default(),
    )?);
    let code = r#"
        class Counter {
          async getCount(input, ctx) {
            if (!(input instanceof Uint8Array) || input.length !== 0) throw Error('input');
            HANDLER_BODY
          }
          async increment(input) { return input; }
        }
        const cells = {Counter};
        const method = (handler, input, output) => Object.freeze({
          handler, input, output, digest:'DIGEST', decode: bytes => bytes, encode: bytes => bytes,
          // This transport-only fixture has identity codecs. Validate the
          // exact native inventory; real descriptor checks live in typed_rpc.
          verify(native) {
            if (JSON.stringify(native) !== JSON.stringify(EXPECTED_CONTRACT)) throw Error('contract');
          },
        });
        const rpc = {Counter: {methods: {
          'example.counter.v1.Counter/GetCount': method('getCount', 'example.counter.v1.GetCountRequest', 'example.counter.v1.GetCountResponse'),
          'example.counter.v1.Counter/Increment': method('increment', 'example.counter.v1.IncrementRequest', 'example.counter.v1.IncrementResponse'),
        }}};
    "#.replace("HANDLER_BODY", body).replace("DIGEST", contract.digest()).replace("EXPECTED_CONTRACT", &contract.inventory().to_string());
    Source::new(code, "attachment-test.js").cell_rpc(
        "Counter",
        "example.counter.v1.Counter",
        contract,
    )
}

#[cfg(feature = "embed")]
#[test]
fn v8_completion_reservation_survives_byte_saturation() -> Result<()> {
    let mut source = source_with_body(
        r#"
        this.calls = (this.calls ?? 0) + 1;
        await ctx.now();
        return new Uint8Array(this.calls === 1 ? 8192 : 1).fill(7);
    "#,
    )?;
    source.code = format!(
        "globalThis.CellarApp = (() => {{ {}\n return {{cells,rpc}}; }})();",
        source.code
    );
    let options = Options {
        max_bridge_bytes: 4 * 1024 * 1024,
        ..Options::default()
    };
    JavaScriptRuntime::run(options, source, |app| {
        let runtime = &app.runtime;
        let owner = Arc::new(AtomicBool::new(true));
        for index in 0..2 {
            let id = runtime.id()?;
            let payload = invocation_payload()?;
            let (sender, mut receive) =
                channel::channel(runtime.options.max_pending_operations + 1);
            runtime.start(Start {
                _completion: Default::default(),
                budget_source: None,
                id,
                _bytes: runtime.reserve(payload.len())?,
                payload,
                body: Some(vec![]),
                active: Arc::new(AtomicBool::new(true)),
                owner: owner.clone(),
                sender,
            })?;
            let Event::Operation {
                id: operation,
                name,
                ..
            } = event(&mut receive)?
            else {
                anyhow::bail!("reply fixture did not reach its clock barrier");
            };
            ensure!(name == "clock.now");
            let payload = json!({"ok":true,"value":42}).to_string();
            if index == 0 {
                // Admit the SDK reply but withhold delivery until every other
                // byte is held. Only its small charge can become available.
                let reply = Reply {
                    invocation: id,
                    id: operation,
                    _bytes: runtime.reserve(payload.len())?,
                    payload,
                    body: None,
                };
                let mut held = Vec::new();
                while runtime.bytes.available_permits() != 0 {
                    let bytes = runtime
                        .bytes
                        .available_permits()
                        .min(runtime.options.max_value_bytes);
                    held.push(runtime.reserve(bytes)?);
                }
                ensure!(runtime.usage().retained_bytes == runtime.options.max_bridge_bytes);
                runtime
                    .replies
                    .try_send(reply)
                    .map_err(|_| anyhow::anyhow!("clock reply refused"))?;
                let completed = event(&mut receive).with_context(|| {
                    format!(
                        "quota failure lost its completion; copied {} reply bytes",
                        runtime.completion_copied.load(Ordering::Relaxed)
                    )
                })?;
                let Event::Complete {
                    payload,
                    body,
                    failed,
                    ..
                } = completed
                else {
                    anyhow::bail!("unexpected work after reply saturation");
                };
                ensure!(failed && body.is_none());
                let failure = cellar_guest::rpc_failure::failure(&payload);
                ensure!(failure.code() == rpc::Code::ResourceExhausted);
                ensure!(failure.outcome() == rpc::EffectOutcome::Unknown);
                ensure!(runtime.completion_copied.load(Ordering::Relaxed) == 0);
                ensure!(
                    !runtime.usage().stopped,
                    "one refused reply stopped the engine"
                );
                drop(held);
            } else {
                runtime.reply(id, operation, payload)?;
                let Event::Complete { body, failed, .. } = event(&mut receive)? else {
                    anyhow::bail!("subsequent call did not complete");
                };
                ensure!(!failed && body.as_deref() == Some(&[7][..]));
            }
        }
        let until = Instant::now() + Duration::from_secs(2);
        while runtime.usage().retained_bytes != 0 {
            ensure!(Instant::now() < until, "completion reservations leaked");
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    })
}

// These runtime-only tests bypass native application dispatch, so supply the
// same private invocation policy that the native adapter normally attaches.
fn invocation_payload() -> Result<String> {
    let contract = rpc::Contract::from_descriptor(
        include_bytes!("../../../tools/rpc/fixtures/counter.binpb"),
        rpc::Limits::default(),
    )?;
    let limits = std::collections::BTreeMap::from([(contract.digest(), contract.limits())]);
    Ok(
        json!({"kind":"rpc", "event":{"method":"example.counter.v1.Counter/GetCount"},
        "identity":null, "object":1,"class":"Counter", "rpc_limits":limits})
        .to_string(),
    )
}

fn event(receiver: &mut channel::Receiver<Event>) -> Result<Event> {
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        match receiver.try_recv() {
            Ok(event) => return Ok(event),
            Err(channel::error::TryRecvError::Disconnected) => {
                anyhow::bail!("attachment invocation closed")
            }
            Err(_) => ensure!(Instant::now() < until, "attachment invocation stalled"),
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn roundtrip(app: Arc<JavaScriptApplication>) -> Result<()> {
    for length in [0, 4096, 65536] {
        let id = app.runtime.id()?;
        let (sender, mut receiver) =
            channel::channel(app.runtime.options.max_pending_operations + 1);
        let payload = invocation_payload()?;
        app.runtime.start(Start {
            _completion: Default::default(),
            budget_source: None,
            id,
            _bytes: app.runtime.reserve(payload.len())?,
            payload,
            body: Some(vec![]),
            active: Arc::new(AtomicBool::new(true)),
            owner: Arc::new(AtomicBool::new(true)),
            sender,
        })?;
        let Event::Operation {
            id: operation,
            name,
            body,
            ..
        } = event(&mut receiver)?
        else {
            anyhow::bail!("missing binary operation")
        };
        ensure!(
            name == "cell.call" && body == Some(vec![0, 255, 128]),
            "binary request slice changed after submission"
        );
        let expected: Vec<_> = (0..length).map(|index| index as u8).collect();
        let mut reply = vec![42];
        reply.extend_from_slice(&expected);
        reply.push(43);
        app.runtime.reply_parts(
            id,
            operation,
            (json!({"ok":true,"value":null}).to_string(), Some(reply)),
        )?;
        let Event::Complete { body, failed, .. } = event(&mut receiver)? else {
            anyhow::bail!("missing binary completion")
        };
        ensure!(
            !failed && body.as_deref() == Some(expected.as_slice()),
            "binary response slice changed"
        );
    }
    let until = Instant::now() + Duration::from_secs(2);
    while app.runtime.bytes.available_permits() != app.runtime.options.max_bridge_bytes {
        ensure!(Instant::now() < until, "attachment reservations leaked");
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}

#[cfg(feature = "node")]
#[test]
fn node_owns_exact_attachment_slices_and_empty_completions() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let entry = directory.path().join("app.mjs");
    let source = source()?;
    std::fs::write(&entry, format!("{}\nexport {{cells, rpc}};", source.code))?;
    node::NodeRuntime::run(
        Options::default(),
        node::NodeOptions::new(entry),
        source,
        roundtrip,
    )
}

#[cfg(feature = "node")]
#[test]
fn node_releases_delivered_invocation_buffers_while_native_work_remains() -> Result<()> {
    const LENGTH: usize = 65_536;
    let directory = tempfile::tempdir()?;
    let entry = directory.path().join("app.mjs");
    let mut source = source()?;
    source.code = source
        .code
        .replace("input.length !== 0", &format!("input.length !== {LENGTH}"));
    std::fs::write(&entry, format!("{}\nexport {{cells, rpc}};", source.code))?;
    node::NodeRuntime::run(
        Options::default(),
        node::NodeOptions::new(entry),
        source,
        |app| {
            let id = app.runtime.id()?;
            let (sender, mut receiver) =
                channel::channel(app.runtime.options.max_pending_operations + 1);
            let payload = invocation_payload()?;
            let bytes = app.runtime.reserve(payload.len() + LENGTH)?;
            app.runtime.start(Start {
                _completion: Default::default(),
                budget_source: None,
                id,
                payload,
                body: Some(vec![0; LENGTH]),
                active: Arc::new(AtomicBool::new(true)),
                owner: Arc::new(AtomicBool::new(true)),
                sender,
                _bytes: bytes,
            })?;
            let Event::Operation {
                id: operation,
                body,
                _bytes: held,
                ..
            } = event(&mut receiver)?
            else {
                anyhow::bail!("delivered invocation did not start its native operation");
            };
            ensure!(body.as_deref() == Some(&[0, 255, 128][..]));
            // The original native operation remains held. Only the already-delivered
            // 64 KiB invocation buffer may disappear from the native bridge charge.
            let until = Instant::now() + Duration::from_secs(2);
            while app.runtime.usage().retained_bytes >= LENGTH {
                ensure!(
                    Instant::now() < until,
                    "Node retained the delivered request allocation"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            ensure!(app.runtime.usage().retained_bytes >= held.num_permits());
            drop(held);
            app.runtime.reply_parts(
                id,
                operation,
                (
                    json!({"ok":true,"value":null}).to_string(),
                    Some(vec![42, 7, 8, 43]),
                ),
            )?;
            let Event::Complete { body, failed, .. } = event(&mut receiver)? else {
                anyhow::bail!("delivered invocation did not complete");
            };
            ensure!(!failed && body.as_deref() == Some(&[7, 8][..]));
            let until = Instant::now() + Duration::from_secs(2);
            while app.runtime.usage().retained_bytes != 0 {
                ensure!(
                    Instant::now() < until,
                    "Node frame reservations did not drain"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            Ok(())
        },
    )
}

#[cfg(all(feature = "node", unix))]
#[test]
fn node_refuses_insufficient_framing_capacity_before_process_start() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut node = node::NodeOptions::new(directory.path().join("app.mjs"));
    node.executable = directory.path().join("missing-node");
    let options = Options {
        max_value_bytes: 128,
        max_bridge_bytes: 128,
        ..Options::default()
    };
    let mut entered = false;
    let error = node::NodeRuntime::run(options, node, Source::new("", "byte-budget.js"), |_| {
        entered = true;
        Ok(())
    })
    .unwrap_err();
    ensure!(!entered);
    ensure!(
        error
            .to_string()
            .contains("Node bridge byte budget cannot fit one maximum framed value"),
        "unexpected startup refusal: {error:#}"
    );
    Ok(())
}

#[cfg(feature = "embed")]
#[test]
fn v8_owns_exact_attachment_slices_and_empty_completions() -> Result<()> {
    let mut source = source()?;
    source.code = format!(
        "globalThis.CellarApp = (() => {{ {}\n return {{cells,rpc}}; }})();",
        source.code
    );
    JavaScriptRuntime::run(Options::default(), source, roundtrip)
}

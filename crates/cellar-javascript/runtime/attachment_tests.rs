//! Runtime-only attachment qualification. These identity codecs intentionally
//! isolate byte ownership/framing; real Protobuf/cell durability has separate tests.
use super::*;
use cellar::sdk::rpc;
use serde_json::json;
use std::time::Instant;

fn source() -> Result<Source> {
    let contract = Arc::new(rpc::Contract::from_descriptor(
        include_bytes!("../../../tools/rpc/fixtures/counter.binpb"),
        rpc::Limits::default(),
    )?);
    let code = r#"
        class Counter {
          async getCount(input, ctx) {
            if (!(input instanceof Uint8Array) || input.length !== 0) throw Error('input');
            const rpc = ctx[Symbol.for('cellar.rpc')];
            rpc.check();
            const bytes = Uint8Array.of(42, 0, 255, 128, 43);
            const pending = rpc.call({binding:'COUNTERS', name:'alice',
              method:'example.counter.v1.Counter/GetCount', contract_digest:'DIGEST'}, bytes.subarray(1,4));
            bytes.fill(99);
            const reply = await pending;
            return reply.subarray(1, reply.length - 1);
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
    "#.replace("DIGEST", contract.digest()).replace("EXPECTED_CONTRACT", &contract.inventory().to_string());
    Source::new(code, "attachment-test.js").cell_rpc(
        "Counter",
        "example.counter.v1.Counter",
        contract,
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
        let payload = json!({"kind":"rpc", "event":{"method":"example.counter.v1.Counter/GetCount"}, "identity":null, "object":1,"class":"Counter"}).to_string();
        app.runtime.start(Start {
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

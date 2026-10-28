// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exercise retained backing at the actual source gate and routing queue.
use super::*;
use bytes::Bytes;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::Poll,
};

struct Backing {
    bytes: Vec<u8>,
    dropped: Arc<AtomicBool>,
}

impl AsRef<[u8]> for Backing {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for Backing {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

fn spare(text: &str) -> String {
    let mut owned = String::with_capacity(1024 * 1024);
    owned.push_str(text);
    owned
}

#[tokio::test]
async fn source_gate_releases_unknown_backing_and_aliases_keep_admission() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let services = domain.services();
    services.configure_runtime(crate::runtime_policy::RuntimePolicy {
        max_outbound_rpc_requests: 1,
        max_outbound_rpc_bytes: 128 * 1024,
        ..Default::default()
    })?;
    let alias = domain
        .run(async {
            let (sender, mut queue) = tokio::sync::mpsc::unbounded_channel();
            crate::host_channels::set_rpc_call_tx(sender);
            let (sender, mut gates) = tokio::sync::mpsc::unbounded_channel();
            crate::host_channels::set_gate_tx(sender);
            let dropped = Arc::new(AtomicBool::new(false));
            let view = Bytes::from_owner(Backing {
                bytes: vec![42; 8 * 1024 * 1024],
                dropped: dropped.clone(),
            })
            .slice(..1);
            let mut calling = Box::pin(call(
                EgressGate::Wrote("source".into(), cellar_logic::Channel::Response, 1, None),
                "Counter:alice".into(),
                Some("alice".into()),
                "method".into(),
                RpcData::Structured(view),
                None,
            ));
            std::future::poll_fn(|cx| {
                assert!(calling.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            let gate = gates.try_recv()?;
            assert!(queue.is_empty());
            assert!(
                dropped.load(Ordering::SeqCst),
                "one-byte source gate pinned 8 MiB of unknown backing"
            );
            let held = services.outbound_rpc_usage();
            assert_eq!(held.active_requests, 1);
            assert!(held.reserved_request_bytes <= 128 * 1024);
            assert!(gate.reply.send(Ok(())).is_ok());
            std::future::poll_fn(|cx| {
                assert!(calling.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            let queued = queue.try_recv()?;
            let RpcData::Structured(bytes) = &queued.args else {
                panic!("structured source changed flavor");
            };
            assert_eq!(bytes.as_ref(), [42]);
            let alias = bytes.slice(..1);
            drop(calling);
            drop(queued);
            assert_eq!(
                services.outbound_rpc_usage(),
                held,
                "a surviving byte alias lost admission"
            );
            Ok::<_, anyhow::Error>(alias)
        })
        .await??;
    domain.shutdown().await;
    assert_eq!(services.outbound_rpc_usage().active_requests, 1);
    std::thread::spawn(move || drop(alias)).join().unwrap();
    assert_eq!(services.outbound_rpc_usage(), Default::default());
    Ok(())
}

#[tokio::test]
async fn queued_json_and_routing_strings_do_not_keep_spare_capacity() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let services = domain.services();
    services.configure_runtime(crate::runtime_policy::RuntimePolicy {
        max_outbound_rpc_bytes: 128 * 1024,
        ..Default::default()
    })?;
    domain
        .run(async {
            let (sender, mut queue) = tokio::sync::mpsc::unbounded_channel();
            crate::host_channels::set_rpc_call_tx(sender);
            let mut calling = Box::pin(call(
                EgressGate::NoCell,
                spare("Counter:alice"),
                Some(spare("alice")),
                spare("method"),
                RpcData::Json(spare("[\"é\",\"🙂\",\"\u{feff}\"]")),
                None,
            ));
            std::future::poll_fn(|cx| {
                assert!(calling.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            let queued = queue.try_recv()?;
            let RpcData::Json(text) = &queued.args else {
                panic!("JSON source changed flavor");
            };
            assert_eq!(text, "[\"é\",\"🙂\",\"\u{feff}\"]");
            for (kind, value) in [
                ("scope", &queued.scope),
                ("name", queued.name.as_ref().unwrap()),
                ("method", &queued.method),
                ("JSON", text),
            ] {
                assert!(
                    value.capacity() <= value.len() * 2,
                    "queued {kind} retained {} capacity for {} encoded bytes",
                    value.capacity(),
                    value.len()
                );
            }
            assert!(services.outbound_rpc_usage().reserved_request_bytes <= 128 * 1024);
            drop((calling, queued));
            assert_eq!(services.outbound_rpc_usage(), Default::default());
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    domain.shutdown().await;
    Ok(())
}

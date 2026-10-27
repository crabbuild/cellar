// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{framing, receiver};
use anyhow::{ensure, Context, Result};
use cellar::sdk::rpc::{self, prost::Message};
use serde_json::{json, Value};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{watch, Notify},
    task::{JoinHandle, JoinSet},
};

const METHOD: &str = "example.counter.v1.Counter/Increment";
const METHOD_HEADER: &str = "x-cells-rpc-method";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Fault {
    Reconnect,
    ReplyLoss,
    MethodRefusal,
    ElementRefusal,
    ByteRefusal,
    DeclaredReplyOverflow,
    StreamedReplyOverflow,
}

impl Fault {
    pub fn name(self) -> &'static str {
        match self {
            Self::Reconnect => "reconnect",
            Self::ReplyLoss => "reply_loss",
            Self::MethodRefusal => "method_refusal",
            Self::ElementRefusal => "element_refusal",
            Self::ByteRefusal => "byte_refusal",
            Self::DeclaredReplyOverflow => "declared_reply_overflow",
            Self::StreamedReplyOverflow => "streamed_reply_overflow",
        }
    }

    pub fn refusal_code(self) -> Option<rpc::Code> {
        match self {
            Self::MethodRefusal => Some(rpc::Code::MethodNotFound),
            Self::ElementRefusal | Self::ByteRefusal => Some(rpc::Code::ResourceExhausted),
            _ => None,
        }
    }

    pub fn reply_overflow(self) -> bool {
        matches!(
            self,
            Self::DeclaredReplyOverflow | Self::StreamedReplyOverflow
        )
    }
}

#[derive(Clone)]
struct Arm {
    fault: Fault,
    class: String,
    id: String,
    expected: i64,
}

struct Control {
    contract: Arc<rpc::Contract>,
    destination: Mutex<Option<SocketAddr>>,
    arm: Mutex<Option<Arm>>,
    observations: Mutex<Vec<Value>>,
    last_request: Mutex<Option<(String, String)>>,
    handshakes: AtomicUsize,
    live: AtomicUsize,
    close_idle: Notify,
}

pub(super) struct Relay {
    pub address: SocketAddr,
    control: Arc<Control>,
    stop: watch::Sender<bool>,
    driver: JoinHandle<Result<()>>,
}

impl Relay {
    pub async fn start(contract: Arc<rpc::Contract>) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let control = Arc::new(Control {
            contract,
            destination: Mutex::default(),
            arm: Mutex::default(),
            observations: Mutex::default(),
            last_request: Mutex::default(),
            handshakes: AtomicUsize::new(0),
            live: AtomicUsize::new(0),
            close_idle: Notify::new(),
        });
        let serving = control.clone();
        let (stop, mut stopped) = watch::channel(false);
        let driver = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            let result = async {
                loop {
                    tokio::select! { biased;
                        _ = stopped.changed() => break,
                        joined = tasks.join_next(), if !tasks.is_empty() => { joined.context("relay task missing")???; },
                        accepted = listener.accept() => {
                            let (stream, _) = accepted?;
                            ensure!(tasks.len() < 128, "relay connection bound exceeded");
                            let control = serving.clone();
                            let mut closing = stopped.clone();
                            tasks.spawn(async move {
                                let result = tokio::select! { biased;
                                    _ = closing.changed() => Ok(()),
                                    result = connection(stream, control) => result,
                                };
                                match result {
                                    Err(error) if error.downcast_ref::<std::io::Error>().is_some_and(|io| matches!(io.kind(), std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe)) => Ok(()),
                                    result => result,
                                }
                            });
                        }
                    }
                }
                Ok::<_, anyhow::Error>(())
            }.await;
            // Every connection is owned by this join set, including failure
            // paths. Stop polling sockets before joining all of their tasks.
            tasks.abort_all();
            let mut failure = result.err();
            while let Some(joined) = tasks.join_next().await {
                let completed = match joined {
                    Ok(result) => result,
                    Err(error) if error.is_cancelled() => Ok(()),
                    Err(error) => Err(error.into()),
                };
                if let Err(error) = completed {
                    failure.get_or_insert(error);
                }
            }
            failure.map_or(Ok(()), Err)
        });
        Ok(Self {
            address,
            control,
            stop,
            driver,
        })
    }

    pub fn destination(&self, address: SocketAddr) {
        *self.control.destination.lock().unwrap() = Some(address);
    }

    pub fn arm(&self, fault: Fault, class: &str, id: &str, expected: i64) -> Result<()> {
        let mut arm = self.control.arm.lock().unwrap();
        ensure!(arm.is_none(), "relay already armed");
        ensure!(
            self.control.observations.lock().unwrap().is_empty(),
            "unconsumed relay proof"
        );
        *self.control.last_request.lock().unwrap() = None;
        *arm = Some(Arm {
            fault,
            class: class.into(),
            id: id.into(),
            expected,
        });
        Ok(())
    }

    pub fn observed(&self) -> Result<Value> {
        let last = self.control.last_request.lock().unwrap().clone();
        self.control
            .observations
            .lock()
            .unwrap()
            .pop()
            .with_context(|| {
                format!("real peer request/reply was not observed; last request: {last:?}")
            })
    }

    pub fn handshakes(&self) -> usize {
        self.control.handshakes.load(Ordering::Acquire)
    }

    pub async fn disconnect_idle(&self) -> Result<()> {
        ensure!(
            self.control.live.load(Ordering::Acquire) > 0,
            "reconnect fixture has no warm tunnel"
        );
        self.control.close_idle.notify_waiters();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while self.control.live.load(Ordering::Acquire) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("idle RPC tunnels did not close")?;
        Ok(())
    }

    pub async fn stop(self) -> Result<()> {
        self.stop.send_replace(true);
        self.driver.await??;
        ensure!(
            self.control.live.load(Ordering::Acquire) == 0,
            "relay retained RPC connections after join"
        );
        ensure!(
            self.control.arm.lock().unwrap().is_none(),
            "relay fault was never exercised"
        );
        ensure!(
            self.control.observations.lock().unwrap().is_empty(),
            "relay evidence was never consumed"
        );
        Ok(())
    }
}

async fn connection(stream: TcpStream, control: Arc<Control>) -> Result<()> {
    let destination = control
        .destination
        .lock()
        .unwrap()
        .context("relay owner address not installed")?;
    let mut incoming = BufReader::new(stream);
    let Some(mut outer) = framing::head(&mut incoming).await? else {
        return Ok(());
    };
    let mut owner = TcpStream::connect(destination).await?;
    owner.set_nodelay(true)?;
    // An ordinary keepalive could later carry an RPC upgrade and bypass the
    // observer. Ask the real server to close only ordinary outer HTTP sessions.
    // This hop-by-hop header is outside the signed canonical request; upgraded
    // tunnels, authentication, payloads and proof replies stay unchanged.
    if outer.get("x-cells-tunnel-kind").is_none() {
        outer.close_connection()?;
    }
    owner.write_all(&outer.raw).await?;
    if outer.get("x-cells-tunnel-kind") != Some("rpc") {
        let _ = tokio::io::copy_bidirectional(&mut incoming, &mut owner).await;
        return Ok(());
    }
    ensure!(
        incoming.buffer().is_empty(),
        "RPC upgrade unexpectedly pipelined bytes before authentication"
    );
    let (client_read, mut client_write) = incoming.into_inner().into_split();
    let (owner_read, mut owner_write) = owner.into_split();
    let mut client_read = BufReader::new(client_read);
    let mut owner_read = BufReader::new(owner_read);
    let action = Arc::new(Mutex::new(None::<(Arm, Value)>));
    let sending = action.clone();
    let send = async {
        while let Some(mut head) = framing::head(&mut client_read).await? {
            let mut body = framing::body(&head, &mut client_read).await?;
            let candidate = control.arm.lock().unwrap().clone();
            if candidate.is_some() {
                *control.last_request.lock().unwrap() = Some((
                    head.get(METHOD_HEADER).unwrap_or("").into(),
                    head.get("x-cells-scope").unwrap_or("").into(),
                ));
            }
            if let Some(arm) = candidate.filter(|arm| {
                head.get(METHOD_HEADER) == Some(METHOD)
                    && head
                        .get("x-cells-scope")
                        .is_some_and(|scope| scope.starts_with(&format!("{}:", arm.class)))
            }) {
                let (digest, bytes) = rpc::decode_envelope(
                    &cellar::engine_api::RpcData::Structured(body.clone().into()),
                    framing::MAX_BODY,
                )?;
                ensure!(
                    digest == control.contract.digest(),
                    "peer request used another descriptor"
                );
                let request = crate::generated::IncrementRequest::decode(bytes.as_slice())?;
                ensure!(
                    request.operation_id == arm.id,
                    "relay observed operation {} instead of {}",
                    request.operation_id,
                    arm.id
                );
                ensure!(
                    request.delta == Some(1),
                    "relay observed another business command"
                );
                let identity = head
                    .get("x-cells-request-id")
                    .context("peer request has no call identity")?;
                ensure!(
                    identity.len() == 32 && identity.bytes().all(|b| b.is_ascii_hexdigit()),
                    "invalid peer request identity"
                );
                let remaining: u64 = head
                    .get("x-cells-rpc-budget-ms")
                    .context("peer request has no remaining budget")?
                    .parse()?;
                ensure!(
                    remaining > 0 && remaining <= 10_000,
                    "peer call has an invalid remaining budget"
                );
                let mut proof = json!({"operation_id":arm.id,"request_id":identity,"scope":head.get("x-cells-scope"),"method":METHOD,"remaining_ms":remaining,"delta":1,
                    "original_message_bytes":bytes.len(),"source_max_message_bytes":control.contract.limits().max_message_bytes,
                    "source_max_elements":control.contract.limits().max_elements,
                    "receiver_max_message_bytes":receiver::BYTE_LIMIT,"receiver_max_elements":receiver::ELEMENT_LIMIT});
                let mut forwarded = bytes;
                let elements = match arm.fault {
                    Fault::ElementRefusal => {
                        // Three valid unknown varints make five occurrences.
                        // The unchanged request remains below the byte ceiling.
                        forwarded.extend_from_slice(&[0xf8, 0x07, 0].repeat(3));
                        5
                    }
                    Fault::ByteRefusal => {
                        // One valid unknown length-delimited field exceeds the
                        // byte ceiling while remaining below the element cap.
                        forwarded.extend_from_slice(&[0xfa, 0x07, 0x80, 0x01]);
                        forwarded.extend_from_slice(&[0; 128]);
                        3
                    }
                    _ => 2,
                };
                control
                    .contract
                    .validate_message("example.counter.v1.IncrementRequest", &forwarded)?;
                proof["forwarded_message_bytes"] = json!(forwarded.len());
                proof["forwarded_elements"] = json!(elements);
                proof["source_message_valid"] = json!(true);
                if matches!(arm.fault, Fault::ElementRefusal | Fault::ByteRefusal) {
                    let cellar::engine_api::RpcData::Structured(encoded) =
                        rpc::encode_envelope(&digest, forwarded, framing::MAX_BODY)?
                    else {
                        anyhow::bail!("binary fault envelope was not structured");
                    };
                    body = encoded.to_vec();
                    head.replace("content-length", &body.len().to_string())?;
                }
                if arm.fault == Fault::MethodRefusal {
                    head.replace(METHOD_HEADER, "example.counter.v1.Counter/PrivateHelper")?;
                }
                control.arm.lock().unwrap().take();
                ensure!(
                    sending.lock().unwrap().replace((arm, proof)).is_none(),
                    "overlapping relay actions"
                );
            }
            framing::write(&mut owner_write, &head, &body).await?;
        }
        Ok::<_, anyhow::Error>(())
    };
    let receive = async {
        let upgrade = framing::head(&mut owner_read)
            .await?
            .context("signed owner did not answer its upgrade")?;
        ensure!(
            upgrade.first.starts_with("HTTP/1.1 101 "),
            "signed RPC upgrade was refused: {}",
            upgrade.first
        );
        framing::write(&mut client_write, &upgrade, &[]).await?;
        control.handshakes.fetch_add(1, Ordering::AcqRel);
        control.live.fetch_add(1, Ordering::AcqRel);

        struct Live<'a>(&'a AtomicUsize);

        impl Drop for Live<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::AcqRel);
            }
        }
        let _live = Live(&control.live);
        while let Some(mut head) = framing::head(&mut owner_read).await? {
            let body = framing::body(&head, &mut owner_read).await?;
            let selected = action.lock().unwrap().take();
            if let Some((arm, mut proof)) = selected {
                ensure!(
                    head.first.starts_with("HTTP/1.1 200 "),
                    "owner returned a transport error"
                );
                let reply = rpc::decode_reply(
                    control.contract.digest(),
                    &cellar::engine_api::RpcData::Structured(body.clone().into()),
                    rpc::Limits::default(),
                );
                if let Some(code) = arm.fault.refusal_code() {
                    let Err(failure) = reply else {
                        anyhow::bail!("unregistered method returned a business result");
                    };
                    ensure!(
                        failure.code() == code
                            && failure.outcome() == rpc::EffectOutcome::NotCommitted,
                        "owner refusal has wrong provenance: {failure}"
                    );
                    proof["trusted_refusal"] = json!(true);
                } else {
                    let response = crate::generated::IncrementResponse::decode(reply?.as_slice())?;
                    ensure!(
                        matches!(response.result, Some(crate::generated::increment_response::Result::Count(value)) if value.value == arm.expected),
                        "owner reply differs from its exact committed result"
                    );
                    proof["successful_owner_reply_observed"] = json!(true);
                }
                proof["reply_lost"] = json!(arm.fault == Fault::ReplyLoss);
                let reply_limit = control.contract.limits().max_message_bytes + 2 * 1024;
                if arm.fault.reply_overflow() {
                    // Preserve the real committed owner result above, then
                    // withhold a replacement response's end. Correct native
                    // collection must refuse the caller's declared byte limit
                    // before waiting for completion or decoding business data.
                    proof["response_byte_limit"] = json!(reply_limit);
                    proof["reply_suffix_withheld"] = json!(true);
                    proof["declared_reply_bytes"] =
                        json!(if arm.fault == Fault::DeclaredReplyOverflow {
                            reply_limit + 1
                        } else {
                            0
                        });
                    proof["streamed_reply_bytes"] =
                        json!(if arm.fault == Fault::StreamedReplyOverflow {
                            reply_limit + 1
                        } else {
                            0
                        });
                }
                {
                    let mut observations = control.observations.lock().unwrap();
                    ensure!(observations.is_empty(), "relay proof bound exceeded");
                    observations.push(proof);
                }
                if arm.fault == Fault::ReplyLoss {
                    return Ok::<_, anyhow::Error>(());
                }
                if arm.fault.reply_overflow() {
                    if arm.fault == Fault::DeclaredReplyOverflow {
                        head.replace("content-length", &(reply_limit + 1).to_string())?;
                        client_write.write_all(&head.raw).await?;
                    } else {
                        head.chunked()?;
                        client_write.write_all(&head.raw).await?;
                        let fragment = [0; 4096];
                        let mut remaining = reply_limit + 1;
                        while remaining != 0 {
                            let count = remaining.min(fragment.len());
                            client_write
                                .write_all(format!("{count:x}\r\n").as_bytes())
                                .await?;
                            client_write.write_all(&fragment[..count]).await?;
                            client_write.write_all(b"\r\n").await?;
                            remaining -= count;
                        }
                    }
                    // Owned by the connection's select and the relay join set.
                    // Never send the declared body or the final chunk marker.
                    return std::future::pending().await;
                }
            }
            framing::write(&mut client_write, &head, &body).await?;
        }
        Ok(())
    };
    tokio::select! { biased;
        _ = control.close_idle.notified() => Ok(()),
        result = send => result,
        result = receive => result,
    }
}

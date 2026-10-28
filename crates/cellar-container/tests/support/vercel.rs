// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Published Vercel named-sandbox/session protocol peer; executes no VM.
#![allow(dead_code, clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use futures_util::StreamExt;
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full, StreamBody};
use hyper::{
    body::{Bytes, Frame, Incoming},
    Request, Response,
};
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot, Notify},
    task::JoinHandle,
};

type Body = UnsyncBoxBody<Bytes, Infallible>;

struct File {
    bytes: Vec<u8>,
    mode: u32,
}

struct Process {
    session: String,
    info: Value,
    send: mpsc::UnboundedSender<Vec<u8>>,
    fault: usize,
}

struct Streaming(Arc<AtomicUsize>);

impl Drop for Streaming {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Default)]
pub struct State {
    pub unavailable: AtomicBool,
    pub refused: AtomicUsize,
    pub creates: AtomicUsize,
    pub deletes: AtomicUsize,
    pub commands: AtomicUsize,
    pub lost_create: AtomicBool,
    pub hold_create: AtomicBool,
    pub create_entered: Notify,
    pub create_release: Notify,
    pub lost_delete: AtomicBool,
    pub hold_delete: AtomicBool,
    pub delete_entered: Notify,
    pub delete_release: Notify,
    pub defer_delete: AtomicBool,
    pub unknown_status: AtomicBool,
    pub hide_list: AtomicBool,
    pub mismatched_metadata: AtomicBool,
    pub changed_session: AtomicBool,
    pub redirect: AtomicBool,
    pub command_fault: AtomicUsize,
    pub hold_command: AtomicBool,
    pub command_entered: Notify,
    pub command_release: Notify,
    pub lost_stdin: AtomicBool,
    pub streams: Arc<AtomicUsize>,
    pub requests: Mutex<Vec<(String, String)>>,
    redirect_target: String,
    sandboxes: Mutex<HashMap<String, Value>>,
    processes: Mutex<HashMap<String, Process>>,
    files: Mutex<HashMap<(String, String), File>>,
}

pub struct Peer {
    pub state: Arc<State>,
    pub address: String,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl Peer {
    pub async fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = format!("http://{}/", listener.local_addr()?);
        let state = Arc::new(State {
            redirect_target: format!("{address}credential-sink"),
            ..Default::default()
        });
        let server = state.clone();
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                cellar::asyncrt::select! {
                    _=&mut stopped=>break,
                    accepted=listener.accept()=>{let Ok((stream,_))=accepted else{break;};let state=server.clone();tasks.spawn(async move {
                        let service=hyper::service::service_fn(move|request|{let state=state.clone();async move {
                            Ok::<_,Infallible>(match state.answer(request).await{Ok(reply)=>reply,Err(error)=>reply(500,error.to_string().into_bytes())})
                        }});
                        let _=hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream),service).await;
                    });}
                }
            }
            tasks.shutdown().await;
        });
        Ok(Self {
            state,
            address,
            stop: Some(stop),
            task: Some(task),
        })
    }

    pub fn provider(&self) -> Result<Arc<dyn cellar_container::Provider>> {
        self.provider_binding("container")
    }

    pub fn provider_binding(&self, binding: &str) -> Result<Arc<dyn cellar_container::Provider>> {
        use cellar_container::vercel::{Config, Ownership, Vercel};
        Ok(Arc::new(Vercel::new(
            Config::new(
                "fixture-team",
                "fixture-project",
                "fixture-fleet",
                "control-key",
                "fixture-image",
                Ownership::LocalChecksAndExpiry,
            )?
            .binding(binding)?
            .api_endpoint(&self.address)?
            .lifetime_ms(60_000)?
            .request_timeout_ms(2000)?,
        )))
    }

    pub async fn stop(mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.await?;
        }
        Ok(())
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn reply(status: u16, bytes: Vec<u8>) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(bytes)).boxed_unsync())
        .unwrap()
}

fn value(status: u16, value: Value) -> Response<Body> {
    reply(status, serde_json::to_vec(&value).unwrap())
}

fn line(value: &Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(value).unwrap();
    bytes.push(b'\n');
    bytes
}

fn helper(value: &Value) -> Vec<u8> {
    line(&json!({"stream":"stdout","data":String::from_utf8(line(value)).unwrap()}))
}

fn metadata(path: &str, file: &File) -> Value {
    json!({"name":path.rsplit('/').next().unwrap(),"kind":"file","size":file.bytes.len(),"mode":file.mode,"modified_ms":null,"link_target":null})
}

impl State {
    pub fn set_running(&self, running: bool) {
        for info in self.sandboxes.lock().unwrap().values_mut() {
            let state = json!(if running { "running" } else { "stopped" });
            info["sandbox"]["status"] = state.clone();
            info["session"]["status"] = state;
        }
    }

    /// Provider lifetime elapses independently of any Cellar invocation.
    pub fn expire(&self) {
        self.sandboxes.lock().unwrap().clear();
        self.files.lock().unwrap().clear();
    }

    async fn answer(&self, request: Request<Incoming>) -> Result<Response<Body>> {
        let method = request.method().as_str().to_owned();
        let path = request.uri().path().to_owned();
        let query = request.uri().query().unwrap_or("").to_owned();
        let headers = request.headers().clone();
        let bytes = request.into_body().collect().await?.to_bytes();
        self.requests
            .lock()
            .unwrap()
            .push((method.clone(), format!("{path}?{query}")));
        ensure!(
            headers.get("authorization").and_then(|v| v.to_str().ok())
                == Some("Bearer control-key")
        );
        let query: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect();
        ensure!(query.get("teamId").map(String::as_str) == Some("fixture-team"));
        if self.unavailable.load(Ordering::SeqCst) {
            self.refused.fetch_add(1, Ordering::SeqCst);
            return Ok(reply(503, b"provider API unavailable".to_vec()));
        }
        if self.redirect.swap(false, Ordering::SeqCst) {
            return Ok(Response::builder()
                .status(302)
                .header("location", &self.redirect_target)
                .body(Full::new(Bytes::new()).boxed_unsync())?);
        }
        if path == "/v3/sandboxes" && method == "POST" {
            let input: Value = serde_json::from_slice(&bytes)?;
            ensure!(
                input["projectId"] == "fixture-project"
                    && input["image"] == "fixture-image"
                    && input["persistent"] == false
                    && input["timeout"] == 60_000
                    && input["ports"] == json!([])
                    && input["resources"]["vcpus"] == 1
                    && input["networkPolicy"]["mode"] == "deny-all"
                    && input["env"]["HELLO"] == "world"
            );
            ensure!(
                input.get("runtime").is_none()
                    && input["tags"]["cellar"].as_str().unwrap().len() == 64
            );
            let name = input["name"].as_str().unwrap().to_owned();
            let n = self.creates.fetch_add(1, Ordering::SeqCst) + 1;
            let id = format!("sbx_{n}");
            let info = json!({"sandbox":{"name":name,"tags":input["tags"],"persistent":false,"currentSessionId":id,"status":"running"},"session":{"id":id,"status":"running"},"routes":[]});
            ensure!(self
                .sandboxes
                .lock()
                .unwrap()
                .insert(name, info.clone())
                .is_none());
            if self.hold_create.swap(false, Ordering::SeqCst) {
                self.create_entered.notify_one();
                self.create_release.notified().await;
            }
            if self.lost_create.swap(false, Ordering::SeqCst) {
                return Ok(reply(503, b"lost response after allocation".to_vec()));
            }
            return Ok(value(200, info));
        }
        if let Some(rest) = path.strip_prefix("/v2/sandboxes/sessions/") {
            let parts = rest.split('/').collect::<Vec<_>>();
            let session = parts[0].to_owned();
            ensure!(self
                .sandboxes
                .lock()
                .unwrap()
                .values()
                .any(|info| info["session"]["id"] == session));
            if parts.as_slice().get(1) == Some(&"cmd") && parts.len() == 2 {
                let input: Value = serde_json::from_slice(&bytes)?;
                ensure!(
                    input["wait"] == true
                        && input["logs"] == true
                        && input["attachStdin"] == true
                        && input["sudo"] == false
                        && input["timeout"] == 2000
                        && input["command"] == "/usr/bin/python3"
                        && input["args"][0] == "-I"
                        && input["args"][1] == "-c"
                );
                ensure!(input["args"][2]
                    .as_str()
                    .unwrap()
                    .contains("Binary-safe RPC helper"));
                let n = self.commands.fetch_add(1, Ordering::SeqCst) + 1;
                let id = format!("cmd_{n}");
                let info = json!({"id":id,"sessionId":session,"name":input["command"],"args":input["args"],"exitCode":null});
                let fault = self.command_fault.swap(0, Ordering::SeqCst);
                let (send, receive) = mpsc::unbounded_channel();
                send.send(line(&json!({"command":info})))?;
                self.processes.lock().unwrap().insert(
                    id,
                    Process {
                        session,
                        info,
                        send,
                        fault,
                    },
                );
                self.streams.fetch_add(1, Ordering::SeqCst);
                let guard = Streaming(self.streams.clone());
                let stream = futures_util::stream::unfold(receive, |mut receive| async move {
                    receive.recv().await.map(|bytes| {
                        (
                            Ok::<_, Infallible>(Frame::data(Bytes::from(bytes))),
                            receive,
                        )
                    })
                })
                .map(move |item| {
                    let _ = &guard;
                    item
                });
                return Ok(Response::builder()
                    .header("content-type", "application/x-ndjson")
                    .body(StreamBody::new(stream).boxed_unsync())?);
            }
            if parts.len() == 4 && parts[1] == "cmd" && parts[3] == "stdin" {
                let body: Value = serde_json::from_slice(&bytes)?;
                ensure!(body["offset"] == 0 && body["close"] == true);
                let input = STANDARD.decode(body["data"].as_str().unwrap())?;
                let request: Value = serde_json::from_slice(&input)?;
                ensure!(request["format"] == 1);
                let process = self.processes.lock().unwrap().remove(parts[2]).unwrap();
                ensure!(process.session == session);
                if process.fault == 4 {
                    self.processes
                        .lock()
                        .unwrap()
                        .insert(parts[2].into(), process);
                    return Ok(value(200, json!({"bytesWritten":input.len()})));
                }
                let mut frames = Vec::new();
                match request["operation"].as_str().unwrap() {
                    "exec" => {
                        frames.push(helper(&json!({"stream":"stdout","data":"AP8q"})));
                        frames.push(helper(
                            &json!({"stream":"stderr","data":STANDARD.encode(b"diagnostic")}),
                        ));
                        frames.push(helper(&json!({"end":7})));
                    }
                    "write" => {
                        let path = request["path"].as_str().unwrap();
                        self.files.lock().unwrap().insert(
                            (session.clone(), path.into()),
                            File {
                                bytes: STANDARD.decode(request["data"].as_str().unwrap())?,
                                mode: request["mode"].as_u64().unwrap() as u32,
                            },
                        );
                        frames.push(helper(&json!({"result":null})));
                        frames.push(helper(&json!({"end":0})));
                    }
                    "stat" => {
                        let path = request["path"].as_str().unwrap();
                        let files = self.files.lock().unwrap();
                        let file = files.get(&(session.clone(), path.into())).unwrap();
                        frames.push(helper(&json!({"result":metadata(path,file)})));
                        frames.push(helper(&json!({"end":0})));
                    }
                    "list" => {
                        let prefix = format!("{}/", request["path"].as_str().unwrap());
                        let files = self.files.lock().unwrap();
                        let mut result = files
                            .iter()
                            .filter(|((owner, path), _)| {
                                owner == &session
                                    && path.starts_with(&prefix)
                                    && !path[prefix.len()..].contains('/')
                            })
                            .map(|((_, path), file)| metadata(path, file))
                            .collect::<Vec<_>>();
                        result.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
                        frames.push(helper(&json!({"result":result})));
                        frames.push(helper(&json!({"end":0})));
                    }
                    _ => anyhow::bail!("unknown helper operation"),
                }
                match process.fault {
                    1 => frames = vec![b"{truncated".to_vec()],
                    2 => {
                        frames = vec![line(
                            &json!({"stream":"error","data":{"code":"lost","message":"transport error"}}),
                        )]
                    }
                    3 => {
                        frames = vec![
                            helper(&json!({"stream":"stdout","data":STANDARD.encode([0;128])})),
                            helper(&json!({"end":7})),
                        ]
                    }
                    _ => {}
                }
                let mut info = process.info;
                info["exitCode"] = json!(0);
                frames.push(line(&json!({"command":info})));
                if self.hold_command.swap(false, Ordering::SeqCst) {
                    self.command_entered.notify_one();
                    self.command_release.notified().await;
                }
                for bytes in frames {
                    process.send.send(bytes)?;
                }
                if self.lost_stdin.swap(false, Ordering::SeqCst) {
                    return Ok(reply(503, b"lost stdin reply after application".to_vec()));
                }
                return Ok(value(200, json!({"bytesWritten":input.len()})));
            }
            if parts.len() == 3 && parts[1] == "fs" && parts[2] == "read" {
                let input: Value = serde_json::from_slice(&bytes)?;
                let files = self.files.lock().unwrap();
                let Some(file) = files.get(&(session, input["path"].as_str().unwrap().into()))
                else {
                    return Ok(reply(404, Vec::new()));
                };
                return Ok(Response::builder()
                    .header("content-type", "application/octet-stream")
                    .body(Full::new(Bytes::from(file.bytes.clone())).boxed_unsync())?);
            }
        }
        if let Some(name) = path.strip_prefix("/v2/sandboxes/") {
            ensure!(query.get("projectId").map(String::as_str) == Some("fixture-project"));
            if method == "DELETE" {
                ensure!(query.get("deleteOrphanSnapshots").map(String::as_str) == Some("true"));
                self.deletes.fetch_add(1, Ordering::SeqCst);
                if self.defer_delete.load(Ordering::SeqCst) {
                    return Ok(value(200, json!({})));
                }
                let info = self.sandboxes.lock().unwrap().remove(name);
                if let Some(info) = &info {
                    let id = info["session"]["id"].as_str().unwrap();
                    self.files
                        .lock()
                        .unwrap()
                        .retain(|(owner, _), _| owner != id);
                }
                if self.hold_delete.swap(false, Ordering::SeqCst) {
                    self.delete_entered.notify_one();
                    self.delete_release.notified().await;
                }
                if self.lost_delete.swap(false, Ordering::SeqCst) {
                    return Ok(reply(503, b"lost delete reply".to_vec()));
                }
                return Ok(value(200, info.unwrap_or(json!({}))));
            }
            ensure!(
                query.get("resume").map(String::as_str) == Some("false"),
                "inspection must not resume"
            );
            if self.hide_list.load(Ordering::SeqCst) {
                return Ok(reply(404, Vec::new()));
            }
            let Some(mut info) = self.sandboxes.lock().unwrap().get(name).cloned() else {
                return Ok(reply(404, Vec::new()));
            };
            if self.mismatched_metadata.load(Ordering::SeqCst) {
                info["sandbox"]["tags"]["cellar"] = json!("foreign");
            }
            if self.unknown_status.load(Ordering::SeqCst) {
                info["sandbox"]["status"] = json!("unknown-status");
            }
            if self.changed_session.load(Ordering::SeqCst) {
                info["sandbox"]["currentSessionId"] = json!("sbx_foreign");
                info["session"]["id"] = json!("sbx_foreign");
            }
            return Ok(value(200, info));
        }
        anyhow::bail!("unknown Vercel protocol route")
    }
}

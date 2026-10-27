// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! E2B HTTP/Connect protocol peer. It does not execute an E2B VM.
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
    sandbox: String,
    path: String,
    bytes: Vec<u8>,
    send: mpsc::UnboundedSender<Vec<u8>>,
}

struct Streaming(Arc<AtomicUsize>);

impl Drop for Streaming {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Default)]
pub struct State {
    pub creates: AtomicUsize,
    pub deletes: AtomicUsize,
    pub commands: AtomicUsize,
    pub lost_create: AtomicBool,
    pub lost_delete: AtomicBool,
    pub defer_delete: AtomicBool,
    pub hide_list: AtomicBool,
    pub mismatched_metadata: AtomicBool,
    pub redirect: AtomicBool,
    redirect_target: String,
    /// 1 truncated, 2 compressed, 3 oversized output, 4 endless stream.
    pub command_fault: AtomicUsize,
    pub hold_command: AtomicBool,
    pub command_entered: Notify,
    pub command_release: Notify,
    pub streams: Arc<AtomicUsize>,
    pub requests: Mutex<Vec<(String, String)>>,
    sandboxes: Mutex<HashMap<String, Value>>,
    files: Mutex<HashMap<(String, String), File>>,
    processes: Mutex<HashMap<u32, Process>>,
    pid: AtomicUsize,
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
                    result=listener.accept()=>{
                        let Ok((stream,_))=result else {break;};let state=server.clone();
                        tasks.spawn(async move {
                            let service=hyper::service::service_fn(move |request| {let state=state.clone();async move {
                                Ok::<_,Infallible>(match state.answer(request).await {Ok(reply)=>reply,Err(error)=>response(500,error.to_string().into_bytes())})
                            }});
                            let _=hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream),service).await;
                        });
                    }
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
        use cellar_container::e2b::{Config, E2b, Ownership};
        Ok(Arc::new(E2b::new(
            Config::new(
                "fixture-team",
                "fixture-fleet",
                "control-key",
                "fixture-template",
                Ownership::LocalChecksAndExpiry,
            )?
            .binding(binding)?
            .api_endpoint(&self.address)?
            .envd_gateway(&format!("{}envd/", self.address))?
            .lifetime_seconds(60)?
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

fn response(status: u16, bytes: Vec<u8>) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(bytes)).boxed_unsync())
        .unwrap()
}

fn json_reply(value: &Value) -> Response<Body> {
    response(200, serde_json::to_vec(value).unwrap())
}

fn frame(value: &Value, terminal: bool) -> Vec<u8> {
    let data = serde_json::to_vec(value).unwrap();
    let mut out = vec![if terminal { 2 } else { 0 }];
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    out.extend(data);
    out
}

fn end(code: i32) -> Vec<u8> {
    let mut out = frame(
        &json!({"event":{"end":{"exitCode":code,"exited":true,"status":format!("exit {code}"),"error":if code==0{""}else{"exit status 7"}}}}),
        false,
    );
    out.extend(frame(&json!({}), true));
    out
}

fn info(path: &str, file: &File) -> Value {
    json!({"name":path.rsplit('/').next().unwrap(),"path":path,"type":"FILE_TYPE_FILE","size":file.bytes.len().to_string(),"mode":file.mode,"modifiedTime":"2026-10-25T00:00:00Z"})
}

impl State {
    pub fn set_running(&self, running: bool) {
        for info in self.sandboxes.lock().unwrap().values_mut() {
            info["state"] = json!(if running { "running" } else { "paused" });
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
        if !path.starts_with("/envd/") {
            ensure!(
                headers.get("x-api-key").and_then(|v| v.to_str().ok()) == Some("control-key"),
                "control authentication"
            );
            ensure!(
                !headers.contains_key("x-access-token"),
                "envd token leaked to control plane"
            );
            if self.redirect.swap(false, Ordering::SeqCst) {
                return Ok(Response::builder()
                    .status(302)
                    .header("location", &self.redirect_target)
                    .body(Full::new(Bytes::new()).boxed_unsync())?);
            }
            if path == "/v2/sandboxes" && method == "POST" {
                let input: Value = serde_json::from_slice(&bytes)?;
                ensure!(
                    input["templateID"] == "fixture-template"
                        && input["timeout"] == 60
                        && input["autoPause"] == false
                        && input["autoResume"]["enabled"] == false
                        && input["network"]["allowPublicTraffic"] == false
                );
                ensure!(
                    input["allow_internet_access"] == false && input["envVars"]["HELLO"] == "world"
                );
                let request_id = headers.get("idempotency-key").unwrap().to_str()?;
                ensure!(input["metadata"]["cellar.request"] == request_id);
                let n = self.creates.fetch_add(1, Ordering::SeqCst) + 1;
                let id = format!("box-{n}");
                let value = json!({"sandboxID":id,"metadata":input["metadata"],"state":"running","envdVersion":"0.6.4","envdAccessToken":format!("envd-{id}"),"domain":"e2b.app"});
                self.sandboxes.lock().unwrap().insert(id, value.clone());
                if self.lost_create.swap(false, Ordering::SeqCst) {
                    return Ok(response(503, b"lost response after allocation".to_vec()));
                }
                return Ok(response(201, serde_json::to_vec(&value)?));
            }
            if path == "/v2/sandboxes" {
                if self.hide_list.load(Ordering::SeqCst) {
                    return Ok(json_reply(&json!([])));
                }
                let query: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes())
                    .into_owned()
                    .collect();
                let filter: HashMap<_, _> =
                    url::form_urlencoded::parse(query["metadata"].as_bytes())
                        .into_owned()
                        .collect();
                let values = self
                    .sandboxes
                    .lock()
                    .unwrap()
                    .values()
                    .filter(|value| {
                        filter
                            .iter()
                            .all(|(key, wanted)| value["metadata"][key] == *wanted)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                return Ok(json_reply(&json!(values)));
            }
            if let Some(id) = path.strip_prefix("/sandboxes/") {
                if method == "DELETE" {
                    self.deletes.fetch_add(1, Ordering::SeqCst);
                    if self.defer_delete.load(Ordering::SeqCst) {
                        return Ok(response(204, Vec::new()));
                    }
                    let existed = self.sandboxes.lock().unwrap().remove(id).is_some();
                    self.files
                        .lock()
                        .unwrap()
                        .retain(|(owner, _), _| owner != id);
                    if self.lost_delete.swap(false, Ordering::SeqCst) {
                        return Ok(response(503, b"lost delete reply".to_vec()));
                    }
                    return Ok(response(if existed { 204 } else { 404 }, Vec::new()));
                }
                let Some(mut value) = self.sandboxes.lock().unwrap().get(id).cloned() else {
                    return Ok(response(404, Vec::new()));
                };
                if self.mismatched_metadata.load(Ordering::SeqCst) {
                    value["metadata"]["cellar.request"] = json!("foreign");
                }
                return Ok(json_reply(&value));
            }
            anyhow::bail!("unknown control route");
        }
        ensure!(
            !headers.contains_key("x-api-key"),
            "control key leaked to envd"
        );
        let id = headers.get("e2b-sandbox-id").unwrap().to_str()?.to_owned();
        ensure!(headers.get("e2b-sandbox-port").unwrap() == "49983");
        ensure!(headers.get("x-access-token").unwrap().to_str()? == format!("envd-{id}"));
        ensure!(headers.get("authorization").unwrap() == "Basic cm9vdDo=");
        ensure!(self.sandboxes.lock().unwrap().contains_key(&id));
        if path == "/envd/files" {
            let query: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes())
                .into_owned()
                .collect();
            let files = self.files.lock().unwrap();
            let file = files.get(&(id, query["path"].clone())).unwrap();
            return Ok(response(200, file.bytes.clone()));
        }
        if path == "/envd/process.Process/Start" {
            ensure!(headers.get("content-type").unwrap() == "application/connect+json");
            ensure!(
                bytes.len() >= 5
                    && bytes[0] == 0
                    && u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize
                        == bytes.len() - 5
            );
            let input: Value = serde_json::from_slice(&bytes[5..])?;
            self.commands.fetch_add(1, Ordering::SeqCst);
            let pid = (self.pid.fetch_add(1, Ordering::SeqCst) + 1) as u32;
            let (send, receive) = mpsc::unbounded_channel();
            send.send(frame(&json!({"event":{"start":{"pid":pid}}}), false))?;
            let cmd = input["process"]["cmd"].as_str().unwrap();
            let fault = self.command_fault.swap(0, Ordering::SeqCst);
            if fault != 0 {
                match fault {
                    1 => {
                        send.send(frame(&json!({"event":{"data":{"stdout":"AP8q"}}}), false))?;
                    }
                    2 => {
                        send.send(vec![1, 0, 0, 0, 0])?;
                    }
                    3 => {
                        send.send(frame(
                            &json!({"event":{"data":{"stdout":STANDARD.encode([0u8;128])}}}),
                            false,
                        ))?;
                        send.send(end(0))?;
                    }
                    4 => {}
                    _ => anyhow::bail!("unknown fixture fault"),
                }
            } else if cmd == "/usr/bin/tee" {
                ensure!(input["stdin"] == true);
                self.processes.lock().unwrap().insert(
                    pid,
                    Process {
                        sandbox: id,
                        path: input["process"]["args"][1].as_str().unwrap().into(),
                        bytes: Vec::new(),
                        send,
                    },
                );
            } else {
                if cmd == "/bin/chmod" {
                    let mode =
                        u32::from_str_radix(input["process"]["args"][1].as_str().unwrap(), 8)?;
                    let path = input["process"]["args"][2].as_str().unwrap();
                    self.files
                        .lock()
                        .unwrap()
                        .get_mut(&(id, path.into()))
                        .unwrap()
                        .mode = mode;
                    send.send(end(0))?;
                } else {
                    if self.hold_command.swap(false, Ordering::SeqCst) {
                        self.command_entered.notify_one();
                        self.command_release.notified().await;
                    }
                    send.send(frame(&json!({"event":{"data":{"stdout":"AP8q"}}}), false))?;
                    send.send(frame(
                        &json!({"event":{"data":{"stderr":STANDARD.encode(b"diagnostic")}}}),
                        false,
                    ))?;
                    send.send(end(7))?;
                }
            }
            self.streams.fetch_add(1, Ordering::SeqCst);
            let guard = Streaming(self.streams.clone());
            let stream = futures_util::stream::unfold(receive, |mut receive| async move {
                receive.recv().await.map(|bytes| {
                    (
                        Ok::<_, Infallible>(Frame::data(Bytes::from(bytes))),
                        receive,
                    )
                })
            });
            let wait = futures_util::stream::unfold(fault == 4, |wait| async move {
                if wait {
                    std::future::pending::<()>().await;
                }
                None::<(Result<Frame<Bytes>, Infallible>, bool)>
            });
            let stream = stream.chain(wait).map(move |item| {
                let _ = &guard;
                item
            });
            return Ok(Response::builder()
                .header("content-type", "application/connect+json")
                .body(StreamBody::new(stream).boxed_unsync())?);
        }
        let input: Value = serde_json::from_slice(&bytes)?;
        if path == "/envd/process.Process/SendInput" {
            let pid = input["process"]["pid"].as_u64().unwrap() as u32;
            let mut processes = self.processes.lock().unwrap();
            let process = processes.get_mut(&pid).unwrap();
            ensure!(process.sandbox == id);
            process
                .bytes
                .extend(STANDARD.decode(input["input"]["stdin"].as_str().unwrap())?);
            return Ok(json_reply(&json!({})));
        }
        if path == "/envd/process.Process/CloseStdin" {
            let pid = input["process"]["pid"].as_u64().unwrap() as u32;
            let process = self.processes.lock().unwrap().remove(&pid).unwrap();
            ensure!(process.sandbox == id);
            self.files.lock().unwrap().insert(
                (id, process.path),
                File {
                    bytes: process.bytes.clone(),
                    mode: 0o644,
                },
            );
            process.send.send(frame(
                &json!({"event":{"data":{"stdout":STANDARD.encode(process.bytes)}}}),
                false,
            ))?;
            process.send.send(end(0))?;
            return Ok(json_reply(&json!({})));
        }
        if path == "/envd/filesystem.Filesystem/Stat" {
            let files = self.files.lock().unwrap();
            let Some(file) = files.get(&(id, input["path"].as_str().unwrap().into())) else {
                return Ok(response(404, Vec::new()));
            };
            return Ok(json_reply(
                &json!({"entry":info(input["path"].as_str().unwrap(),file)}),
            ));
        }
        if path == "/envd/filesystem.Filesystem/ListDir" {
            ensure!(input["depth"] == 1);
            let prefix = format!("{}/", input["path"].as_str().unwrap());
            let files = self.files.lock().unwrap();
            let entries = files
                .iter()
                .filter(|((owner, path), _)| {
                    owner == &id && path.starts_with(&prefix) && !path[prefix.len()..].contains('/')
                })
                .map(|((_, path), file)| info(path, file))
                .collect::<Vec<_>>();
            return Ok(json_reply(&json!({"entries":entries})));
        }
        anyhow::bail!("unknown envd route")
    }
}

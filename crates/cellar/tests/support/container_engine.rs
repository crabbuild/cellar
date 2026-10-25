// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Deterministic Docker Engine API peer shared by native and adapter tests.
use anyhow::Result;
use http_body_util::{BodyExt, Full};
use hyper::{
    body::{Bytes, Incoming},
    Request as HttpRequest, Response,
};
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::watch,
};

#[derive(Default)]
pub struct Engine {
    pub startup_gate: Mutex<Option<watch::Receiver<bool>>>,
    pub start_entered: AtomicBool,
    pub fail_next_start: AtomicBool,
    pub refuse_remove: AtomicBool,
    pub app_image_loaded: AtomicBool,
    pub containers: Mutex<HashMap<String, watch::Sender<Option<i64>>>>,
    pub labels: Mutex<HashMap<String, Value>>,
    pub networks: Mutex<HashMap<String, Value>>,
    pub execs: Mutex<HashMap<String, Arc<AtomicBool>>>,
    pub calls: Mutex<Vec<(String, String, Value)>>,
    pub http_port: u16,
    pub tcp_port: u16,
}

impl Engine {
    pub async fn answer(
        self: Arc<Self>,
        mut request: HttpRequest<Incoming>,
    ) -> Result<Response<Full<Bytes>>> {
        let path = request
            .uri()
            .path()
            .strip_prefix("/v1.43")
            .unwrap_or(request.uri().path())
            .to_string();
        let query = request.uri().query().unwrap_or("").to_string();
        let method = request.method().as_str().to_string();
        let body = request.body_mut().collect().await?.to_bytes();
        let body: Value = if path == "/images/load" {
            json!({"bytes":body.len()})
        } else if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&body)?
        };
        self.calls
            .lock()
            .unwrap()
            .push((method.clone(), format!("{path}?{query}"), body.clone()));
        let mut status = 200;
        let value = if path == "/containers/json" {
            let filters = url::form_urlencoded::parse(query.as_bytes())
                .find(|(key, _)| key == "filters")
                .unwrap()
                .1;
            let filters: Value = serde_json::from_str(&filters)?;
            let labels = self.labels.lock().unwrap();
            let containers = self.containers.lock().unwrap();
            json!(containers
                .keys()
                .filter(
                    |name| filters["label"].as_array().unwrap().iter().all(|filter| {
                        let (key, value) = filter.as_str().unwrap().split_once('=').unwrap();
                        labels
                            .get(*name)
                            .and_then(|labels| labels.get(key))
                            .and_then(Value::as_str)
                            == Some(value)
                    })
                )
                .map(|name| json!({"Id":name}))
                .collect::<Vec<_>>())
        } else if path == "/images/load" {
            self.app_image_loaded.store(true, Ordering::SeqCst);
            json!({"stream":"Loaded image"})
        } else if path.starts_with("/images/") {
            if path
                == format!(
                    "/images/{}/json",
                    cellar::container::image_reference(&"a".repeat(64))
                )
                && !self.app_image_loaded.load(Ordering::SeqCst)
            {
                status = 404;
            }
            json!({"Config":{}})
        } else if path == "/networks/create" {
            self.networks
                .lock()
                .unwrap()
                .insert(body["Name"].as_str().unwrap().into(), body.clone());
            json!({"Id":body["Name"]})
        } else if path.starts_with("/networks/") && method == "GET" {
            match self
                .networks
                .lock()
                .unwrap()
                .get(path.trim_start_matches("/networks/"))
                .cloned()
            {
                Some(network) => network,
                None => {
                    status = 404;
                    json!({})
                }
            }
        } else if path == "/containers/create" {
            if query.is_empty() {
                json!({"Id":"fence"})
            } else {
                let name = query.strip_prefix("name=").unwrap().to_string();
                self.labels
                    .lock()
                    .unwrap()
                    .insert(name.clone(), body["Labels"].clone());
                self.containers
                    .lock()
                    .unwrap()
                    .insert(name.clone(), watch::channel(None).0);
                json!({"Id":name})
            }
        } else if path.starts_with("/containers/")
            && path.ends_with("/start")
            && path != "/containers/fence/start"
        {
            if self.fail_next_start.swap(false, Ordering::SeqCst) {
                status = 500;
                json!({"message":"startup failed after submission"})
            } else {
                let gate = self.startup_gate.lock().unwrap().clone();
                if let Some(mut gate) = gate {
                    self.start_entered.store(true, Ordering::SeqCst);
                    while !*gate.borrow_and_update() {
                        gate.changed().await?;
                    }
                }
                json!({})
            }
        } else if path == "/containers/fence/wait" {
            json!({"StatusCode":0})
        } else if path.ends_with("/wait") {
            let name = path
                .trim_start_matches("/containers/")
                .trim_end_matches("/wait");
            let sender = self.containers.lock().unwrap().get(name).cloned().unwrap();
            let mut receiver = sender.subscribe();
            let code = loop {
                if let Some(code) = *receiver.borrow_and_update() {
                    break code;
                }
                receiver.changed().await?;
            };
            json!({"StatusCode":code})
        } else if path.starts_with("/containers/") && path.ends_with("/json") {
            let name = path
                .trim_start_matches("/containers/")
                .trim_end_matches("/json");
            let running = self
                .containers
                .lock()
                .unwrap()
                .get(name)
                .map(|sender| sender.borrow().is_none());
            match running {
                Some(running) => {
                    json!({
                        "State": {"Running": running},
                        "NetworkSettings": {
                            "Networks": {"fixture": {"IPAddress": "127.0.0.1"}},
                            "Ports": {
                                format!("{}/tcp", self.http_port): [{"HostPort": self.http_port.to_string()}],
                                format!("{}/tcp", self.tcp_port): [{"HostPort": self.tcp_port.to_string()}]
                            }
                        }
                    })
                }
                None => {
                    status = 404;
                    json!({})
                }
            }
        } else if path.starts_with("/containers/")
            && (method == "DELETE" || path.ends_with("/kill"))
        {
            let name = path
                .trim_start_matches("/containers/")
                .trim_end_matches("/kill");
            if method == "DELETE" || query.contains("SIGKILL") || query.ends_with("=9") {
                if let Some(sender) = self.containers.lock().unwrap().get(name) {
                    sender.send_replace(Some(137));
                }
            }
            if method == "DELETE" {
                if self.refuse_remove.load(Ordering::SeqCst) {
                    status = 500;
                } else {
                    self.labels.lock().unwrap().remove(name);
                    self.containers.lock().unwrap().remove(name);
                }
            }
            json!({})
        } else if path.starts_with("/containers/") && path.ends_with("/exec") {
            let mut execs = self.execs.lock().unwrap();
            let id = format!("exec{}", execs.len() + 1);
            execs.insert(id.clone(), Arc::new(AtomicBool::new(false)));
            json!({"Id":id})
        } else if path.starts_with("/exec/") && path.ends_with("/json") {
            let id = path.trim_start_matches("/exec/").trim_end_matches("/json");
            let ended = self
                .execs
                .lock()
                .unwrap()
                .get(id)
                .unwrap()
                .load(Ordering::SeqCst);
            json!({"Pid":77,"Running":!ended,"ExitCode":7})
        } else if path.starts_with("/exec/") && path.ends_with("/start") && body["Detach"] == false
        {
            let id = path.trim_start_matches("/exec/").trim_end_matches("/start");
            let ended = self.execs.lock().unwrap().get(id).unwrap().clone();
            let upgrade = hyper::upgrade::on(&mut request);
            tokio::spawn(async move {
                let mut stream = TokioIo::new(upgrade.await.unwrap());
                let mut stdin = Vec::new();
                stream.read_to_end(&mut stdin).await.unwrap();
                for (channel, bytes) in [(1u8, stdin.as_slice()), (2, b"stderr".as_slice())] {
                    let mut header = [0u8; 8];
                    header[0] = channel;
                    header[4..].copy_from_slice(&(bytes.len() as u32).to_be_bytes());
                    if stream.write_all(&header).await.is_err()
                        || stream.write_all(bytes).await.is_err()
                    {
                        break;
                    }
                }
                ended.store(true, Ordering::SeqCst);
                let _ = stream.shutdown().await;
            });
            return Ok(Response::builder()
                .status(101)
                .header("connection", "Upgrade")
                .header("upgrade", "tcp")
                .body(Full::new(Bytes::new()))?);
        } else {
            json!({"Id":"network"})
        };
        Ok(Response::builder()
            .status(status)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(serde_json::to_vec(&value)?)))?)
    }

    pub fn app_creates(&self) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, path, _)| path.starts_with("/containers/create?name="))
            .count()
    }
}

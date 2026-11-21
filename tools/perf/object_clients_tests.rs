// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real HTTP connection ownership across the measuring application's two runtimes.
// The embedding application owns and joins these runtimes and server threads.
#![allow(clippy::disallowed_methods)]
use super::*;
use anyhow::{anyhow, ensure, Context, Result};
use bytes::Bytes;
use object_store::aws::S3ConditionalPut;
use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

#[derive(Clone, Debug)]
struct Event {
    connection: usize,
    path: String,
}

type Objects = Arc<Mutex<HashMap<String, (Vec<u8>, String)>>>;

struct Server {
    endpoint: String,
    stop: Arc<AtomicBool>,
    events: Arc<Mutex<Vec<Event>>>,
    worker: Option<thread::JoinHandle<Result<()>>>,
}

fn connection(
    stream: TcpStream,
    id: usize,
    stop: Arc<AtomicBool>,
    events: Arc<Mutex<Vec<Event>>>,
    data: Objects,
) -> Result<()> {
    stream.set_read_timeout(Some(Duration::from_millis(100)))?;
    let mut reader = BufReader::new(stream);
    while !stop.load(Ordering::Acquire) {
        let mut first = String::new();
        match reader.read_line(&mut first) {
            Ok(0) => return Ok(()),
            Ok(_) => (),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                continue
            }
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => return Ok(()),
            Err(e) => return Err(e.into()),
        }
        let fields: Vec<_> = first.split_whitespace().collect();
        ensure!(fields.len() == 3, "HTTP request line");
        let method = fields[0].to_owned();
        let path = fields[1].to_owned();
        let mut headers = HashMap::new();
        loop {
            let mut line = String::new();
            ensure!(reader.read_line(&mut line)? > 0, "HTTP header EOF");
            if line == "\r\n" {
                break;
            }
            let (key, value) = line.split_once(':').context("HTTP header")?;
            headers.insert(key.to_ascii_lowercase(), value.trim().to_owned());
        }
        let size: usize = headers
            .get("content-length")
            .map(|n| n.parse())
            .transpose()?
            .unwrap_or(0);
        ensure!(size <= 65536, "body bound");
        let mut body = vec![0; size];
        reader.read_exact(&mut body)?;
        let mut values = data.lock().unwrap();
        let current = values.get(&path);
        let allowed = (!headers.contains_key("if-none-match") || current.is_none())
            && headers
                .get("if-match")
                .is_none_or(|etag| current.is_some_and(|v| &v.1 == etag));
        let (status, etag, reply) = if method == "PUT" && allowed {
            let etag = format!("\"etag-{}\"", events.lock().unwrap().len() + 1);
            values.insert(path.clone(), (body, etag.clone()));
            ("200 OK", etag, vec![])
        } else if method == "PUT" {
            ("412 Precondition Failed", String::new(), vec![])
        } else if let Some((body, etag)) = current {
            ("200 OK", etag.clone(), body.clone())
        } else {
            ("404 Not Found", String::new(), vec![])
        };
        drop(values);
        events.lock().unwrap().push(Event {
            connection: id,
            path,
        });
        let reply = if method == "HEAD" { vec![] } else { reply };
        write!(reader.get_mut(),"HTTP/1.1 {status}\r\nContent-Length: {}\r\nETag: {etag}\r\nConnection: keep-alive\r\n\r\n",reply.len())?;
        reader.get_mut().write_all(&reply)?;
        reader.get_mut().flush()?;
    }
    Ok(())
}

impl Server {
    fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let stop = Arc::new(AtomicBool::new(false));
        let events = Arc::new(Mutex::new(Vec::new()));
        let s = stop.clone();
        let e = events.clone();
        let worker = thread::spawn(move || {
            let data = Arc::new(Mutex::new(HashMap::new()));
            let mut workers = Vec::new();
            let mut count = 0;
            let mut errors = Vec::new();
            while !s.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        count += 1;
                        let (s, e, d) = (s.clone(), e.clone(), data.clone());
                        workers.push(thread::spawn(move || connection(stream, count, s, e, d)));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1))
                    }
                    Err(error) => {
                        errors.push(error.to_string());
                        break;
                    }
                }
            }
            for worker in workers {
                match worker.join() {
                    Ok(Ok(())) => (),
                    Ok(Err(error)) => errors.push(error.to_string()),
                    Err(_) => errors.push("HTTP connection panicked".to_owned()),
                }
            }
            ensure!(errors.is_empty(), "HTTP server errors: {errors:?}");
            Ok(())
        });
        Ok(Self {
            endpoint,
            stop,
            events,
            worker: Some(worker),
        })
    }

    fn close(&mut self) -> Result<()> {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| anyhow!("HTTP server panicked"))??;
        }
        Ok(())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

struct Release {
    begin: Option<tokio::sync::oneshot::Sender<()>>,
    release: Option<mpsc::Sender<()>>,
    worker: Option<thread::JoinHandle<Result<()>>>,
}

impl Release {
    fn finish(&mut self) -> Result<()> {
        if let Some(begin) = self.begin.take() {
            let _ = begin.send(());
        }
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| anyhow!("application thread panicked"))??;
        }
        Ok(())
    }
}

impl Drop for Release {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

async fn put(bucket: &Bucket, key: &str) -> Result<()> {
    bucket
        .put_cas(key, Bytes::from_static(b"lease"), None)
        .await?
        .context("conditional PUT was rejected")?;
    Ok(())
}

fn scenario() -> Result<serde_json::Value> {
    let mut server = Server::start()?;
    let builder = AmazonS3Builder::new()
        .with_bucket_name("runtime-probe")
        .with_region("us-east-1")
        .with_endpoint(&server.endpoint)
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .with_access_key_id("probe-key")
        .with_secret_access_key("probe-secret")
        .with_conditional_put(S3ConditionalPut::ETagMatch);
    let Clients { data, lease } = connect(
        builder,
        RetryConfig {
            max_retries: 0,
            ..Default::default()
        },
        "runtime-probe".to_owned(),
    )?;
    let app = data.clone();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (begin_tx, begin_rx) = tokio::sync::oneshot::channel();
    let (held_tx, held_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let worker = thread::spawn(move || -> Result<()> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async {
            put(&app, "data/seed").await?;
            tokio::time::sleep(Duration::from_millis(20)).await;
            ready_tx.send(())?;
            begin_rx.await?;
            held_tx.send(())?;
            release_rx.recv_timeout(Duration::from_secs(10))?;
            Ok(())
        })
    });
    let mut release = Release {
        begin: Some(begin_tx),
        release: Some(release_tx),
        worker: Some(worker),
    };
    ready_rx.recv_timeout(Duration::from_secs(5))?;
    let core = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    core.block_on(put(&lease, "nodes/warmup"))?;
    let initial = server.events.lock().unwrap().clone();
    ensure!(initial.len() == 2, "two warmup requests");
    release
        .begin
        .take()
        .unwrap()
        .send(())
        .map_err(|_| anyhow!("block start"))?;
    held_rx.recv_timeout(Duration::from_secs(5))?;
    let stamp = Instant::now();
    let result = core.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), put(&lease, "nodes/lease")).await
    });
    let elapsed = stamp.elapsed();
    let success = matches!(result, Ok(Ok(())));
    let timeout = result.is_err();
    let observed = server.events.lock().unwrap().clone();
    release.finish()?;
    drop(core);
    drop(data);
    drop(lease);
    server.close()?;
    Ok(
        serde_json::json!({"succeeded":success,"deadline_elapsed":timeout,"elapsed_ms":elapsed.as_millis(),"observed_request_count_while_app_held":observed.len(),"warmup_connections":initial.iter().map(|v|v.connection).collect::<Vec<_>>(),"observed_paths":observed.iter().map(|v|v.path.clone()).collect::<Vec<_>>(),"application_and_server_joined":true}),
    )
}

#[test]
fn lease_client_does_not_depend_on_application_runtime() -> Result<()> {
    let observed = scenario()?;
    ensure!(
        observed["succeeded"] == true,
        "lease CAS waited for the blocked application runtime: {observed}"
    );
    ensure!(
        observed["warmup_connections"][0] != observed["warmup_connections"][1],
        "data and lease CAS unexpectedly shared a connection"
    );
    ensure!(observed["observed_request_count_while_app_held"] == 3);
    Ok(())
}

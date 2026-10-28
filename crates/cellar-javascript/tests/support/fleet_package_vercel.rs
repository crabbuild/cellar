// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Installed production entry, with only the external model replaced by HTTP.
use super::{install_staged, run_in_domain, Node};
use anyhow::{ensure, Context as _, Result};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{body::Incoming, server::conn::http1, service::service_fn, Request, Response};
use hyper_util::rt::TokioIo;
use reqwest::Client;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    convert::Infallible,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{net::TcpListener, sync::watch, task::JoinSet};

const RECOVERED_TASKS: usize = 2;
const TOTAL_TASKS: usize = RECOVERED_TASKS + 1;

#[derive(Default)]
struct Observations {
    calls: [[usize; 5]; TOTAL_TASKS],
    failures: Vec<String>,
}

struct Provider {
    origin: String,
    observations: Arc<Mutex<Observations>>,
    release: watch::Sender<bool>,
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<()>>,
}

impl Provider {
    async fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}/v1", listener.local_addr()?);
        let observations = Arc::new(Mutex::new(Observations::default()));
        let (release, released) = watch::channel(false);
        let (stop, mut stopped) = watch::channel(false);
        let state = observations.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            let result = loop {
                cellar::asyncrt::select! {
                    _ = stopped.changed() => break Ok(()),
                    accepted = listener.accept() => {
                        let (stream, _) = match accepted {
                            Ok(value) => value,
                            Err(error) => break Err(error.into()),
                        };
                        let state = state.clone();
                        let released = released.clone();
                        connections.spawn(async move {
                            // A killed host can close a pending provider request.
                            let _ = http1::Builder::new().serve_connection(
                                TokioIo::new(stream),
                                service_fn(move |request| serve(request, state.clone(), released.clone())),
                            ).await;
                        });
                    }
                    Some(joined) = connections.join_next() => {
                        if let Err(error) = joined { break Err(error.into()); }
                    }
                }
            };
            connections.abort_all();
            while connections.join_next().await.is_some() {}
            result
        });
        Ok(Self {
            origin,
            observations,
            release,
            stop,
            task,
        })
    }

    fn calls(&self) -> Result<[[usize; 5]; TOTAL_TASKS]> {
        let state = self.observations.lock().unwrap();
        ensure!(
            state.failures.is_empty(),
            "provider failures: {:?}",
            state.failures
        );
        Ok(state.calls)
    }

    async fn wait_for_cut(&self, node: &Node) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let calls = self.calls()?;
            if calls[..RECOVERED_TASKS]
                .iter()
                .all(|row| *row == [1, 1, 1, 1, 0])
            {
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "provider cut timed out: {calls:?}\n{}",
                node.logs()?
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn stop(self) -> Result<()> {
        self.stop.send_replace(true);
        self.task.await??;
        Ok(())
    }
}

async fn serve(
    request: Request<Incoming>,
    observations: Arc<Mutex<Observations>>,
    mut released: watch::Receiver<bool>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let result: Result<_> = async {
        ensure!(request.method() == "POST" && request.uri().path() == "/v1/responses");
        let body = Limited::new(request.into_body(), 1024 * 1024)
            .collect()
            .await
            .map_err(anyhow::Error::msg)?
            .to_bytes();
        let request: Value = serde_json::from_slice(&body)?;
        let (id, step, response) = decision(&request)?;
        observations.lock().unwrap().calls[id][step] += 1;
        // The application proves the previous file effects and saved tool reads
        // before calling its next model. Hold only this external HTTP response.
        if id < RECOVERED_TASKS && step == 3 {
            released.wait_for(|value| *value).await?;
        }
        Ok(response)
    }
    .await;
    let (status, body) = match result {
        Ok(value) => (200, value),
        Err(error) => {
            observations
                .lock()
                .unwrap()
                .failures
                .push(format!("{error:#}"));
            (
                400,
                json!({"error": {"message":error.to_string(), "type":"fixture", "code":"fixture"}}),
            )
        }
    };
    Ok(Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap())
}

fn decision(request: &Value) -> Result<(usize, usize, Value)> {
    ensure!(request["model"] == "cellar-filesystem-package");
    let input = request["input"].as_array().context("missing model input")?;
    let prompt = input
        .iter()
        .find(|item| item["role"] == "user")
        .context("missing user message")?["content"][0]["text"]
        .as_str()
        .context("missing prompt")?;
    let id: usize = prompt
        .strip_prefix("packaged-file-")
        .context("unexpected prompt")?
        .parse()?;
    ensure!(id < TOTAL_TASKS, "unknown task");
    let replies = input
        .iter()
        .filter(|item| item["type"] == "function_call_output")
        .map(|item| {
            Ok((
                item["call_id"].as_str().context("missing call id")?,
                serde_json::from_str::<Value>(item["output"].as_str().context("missing output")?)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let call = |name: &str, id: &str, args: Value| {
        json!({
            "type":"function_call", "id":format!("fc_{id}"), "call_id":id,
            "name":name, "arguments":args.to_string(),
        })
    };
    let original = format!("first contents {id}");
    let edited = format!("other contents {id}");
    let (step, output) = match replies.len() {
        0 => (
            0,
            vec![call(
                "writeFile",
                "write",
                json!({"path":"/workspace.txt", "content":original}),
            )],
        ),
        1 => (
            1,
            vec![
                call("readFile", "read", json!({"path":"/workspace.txt"})),
                call("listFiles", "list", json!({"path":"/"})),
                call(
                    "searchFiles",
                    "search",
                    json!({"path":"/", "pattern":"first"}),
                ),
            ],
        ),
        4 => {
            let read = replies.get("read").context("missing saved read")?;
            ensure!(read["text"] == original, "incorrect original bytes: {read}");
            let edit = |insert| {
                json!({"path":"/workspace.txt", "inode":read["inode"], "version":read["version"],
                "offset":"0", "delete_bytes":"5", "insert":insert})
            };
            (
                2,
                vec![
                    call("editFile", "edit", edit("other")),
                    call("editFile", "stale", edit("wrong")),
                    call(
                        "renameFile",
                        "rename",
                        json!({"path":"/workspace.txt", "destination":"/result.txt"}),
                    ),
                ],
            )
        }
        7 => {
            ensure!(
                replies["read"]["text"] == original,
                "saved read changed after edit"
            );
            ensure!(
                replies["stale"]["error"]
                    == json!({"code":"Conflict", "errno":"EBUSY", "outcome":"not_committed"})
            );
            (
                3,
                vec![
                    call("readFile", "renamed", json!({"path":"/result.txt"})),
                    call("deleteFile", "delete", json!({"path":"/result.txt"})),
                ],
            )
        }
        9 => {
            ensure!(replies["read"]["text"] == original);
            ensure!(
                replies["renamed"]["text"] == edited,
                "recovered bytes changed"
            );
            (
                4,
                vec![
                    json!({"type":"message", "id":"msg_done", "role":"assistant", "content":[
                        {"type":"output_text", "text":"Verified durable file tools.", "annotations":[]}
                    ]}),
                ],
            )
        }
        count => anyhow::bail!("unexpected tool reply count {count}: {replies:?}"),
    };
    Ok((
        id,
        step,
        json!({"id":format!("resp_{id}_{step}"), "model":"cellar-filesystem-package",
        "output":output, "usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}),
    ))
}

async fn result(client: &Client, node: &Node, id: usize) -> Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let response = client
            .get(format!("{}/agents/task-{id}", node.ingress))
            .send()
            .await?;
        if response.status().is_success() {
            let value: Value = response.json().await?;
            ensure!(value["status"] != "failed", "task failed: {value}");
            if value["status"] == "completed" {
                return Ok(value);
            }
        }
        ensure!(
            Instant::now() < deadline,
            "task-{id} timed out\n{}",
            node.logs()?
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// Inspect private fixture nodes after the selected checkpoint. These rows are observations;
// they never feed the replacement host, which must restore from the bucket.
fn inspect_runs(
    directory: &Path,
    inspect: &mut dyn FnMut(&rusqlite::Connection, &Value) -> Result<()>,
) -> Result<()> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            inspect_runs(&entry.path(), inspect)?;
        } else if entry.file_name() == "db.sqlite" {
            let db = rusqlite::Connection::open_with_flags(
                entry.path(),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )?;
            let run: Option<String> = rusqlite::OptionalExtension::optional(db.query_row(
                "SELECT v FROM __sys_cell_kv WHERE k='run'",
                [],
                |row| row.get(0),
            ))?;
            if let Some(run) = run {
                inspect(&db, &serde_json::from_str(&run)?)?;
            }
        }
    }
    Ok(())
}

fn files_after_cut(directory: &Path) -> Result<BTreeMap<String, Value>> {
    let mut files = BTreeMap::new();
    inspect_runs(directory, &mut |db, run| {
        ensure!(run["models"].as_array().context("missing decisions")?.len() == 3);
        ensure!(run["tools"].as_object().context("missing receipts")?.len() == 7);
        let (name, inode, version, data): (Vec<u8>, i64, i64, Vec<u8>) = db.query_row(
            "SELECT e.name, i.id, i.version, substr(c.data,1,i.size) FROM __sys_fs_entries e
             JOIN __sys_fs_inodes i ON e.inode=i.id JOIN __sys_fs_chunks c ON c.inode=i.id
             WHERE e.parent=1 AND i.kind=0 AND c.chunk_index=0",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        ensure!(name == b"result.txt");
        let prompt = run["prompt"].as_str().context("missing saved prompt")?;
        let id = prompt
            .strip_prefix("packaged-file-")
            .context("unexpected saved prompt")?;
        ensure!(data == format!("other contents {id}").as_bytes());
        ensure!(files
            .insert(
                prompt.to_owned(),
                json!({
                    "inode":inode.to_string(), "version":version.to_string(),
                    "text":String::from_utf8(data)?, "receipts":run["tools"],
                })
            )
            .is_none());
        Ok(())
    })?;
    ensure!(
        files.len() == RECOVERED_TASKS,
        "missing cell files: {files:?}"
    );
    Ok(files)
}

fn verify_deleted_files(directory: &Path, completed: &[Value]) -> Result<()> {
    let mut verified = 0;
    inspect_runs(directory, &mut |db, run| {
        let expected = completed
            .iter()
            .find(|value| value["prompt"] == run["prompt"])
            .context("unexpected recovered task")?;
        ensure!(
            run["status"] == "completed"
                && run["models"].as_array().context("missing decisions")?.len() == 5
        );
        let receipts = run["tools"]
            .as_object()
            .context("missing stored receipts")?;
        let public = expected["receipts"]
            .as_array()
            .context("missing public receipts")?;
        ensure!(receipts.len() == 9 && receipts.values().all(|receipt| public.contains(receipt)));
        let live_files: u64 = db.query_row(
            "SELECT count(*) FROM __sys_fs_entries e JOIN __sys_fs_inodes i ON e.inode=i.id WHERE i.kind=0",
            [], |row| row.get(0),
        )?;
        ensure!(
            live_files == 0,
            "delete receipt survived without deleting its file"
        );
        verified += 1;
        Ok(())
    })?;
    ensure!(
        verified == TOTAL_TASKS,
        "expected {TOTAL_TASKS} completed task databases, found {verified}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires CELLAR_VERCEL_PACKAGE with its captured runtime prefix absent"]
async fn installed_vercel_filesystem_recovers_saved_reads_and_receipts() -> Result<()> {
    run_in_domain(async {
        let staged = PathBuf::from(
            std::env::var_os("CELLAR_VERCEL_PACKAGE").context("CELLAR_VERCEL_PACKAGE required")?,
        )
        .canonicalize()?;
        let package: cellar::native_deploy::Package =
            serde_json::from_slice(&std::fs::read(staged.join("package.json"))?)?;
        let prefix = &package
            .primary
            .native
            .as_ref()
            .context("native required")?
            .node
            .first()
            .context("Node runtime required")?
            .prefix;
        ensure!(
            !Path::new(prefix).exists(),
            "original runtime prefix must be absent"
        );
        let directory = tempfile::tempdir()?;
        let installed = install_staged(directory.path(), &staged).await?;
        ensure!(
            Path::new(prefix).exists(),
            "runtime archive was not restored"
        );
        let provider = Provider::start().await?;
        let outcome = scenario(directory.path(), &installed, &provider).await;
        let joined = provider.stop().await;
        outcome?;
        joined?;
        Ok(())
    })
    .await
}

async fn scenario(root: &Path, installed: &Path, provider: &Provider) -> Result<()> {
    let database = root.join("bucket.sqlite");
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .build()?;
    let environment = [
        ("CELLAR_AI_PROVIDER", "openai".into()),
        ("CELLAR_AI_MODEL", "cellar-filesystem-package".into()),
        ("OPENAI_API_KEY", "private-local-fixture".into()),
        ("OPENAI_BASE_URL", provider.origin.clone()),
        ("CELLAR_HANDLER_BUDGET_S", "120".into()),
        ("CELLAR_WAKER_TICK_MS", "250".into()),
    ];
    let mut first =
        Node::start_with_env(root, "vercel-first", &database, installed, &environment).await?;
    first.ready(&client).await?;
    for id in 0..RECOVERED_TASKS {
        let response = client
            .post(format!("{}/agents/task-{id}", first.ingress))
            .json(&json!({"prompt":format!("packaged-file-{id}")}))
            .send()
            .await?;
        ensure!(response.status() == 202);
    }
    provider.wait_for_cut(&first).await?;
    first.process.kill()?;
    first.process.wait()?;
    let old_data = root.join("vercel-first-data");
    let captured = files_after_cut(&old_data)?;
    std::fs::remove_dir_all(&old_data)?;
    ensure!(!old_data.exists());
    provider.release.send_replace(true);
    ensure!(!root.join("vercel-second-data").exists());
    let mut second =
        Node::start_with_env(root, "vercel-second", &database, installed, &environment).await?;
    second.ready(&client).await?;
    let mut completed = Vec::new();
    for id in 0..TOTAL_TASKS {
        if id == RECOVERED_TASKS {
            ensure!(
                client
                    .post(format!("{}/agents/task-{id}", second.ingress))
                    .json(&json!({"prompt":format!("packaged-file-{id}")}))
                    .send()
                    .await?
                    .status()
                    == 202
            );
        }
        let value = result(&client, &second, id).await?;
        ensure!(value["count"] == 0 && value["toolReceipts"] == 9 && value["modelSteps"] == 5);
        ensure!(value["attempts"] == if id < RECOVERED_TASKS { 2 } else { 1 });
        let receipts = value["receipts"].as_array().context("missing receipts")?;
        let mut names = receipts
            .iter()
            .map(|item| item["name"].as_str().context("missing tool name"))
            .collect::<Result<Vec<_>>>()?;
        names.sort_unstable();
        ensure!(
            names
                == [
                    "deleteFile",
                    "editFile",
                    "editFile",
                    "listFiles",
                    "readFile",
                    "readFile",
                    "renameFile",
                    "searchFiles",
                    "writeFile"
                ]
        );
        let edits = receipts
            .iter()
            .filter(|item| item["name"] == "editFile")
            .collect::<Vec<_>>();
        ensure!(edits[0]["result"]["inserted_bytes"] == 5);
        ensure!(
            edits[1]["result"]["error"]
                == json!({"code":"Conflict", "errno":"EBUSY", "outcome":"not_committed"})
        );
        for (tool, field) in [("listFiles", "entries"), ("searchFiles", "matches")] {
            let receipt = receipts
                .iter()
                .find(|item| item["name"] == tool)
                .context("missing tool receipt")?;
            ensure!(
                receipt["result"][field]
                    .as_array()
                    .context("missing tool result")?
                    .len()
                    == 1
            );
        }
        let reads = receipts
            .iter()
            .filter(|item| item["name"] == "readFile")
            .collect::<Vec<_>>();
        ensure!(reads.len() == 2 && reads[0]["result"]["text"] == format!("first contents {id}"));
        ensure!(reads[1]["result"]["text"] == format!("other contents {id}"));
        if id < RECOVERED_TASKS {
            let before = &captured[&format!("packaged-file-{id}")];
            for receipt in before["receipts"]
                .as_object()
                .context("missing captured receipts")?
                .values()
            {
                ensure!(
                    receipts.contains(receipt),
                    "saved receipt changed after cold recovery: {receipt}"
                );
            }
            for key in ["inode", "version", "text"] {
                ensure!(
                    reads[1]["result"][key] == before[key],
                    "recovered file {key} changed: {value}"
                );
            }
        }
        completed.push(value);
    }
    let expected = [[1, 1, 1, 2, 1], [1, 1, 1, 2, 1], [1, 1, 1, 1, 1]];
    ensure!(
        provider.calls()? == expected,
        "saved decisions were regenerated"
    );
    for (id, before) in completed.iter().enumerate() {
        for _ in 0..4 {
            ensure!(
                client
                    .post(format!("{}/agents/task-{id}", second.ingress))
                    .json(&json!({"prompt":format!("packaged-file-{id}")}))
                    .send()
                    .await?
                    .status()
                    == 200
            );
        }
        ensure!(
            &result(&client, &second, id).await? == before,
            "duplicate changed receipts"
        );
    }
    // Completed tasks have no pending alarms or tool effects. Inspect their
    // resident SQLite state before shutdown can release the local cell cache.
    verify_deleted_files(&root.join("vercel-second-data"), &completed)?;
    second.stop(&client).await?;
    ensure!(provider.calls()? == expected);
    let report = json!({"scenario":"installed-vercel-filesystem", "captured_files":captured,
        "completed":completed, "provider_calls":expected, "original_prefix_absent":true,
        "old_local_state_deleted":true, "replacement_started_empty":true, "duplicates":12,
        "host_joined":true, "deleted_files_verified":true});
    if let Some(path) = std::env::var_os("CELLAR_VERCEL_PACKAGE_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&report)?)?;
    }
    cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(format_args!(
        "PACKAGED_VERCEL_FILESYSTEM: two isolated cells recovered exact edited files and saved reads; 27 receipts, 15 saved decisions, 12 unchanged retries, new-owner writes; host joined"
    ))?;
    Ok(())
}

// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The actual authenticated operator HTTP and CLI paths, including lost replies.
#![allow(clippy::disallowed_methods)]

use super::*;

pub(super) async fn send(
    cluster: &ManagedCluster,
    node: usize,
    body: serde_json::Value,
) -> Result<(u16, serde_json::Value)> {
    let info = cluster.nodes[node].info();
    let path = format!(
        "/runtime/{}?name={}",
        cellar::engine_api::sql_cell_scope(DATABASE),
        url::form_urlencoded::byte_serialize(DATABASE.as_bytes()).collect::<String>()
    );
    let payload = serde_json::to_vec(&body)?;
    let auth = cellar::peer_auth::PeerAuth::new([11; 32], "sql-operator-fixture")?;
    let request = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT + Duration::from_secs(1))
        .build()?
        .post(format!("http://{}{path}", info.internal_address))
        .body(payload.clone());
    let response = auth
        .sign(request, "POST", &path, &payload, &info.node)?
        .send()
        .await?;
    let status = response.status().as_u16();
    let body = response.text().await?;
    Ok((status, serde_json::from_str(&body)?))
}

async fn ok(
    cluster: &ManagedCluster,
    node: usize,
    body: serde_json::Value,
) -> Result<serde_json::Value> {
    let (status, reply) = send(cluster, node, body).await?;
    ensure!(
        status == 200,
        "SQL operator node={node}: HTTP {status}: {reply}"
    );
    reply.get("result").cloned().context("SQL operator result")
}

async fn rows(cluster: &ManagedCluster, sql: &str) -> Result<serde_json::Value> {
    let result = ok(cluster, 1, json!({"exec":{"sql":sql,"rows":true}})).await?;
    Ok(result["results"][0]["rows"].clone())
}

pub(super) async fn before(
    cluster: &ManagedCluster,
    backend: Option<&rustfs::StoreConfig>,
    sql_group: GroupHistory,
) -> Result<()> {
    // The operator is the database's first caller, before any SDK construction.
    let created = ok(cluster, 0, json!({"exec":{"sql":"CREATE TABLE operator_data(v INTEGER); INSERT INTO operator_data VALUES(1); SELECT v FROM operator_data;","rows":true}})).await?;
    ensure!(
        created["count"] == 3 && created["results"][0]["rows"] == json!([[1]]),
        "operator row sets: {created}"
    );
    let prepared = ok(
        cluster,
        2,
        json!({"statements":[{"sql":"SELECT v FROM operator_data WHERE v=?","params":[1]}]}),
    )
    .await?;
    ensure!(prepared[0]["rows"] == json!([[1]]));
    for body in [
        json!({"console":{"sql":"INSERT INTO operator_data VALUES(99)","readonly":true}}),
        json!({"console":{"sql":"SELECT * FROM __sys_raft_clients","readonly":true}}),
        json!({"console":{"sql":"CREATE TABLE operator_ghost(v); SELECT * FROM missing_table;","readonly":false}}),
        json!({"exec":{"sql":"INSERT INTO operator_data VALUES(99); SELECT * FROM missing_table;","rows":true}}),
    ] {
        let (status, result) = send(cluster, 0, body).await?;
        ensure!(
            status == 400 && result["error"].is_string(),
            "SQL operator refusal: {status}: {result}"
        );
    }
    ensure!(rows(cluster, "SELECT v FROM operator_data").await? == json!([[1]]));
    ensure!(
        rows(
            cluster,
            "SELECT count(*) FROM sqlite_schema WHERE name='operator_ghost'"
        )
        .await?
            == json!([[0]])
    );
    let console = ok(
        cluster,
        1,
        json!({"console":{"sql":"SELECT v FROM operator_data","readonly":true}}),
    )
    .await?;
    ensure!(console["results"][0]["rows"] == json!([[1]]));
    let history = ok(
        cluster,
        1,
        json!({"console_migrations":{"table":"__sys_sql_migrations_operator","after":0}}),
    )
    .await?;
    ensure!(history["exists"] == false && history["items"] == json!([]));
    ensure!(
        rows(
            cluster,
            "SELECT count(*) FROM sqlite_schema WHERE name='__sys_sql_migrations_operator'"
        )
        .await?
            == json!([[0]])
    );
    ok(cluster, 0, json!({"migrate":{"name":"initial","sql":"CREATE TABLE operator_migrated(v); INSERT INTO operator_migrated VALUES(7);","table":"__sys_sql_migrations_operator"}})).await?;
    let history = ok(
        cluster,
        2,
        json!({"console_migrations":{"table":"__sys_sql_migrations_operator","after":0}}),
    )
    .await?;
    ensure!(history["exists"] == true && history["items"][0]["name"] == "initial");
    let (status, _) = send(cluster, 1, json!({"migrate":{"name":"broken","sql":"INSERT INTO operator_migrated VALUES(99); SELECT * FROM missing_table;","table":"__sys_sql_migrations_operator"}})).await?;
    ensure!(status == 400);
    ensure!(rows(cluster, "SELECT v FROM operator_migrated").await? == json!([[7]]));
    ensure!(
        ok(
            cluster,
            2,
            json!({"migrations":{"table":"__sys_sql_migrations_operator"}})
        )
        .await?["names"]
            == json!(["initial"])
    );
    let scope = cellar::engine_api::sql_cell_scope(DATABASE);
    let address = cluster.nodes[0].info().internal_address;
    let unsigned = reqwest::Client::new()
        .post(format!("http://{address}/runtime/{scope}"))
        .json(&json!({"exec":{"sql":"INSERT INTO operator_data VALUES(99)","rows":true}}))
        .send()
        .await?;
    ensure!(unsigned.status() == 401);
    let hidden = public_sql(cluster, 1, "IngressOperatorRefused", json!(null)).await?;
    ensure!(hidden == json!(true));
    ensure!(
        rows(
            cluster,
            "SELECT count(*) FROM sqlite_schema WHERE name='operator_escape'"
        )
        .await?
            == json!([[0]])
    );
    let leader = managed_fixture::leader(&cluster.hosts, sql_group).await?;
    let origin = (leader + 1) % 3;
    let source = cluster.hosts[origin].identity().node;
    for (index, host) in cluster.hosts.iter().enumerate() {
        if index != origin {
            host.revoke_command_origin(sql_group.cluster, sql_group.namespace, source, 2)?;
        }
    }
    let denied = send(
        cluster,
        origin,
        json!({"exec":{"sql":"INSERT INTO operator_data VALUES(99)","rows":true}}),
    )
    .await;
    for (index, host) in cluster.hosts.iter().enumerate() {
        if index != origin {
            host.grant_command_origin(NamespaceCommandOrigin {
                cluster: sql_group.cluster,
                namespace: sql_group.namespace,
                revision: 3,
                peer: cluster.peers[origin].clone(),
            })?;
        }
    }
    let (status, reply) = denied?;
    ensure!(
        status == 503,
        "revoked SQL operator write was admitted: {status}: {reply}"
    );
    ensure!(rows(cluster, "SELECT v FROM operator_data").await? == json!([[1]]));
    if let Some(backend) = backend {
        cli_qualification(cluster, backend).await?;
    }
    Ok(())
}

pub(super) async fn after_restart(cluster: &ManagedCluster) -> Result<()> {
    ensure!(rows(cluster, "SELECT v FROM operator_data ORDER BY v").await? == json!([[1]]));
    ok(
        cluster,
        2,
        json!({"exec":{"sql":"INSERT INTO operator_data VALUES(2)","rows":true}}),
    )
    .await?;
    ensure!(rows(cluster, "SELECT v FROM operator_data ORDER BY v").await? == json!([[1], [2]]));
    ensure!(
        ok(
            cluster,
            1,
            json!({"migrations":{"table":"__sys_sql_migrations_operator"}})
        )
        .await?["names"]
            == json!(["initial"])
    );
    Ok(())
}

pub(super) async fn quorum_refused(cluster: &ManagedCluster) -> Result<()> {
    let (status, body) = send(
        cluster,
        0,
        json!({"exec":{"sql":"INSERT INTO operator_data VALUES(99)","rows":true}}),
    )
    .await?;
    ensure!(
        status == 503 && body["outcome"] == "unknown",
        "operator quorum refusal: {status}: {body}"
    );
    Ok(())
}

async fn cli(
    backend: &rustfs::StoreConfig,
    project: &std::path::Path,
    action: &[&str],
    entrances: &[(String, String)],
    success: bool,
) -> Result<String> {
    use std::process::Stdio;
    use tokio::io::AsyncReadExt as _;
    let binary = std::env::var_os("CELLAR_CELL_GROUPS_SQL_CLI")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(env!("CARGO_BIN_EXE_cellar")));
    let mut command = tokio::process::Command::new(binary);
    command.arg("sql").args(action).arg(project).args([
        "--bucket",
        &format!("s3://{}/{}", backend.bucket, backend.prefix),
        "--endpoint",
        &backend.endpoint,
        "--region",
        "us-east-1",
        "--json",
    ]);
    for (node, address) in entrances {
        command.args(["--entrance", &format!("{node}={address}")]);
    }
    command
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn()?;
    let mut output = child.stdout.take().context("CLI stdout")?;
    let mut errors = child.stderr.take().context("CLI stderr")?;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let completed = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::try_join!(
            child.wait(),
            output.read_to_end(&mut stdout),
            errors.read_to_end(&mut stderr)
        )
    })
    .await;
    let status = match completed {
        Ok(Ok(result)) => result.0,
        Ok(Err(error)) => {
            let _ = child.start_kill();
            child.wait().await?;
            return Err(error.into());
        }
        Err(error) => {
            let _ = child.start_kill();
            child.wait().await?;
            return Err(error.into());
        }
    };
    let error = String::from_utf8_lossy(&stderr)
        .replace(&backend.access, "[redacted]")
        .replace(&backend.secret, "[redacted]");
    ensure!(
        status.success() == success,
        "SQL CLI status={status}: {error}"
    );
    Ok(String::from_utf8(stdout)?)
}

async fn cli_qualification(cluster: &ManagedCluster, backend: &rustfs::StoreConfig) -> Result<()> {
    use object_store::ObjectStore as _;
    cluster.fixtures[0]
        .store
        .put(
            &object_store::path::Path::from("fleet/peer-auth.json"),
            serde_json::to_vec(&json!({"version":1,"key":"0b".repeat(32)}))?.into(),
        )
        .await?;
    let project = cluster.roots[0].path().join("operator-project");
    std::fs::create_dir_all(project.join("migrations"))?;
    std::fs::write(
        project.join("wrangler.jsonc"),
        serde_json::to_vec(
            &json!({"name":SCRIPT,"main":"index.js","sql_databases":[{"binding":"DB","database_name":"orders","database_id":DATABASE}]}),
        )?,
    )?;
    std::fs::write(
        project.join("index.js"),
        "// CLI configuration fixture; never executed.\n",
    )?;
    std::fs::write(
        project.join("migrations/0001_initial.sql"),
        "CREATE TABLE cli_data(v INTEGER); INSERT INTO cli_data VALUES(7);",
    )?;
    let entrances: Vec<_> = cluster
        .nodes
        .iter()
        .map(|node| (node.info().node.clone(), node.info().advertise.clone()))
        .collect();
    cli(
        backend,
        &project,
        &["migrations", "apply", "orders"],
        &entrances,
        true,
    )
    .await?;
    let output = cli(
        backend,
        &project,
        &["execute", "orders", "--command", "SELECT v FROM cli_data"],
        &entrances,
        true,
    )
    .await?;
    ensure!(
        serde_json::from_str::<serde_json::Value>(output.trim())? == json!({"v":7}),
        "SQL CLI row output: {output}"
    );
    cli(
        backend,
        &project,
        &["migrations", "apply", "orders"],
        &entrances,
        true,
    )
    .await?;
    ensure!(rows(cluster, "SELECT count(*) FROM cli_data").await? == json!([[1]]));
    ok(
        cluster,
        0,
        json!({"exec":{"sql":"CREATE TABLE operator_replay(v INTEGER)","rows":true}}),
    )
    .await?;
    let mut proxy = lost_reply_proxy(cluster).await?;
    let result = cli(
        backend,
        &project,
        &[
            "execute",
            "orders",
            "--command",
            "INSERT INTO operator_replay VALUES(1)",
        ],
        &[proxy.entrance.clone(), entrances[1].clone()],
        false,
    )
    .await;
    proxy.close().await?;
    result?;
    ensure!(
        proxy.committed.load(Ordering::Acquire) == 1,
        "lost reply did not follow one committed operator call"
    );
    ensure!(
        rows(cluster, "SELECT count(*) FROM operator_replay").await? == json!([[1]]),
        "CLI retried an unknown mutation with a new identity"
    );
    Ok(())
}

struct LostReply {
    entrance: (String, String),
    committed: Arc<AtomicUsize>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
}

impl LostReply {
    async fn close(&mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(mut task) = self.task.take() {
            match tokio::time::timeout(TIMEOUT, &mut task).await {
                Ok(result) => result??,
                Err(error) => {
                    task.abort();
                    let _ = task.await;
                    return Err(error.into());
                }
            }
        }
        Ok(())
    }
}

impl Drop for LostReply {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn lost_reply_proxy(cluster: &ManagedCluster) -> Result<LostReply> {
    let info = cluster.nodes[0].info();
    let target = info.internal_address;
    let node = info.node.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let committed = Arc::new(AtomicUsize::new(0));
    let observed = committed.clone();
    let router = axum::Router::new().fallback(move |request: axum::extract::Request| {
        let observed = observed.clone();
        async move {
            let (parts, body) = request.into_parts();
            let body = axum::body::to_bytes(body, 1024 * 1024).await.unwrap();
            let result = reqwest::Client::builder()
                .no_proxy()
                .timeout(TIMEOUT + Duration::from_secs(1))
                .build()
                .unwrap()
                .post(format!("http://{target}{}", parts.uri))
                .headers(parts.headers)
                .body(body)
                .send()
                .await
                .unwrap();
            if result.status() == 200 {
                observed.fetch_add(1, Ordering::AcqRel);
            }
            (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                axum::Json(json!({"error":"dispatcher unavailable","outcome":"unknown"})),
            )
        }
    });
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    Ok(LostReply {
        entrance: (node, address),
        committed,
        stop: Some(stop),
        task: Some(task),
    })
}

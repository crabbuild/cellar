// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native invocation -> shared adapter -> published E2B HTTP/Connect protocol.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use cellar::sdk::*;
use cellar_container::{Adapter, Limits, Operation, Validation};
use serde_json::{json, Value};
use std::{
    sync::{atomic::Ordering, Arc, Mutex},
    time::Duration,
};
#[path = "../../cellar-javascript/tests/support/native.rs"]
mod native;
#[path = "support/e2b.rs"]
mod peer;

struct Root {
    adapter: Adapter,
    bindings: cellar_container::Bindings,
    retained: Mutex<Option<cellar_container::BindingSessions>>,
}

impl Cell for Root {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            let session = self.adapter.session();
            let limits = Limits {
                max_body_bytes: 65536,
            };
            let route = request.url.rsplit('/').next().unwrap();
            let start = json!({"options":{"env":[["HELLO","world"]]}});
            let value = match route {
                "named" => {
                    let sessions = self.bindings.sessions();
                    let backup = sessions.get("backup")?;
                    ensure!(
                        backup
                            .execute(&context, Operation::Start, start, &limits)
                            .await?
                            == "1"
                    );
                    backup
                        .execute(
                            &context,
                            Operation::WriteFile,
                            json!({"path":"/tmp/named.bin","data":"AQID","mode":384}),
                            &limits,
                        )
                        .await?;
                    ensure!(
                        backup
                            .execute(
                                &context,
                                Operation::ReadFile,
                                json!({"path":"/tmp/named.bin","max_bytes":3}),
                                &limits
                            )
                            .await?
                            == "AQID"
                    );
                    ensure!(
                        session
                            .execute(&context, Operation::Running, json!({}), &limits)
                            .await?
                            == false
                    );
                    backup
                        .execute(&context, Operation::Destroy, json!({}), &limits)
                        .await?;
                    *self.retained.lock().unwrap() = Some(sessions);
                    json!(true)
                }
                "borrow-named" => {
                    let old = self.retained.lock().unwrap().take().unwrap();
                    ensure!(old
                        .get("backup")?
                        .execute(&context, Operation::Capabilities, json!({}), &limits)
                        .await
                        .is_err());
                    json!(true)
                }
                "reconcile" => {
                    session
                        .execute(&context, Operation::Reconcile, json!({}), &limits)
                        .await?
                }
                "unknown-reconcile" => {
                    let error = session
                        .execute(&context, Operation::Reconcile, json!({}), &limits)
                        .await
                        .unwrap_err();
                    ensure!(!error.is::<Validation>());
                    json!(true)
                }
                "start" => {
                    session
                        .execute(&context, Operation::Start, start, &limits)
                        .await?
                }
                "unknown-start" => {
                    let error = session
                        .execute(&context, Operation::Start, start, &limits)
                        .await
                        .unwrap_err();
                    ensure!(!error.is::<Validation>());
                    json!(true)
                }
                "refuse" => {
                    for (operation, input) in [
                        (Operation::Signal, json!({"signal":9})),
                        (
                            Operation::Start,
                            json!({"options":{"entrypoint":["/bin/sh"]}}),
                        ),
                        (
                            Operation::Start,
                            json!({"options":{"labels":[["cellar.scope","forged"]]}}),
                        ),
                        (
                            Operation::Start,
                            json!({"options":{"enable_internet":true}}),
                        ),
                        (
                            Operation::ExecOutput,
                            json!({"options":{"cmd":["child-exit"],"user":"1000:1000"},"max_bytes":64}),
                        ),
                        (
                            Operation::ExecOutput,
                            json!({"options":{"cmd":["child-exit"],"env":[["KEY","one"],["KEY","two"]]},"max_bytes":64}),
                        ),
                    ] {
                        ensure!(session
                            .execute(&context, operation, input, &limits)
                            .await
                            .unwrap_err()
                            .is::<Validation>());
                    }
                    json!(true)
                }
                "running" => {
                    session
                        .execute(&context, Operation::Running, json!({}), &limits)
                        .await?
                }
                "command-failure" => {
                    let error = session
                        .execute(
                            &context,
                            Operation::ExecOutput,
                            json!({"options":{"cmd":["child-exit"]},"max_bytes":64}),
                            &limits,
                        )
                        .await
                        .unwrap_err();
                    ensure!(!error.is::<Validation>());
                    json!(true)
                }
                "combined" => {
                    let output=session.execute(&context,Operation::ExecOutput,json!({"options":{"cmd":["child-exit"],"combined":true},"max_bytes":13}),&limits).await?;
                    ensure!(
                        output["exit_code"] == 7
                            && output["stdout"] == "AP8qZGlhZ25vc3RpYw=="
                            && output["stderr"] == ""
                    );
                    json!(true)
                }
                "mismatch" => {
                    ensure!(session
                        .execute(&context, Operation::Reconcile, json!({}), &limits)
                        .await
                        .is_err());
                    ensure!(session
                        .execute(
                            &context,
                            Operation::ExecOutput,
                            json!({"options":{"cmd":["child-exit"]},"max_bytes":64}),
                            &limits
                        )
                        .await
                        .is_err());
                    ensure!(session
                        .execute(&context, Operation::Destroy, json!({}), &limits)
                        .await
                        .is_err());
                    json!(true)
                }
                "files" => {
                    let path = "/tmp/é-?.bin";
                    session
                        .execute(
                            &context,
                            Operation::WriteFile,
                            json!({"path":path,"data":"AP8q","mode":416}),
                            &limits,
                        )
                        .await?;
                    let value = session
                        .execute(
                            &context,
                            Operation::ReadFile,
                            json!({"path":path,"max_bytes":3}),
                            &limits,
                        )
                        .await?;
                    ensure!(value == "AP8q");
                    let info = session
                        .execute(&context, Operation::StatFile, json!({"path":path}), &limits)
                        .await?;
                    ensure!(info["size"] == 3 && info["mode"] == 416 && info["kind"] == "file");
                    let list = session
                        .execute(
                            &context,
                            Operation::ListFiles,
                            json!({"path":"/tmp","max_bytes":4096,"max_entries":1}),
                            &limits,
                        )
                        .await?;
                    ensure!(list.as_array().unwrap().len() == 1);
                    ensure!(session
                        .execute(
                            &context,
                            Operation::ReadFile,
                            json!({"path":path,"max_bytes":2}),
                            &limits
                        )
                        .await
                        .is_err());
                    let output = session
                        .execute(
                            &context,
                            Operation::ExecOutput,
                            json!({"options":{"cmd":["child-exit"]},"max_bytes":64}),
                            &limits,
                        )
                        .await?;
                    ensure!(
                        output["exit_code"] == 7
                            && output["stdout"] == "AP8q"
                            && output["stderr"] == "ZGlhZ25vc3RpYw=="
                    );
                    ensure!(session
                        .execute(
                            &context,
                            Operation::ExecOutput,
                            json!({"options":{"cmd":["child-exit"]},"max_bytes":2}),
                            &limits
                        )
                        .await
                        .is_err());
                    json!(true)
                }
                "read-later" => {
                    ensure!(
                        session
                            .execute(
                                &context,
                                Operation::ReadFile,
                                json!({"path":"/tmp/é-?.bin","max_bytes":3}),
                                &limits
                            )
                            .await?
                            == "AP8q"
                    );
                    json!(true)
                }
                "large-files" => {
                    use base64::Engine as _;
                    let limits = Limits {
                        max_body_bytes: 1024 * 1024,
                    };
                    for bytes in [Vec::new(), vec![0xff; 512 * 1024]] {
                        let encoded = base64::engine::general_purpose::STANDARD.encode(&bytes);
                        session
                            .execute(
                                &context,
                                Operation::WriteFile,
                                json!({"path":"/tmp/large.bin","data":encoded,"mode":384}),
                                &limits,
                            )
                            .await?;
                        ensure!(
                            session
                                .execute(
                                    &context,
                                    Operation::ReadFile,
                                    json!({"path":"/tmp/large.bin","max_bytes":bytes.len().max(1)}),
                                    &limits
                                )
                                .await?
                                == encoded
                        );
                    }
                    json!(true)
                }
                "destroy" => {
                    session
                        .execute(&context, Operation::Destroy, json!({}), &limits)
                        .await?
                }
                "unknown-destroy" => {
                    ensure!(session
                        .execute(&context, Operation::Destroy, json!({}), &limits)
                        .await
                        .is_err());
                    json!(true)
                }
                _ => anyhow::bail!("unknown E2B scenario"),
            };
            Ok(Output::Http(
                http::Response::from_json(&value)?.into_http(&context)?,
            ))
        })
    }
}

struct App(Arc<Root>);

impl Application for App {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            let response = context
                .namespace("COUNTERS")?
                .get_by_name("alice")
                .fetch(&context, request)
                .await?;
            Ok(Output::Http(response))
        })
    }

    fn supports_cell(&self, class: &str) -> bool {
        class == "Counter"
    }

    fn create_cell(&self, _: Context, _: &CellIdentity) -> Result<Arc<dyn Cell>> {
        Ok(self.0.clone())
    }
}

async fn call(client: &reqwest::Client, address: &str, route: &str) -> Result<Value> {
    let response = client.get(format!("{address}/{route}")).send().await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(status.is_success(), "{route}: {status} {body}");
    Ok(serde_json::from_str(&body)?)
}

#[test]
fn e2b_protocol_reconciles_lost_creates_and_preserves_files_across_native_recovery() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(async {
        let directory=tempfile::tempdir()?;let peer=peer::Peer::start().await?;
        let mut bindings = cellar_container::Bindings::default();
        bindings.insert("backup", peer.provider_binding("backup")?)?;
        let adapter = Adapter::new(peer.provider()?);
        bindings.validate(adapter.provider())?;
        let root=Arc::new(Root {adapter, bindings, retained: Mutex::new(None)});
        let caps=root.adapter.capabilities(&Limits {max_body_bytes:65536});
        ensure!(caps.provider=="e2b" && caps.fencing==cellar_container::Fencing::LocalChecksAndExpiry && !caps.queued_lifecycle && !caps.synchronous_running);
        let client=reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?;
        for round in 0..3 {
            let (node,address)=native::start_configured(directory.path(),Arc::new(App(root.clone())),|config,_,_| {
                // This new fixture includes multiple vendor HTTP/RPC exchanges;
                // the E2B client has its own explicit two-second operation limit.
                config.runtime.handler_budget=Duration::from_secs(5);
            }).await?;
            let result=async {
                if round==0 {
                    ensure!(call(&client,&address,"reconcile").await?==json!({"state":"unbound","generation":null}));
                    ensure!(call(&client,&address,"refuse").await?==true);
                    ensure!(peer.state.requests.lock().unwrap().is_empty());
                    peer.state.lost_create.store(true,Ordering::SeqCst);
                    ensure!(call(&client,&address,"unknown-start").await?==true);
                    ensure!(peer.state.creates.load(Ordering::SeqCst)==1);
                    peer.state.hide_list.store(true,Ordering::SeqCst);
                    ensure!(call(&client,&address,"unknown-reconcile").await?==true);
                    ensure!(call(&client,&address,"unknown-start").await?==true);
                    ensure!(peer.state.creates.load(Ordering::SeqCst)==1);
                } else if round == 1 {
                    peer.state.hide_list.store(false,Ordering::SeqCst);
                    ensure!(call(&client,&address,"reconcile").await?==json!({"state":"running","generation":"1"}));
                    ensure!(peer.state.creates.load(Ordering::SeqCst)==1,"lost create was replayed");
                    ensure!(call(&client,&address,"running").await?==true);
                    peer.state.set_running(false);
                    ensure!(call(&client,&address,"reconcile").await?==json!({"state":"stopped","generation":"1"}));
                    peer.state.set_running(true);
                    ensure!(call(&client,&address,"files").await?==true);
                    ensure!(call(&client,&address,"read-later").await?==true);
                    ensure!(call(&client,&address,"large-files").await?==true);
                    ensure!(call(&client,&address,"combined").await?==true);
                    for fault in 1..=4 {
                        let commands=peer.state.commands.load(Ordering::SeqCst);
                        peer.state.command_fault.store(fault,Ordering::SeqCst);
                        ensure!(call(&client,&address,"command-failure").await?==true);
                        ensure!(peer.state.commands.load(Ordering::SeqCst)==commands+1,"failed command was replayed");
                        tokio::time::timeout(Duration::from_secs(3),async {
                            while peer.state.streams.load(Ordering::SeqCst)!=0 {tokio::task::yield_now().await;}
                        }).await?;
                    }
                    let commands=peer.state.commands.load(Ordering::SeqCst);
                    peer.state.redirect.store(true,Ordering::SeqCst);
                    ensure!(call(&client,&address,"command-failure").await?==true);
                    ensure!(peer.state.commands.load(Ordering::SeqCst)==commands,"redirect reached envd submission");
                    ensure!(!peer.state.requests.lock().unwrap().iter().any(|(_,path)|path.starts_with("/credential-sink")),"provider credentials followed a redirect");
                    let commands=peer.state.commands.load(Ordering::SeqCst);
                    peer.state.mismatched_metadata.store(true,Ordering::SeqCst);
                    ensure!(call(&client,&address,"mismatch").await?==true);
                    ensure!(peer.state.commands.load(Ordering::SeqCst)==commands && peer.state.deletes.load(Ordering::SeqCst)==0);
                    peer.state.mismatched_metadata.store(false,Ordering::SeqCst);
                    peer.state.lost_delete.store(true,Ordering::SeqCst);
                    ensure!(call(&client,&address,"unknown-destroy").await?==true);
                    ensure!(peer.state.deletes.load(Ordering::SeqCst)==1);
                    ensure!(call(&client,&address,"reconcile").await?==json!({"state":"deleted","generation":"1"}));
                    ensure!(peer.state.deletes.load(Ordering::SeqCst)==1,"lost deletion was blindly replayed");
                    ensure!(call(&client,&address,"running").await?==false);
                    ensure!(call(&client,&address,"start").await?=="2");
                    peer.state.expire();
                    ensure!(call(&client,&address,"reconcile").await?==json!({"state":"deleted","generation":"2"}));
                    ensure!(peer.state.deletes.load(Ordering::SeqCst)==1,"expiry submitted a delete");
                    ensure!(call(&client,&address,"start").await?=="3");
                    peer.state.defer_delete.store(true,Ordering::SeqCst);
                    ensure!(call(&client,&address,"unknown-destroy").await?==true);
                    ensure!(call(&client,&address,"reconcile").await?==json!({"state":"deleting","generation":"3"}));
                } else {
                    ensure!(call(&client,&address,"reconcile").await?==json!({"state":"deleting","generation":"3"}));
                    ensure!(peer.state.creates.load(Ordering::SeqCst)==3 && peer.state.deletes.load(Ordering::SeqCst)==2,"reconciliation submitted a mutation");
                    peer.state.expire();
                    peer.state.defer_delete.store(false,Ordering::SeqCst);
                    ensure!(call(&client,&address,"reconcile").await?==json!({"state":"deleted","generation":"3"}));
                    ensure!(peer.state.deletes.load(Ordering::SeqCst)==2,"pending deletion was replayed");
                    ensure!(call(&client,&address,"start").await?=="4");
                    ensure!(call(&client,&address,"destroy").await?.is_null());
                    ensure!(call(&client,&address,"named").await?==true);
                    ensure!(call(&client,&address,"borrow-named").await?==true);
                }
                Ok::<_,anyhow::Error>(())
            }.await;
            let stopped=node.shutdown(ShutdownMode::Preserve).await?;
            ensure!(matches!(stopped,NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out));
            if let Err(error)=result {peer.stop().await?;return Err(error);}
        }
        ensure!(peer.state.creates.load(Ordering::SeqCst)==5 && peer.state.deletes.load(Ordering::SeqCst)==4);
        peer.stop().await?;Ok(())
    })
}

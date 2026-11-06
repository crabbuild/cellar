// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A fixture admin with the same routed control and CLI as an operator.
use anyhow::{ensure, Context as _, Result};
use cellar::{
    asyncrt::TaskHandle,
    bucket::Bucket,
    sdk::{consensus::RaftManagedFirstGroupRequest, fleet_operations::*, ExecutionDomain},
};
use cellar_admin::{AuditStore, Console, Fleet, FleetInfo, Permission, Principal};
use serde_json::Value;
use std::time::Duration;
use std::{collections::BTreeMap, path::Path, sync::Arc};
use tokio::sync::oneshot;

pub const FLEET: &str = "first-group-fixture";
pub const OWNER: &str = "fixture-first-group-owner-credential-000";
pub const READER: &str = "fixture-first-group-reader-credential-000";

pub struct OperatorCli {
    pub url: String,
    pub catalog: String,
    stop: Option<oneshot::Sender<()>>,
    joined: Option<TaskHandle<std::io::Result<()>>>,
}

impl OperatorCli {
    pub async fn start(
        domain: &Arc<ExecutionDomain>,
        bucket: Bucket,
        control: RaftFleetControl,
        root: &Path,
        owner_id: &str,
    ) -> Result<Self> {
        let catalog = control.catalog_id().to_owned();
        let router = Self::router(domain, bucket, control, root, owner_id)?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let (stop, stopped) = oneshot::channel();
        let server = domain.spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
        })?;
        Ok(Self {
            url,
            catalog,
            stop: Some(stop),
            joined: Some(server),
        })
    }

    pub fn connect(url: String, catalog: String) -> Self {
        Self {
            url,
            catalog,
            stop: None,
            joined: None,
        }
    }

    pub fn router(
        domain: &Arc<ExecutionDomain>,
        bucket: Bucket,
        control: RaftFleetControl,
        root: &Path,
        owner_id: &str,
    ) -> Result<axum::Router> {
        let principal = |id: &str, token: &str, grants| {
            Principal::new(id.into(), token, BTreeMap::from([(FLEET.into(), grants)]))
        };
        Ok(cellar_admin::router(Console {
            domain: domain.clone(),
            fleets: BTreeMap::from([(
                FLEET.into(),
                Fleet {
                    info: FleetInfo {
                        id: FLEET.into(),
                        name: FLEET.into(),
                        environment: "fixture".into(),
                    },
                    client: cellar::management::FleetClient::new(bucket, false)
                        .with_raft_fleet(control),
                },
            )]),
            principals: vec![
                principal(
                    owner_id,
                    OWNER,
                    vec![
                        Permission::MetadataRead,
                        Permission::FleetCapacity,
                        Permission::FleetTopology,
                    ],
                )?,
                principal("reader", READER, vec![Permission::MetadataRead])?,
            ],
            audit: AuditStore::open(&root.join("first-group-audit.sqlite"))?,
            assets: root.join("assets"),
            origins: vec![],
        })?)
    }

    pub async fn call(&self, args: &[&str], token: &str) -> Result<CliReply> {
        self.call_with_timeout(args, token, Duration::from_secs(5))
            .await
    }

    pub async fn call_with_timeout(
        &self,
        args: &[&str],
        token: &str,
        timeout: Duration,
    ) -> Result<CliReply> {
        let executable = std::env::var_os("CELLAR_RAFT_OPERATOR_CLI")
            .unwrap_or_else(|| env!("CARGO_BIN_EXE_cellar").into());
        let mut command = tokio::process::Command::new(executable);
        command
            .args(["raft", "fleet"])
            .args(args)
            .args([
                "--fleet",
                FLEET,
                "--admin-url",
                &self.url,
                "--catalog-id",
                &self.catalog,
                "--json",
            ])
            .env_remove("AWS_ACCESS_KEY_ID")
            .env_remove("AWS_SECRET_ACCESS_KEY")
            .env_remove("AWS_SESSION_TOKEN")
            .env("CELLAR_ADMIN_TOKEN", token)
            .kill_on_drop(true);
        let output = tokio::time::timeout(timeout, command.output())
            .await
            .with_context(|| {
                format!(
                    "managed CLI invocation deadline for {:?}",
                    &args[..args.len().min(2)]
                )
            })??;
        for bytes in [&output.stdout, &output.stderr] {
            ensure!(!String::from_utf8_lossy(bytes).contains(token));
        }
        let value =
            serde_json::from_slice(&output.stdout).context("first group CLI JSON response")?;
        Ok(CliReply {
            code: output.status.code().context("CLI exited by signal")?,
            value,
        })
    }

    pub async fn revision(&self) -> Result<u64> {
        let status: RaftFleetStatus =
            serde_json::from_value(self.call(&["status"], OWNER).await?.success()?)?;
        Ok(status.observed_revision)
    }

    pub async fn close(mut self) -> Result<()> {
        self.signal_stop();
        if let Some(joined) = self.joined.take() {
            joined.await??;
        }
        Ok(())
    }

    fn signal_stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

impl Drop for OperatorCli {
    fn drop(&mut self) {
        self.signal_stop();
    }
}

pub struct CliReply {
    pub code: i32,
    pub value: Value,
}

impl CliReply {
    pub fn success(self) -> Result<Value> {
        ensure!(self.code == 0, "managed CLI refused: {}", self.value);
        Ok(self.value)
    }
}

pub fn first_group_plan(
    catalog: &str,
    setup: RaftManagedFirstGroupRequest,
) -> Result<RaftFleetFirstGroupPlan> {
    let history = setup.group.history;
    let voters = setup
        .group
        .voters
        .into_iter()
        .map(|member| RaftFleetFirstGroupVoter {
            node_id: hex::encode(member.node.bytes()),
            member_id: member.member.get(),
        })
        .collect::<Vec<_>>()
        .try_into()
        .ok()
        .context("expected RF3")?;
    Ok(RaftFleetFirstGroupPlan {
        catalog_id: catalog.into(),
        namespace_id: hex::encode(history.namespace.bytes()),
        standby: hex::encode(setup.standby),
        request_id: hex::encode(setup.reservation.id),
        operation_id: hex::encode(setup.reservation.operation),
        expected_revision: setup.reservation.expected_revision,
        publication_id: hex::encode(setup.publication.bytes()),
        group_id: hex::encode(history.group.bytes()),
        incarnation_id: hex::encode(history.incarnation.bytes()),
        creation_id: hex::encode(setup.group.creation.bytes()),
        voters,
    })
}

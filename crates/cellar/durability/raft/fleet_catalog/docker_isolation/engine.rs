// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use records::Terminal;
use serde::Deserialize;
use serde_json::{json, Value};

pub(super) struct Engine {
    docker: Docker,
    owner: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MemorySample {
    pub read: String,
    pub used: u64,
    pub limit: u64,
}

impl Engine {
    pub fn new(docker: Docker, owner: &str) -> Result<Self> {
        if !records::identifier(owner) || !docker.socket().is_absolute() {
            return Err(FleetCatalogError::Invalid("Docker isolation configuration"));
        }
        Ok(Self {
            docker,
            owner: owner.into(),
        })
    }

    async fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
        expected: u16,
    ) -> Result<Value> {
        // Never print daemon responses: inspect contains environment secrets.
        let reply = self
            .docker
            .call_bounded(method, path, body, 256 * 1024)
            .await
            .map_err(|_| FleetCatalogError::Unconfirmed)?;
        if reply.status.as_u16() != expected {
            return Err(FleetCatalogError::Unconfirmed);
        }
        if reply.body.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_slice(&reply.body)
            .map_err(|_| FleetCatalogError::Corrupt("Docker response"))
    }

    async fn identity(&self) -> Result<String> {
        let value = self.call("GET", "/info", None, 200).await?;
        let id = string(&value["ID"])?;
        if value["OSType"] != "linux" || !records::identifier(id) {
            return Err(FleetCatalogError::Invalid("Docker Linux engine identity"));
        }
        Ok(id.into())
    }

    async fn inspect(&self, boot: &BootBinding, container: &str) -> Result<Value> {
        ContainerId::new(container)
            .map_err(|_| FleetCatalogError::Invalid("Docker container ID"))?;
        let v = self
            .call("GET", &format!("/containers/{container}/json"), None, 200)
            .await?;
        let labels = &v["Config"]["Labels"];
        if v["Id"] != container
            || labels["io.cellar.fleet.owner"] != self.owner
            || labels["io.cellar.fleet.catalog"] != hex::encode(boot.catalog)
            || labels["io.cellar.fleet.node"] != hex::encode(boot.node)
            || labels["io.cellar.fleet.boot-mode"] != "single-use"
            || labels["io.cellar.fleet.workers"] != "contained"
            || v["HostConfig"]["AutoRemove"] != false
            || v["HostConfig"]["Privileged"] != false
            || v["HostConfig"]["PidMode"] != ""
        {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(v)
    }

    pub async fn bind(&self, boot: &BootBinding, container: &ContainerId) -> Result<Binding> {
        let engine = self.identity().await?;
        let v = self.inspect(boot, container.as_str()).await?;
        if v["State"]["Running"] != true
            || v["State"]["Restarting"] != false
            || v["State"]["Paused"] != false
            || v["State"]["Dead"] != false
            || !restart_disabled(&v)
        {
            return Err(FleetCatalogError::Conflict);
        }
        let binding = Binding {
            format: 1,
            boot: boot.clone(),
            owner: self.owner.clone(),
            engine,
            container: container.as_str().into(),
            started: string(&v["State"]["StartedAt"])?.into(),
            restarts: v["RestartCount"]
                .as_u64()
                .ok_or(FleetCatalogError::Corrupt("Docker restart count"))?,
        };
        binding.validate()?;
        Ok(binding)
    }

    fn check_start(&self, binding: &Binding, v: &Value) -> Result<()> {
        if binding.owner != self.owner
            || v["State"]["StartedAt"] != binding.started
            || v["RestartCount"].as_u64() != Some(binding.restarts)
        {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(())
    }

    /// Keep cache charged. CLI-style cache subtraction is not an admission
    /// bound. Both inspections must match the same live, single-use boot.
    pub async fn memory(
        &self,
        binding: &Binding,
        capacity: &FleetCapacityAttestation,
    ) -> Result<MemorySample> {
        if binding.boot != capacity.boot || self.identity().await? != binding.engine {
            return Err(FleetCatalogError::Conflict);
        }
        let before = self.inspect(&binding.boot, &binding.container).await?;
        self.check_resources(binding, capacity, &before)?;
        let stats = self
            .call(
                "GET",
                &format!(
                    "/containers/{}/stats?stream=false&one-shot=true",
                    binding.container
                ),
                None,
                200,
            )
            .await?;
        let sample = MemorySample {
            read: string(&stats["read"])?.into(),
            used: stats["memory_stats"]["usage"]
                .as_u64()
                .ok_or(FleetCatalogError::Unconfirmed)?,
            limit: stats["memory_stats"]["limit"]
                .as_u64()
                .ok_or(FleetCatalogError::Unconfirmed)?,
        };
        if stats["id"] != binding.container
            || !records::timestamp(&sample.read)
            || sample.limit != capacity.memory_bytes
            || sample.used > sample.limit
        {
            return Err(FleetCatalogError::Conflict);
        }
        let after = self.inspect(&binding.boot, &binding.container).await?;
        self.check_resources(binding, capacity, &after)?;
        Ok(sample)
    }

    fn check_resources(
        &self,
        binding: &Binding,
        capacity: &FleetCapacityAttestation,
        v: &Value,
    ) -> Result<()> {
        self.check_start(binding, v)?;
        let config = &v["HostConfig"];
        if !restart_disabled(v)
            || v["State"]["Running"] != true
            || v["State"]["Paused"] != false
            || v["State"]["Restarting"] != false
            || v["State"]["Dead"] != false
            || config["NanoCpus"].as_u64() != Some(u64::from(capacity.cpu_millis) * 1_000_000)
            || config["Memory"].as_u64() != Some(capacity.memory_bytes)
            || config["MemorySwap"].as_u64() != Some(capacity.memory_bytes)
            || !capacity.memory_swap_disabled
        {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(())
    }

    pub async fn isolate(&self, binding: &Binding) -> Result<Terminal> {
        if self.identity().await? != binding.engine {
            return Err(FleetCatalogError::Conflict);
        }
        let path = format!("/containers/{}", binding.container);
        let v = self.inspect(&binding.boot, &binding.container).await?;
        self.check_start(binding, &v)?;
        // A single-use container is bound only with automatic restart disabled.
        // Refuse externally changed policy instead of racing a successor boot.
        if !restart_disabled(&v) {
            return Err(FleetCatalogError::Conflict);
        }
        self.call(
            "POST",
            &format!("{path}/update"),
            Some(json!({"RestartPolicy":{"Name":"no","MaximumRetryCount":0}})),
            200,
        )
        .await?;
        let v = self.inspect(&binding.boot, &binding.container).await?;
        self.check_start(binding, &v)?;
        if !restart_disabled(&v) {
            return Err(FleetCatalogError::Unconfirmed);
        }
        if v["State"]["Running"] == true {
            // SIGKILL also handles paused tasks in the Linux Engine. Its reply
            // is not closure evidence; inspect below must prove terminal state.
            self.call("POST", &format!("{path}/kill?signal=SIGKILL"), None, 204)
                .await?;
        }
        let v = self.inspect(&binding.boot, &binding.container).await?;
        self.check_start(binding, &v)?;
        if !restart_disabled(&v)
            || v["State"]["Running"] != false
            || v["State"]["Restarting"] != false
            || v["State"]["Paused"] != false
            || v["State"]["Dead"] != false
            || v["State"]["Status"] != "exited"
            || v["State"]["Pid"].as_u64() != Some(0)
        {
            return Err(FleetCatalogError::Unconfirmed);
        }
        let finished = string(&v["State"]["FinishedAt"])?;
        if !records::timestamp(finished) {
            return Err(FleetCatalogError::Unconfirmed);
        }
        Ok(Terminal {
            finished: finished.into(),
            exit_code: v["State"]["ExitCode"]
                .as_i64()
                .ok_or(FleetCatalogError::Corrupt("Docker exit status"))?,
        })
    }
}

fn string(v: &Value) -> Result<&str> {
    v.as_str()
        .ok_or(FleetCatalogError::Corrupt("Docker response field"))
}

fn restart_disabled(v: &Value) -> bool {
    v["HostConfig"]["RestartPolicy"]["Name"] == "no"
        && v["HostConfig"]["RestartPolicy"]["MaximumRetryCount"].as_u64() == Some(0)
}

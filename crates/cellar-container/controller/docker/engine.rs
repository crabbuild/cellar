// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical operations use full immutable IDs and independently checked labels.
use super::{journal, DockerBackend, DockerOptions, Reply, Request};
use crate::Operation;
use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::{
    container::{EngineFiles, FilePath},
    docker::{ContainerId, Docker},
    sdk::containers::{ExecutionClaim, ExecutionOperation},
};
use serde_json::{json, Value};
use std::sync::{Arc, OnceLock};
use tokio::io::AsyncReadExt;

const OWNER: &str = "cellar.execution.owner";
const KEY: &str = "cellar.execution.resource";
const GENERATION: &str = "cellar.execution.generation";
const REQUEST: &str = "cellar.execution.request";
const VOLUME: &str = "cellar.execution.volume";
const ENVIRONMENT: &str = "cellar.execution.environment";
const CONTROL_BYTES: usize = 1024 * 1024;

pub(super) struct Engine {
    docker: Docker,
    options: DockerOptions,
    identity: OnceLock<String>,
}

impl Engine {
    pub fn new(options: &DockerOptions) -> Self {
        Self {
            docker: Docker::new(options.socket.clone()),
            options: options.clone(),
            identity: OnceLock::new(),
        }
    }

    async fn call(&self, method: &str, path: &str, body: Option<Value>) -> Result<Value> {
        if self.identity.get().is_some() {
            self.check_identity().await?;
        }
        let reply = self
            .docker
            .call_bounded(method, path, body, CONTROL_BYTES)
            .await?;
        ensure!(
            reply.status.is_success(),
            "Docker operation refused: {}",
            reply.message()
        );
        if reply.body.is_empty() {
            Ok(Value::Null)
        } else {
            reply.json()
        }
    }

    async fn check_identity(&self) -> Result<()> {
        let expected = self
            .identity
            .get()
            .context("Docker daemon identity not established")?;
        let reply = self
            .docker
            .call_bounded("GET", "/info", None, CONTROL_BYTES)
            .await?;
        ensure!(reply.status.is_success() && reply.json()?["ID"].as_str()==Some(expected),
            "Docker daemon identity changed; original execution cannot be fenced on a replacement engine");
        Ok(())
    }

    pub async fn check(&self) -> Result<String> {
        let engine = self.call("GET", "/info", None).await?;
        ensure!(
            engine["OSType"].as_str() == Some("linux"),
            "Docker execution requires Linux process isolation"
        );
        ensure!(
            engine["MemoryLimit"].as_bool() == Some(true)
                && engine["SwapLimit"].as_bool() == Some(true)
                && engine["CpuCfsQuota"].as_bool() == Some(true)
                && engine["PidsLimit"].as_bool() == Some(true),
            "Docker kernel cannot enforce the selected execution bounds"
        );
        let id = engine["ID"]
            .as_str()
            .context("Docker daemon identity missing")?;
        ensure!(
            !id.is_empty() && id.len() <= 256,
            "invalid Docker daemon identity"
        );
        let image = self
            .call("GET", &format!("/images/{}/json", self.options.image), None)
            .await?;
        ensure!(
            image["Id"].as_str() == Some(&self.options.image),
            "Docker image identity changed"
        );
        ensure!(
            image["Config"]["Volumes"].is_null()
                || image["Config"]["Volumes"]
                    .as_object()
                    .is_some_and(|volumes| volumes.is_empty()),
            "Docker scaffold image declares uncontrolled volumes"
        );
        ensure!(
            image["Config"]["Env"].is_null()
                || image["Config"]["Env"]
                    .as_array()
                    .is_some_and(|env| env.iter().all(|entry| entry
                        .as_str()
                        .is_some_and(|entry| entry.starts_with("PATH=")))),
            "Docker scaffold image has application startup environment"
        );
        self.identity
            .set(id.to_owned())
            .map_err(|_| anyhow::anyhow!("Docker daemon identity already initialized"))?;
        Ok(id.into())
    }

    pub async fn resources(&self, key: Option<&str>) -> Result<Vec<ContainerId>> {
        let mut labels = vec![format!("{OWNER}={}", self.options.owner)];
        if let Some(key) = key {
            labels.push(format!("{KEY}={key}"));
        }
        let filters = serde_json::to_string(&json!({"label":labels}))?;
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("all", "true")
            .append_pair("filters", &filters)
            .finish();
        let value = self
            .call("GET", &format!("/containers/json?{query}"), None)
            .await?;
        let entries = value
            .as_array()
            .context("invalid Docker resource inventory")?;
        ensure!(
            entries.len() <= self.options.max_resources,
            "Docker resource inventory exceeds bound"
        );
        entries
            .iter()
            .map(|entry| {
                ensure!(
                    entry["Labels"][OWNER].as_str() == Some(&self.options.owner)
                        && key.is_none_or(|key| entry["Labels"][KEY].as_str() == Some(key)),
                    "Docker inventory ownership differs"
                );
                ContainerId::new(entry["Id"].as_str().context("Docker resource ID missing")?)
            })
            .collect()
    }

    pub async fn volumes(&self) -> Result<Vec<String>> {
        let filters =
            serde_json::to_string(&json!({"label":[format!("{OWNER}={}",self.options.owner)]}))?;
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("filters", &filters)
            .finish();
        let value = self.call("GET", &format!("/volumes?{query}"), None).await?;
        if value["Volumes"].is_null() {
            return Ok(Vec::new());
        }
        let entries = value["Volumes"]
            .as_array()
            .context("invalid Docker volume inventory")?;
        ensure!(
            entries.len() <= self.options.max_resources,
            "Docker volume inventory exceeds bound"
        );
        entries
            .iter()
            .map(|entry| {
                ensure!(
                    entry["Labels"][OWNER].as_str() == Some(&self.options.owner),
                    "Docker volume owner differs"
                );
                Ok(entry["Name"]
                    .as_str()
                    .context("Docker volume name missing")?
                    .to_owned())
            })
            .collect()
    }

    fn volume_name(&self, claim: &ExecutionClaim) -> String {
        format!(
            "cellar-{}-{}-{}",
            self.options.owner,
            journal::key(claim),
            claim.checkpoint().record.request_id
        )
    }

    async fn volume(&self, claim: &ExecutionClaim) -> Result<Option<Value>> {
        self.check_identity().await?;
        let name = self.volume_name(claim);
        let reply = self
            .docker
            .call_bounded("GET", &format!("/volumes/{name}"), None, CONTROL_BYTES)
            .await?;
        if reply.status.as_u16() == 404 {
            return Ok(None);
        }
        ensure!(
            reply.status.is_success(),
            "Docker volume inspect refused: {}",
            reply.message()
        );
        let value = reply.json()?;
        let record = &claim.checkpoint().record;
        ensure!(
            value["Name"].as_str() == Some(&name)
                && value["Driver"].as_str() == Some("local")
                && value["Labels"][OWNER].as_str() == Some(&self.options.owner)
                && value["Labels"][KEY].as_str() == Some(journal::key(claim).as_str())
                && value["Labels"][GENERATION].as_str()
                    == Some(record.generation.to_string().as_str())
                && value["Labels"][REQUEST].as_str() == Some(&record.request_id),
            "Docker volume ownership differs"
        );
        Ok(Some(value))
    }

    async fn inspect(&self, id: &ContainerId, key: Option<&str>) -> Result<Option<Value>> {
        self.check_identity().await?;
        let reply = self
            .docker
            .call_bounded(
                "GET",
                &format!("/containers/{id}/json"),
                None,
                CONTROL_BYTES,
            )
            .await?;
        if reply.status.as_u16() == 404 {
            return Ok(None);
        }
        ensure!(
            reply.status.is_success(),
            "Docker inspect refused: {}",
            reply.message()
        );
        let value = reply.json()?;
        ensure!(
            value["Id"].as_str() == Some(id.as_str())
                && value["Config"]["Labels"][OWNER].as_str() == Some(&self.options.owner)
                && key.is_none_or(|key| value["Config"]["Labels"][KEY].as_str() == Some(key))
                && value["Config"]["Image"].as_str() == Some(&self.options.image)
                && value["HostConfig"]["RestartPolicy"]["Name"].as_str() == Some("no")
                && value["HostConfig"]["ReadonlyRootfs"].as_bool() == Some(true)
                && value["HostConfig"]["NetworkMode"].as_str() == Some("none")
                && value["HostConfig"]["Privileged"].as_bool() == Some(false)
                && value["HostConfig"]["CapDrop"] == json!(["ALL"])
                && value["Config"]["Entrypoint"] == json!(["/bin/sleep"])
                && value["Config"]["Cmd"] == json!(["2147483647"])
                && value["Config"]["Env"]
                    == json!(["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"]),
            "Docker resource ownership, scaffold or root policy differs"
        );
        Ok(Some(value))
    }

    pub async fn stop_all(&self, key: Option<&str>) -> Result<()> {
        for id in self.resources(key).await? {
            let Some(value) = self.inspect(&id, key).await? else {
                continue;
            };
            if value["State"]["Running"].as_bool() == Some(true) {
                let reply = self
                    .docker
                    .call_bounded(
                        "POST",
                        &format!("/containers/{id}/kill?signal=KILL"),
                        None,
                        CONTROL_BYTES,
                    )
                    .await?;
                ensure!(
                    reply.status.is_success() || matches!(reply.status.as_u16(), 404 | 409),
                    "Docker process revocation refused: {}",
                    reply.message()
                );
                // Engine wait joins the container's process namespace, including
                // detached exec children; a kill request alone is not a fence.
                let reply = self
                    .docker
                    .call_bounded(
                        "POST",
                        &format!("/containers/{id}/wait?condition=not-running"),
                        None,
                        CONTROL_BYTES,
                    )
                    .await?;
                ensure!(
                    reply.status.is_success() || reply.status.as_u16() == 404,
                    "Docker process join refused: {}",
                    reply.message()
                );
            }
            if let Some(value) = self.inspect(&id, key).await? {
                ensure!(
                    value["State"]["Running"].as_bool() == Some(false)
                        && value["State"]["Pid"].as_u64() == Some(0),
                    "Docker old execution is not joined"
                );
            }
        }
        Ok(())
    }

    pub fn validate(&self, request: &Request) -> Result<()> {
        super::validate_parameters(Operation::parse(&request.operation)?, &request.input)
    }

    pub async fn resume_scaffold(
        &self,
        backend: &DockerBackend,
        claim: &ExecutionClaim,
    ) -> Result<()> {
        let record = &claim.checkpoint().record;
        if record.resource_id.is_none()
            || record.intent != cellar::sdk::containers::BindingIntent::Running
        {
            return Ok(());
        }
        let (id, state) = self.resource(claim).await?;
        ensure!(
            state["State"]["Running"].as_bool() == Some(false),
            "old Docker process tree remains running"
        );
        // This is a fixed read-only infrastructure process, with no caller
        // entrypoint, command or environment. It restores the execution
        // workspace after fencing; it cannot replay an application callback.
        backend.journal.view(|state| {
            let slot = state
                .slots
                .get(&journal::key(claim))
                .context("Docker fence missing")?;
            ensure!(
                !slot.active && slot.high == *claim,
                "Docker scaffold fence changed"
            );
            Ok(())
        })?;
        self.call("POST", &format!("/containers/{id}/start"), None)
            .await?;
        Ok(())
    }

    fn before(
        &self,
        backend: &DockerBackend,
        claim: &ExecutionClaim,
        operation: &ExecutionOperation,
    ) -> Result<()> {
        backend.check_operation(claim, operation)
    }

    async fn resource(&self, claim: &ExecutionClaim) -> Result<(ContainerId, Value)> {
        let record = &claim.checkpoint().record;
        let id = ContainerId::new(
            record
                .resource_id
                .as_deref()
                .context("Docker operation lacks immutable resource")?,
        )?;
        let value = self
            .inspect(&id, Some(&journal::key(claim)))
            .await?
            .context("original Docker resource is absent")?;
        let volume = self.volume_name(claim);
        ensure!(
            value["Config"]["Labels"][GENERATION].as_str()
                == Some(record.generation.to_string().as_str())
                && value["Config"]["Labels"][REQUEST].as_str() == Some(&record.request_id)
                && value["Config"]["Labels"][VOLUME].as_str() == Some(&volume)
                && value["Mounts"]
                    .as_array()
                    .is_some_and(|mounts| mounts.len() == 1
                        && mounts[0]["Type"].as_str() == Some("volume")
                        && mounts[0]["Name"].as_str() == Some(&volume)
                        && mounts[0]["Destination"].as_str() == Some("/workspace")),
            "Docker resource belongs to another generation or volume"
        );
        self.volume(claim)
            .await?
            .context("original Docker workspace volume missing")?;
        Ok((id, value))
    }

    async fn start(
        &self,
        backend: &DockerBackend,
        claim: &ExecutionClaim,
        operation: &ExecutionOperation,
        request: &Request,
    ) -> Result<Reply> {
        self.before(backend, claim, operation)?;
        ensure!(
            self.resources(None).await?.len() < self.options.max_resources,
            "Docker resource capacity exhausted"
        );
        let record = &claim.checkpoint().record;
        let volume = self.volume_name(claim);
        let mut labels = json!({ OWNER:self.options.owner, KEY:journal::key(claim), GENERATION:record.generation.to_string(), REQUEST:record.request_id, VOLUME:volume });
        if let Some(extra) = request.input["options"]["labels"].as_array() {
            for pair in extra {
                labels[pair[0].as_str().context("invalid Docker label")?] = pair[1].clone();
            }
        }
        let env = request.input["options"]["env"]
            .as_array()
            .map(|pairs| {
                pairs
                    .iter()
                    .map(|pair| {
                        Ok(format!(
                            "{}={}",
                            pair[0]
                                .as_str()
                                .context("invalid Docker environment name")?,
                            pair[1]
                                .as_str()
                                .context("invalid Docker environment value")?
                        ))
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default();
        labels[ENVIRONMENT] = json!(serde_json::to_string(&env)?);
        ensure!(
            self.volumes().await?.len() < self.options.max_resources
                && self.volume(claim).await?.is_none(),
            "Docker workspace volume already exists or capacity is exhausted"
        );
        self.before(backend, claim, operation)?;
        self.call(
            "POST",
            "/volumes/create",
            Some(json!({"Name":volume,"Driver":"local","Labels":labels})),
        )
        .await?;
        self.before(backend, claim, operation)?;
        let name = volume.clone();
        let created = self.call("POST", &format!("/containers/create?name={name}"), Some(json!({
            "Image":self.options.image,"Entrypoint":["/bin/sleep"],"Cmd":["2147483647"],"User":"0:0",
            "Env":["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"],
            "Labels":labels,"HostConfig":{"NetworkMode":"none","RestartPolicy":{"Name":"no"},
            "CapDrop":["ALL"],"SecurityOpt":["no-new-privileges:true"],"Privileged":false,
            "ReadonlyRootfs":true,"Mounts":[{"Type":"volume","Source":volume,"Target":"/workspace","VolumeOptions":{"NoCopy":true}}],
            "Memory":self.options.memory_bytes,"MemorySwap":self.options.memory_bytes,
            "NanoCpus":self.options.nano_cpus,"PidsLimit":self.options.pids,"Init":true}
        }))).await?;
        let id = ContainerId::new(created["Id"].as_str().context("Docker create ID missing")?)?;
        self.inspect(&id, Some(&journal::key(claim)))
            .await?
            .context("created Docker resource missing")?;
        self.before(backend, claim, operation)?;
        self.call("POST", &format!("/containers/{id}/start"), None)
            .await?;
        Ok(Reply {
            format: 1,
            resource_id: id.to_string(),
            value: json!(record.generation.to_string()),
        })
    }

    async fn exec(
        &self,
        backend: &DockerBackend,
        claim: &ExecutionClaim,
        operation: &ExecutionOperation,
        id: &ContainerId,
        state: &Value,
        input: &Value,
    ) -> Result<Value> {
        // Re-enter only the fixed idle scaffold after a joined fence. The
        // application command below is always a new, source-proven operation;
        // neither an application entrypoint nor an old command is replayed.
        if state["State"]["Running"].as_bool() != Some(true) {
            self.before(backend, claim, operation)?;
            self.call("POST", &format!("/containers/{id}/start"), None)
                .await?;
        }
        self.before(backend, claim, operation)?;
        let options = &input["options"];
        ensure!(
            options["pty"].is_null(),
            "buffered Docker profile has no interactive terminal"
        );
        let mut env: Vec<String> = serde_json::from_str(
            state["Config"]["Labels"][ENVIRONMENT]
                .as_str()
                .context("Docker application environment missing")?,
        )?;
        let overrides = options["env"]
            .as_array()
            .map(|pairs| {
                pairs
                    .iter()
                    .map(|pair| {
                        Ok(format!(
                            "{}={}",
                            pair[0].as_str().context("invalid exec environment name")?,
                            pair[1].as_str().context("invalid exec environment value")?
                        ))
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default();
        env.extend(overrides);
        let created = self.call("POST", &format!("/containers/{id}/exec"), Some(json!({
            "AttachStdout":true,"AttachStderr":true,"AttachStdin":false,"Tty":false,
            "Cmd":options["cmd"],"Env":env,"WorkingDir":options["cwd"].as_str().unwrap_or(""),"User":options["user"].as_str().unwrap_or("")
        }))).await?;
        let exec = ContainerId::new(created["Id"].as_str().context("Docker exec ID missing")?)?;
        self.before(backend, claim, operation)?;
        let mut stream = self
            .docker
            .hijack(
                &format!("/exec/{exec}/start"),
                json!({"Detach":false,"Tty":false}),
            )
            .await?;
        let max = usize::try_from(
            input["max_bytes"]
                .as_u64()
                .context("exec output bound missing")?,
        )?;
        ensure!(
            max <= self.options.max_body_bytes,
            "exec output exceeds backend budget"
        );
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        loop {
            let mut header = [0u8; 8];
            if stream.read(&mut header[..1]).await? == 0 {
                break;
            }
            stream.read_exact(&mut header[1..]).await?;
            ensure!(
                matches!(header[0], 1 | 2) && header[1..4] == [0, 0, 0],
                "invalid Docker exec output frame"
            );
            let length = u32::from_be_bytes(header[4..].try_into()?) as usize;
            ensure!(
                length <= max.saturating_sub(stdout.len() + stderr.len()),
                "Docker exec output exceeds budget"
            );
            let combined = options["combined"].as_bool() == Some(true);
            let target = if header[0] == 2 && !combined {
                &mut stderr
            } else {
                &mut stdout
            };
            let offset = target.len();
            target.resize(offset + length, 0);
            stream.read_exact(&mut target[offset..]).await?;
        }
        let result = self
            .call("GET", &format!("/exec/{exec}/json"), None)
            .await?;
        ensure!(
            result["Running"].as_bool() == Some(false)
                && result["ContainerID"].as_str() == Some(id.as_str()),
            "Docker exec has not joined its original resource"
        );
        Ok(
            json!({"exit_code":result["ExitCode"].as_i64().context("Docker exec exit missing")?,"stdout":STANDARD.encode(stdout),"stderr":STANDARD.encode(stderr)}),
        )
    }

    pub async fn execute(
        &self,
        backend: &Arc<DockerBackend>,
        claim: &ExecutionClaim,
        operation: &ExecutionOperation,
        request: Request,
    ) -> Result<Reply> {
        let method = Operation::parse(&request.operation)?;
        if method == Operation::Start {
            return self.start(backend, claim, operation, &request).await;
        }
        self.before(backend, claim, operation)?;
        let id = ContainerId::new(
            claim
                .checkpoint()
                .record
                .resource_id
                .as_deref()
                .context("Docker resource missing")?,
        )?;
        if method == Operation::Destroy {
            if self
                .inspect(&id, Some(&journal::key(claim)))
                .await?
                .is_some()
            {
                let (owned, _) = self.resource(claim).await?;
                self.before(backend, claim, operation)?;
                self.call(
                    "DELETE",
                    &format!("/containers/{owned}?force=true&v=true"),
                    None,
                )
                .await?;
            }
            ensure!(
                self.inspect(&id, Some(&journal::key(claim)))
                    .await?
                    .is_none(),
                "Docker deletion not confirmed"
            );
            if self.volume(claim).await?.is_some() {
                self.before(backend, claim, operation)?;
                self.call(
                    "DELETE",
                    &format!("/volumes/{}", self.volume_name(claim)),
                    None,
                )
                .await?;
            }
            ensure!(
                self.volume(claim).await?.is_none(),
                "Docker workspace deletion not confirmed"
            );
            return Ok(Reply {
                format: 1,
                resource_id: id.to_string(),
                value: Value::Null,
            });
        }
        let (id, state) = self.resource(claim).await?;
        self.before(backend, claim, operation)?;
        let files = EngineFiles::new(self.docker.clone(), id.clone());
        let input = &request.input;
        let path = || FilePath::new(input["path"].as_str().context("container path missing")?);
        let max = || -> Result<usize> {
            Ok(usize::try_from(
                input["max_bytes"]
                    .as_u64()
                    .context("container byte bound missing")?,
            )?)
        };
        let value = match method {
            Operation::Running => json!(state["State"]["Running"]
                .as_bool()
                .context("Docker running state missing")?),
            Operation::Monitor => self
                .call(
                    "POST",
                    &format!("/containers/{id}/wait?condition=not-running"),
                    None,
                )
                .await?["StatusCode"]
                .clone(),
            Operation::Signal => {
                let signal = input["signal"].as_u64().context("Docker signal missing")?;
                self.call(
                    "POST",
                    &format!("/containers/{id}/kill?signal={signal}"),
                    None,
                )
                .await?;
                Value::Null
            }
            Operation::ExecOutput => {
                self.exec(backend, claim, operation, &id, &state, input)
                    .await?
            }
            Operation::ReadFile => json!(STANDARD.encode(files.read(path()?, max()?).await?)),
            Operation::WriteFile => {
                let data =
                    STANDARD.decode(input["data"].as_str().context("container data missing")?)?;
                files
                    .write(
                        path()?,
                        &data,
                        u32::try_from(input["mode"].as_u64().context("container mode missing")?)?,
                    )
                    .await?;
                Value::Null
            }
            Operation::StatFile => serde_json::to_value(files.stat(path()?).await?)?,
            Operation::ListFiles => serde_json::to_value(
                files
                    .list(
                        path()?,
                        max()?,
                        usize::try_from(
                            input["max_entries"]
                                .as_u64()
                                .context("container entry bound missing")?,
                        )?,
                    )
                    .await?,
            )?,
            _ => anyhow::bail!("unsupported Docker execution operation"),
        };
        Ok(Reply {
            format: 1,
            resource_id: id.to_string(),
            value,
        })
    }
}

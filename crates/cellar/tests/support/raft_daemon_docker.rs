// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Constrained Linux daemon voters with disjoint retained disks in the lab VM.
use super::*;

const RUN: &str = "CELLAR_RAFT_DOCKER_RUN";
const IMAGE: &str = "CELLAR_RAFT_DOCKER_IMAGE";
const MEMORY_GIB: &str = "CELLAR_RAFT_DOCKER_MEMORY_GIB";
const ENGINE_SOCKET: &str = "/run/docker.sock";
const LABEL: &str = "io.cellar.daemon.run";
pub(super) const VOTER: &str = "CELLAR_RAFT_DOCKER_VOTER";
const INSPECT: &str = r#"{"id":{{json .Id}},"image":{{json .Image}},"run":{{json (index .Config.Labels "io.cellar.daemon.run")}},"running":{{json .State.Running}},"pid":{{json .State.Pid}},"exit_code":{{json .State.ExitCode}},"cpu":{{json .HostConfig.NanoCpus}},"memory":{{json .HostConfig.Memory}},"swap":{{json .HostConfig.MemorySwap}},"restart":{{json .HostConfig.RestartPolicy.Name}},"network":{{json .HostConfig.NetworkMode}},"mounts":{{json .Mounts}}}"#;

pub(super) fn enabled() -> bool {
    std::env::var_os(RUN).is_some()
}

pub(super) fn memory_bytes() -> Result<u64> {
    let gib: u64 = match std::env::var(MEMORY_GIB) {
        Ok(value) => value.parse().context("voter memory GiB")?,
        Err(std::env::VarError::NotPresent) => 3,
        Err(error) => return Err(error.into()),
    };
    ensure!(
        matches!(gib, 2 | 3),
        "voter memory must be two or three GiB"
    );
    Ok(gib * 1024u64.pow(3))
}

fn fleet_owner(run: &str) -> String {
    format!("daemon-{run}")
}

pub(super) fn disk(root: &Path, index: usize) -> PathBuf {
    if !enabled() {
        return root.join(format!("disk-{index}"));
    }
    // Keep the individual disks outside the shared process rendezvous mount.
    let mut name = root
        .file_name()
        .expect("fixture temporary root")
        .to_os_string();
    name.push(format!("-disk-{index}"));
    root.with_file_name(name)
}

fn docker() -> Command {
    let mut command = Command::new("docker");
    command
        .env_remove("DOCKER_CONTEXT")
        .env_remove("DOCKER_HOST")
        .args(["--host", "unix:///var/run/docker.sock"]);
    command
}

fn output(command: &mut Command, operation: &str) -> Result<String> {
    let result = command
        .output()
        .with_context(|| format!("Docker {operation}"))?;
    ensure!(
        result.status.success(),
        "Docker {operation} failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    Ok(String::from_utf8(result.stdout)?.trim().to_owned())
}

fn lower_hex(value: &str, bytes: usize) -> bool {
    value.len() == bytes * 2
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

pub(super) struct Voter {
    id: String,
    run: String,
    image: String,
    root: PathBuf,
    disk: PathBuf,
    mode: String,
    repair_worker: bool,
    memory_bytes: u64,
}

impl Voter {
    pub(super) fn create(root: &Path, mode: &str, child: &Command) -> Result<Self> {
        ensure!(
            cfg!(target_os = "linux"),
            "Docker voter controller must run inside the Linux lab VM"
        );
        let run = std::env::var(RUN)?;
        let image = std::env::var(IMAGE)?;
        ensure!(lower_hex(&run, 8), "invalid owned Docker run ID");
        ensure!(
            image
                .strip_prefix("sha256:")
                .is_some_and(|id| lower_hex(id, 32)),
            "Docker voter requires an immutable image ID"
        );
        let (retained, index) = match managed::retained_index(mode) {
            Some(index) => (true, index.parse::<usize>()?),
            None => (
                false,
                mode.strip_prefix("cluster-")
                    .context("managed voter mode")?
                    .parse()?,
            ),
        };
        ensure!(
            index <= usize::from(managed::standby::INDEX) && root.is_absolute() && root.is_dir(),
            "invalid voter inventory"
        );
        let repair_enabled = std::env::var_os(managed::repair::ENABLED).is_some();
        let repair_worker = repair_enabled && index == usize::from(managed::repair::WORKER);
        let disk = disk(root, index);
        if retained {
            ensure!(disk.join("raft").is_dir(), "retained voter disk is missing");
        } else {
            std::fs::create_dir(&disk)?;
        }
        let catalog = std::fs::read_to_string(root.join(managed::DOCKER_CATALOG))?;
        ensure!(lower_hex(&catalog, 32), "invalid rooted Fleet catalog ID");
        let fleet_node = hex::encode(managed::node(u8::try_from(index)?)?.bytes());
        let owner = output(Command::new("id").arg("-u"), "guest user lookup")?;
        let group = output(Command::new("id").arg("-g"), "guest group lookup")?;
        let mut command = docker();
        let memory_bytes = memory_bytes()?;
        let memory = memory_bytes.to_string();
        command.args([
            "create",
            "--name",
            &format!("cellar-daemon-{run}-{mode}"),
            "--label",
            &format!("{LABEL}={run}"),
            "--label",
            &format!("io.cellar.fleet.owner={}", fleet_owner(&run)),
            "--label",
            &format!("io.cellar.fleet.catalog={catalog}"),
            "--label",
            &format!("io.cellar.fleet.node={fleet_node}"),
            "--label",
            "io.cellar.fleet.boot-mode=single-use",
            "--label",
            "io.cellar.fleet.workers=contained",
            "--network",
            "host",
            "--restart",
            "no",
            "--cpus",
            "1",
            "--memory",
            &memory,
            "--memory-swap",
            &memory,
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--read-only",
            "--tmpfs",
            "/tmp:rw,nosuid,nodev,size=134217728",
            "--user",
            &format!("{owner}:{group}"),
            "--log-driver",
            "local",
            "--log-opt",
            "max-size=8m",
            "--log-opt",
            "max-file=2",
        ]);
        for path in [root, disk.as_path()] {
            let path = path.to_str().context("UTF-8 lab mount")?;
            ensure!(!path.contains(','), "lab mount cannot contain a comma");
            command.args(["--mount", &format!("type=bind,src={path},dst={path}")]);
        }
        for (key, value) in child.get_envs() {
            if let Some(value) = value {
                command.env(key, value).arg("--env").arg(key);
            }
        }
        command.args([
            "--env",
            &format!("{MEMORY_GIB}={}", memory_bytes / 1024u64.pow(3)),
            "--env",
            "CELLAR_RAFT_OPERATOR_CLI=/usr/local/bin/cellar",
            "--env",
            "CELLAR_RAFT_DOCKER_VOTER=1",
        ]);
        if repair_enabled {
            command.args(["--env", "RUST_LOG=warn,cellar_raft_repair=debug"]);
        }
        if repair_worker {
            let engine_group = output(
                Command::new("stat").args(["-c", "%g", ENGINE_SOCKET]),
                "guest Docker socket group",
            )?;
            ensure!(
                engine_group.parse::<u32>().is_ok(),
                "invalid Docker socket group"
            );
            command.args([
                "--group-add",
                &engine_group,
                "--mount",
                &format!("type=bind,src={ENGINE_SOCKET},dst={ENGINE_SOCKET},readonly"),
                "--env",
                "CELLAR_RAFT_DAEMON_REPAIR_GATE=1",
            ]);
        }
        command.arg(&image).args(child.get_args());
        let id = output(&mut command, "create owned voter")?;
        ensure!(lower_hex(&id, 32), "Docker voter identity is ambiguous");
        let voter = Self {
            id,
            run,
            image,
            root: root.into(),
            disk,
            mode: mode.into(),
            repair_worker,
            memory_bytes,
        };
        ensure!(
            voter.inspect()?["running"] == false,
            "voter was started before qualification"
        );
        Ok(voter)
    }

    fn inspect(&self) -> Result<Value> {
        let raw = output(
            docker().args([
                "inspect",
                "--type",
                "container",
                "--format",
                INSPECT,
                &self.id,
            ]),
            "inspect owned voter",
        )?;
        let state: Value = serde_json::from_str(&raw)?;
        ensure!(
            state["id"] == self.id && state["run"] == self.run && state["image"] == self.image,
            "foreign voter container"
        );
        ensure!(
            state["network"] == "host"
                && state["restart"] == "no"
                && state["cpu"] == 1_000_000_000u64
                && state["memory"] == self.memory_bytes
                && state["swap"] == state["memory"],
            "voter cgroup or restart settings differ"
        );
        let mounts = state["mounts"].as_array().context("voter mounts absent")?;
        ensure!(
            mounts.len() == 2 + usize::from(self.repair_worker),
            "unexpected voter mount inventory"
        );
        if self.repair_worker {
            ensure!(
                mounts.iter().any(|mount| mount["Type"] == "bind"
                    && mount["Source"] == ENGINE_SOCKET
                    && mount["Destination"] == ENGINE_SOCKET
                    && mount["RW"] == false),
                "repair worker Docker socket mount differs"
            );
        }
        for path in [&self.root, &self.disk] {
            let path = path.to_str().context("UTF-8 voter mount")?;
            ensure!(
                mounts.iter().any(|m| m["Type"] == "bind"
                    && m["Source"] == path
                    && m["Destination"] == path
                    && m["RW"] == true),
                "voter storage mount differs"
            );
        }
        Ok(state)
    }

    pub(super) fn start(&self, log: File, diagnostics: File) -> Result<Child> {
        Ok(docker()
            .args(["start", "--attach", &self.id])
            .stdout(log)
            .stderr(diagnostics)
            .spawn()?)
    }

    pub(super) fn fleet_identity(&self) -> Result<(String, cellar::docker::ContainerId)> {
        self.inspect()?;
        Ok((
            fleet_owner(&self.run),
            cellar::docker::ContainerId::new(&self.id)?,
        ))
    }

    pub(super) fn terminate(&self) -> Result<()> {
        ensure!(self.inspect()?["running"] == true, "voter is not running");
        output(
            docker().args(["kill", "--signal", "TERM", &self.id]),
            "stop waiting standby",
        )?;
        Ok(())
    }

    pub(super) fn running(&self) -> Result<()> {
        let mut state = self.inspect()?;
        ensure!(state["running"] == true, "voter exited before readiness");
        let limits = output(
            docker().args([
                "exec",
                &self.id,
                "cat",
                "/sys/fs/cgroup/cpu.max",
                "/sys/fs/cgroup/memory.max",
                "/sys/fs/cgroup/memory.swap.max",
            ]),
            "read effective voter cgroup limits",
        )?;
        let values = limits
            .split_whitespace()
            .map(str::parse::<u64>)
            .collect::<Result<Vec<_>, _>>()?;
        let [quota, period, memory, swap] = values.as_slice() else {
            anyhow::bail!("invalid voter cgroup limit inventory");
        };
        ensure!(
            *quota > 0 && quota == period && *memory == self.memory_bytes && *swap == 0,
            "effective voter cgroup limits differ"
        );
        state["cgroup"] = serde_json::json!({"cpu_quota_us":quota, "cpu_period_us":period, "memory_bytes":memory, "swap_bytes":swap});
        std::fs::write(
            self.root.join(format!("{}-docker.json", self.mode)),
            serde_json::to_vec_pretty(&state)?,
        )?;
        Ok(())
    }

    pub(super) fn pid(&self) -> Result<u64> {
        let pid = self.inspect()?["pid"]
            .as_u64()
            .context("voter guest PID absent")?;
        ensure!(pid > 0, "voter process is not running");
        Ok(pid)
    }

    pub(super) fn kill_if_running(&self) -> Result<()> {
        if self.inspect()?["running"] == true {
            output(
                docker().args(["kill", "--signal", "KILL", &self.id]),
                "kill owned voter",
            )?;
        }
        Ok(())
    }

    pub(super) fn killed(&self) -> Result<()> {
        let state = self.inspect()?;
        ensure!(
            state["running"] == false && state["exit_code"] == 137,
            "expected stopped SIGKILL voter"
        );
        std::fs::write(
            self.root.join(format!("{}-killed-docker.json", self.mode)),
            serde_json::to_vec_pretty(&state)?,
        )?;
        Ok(())
    }
}

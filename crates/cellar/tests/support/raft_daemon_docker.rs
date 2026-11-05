// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Constrained Linux daemon voters with disjoint retained disks in the lab VM.
use super::*;

const RUN: &str = "CELLAR_RAFT_DOCKER_RUN";
const IMAGE: &str = "CELLAR_RAFT_DOCKER_IMAGE";
const LABEL: &str = "io.cellar.daemon.run";
pub(super) const VOTER: &str = "CELLAR_RAFT_DOCKER_VOTER";
const INSPECT: &str = r#"{"id":{{json .Id}},"image":{{json .Image}},"run":{{json (index .Config.Labels "io.cellar.daemon.run")}},"running":{{json .State.Running}},"exit_code":{{json .State.ExitCode}},"cpu":{{json .HostConfig.NanoCpus}},"memory":{{json .HostConfig.Memory}},"swap":{{json .HostConfig.MemorySwap}},"restart":{{json .HostConfig.RestartPolicy.Name}},"network":{{json .HostConfig.NetworkMode}},"mounts":{{json .Mounts}}}"#;

pub(super) fn enabled() -> bool {
    std::env::var_os(RUN).is_some()
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
        let (retained, index) = match mode.strip_prefix("cluster-resume-") {
            Some(index) => (true, index.parse::<usize>()?),
            None => (
                false,
                mode.strip_prefix("cluster-")
                    .context("managed voter mode")?
                    .parse()?,
            ),
        };
        ensure!(
            index < 3 && root.is_absolute() && root.is_dir(),
            "invalid voter inventory"
        );
        let disk = disk(root, index);
        if retained {
            ensure!(disk.join("raft").is_dir(), "retained voter disk is missing");
        } else {
            std::fs::create_dir(&disk)?;
        }
        let owner = output(Command::new("id").arg("-u"), "guest user lookup")?;
        let group = output(Command::new("id").arg("-g"), "guest group lookup")?;
        let mut command = docker();
        command.args([
            "create",
            "--name",
            &format!("cellar-daemon-{run}-{mode}"),
            "--label",
            &format!("{LABEL}={run}"),
            "--network",
            "host",
            "--restart",
            "no",
            "--cpus",
            "1",
            "--memory",
            "3g",
            "--memory-swap",
            "3g",
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
            "CELLAR_RAFT_OPERATOR_CLI=/usr/local/bin/cellar",
            "--env",
            "CELLAR_RAFT_DOCKER_VOTER=1",
        ]);
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
                && state["memory"] == 3 * 1024u64.pow(3)
                && state["swap"] == state["memory"],
            "voter cgroup or restart settings differ"
        );
        let mounts = state["mounts"].as_array().context("voter mounts absent")?;
        ensure!(mounts.len() == 2, "unexpected voter mount inventory");
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

    pub(super) fn running(&self) -> Result<()> {
        let state = self.inspect()?;
        ensure!(state["running"] == true, "voter exited before readiness");
        std::fs::write(
            self.root.join(format!("{}-docker.json", self.mode)),
            serde_json::to_vec_pretty(&state)?,
        )?;
        Ok(())
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

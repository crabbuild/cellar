// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Adapted for Cellar from Celld: crates/celld/dev.rs.
//! Cargo development over the retained local store and node supervision path.
#![allow(clippy::disallowed_methods)]

use super::*;
use crate::dev::{
    Console, NodeProgram, ProjectWatcher, RunningNode, ShutdownSignals, StackOptions, Store,
};
use std::fs::File;
use std::process::{Output, Stdio};
use tokio::process::Command;

struct State {
    directory: PathBuf,
    lock: File,
    hash: String,
    cleaned: bool,
}

impl State {
    fn open(project: &Path, clean: bool) -> Result<Self> {
        let hash = format!(
            "{:x}",
            Sha256::digest(project.as_os_str().as_encoded_bytes())
        );
        let root = project
            .parent()
            .context("native project has no directory")?;
        let parent = root.join(".cellar/native-dev");
        std::fs::create_dir_all(&parent)?;
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(parent.join(format!("{hash}.lock")))?;
        lock.try_lock().map_err(|error| {
            anyhow::anyhow!("another native dev supervisor owns this project: {error}")
        })?;
        let directory = parent.join(&hash);
        let mut cleaned = false;
        if clean {
            // Only this constructor supplies the project-owned path; the lock
            // is outside it, so deletion cannot release the concurrency guard.
            match std::fs::remove_dir_all(&directory) {
                Ok(()) => cleaned = true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        crate::dev::prepare_state(&directory)?;
        Ok(Self {
            directory,
            lock,
            hash,
            cleaned,
        })
    }
}

struct WorkspaceWatcher {
    roots: Vec<PathBuf>,
    watchers: Vec<ProjectWatcher>,
    ignored: Vec<glob::Pattern>,
    ignored_directories: Vec<PathBuf>,
}

impl WorkspaceWatcher {
    fn new(project: &Path, ignored: Vec<glob::Pattern>) -> Result<Self> {
        let mut watcher = Self {
            roots: Vec::new(),
            watchers: Vec::new(),
            ignored,
            ignored_directories: Vec::new(),
        };
        watcher.extend(super::build::WatchPaths {
            roots: vec![project.to_path_buf()],
            ignored: Vec::new(),
        })?;
        Ok(watcher)
    }
    fn extend(&mut self, paths: super::build::WatchPaths) -> Result<bool> {
        self.ignored_directories.extend(paths.ignored);
        self.ignored_directories.sort();
        self.ignored_directories.dedup();
        for watcher in &mut self.watchers {
            for directory in &self.ignored_directories {
                watcher.ignore_directory(directory);
            }
        }
        let mut added = false;
        for root in paths.roots {
            let root = match root.canonicalize() {
                Ok(root) => root,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            if self.roots.iter().any(|parent| root.starts_with(parent)) {
                continue;
            }
            let mut watcher = ProjectWatcher::new(&root, self.ignored.clone())?;
            for directory in &self.ignored_directories {
                watcher.ignore_directory(directory);
            }
            self.watchers.push(watcher);
            self.roots.push(root);
            added = true;
        }
        Ok(added)
    }
    async fn changed(&mut self) -> Result<()> {
        let futures: Vec<_> = self
            .watchers
            .iter_mut()
            .map(|watcher| Box::pin(watcher.changed()))
            .collect();
        futures_util::future::select_all(futures).await.0
    }
}

// Cancellation must also stop Cargo's compiler/build-script descendants.
// The node itself uses the retained process-group and graceful stop path.
struct ProcessGroup(Option<u32>);
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(pid) = self.0 {
            unsafe {
                libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
            }
        }
    }
}

async fn execute(mut command: Command) -> Result<Output> {
    command
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    crate::dev::own_child_command(&mut command);
    let child = command
        .spawn()
        .context("start native build/check process")?;
    let mut group = ProcessGroup(child.id());
    let output = child.wait_with_output().await?;
    group.0 = None;
    Ok(output)
}

async fn cancellable<T>(
    future: impl std::future::Future<Output = Result<T>>,
    signals: &mut ShutdownSignals,
) -> Option<Result<T>> {
    crate::asyncrt::select! {
        result = future => Some(result),
        _ = signals.received() => None,
    }
}

fn successful(output: Output, operation: &str) -> Result<Output> {
    anyhow::ensure!(
        output.status.success(),
        "{operation} failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stderr).trim_end(),
        String::from_utf8_lossy(&output.stdout).trim_end()
    );
    Ok(output)
}

struct Candidate {
    package: Package,
    directory: tempfile::TempDir,
    executable: PathBuf,
}

async fn build(tool: &Path, project: &Path, directory: &Path) -> Result<Package> {
    let mut command = Command::new(tool);
    command
        .arg("build-dev")
        .arg(project)
        .arg("--output")
        .arg(directory);
    successful(execute(command).await?, "native build")?;
    let package: Package = serde_json::from_slice(&std::fs::read(directory.join("package.json"))?)?;
    package.validate()?;
    Ok(package)
}

async fn check(candidate: &Candidate) -> Result<()> {
    let mut command = Command::new(&candidate.executable);
    command
        .arg("--check-native-package")
        .arg(candidate.directory.path().join("package.json"));
    let output = tokio::time::timeout(Duration::from_secs(30), execute(command))
        .await
        .context("native application package check timed out")??;
    successful(output, "native package check")?;
    Ok(())
}

async fn start(
    candidate: &Candidate,
    state: &State,
    options: &StackOptions,
    store: &Store,
    console: &Console,
) -> Result<RunningNode> {
    crate::dev::start_program(
        NodeProgram {
            path: &candidate.executable,
            native: true,
        },
        &state.directory,
        options.listener,
        options.logs,
        &state.hash,
        store,
        console,
    )
    .await
}

pub async fn run(arguments: Vec<String>) -> Result<()> {
    let Some(options) =
        crate::dev::stack_options_from_arguments(arguments, "cellar-native dev", true)?
    else {
        crate::cli_output::Output::new(crate::cli_output::Format::Text).line(format_args!(
            "cellar-native dev PROJECT.json [--host IP] [--port PORT] [--logs] [--no-watch]\n\
             Options: --clean discards this project's local state; --watch-ignore GLOB may repeat.\n\
             Rust sources, local dependencies, configuration and assets trigger verified rebuilds.\n\
             Failed builds/checks leave the current node serving. Local state survives restarts."
        ))?;
        return Ok(());
    };
    let project = options
        .project
        .context("cellar-native dev requires PROJECT.json")?
        .canonicalize()?;
    let state = State::open(&project, options.clean)?;
    let console = Console::new();
    console.header_as("cellar-native dev");
    console.detail("Project", &project.display().to_string());
    console.detail("State", &state.directory.display().to_string());
    if options.clean {
        console.detail(
            "Clean",
            if state.cleaned {
                "discarded the project's previous local state"
            } else {
                "no previous local state"
            },
        );
    }
    if !options.stack.logs {
        console.detail("Logs", "hidden (use --logs)");
    }
    if !options.stack.watch {
        console.detail("Watch", "disabled");
    }
    let mut signals = ShutdownSignals::install()?;
    let store = crate::dev::open_store(&state.directory, &console).await?;
    let tool = std::env::current_exe()?;
    let mut watcher = options
        .stack
        .watch
        .then(|| {
            WorkspaceWatcher::new(
                project.parent().unwrap(),
                options.stack.watch_ignores.clone(),
            )
        })
        .transpose()?;
    if let Some(watcher) = &mut watcher {
        let mut command = Command::new(&tool);
        command.arg("watch-roots").arg(&project);
        let Some(output) = cancellable(execute(command), &mut signals).await else {
            return Ok(());
        };
        let output = successful(output?, "resolve native workspace")?;
        watcher.extend(serde_json::from_slice(&output.stdout)?)?;
    }
    let mut current: Option<(Candidate, RunningNode)> = None;
    let mut rebuild = true;
    let result = async {
        loop {
            if rebuild {
                console.progress(if current.is_some() { "change detected; rebuilding the application" } else { "building the application" });
                let directory = tempfile::tempdir_in(&state.directory)?;
                let built = cancellable(build(&tool, &project, directory.path()), &mut signals).await;
                let Some(built) = built else { return Ok(()); };
                let roots = directory.path().join("watch-roots.json");
                rebuild = if let Some(watcher) = &mut watcher {
                    if roots.exists() { watcher.extend(serde_json::from_slice(&std::fs::read(roots)?)?)? } else { false }
                } else { false };
                let package = match built {
                    Ok(package) => package,
                    Err(error) if current.is_some() => { console.failure(&format!("rebuild failed: {error:#}")); continue; }
                    Err(error) => return Err(error),
                };
                let artifact = &package.primary.native.as_ref().unwrap().artifacts[0];
                let candidate = Candidate { executable: directory.path().join(&artifact.sha256), directory, package };
                let Some(checked) = cancellable(check(&candidate), &mut signals).await else { return Ok(()); };
                if let Err(error) = checked {
                    if current.is_none() { return Err(error); }
                    console.failure(&format!("rebuild failed: {error:#}")); continue;
                }
                if let Some((previous, _)) = &current {
                    if serde_json::to_vec(&previous.package)? == serde_json::to_vec(&candidate.package)? { continue; }
                }
                let bucket = crate::dev::open_local_bucket(&store.database)?;
                crate::wake_format::ensure_ready(&bucket).await?;
                if let Err(error) = candidate.package.publish_staged(&bucket, candidate.directory.path()).await {
                    if let Some((previous, _)) = &current {
                        previous.package.publish_staged(&bucket, previous.directory.path()).await?;
                        console.failure(&format!("publication failed: {error:#}")); continue;
                    }
                    return Err(error);
                }
                let previous = if let Some((previous, running)) = current.take() {
                    console.progress("restarting the application");
                    crate::dev::stop_running_node(running).await;
                    Some(previous)
                } else { None };
                match start(&candidate, &state, &options.stack, &store, &console).await {
                    Ok(running) => current = Some((candidate, running)),
                    Err(error) => {
                        let Some(previous) = previous else { return Err(error); };
                        console.failure(&format!("activation failed; restoring previous package: {error:#}"));
                        previous.package.publish_staged(&bucket, previous.directory.path()).await?;
                        let running = start(&previous, &state, &options.stack, &store, &console).await?;
                        current = Some((previous, running));
                    }
                }
                continue;
            }
            let (_, running) = current.as_mut().context("native dev has no running application")?;
            crate::asyncrt::select! {
                _ = signals.received() => return Ok(()),
                status = running.child.wait() => {
                    let status = status?;
                    anyhow::ensure!(status.success(), "the compiled application exited with {status}");
                    return Ok(());
                }
                changed = async { match watcher.as_mut() {
                    Some(watcher) => watcher.changed().await,
                    None => std::future::pending().await,
                } } => { changed?; rebuild = true; }
            }
        }
    }.await;
    if let Some((_, running)) = current {
        crate::dev::stop_running_node(running).await;
    }
    // Keep the file descriptor alive until the owned child has been reaped.
    drop(state.lock);
    result
}

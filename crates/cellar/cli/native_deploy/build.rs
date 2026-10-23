use super::*;
use std::process::Command;

fn release_profile() -> String {
    "release".into()
}

/// Cargo project plus the primary and cohosted application configuration.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Project {
    pub cargo_manifest: PathBuf,
    pub package: String,
    pub binary: String,
    #[serde(default = "release_profile")]
    pub profile: String,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub features: Vec<String>,
    /// Explicitly permit the optional cellar-javascript V8 adapter.
    #[serde(default)]
    pub javascript: bool,
    pub primary: ApplicationConfig,
    #[serde(default)]
    pub python_environment: Option<PythonProject>,
    #[serde(default)]
    pub cohosted: Vec<ApplicationConfig>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct WatchPaths {
    pub(crate) roots: Vec<PathBuf>,
    pub(crate) ignored: Vec<PathBuf>,
}

fn cargo_features(command: &mut Command, features: &[String]) {
    command.args(["--no-default-features", "--locked"]);
    if !features.is_empty() {
        command.arg("--features").arg(features.join(","));
    }
}

/// Check the selected application's normal/build dependency graph, including
/// dependencies' enabled features, before invoking the compiler.
pub fn verify_native_dependencies(metadata: &serde_json::Value, package: &str) -> Result<()> {
    verify_dependencies(metadata, package, false)
}

fn verify_dependencies(
    metadata: &serde_json::Value,
    package: &str,
    javascript: bool,
) -> Result<()> {
    let packages = metadata["packages"]
        .as_array()
        .context("Cargo metadata has no packages")?;
    let roots: Vec<_> = packages.iter().filter(|p| p["name"] == package).collect();
    anyhow::ensure!(
        roots.len() == 1,
        "Cargo package {package} is missing or ambiguous"
    );
    let root = roots[0]["id"].as_str().context("Cargo package has no id")?;
    let nodes = metadata["resolve"]["nodes"]
        .as_array()
        .context("Cargo metadata has no dependency graph")?;
    let nodes: BTreeMap<_, _> = nodes
        .iter()
        .filter_map(|n| Some((n["id"].as_str()?, n)))
        .collect();
    let packages: BTreeMap<_, _> = packages
        .iter()
        .filter_map(|p| Some((p["id"].as_str()?, p)))
        .collect();
    let mut adapter = false;
    let mut engine = false;
    let mut visited = std::collections::BTreeSet::new();
    let mut pending = vec![root];
    while let Some(id) = pending.pop() {
        if !visited.insert(id) {
            continue;
        }
        let name = packages.get(id).context("dependency package is missing")?["name"]
            .as_str()
            .context("dependency has no name")?;
        adapter |= name == "cellar-javascript";
        engine |= name == "v8";
        anyhow::ensure!(
            (javascript && name == "v8")
                || !matches!(
                    name,
                    "v8" | "deno_core"
                        | "boa_engine"
                        | "quickjs"
                        | "quick-js"
                        | "libquickjs-sys"
                        | "rquickjs"
                        | "rquickjs-core"
                        | "rquickjs-sys"
                        | "javascriptcore"
                        | "javascriptcore-rs"
                        | "javascriptcore-rs-sys"
                        | "mozjs"
                        | "mozjs_sys"
                ),
            "native application dependency graph contains JavaScript engine {name}"
        );
        let node = nodes.get(id).context("dependency graph node is missing")?;
        for dependency in node["deps"]
            .as_array()
            .context("dependency node has no deps")?
        {
            let kinds = dependency["dep_kinds"]
                .as_array()
                .context("dependency has no kinds")?;
            if kinds.iter().any(|kind| kind["kind"] != "dev") {
                pending.push(
                    dependency["pkg"]
                        .as_str()
                        .context("dependency has no package id")?,
                );
            }
        }
    }
    anyhow::ensure!(
        !javascript || (adapter && engine),
        "JavaScript build requires cellar-javascript with embed enabled"
    );
    Ok(())
}

impl Project {
    /// Build with Cargo, reject engine dependencies and stage a verified
    /// native package. The project file determines paths relative to itself.
    pub fn build(project_file: &Path, output: &Path) -> Result<Package> {
        Self::build_mode(project_file, output, false)
    }

    pub(crate) fn build_dev(project_file: &Path, output: &Path) -> Result<Package> {
        Self::build_mode(project_file, output, true)
    }

    pub(crate) fn watch_roots(project_file: &Path) -> Result<WatchPaths> {
        let project: Self = serde_json::from_slice(&std::fs::read(project_file)?)?;
        let base = project_file
            .parent()
            .context("native project has no directory")?;
        let manifest = base.join(&project.cargo_manifest).canonicalize()?;
        let metadata = inspect_metadata(&project, &manifest)?;
        Ok(watch_roots(&metadata, base))
    }

    fn build_mode(project_file: &Path, output: &Path, dev: bool) -> Result<Package> {
        let mut project: Self = serde_json::from_slice(&std::fs::read(project_file)?)
            .context("decode native Cargo project")?;
        if dev {
            let variables = crate::dev::read_dev_vars(project_file)?;
            for application in std::iter::once(&mut project.primary).chain(&mut project.cohosted) {
                apply_dev_variables(application, &variables)?;
            }
        }
        let base = project_file.parent().unwrap_or_else(|| Path::new("."));
        let manifest = base.join(&project.cargo_manifest).canonicalize()?;
        let metadata = inspect_metadata(&project, &manifest)?;
        let python = project
            .python_environment
            .as_ref()
            .map(|config| config.prepare(base))
            .transpose()?;
        if dev {
            std::fs::create_dir_all(output)?;
            std::fs::write(
                output.join("watch-roots.json"),
                serde_json::to_vec(&watch_roots(&metadata, base))?,
            )?;
        }
        let mut build = Command::new("cargo");
        build.current_dir(manifest.parent().unwrap());
        build
            .args([
                if python.is_some() { "rustc" } else { "build" },
                "--manifest-path",
            ])
            .arg(&manifest)
            .args([
                "--package",
                &project.package,
                "--bin",
                &project.binary,
                "--profile",
                &project.profile,
                "--message-format=json-render-diagnostics",
            ]);
        cargo_features(&mut build, &project.features);
        build.env(
            "CELLAR_APPLICATION_VERSION",
            &project.primary.application_version,
        );
        // Only explicit project metadata may bind a provisioned environment.
        // Ambient build flags must not silently change unrelated native hosts.
        if let Some(python) = &python {
            anyhow::ensure!(
                python.target() == project.target.as_deref().unwrap_or(current_target()),
                "Python inventory target differs from project target"
            );
            build
                .env("CELLAR_PYTHON_INVENTORY", &python.inventory_path)
                .env("PYTHONDONTWRITEBYTECODE", "1");
            if project.target.as_deref().unwrap_or(current_target()) == current_target() {
                build.env("PYO3_PYTHON", python.executable());
            }
        } else {
            build.env_remove("CELLAR_PYTHON_INVENTORY");
        }
        if let Some(target) = &project.target {
            build.arg("--target").arg(target);
        }
        if let Some(python) = &python {
            build.arg("--").args(python.linker_arguments()?);
        }
        let built = build.output().context("build native Rust application")?;
        anyhow::ensure!(
            built.status.success(),
            "Cargo build failed: {}",
            String::from_utf8_lossy(&built.stderr)
        );
        let mut executable = None;
        for line in String::from_utf8_lossy(&built.stdout).lines() {
            let Ok(message) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            if message["reason"] == "compiler-artifact"
                && message["target"]["name"] == project.binary
            {
                if let Some(path) = message["executable"].as_str() {
                    executable = Some(PathBuf::from(path));
                }
            }
        }
        let executable = executable.context("Cargo did not emit the requested executable")?;
        let target = match project.target {
            Some(target) => target,
            None => {
                let rustc = Command::new("rustc")
                    .current_dir(manifest.parent().unwrap())
                    .arg("-vV")
                    .output()?;
                anyhow::ensure!(
                    rustc.status.success(),
                    "cannot determine native Cargo host target"
                );
                String::from_utf8_lossy(&rustc.stdout)
                    .lines()
                    .find_map(|line| line.strip_prefix("host: "))
                    .context("rustc has no host target")?
                    .to_string()
            }
        };
        let artifact = executable_identity(&executable, &target)?;
        std::fs::create_dir_all(output)?;
        let mut package = Package {
            primary: project.primary.stage_manifest_mode(
                vec![artifact.clone()],
                base,
                output,
                dev,
            )?,
            cohosted: project
                .cohosted
                .iter()
                .map(|c| c.stage_manifest_mode(vec![artifact.clone()], base, output, dev))
                .collect::<Result<_>>()?,
        };
        if let Some(python) = &python {
            for manifest in std::iter::once(&mut package.primary).chain(&mut package.cohosted) {
                python.stage(manifest, &artifact, output)?;
            }
        }
        package.validate()?;
        let destination = output.join(&artifact.sha256);
        if destination.exists() {
            artifact.verify(&std::fs::read(&destination)?)?;
        } else {
            std::fs::copy(&executable, &destination)?;
        }
        let manifest_file = output.join("package.json");
        let mut temporary = tempfile::NamedTempFile::new_in(output)?;
        use std::io::Write;
        temporary.write_all(&serde_json::to_vec_pretty(&package)?)?;
        temporary.as_file().sync_all()?;
        temporary
            .persist(&manifest_file)
            .map_err(|e| anyhow::anyhow!(e.error))?;
        Ok(package)
    }
}

fn inspect_metadata(project: &Project, manifest: &Path) -> Result<serde_json::Value> {
    let mut command = Command::new("cargo");
    command.current_dir(manifest.parent().unwrap());
    command
        .args(["metadata", "--format-version", "1", "--manifest-path"])
        .arg(manifest);
    cargo_features(&mut command, &project.features);
    if let Some(target) = &project.target {
        command.arg("--filter-platform").arg(target);
    }
    let output = command
        .output()
        .context("inspect native application dependencies")?;
    anyhow::ensure!(
        output.status.success(),
        "Cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata = serde_json::from_slice(&output.stdout)?;
    verify_dependencies(&metadata, &project.package, project.javascript)?;
    Ok(metadata)
}

fn apply_dev_variables(
    config: &mut ApplicationConfig,
    overrides: &crate::deploy::VarOverrides,
) -> Result<()> {
    let (entries, strict) = match overrides {
        crate::deploy::VarOverrides::None => return Ok(()),
        crate::deploy::VarOverrides::DevVars(entries) => (entries, true),
        crate::deploy::VarOverrides::DotEnv(entries) => (entries, false),
    };
    for (name, value) in entries {
        let binding = config
            .bindings
            .iter_mut()
            .find(|binding| binding["name"] == name.as_str());
        let taken = binding
            .as_ref()
            .is_some_and(|binding| binding["type"] != "plain_text")
            || config
                .assets
                .as_ref()
                .and_then(|assets| assets.binding.as_ref())
                == Some(name);
        if !crate::deploy::valid_binding(name) || taken {
            if strict {
                bail!("invalid or conflicting native dev variable {name:?}");
            }
            crate::note!("warning: skipped invalid or conflicting dotenv entry {name:?}");
            continue;
        }
        if let Some(binding) = binding {
            binding["text"] = value.clone().into();
        } else {
            config
                .bindings
                .push(serde_json::json!({"type":"plain_text", "name":name, "text":value}));
        }
    }
    Ok(())
}

fn watch_roots(metadata: &serde_json::Value, project: &Path) -> WatchPaths {
    let mut roots = vec![project.to_path_buf()];
    if let Some(root) = metadata["workspace_root"].as_str() {
        roots.push(root.into());
    }
    if let Some(packages) = metadata["packages"].as_array() {
        for package in packages
            .iter()
            .filter(|package| package["source"].is_null())
        {
            if let Some(path) = package["manifest_path"]
                .as_str()
                .and_then(|path| Path::new(path).parent())
            {
                roots.push(path.to_path_buf());
                // Path dependencies may inherit their dependency versions
                // from a different workspace's Cargo.toml.
                for parent in path.ancestors().skip(1) {
                    if parent.join("Cargo.toml").is_file() {
                        roots.push(parent.to_path_buf());
                        break;
                    }
                }
            }
        }
    }
    roots.sort();
    roots.dedup();
    let all = roots.clone();
    roots.retain(|root| {
        !all.iter()
            .any(|parent| parent != root && root.starts_with(parent))
    });
    let ignored = metadata["target_directory"]
        .as_str()
        .map(PathBuf::from)
        .filter(|directory| !roots.iter().any(|root| root.starts_with(directory)))
        .into_iter()
        .map(|directory| directory.canonicalize().unwrap_or(directory))
        .collect();
    WatchPaths { roots, ignored }
}

#[cfg(test)]
#[path = "build_tests.rs"]
mod tests;

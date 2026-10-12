use super::*;
use crate::application::Application;
use crate::generation::GenerationId;
use std::collections::{BTreeSet, HashMap};

type ApplicationFactory = dyn Fn() -> anyhow::Result<Arc<dyn Application>> + Send + Sync;

pub(super) fn print_help() -> anyhow::Result<()> {
    crate::cli_output::Output::new(crate::cli_output::Format::Text).line(format_args!(
        "Compiled Rust cell application\n\n\
         USAGE:\n\
           APPLICATION [NODE OPTIONS]\n\
           APPLICATION cell|sql|kv|queue|blob [COMMAND OPTIONS]\n\
           APPLICATION diagnose --bucket NAME [OPTIONS]\n\n\
         NODE OPTIONS:\n\
           --bucket [s3://|gs://|az://]NAME[/PREFIX]  Fleet storage\n\
           --endpoint URL                          S3-compatible endpoint\n\
           --region REGION                         Storage region\n\
           --listen IP:PORT                        Public application listener\n\
           --internal-listen IP:PORT               Peer and operator listener\n\
           --advertise HOST:PORT                   Address reachable by peers\n\
           --control-plane                         Use enrolled fleet configuration\n\
           --trust-forwarded-headers               Trust proxy scheme/host headers\n\n\
         Embedded graphs serve the code and configuration compiled into this\n\
         application. Fleet graphs watch the deployment pointer and accept only\n\
         registered script versions. Use the application's Rust delivery workflow\n\
         to build and publish code. The source deploy/dev commands are unavailable.\n\n\
         Node environment and operator options are documented in the cellar docs."
    ))
}

/// A compiled application's production host. Uses the same command-line node
/// settings, routing, replication, operator endpoints and shutdown as `cellar`.
///
/// Every script/code version in a deployment must have a compiled registration.
/// Factories are called anew at boot and each reload. A
/// deployment naming code absent from this executable fails before adoption.
/// Use [`Self::local`] for an embedded graph or [`Self::fleet`] to watch the
/// fleet deployment pointer. Publishing native artifacts remains a separate
/// delivery concern; this runner does not compile or download Rust code.
///
/// Call once from `main`, outside a Tokio runtime. Like the source daemon,
/// the runner owns process signals and can exit the process after shutdown.
/// Production applications should install jemalloc as their global allocator
/// so the existing allocator pressure metrics measure application allocations.
pub struct NativeDaemon {
    local_graph: Option<DeploymentGraph>,
    applications: NativeApplications,
}

impl NativeDaemon {
    /// Serve an embedded deployment graph. With a fleet bucket configured,
    /// this retains bucket ownership, replication, durable wakes and fencing.
    /// The embedded graph has no deployment pointer to reload automatically.
    /// `CELLAR_NATIVE_FLEET=1` selects the published fleet graph instead, as does
    /// the private local development supervisor.
    pub fn local(graph: DeploymentGraph) -> Self {
        Self {
            local_graph: Some(graph),
            applications: NativeApplications::default(),
        }
    }

    /// Load and reload deployment configuration from the fleet bucket using
    /// compiled registrations. All requested script/code-version pairs must exist,
    /// and native manifests must declare this executable's target and digest.
    pub fn fleet() -> Self {
        Self {
            local_graph: None,
            applications: NativeApplications::default(),
        }
    }

    /// Register one immutable application code version. Register additional versions
    /// to admit transitions within the same executable. Native deployment revisions
    /// include configuration and artifact identity independently of this version.
    pub fn register(
        mut self,
        script: impl Into<String>,
        version: impl Into<String>,
        factory: impl Fn() -> anyhow::Result<Arc<dyn Application>> + Send + Sync + 'static,
    ) -> anyhow::Result<Self> {
        let script = script.into();
        let version = version.into();
        anyhow::ensure!(
            !script.is_empty(),
            "native application script must not be empty"
        );
        anyhow::ensure!(
            !version.is_empty(),
            "native application version must not be empty"
        );
        let key = (script, version);
        anyhow::ensure!(
            !self.applications.factories.contains_key(&key),
            "duplicate compiled application {} at version {}",
            key.0,
            key.1
        );
        self.applications.factories.insert(key, Arc::new(factory));
        Ok(self)
    }

    /// Run the production node, without initializing a JavaScript engine.
    pub fn run(self) -> anyhow::Result<()> {
        let arguments: Vec<_> = std::env::args_os().skip(1).collect();
        if arguments
            .first()
            .is_some_and(|arg| arg == "--check-native-package")
        {
            anyhow::ensure!(
                arguments.len() == 2,
                "--check-native-package requires one package.json path"
            );
            let package: crate::native_deploy::Package =
                serde_json::from_slice(&std::fs::read(std::path::Path::new(&arguments[1]))?)?;
            package.validate()?;
            rustls::crypto::ring::default_provider()
                .install_default()
                .ok();
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            return runtime.block_on(async {
                crate::asyncrt::set_host_handle(tokio::runtime::Handle::current());
                self.applications.check_package(&package)
            });
        }
        let supervised = crate::env_vars::flag("CELLAR_NATIVE_DEV", false)?;
        anyhow::ensure!(
            !supervised || std::env::var_os("CELLAR_INTERNAL_DEV_STORE").is_some(),
            "native dev supervision requires its private local storage"
        );
        let local_graph = if supervised || crate::env_vars::flag("CELLAR_NATIVE_FLEET", false)? {
            None
        } else {
            self.local_graph
        };
        super::run_host(Backend::Native(Arc::new(self.applications)), local_graph)
    }
}

#[derive(Default)]
pub(super) struct NativeApplications {
    factories: HashMap<(String, String), Arc<ApplicationFactory>>,
}

impl NativeApplications {
    fn check_package(&self, package: &crate::native_deploy::Package) -> anyhow::Result<()> {
        let executable = crate::native_deploy::current_executable_identity()?;
        // Validate every registration before invoking any application factory.
        for manifest in std::iter::once(&package.primary).chain(&package.cohosted) {
            let native = manifest.native.as_ref().unwrap();
            anyhow::ensure!(
                native
                    .artifacts
                    .iter()
                    .any(|artifact| artifact == executable),
                "running executable is not declared for native application {}",
                manifest.script_name
            );
            anyhow::ensure!(
                self.factories.contains_key(&(
                    manifest.script_name.clone(),
                    native.application_version.clone()
                )),
                "no compiled application registered for script {} at version {}",
                manifest.script_name,
                native.application_version
            );
        }
        for manifest in std::iter::once(&package.primary).chain(&package.cohosted) {
            let native = manifest.native.as_ref().unwrap();
            let application = self.factories[&(
                manifest.script_name.clone(),
                native.application_version.clone(),
            )]()?;
            let workflows = manifest
                .raw_metadata
                .get("bindings")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter(|binding| binding["type"] == "workflow")
                .filter_map(|binding| binding["class_name"].as_str());
            crate::native_worker::validate_application_classes(
                application.as_ref(),
                &manifest.script_name,
                manifest.do_classes.iter().map(String::as_str),
                workflows,
            )?;
        }
        Ok(())
    }

    fn build(
        &self,
        graph: &DeploymentGraph,
    ) -> anyhow::Result<HashMap<String, Arc<dyn Application>>> {
        // Validate the entire graph before invoking application factories.
        let deployments = || std::iter::once(&graph.primary).chain(&graph.cohosted);
        let mut scripts = BTreeSet::new();
        for deployment in deployments() {
            anyhow::ensure!(
                scripts.insert(&deployment.script_name),
                "duplicate native application script {}",
                deployment.script_name
            );
            anyhow::ensure!(
                deployment.options.script_name == deployment.script_name,
                "native application script disagrees with its Worker configuration"
            );
            let version = registration_version(deployment);
            anyhow::ensure!(
                self.factories.contains_key(&(deployment.script_name.clone(), version.to_string())),
                "no compiled application registered for script {} at version {}; deploy an executable containing this version",
                deployment.script_name,
                version
            );
            if let Some(native) = &deployment.native {
                native.validate()?;
                let executable = crate::native_deploy::current_executable_identity()?;
                anyhow::ensure!(native.artifacts.iter().any(|a| a == executable),
                    "running executable is not declared for native application {} at code version {} on target {}; install a declared artifact before serving",
                    deployment.script_name, version, executable.target);
            }
        }
        deployments()
            .map(|deployment| {
                let factory = &self.factories[&(
                    deployment.script_name.clone(),
                    registration_version(deployment).to_string(),
                )];
                let application = factory().with_context(|| {
                    format!(
                        "construct compiled application {} at version {}",
                        deployment.script_name, deployment.version
                    )
                })?;
                Ok((deployment.script_name.clone(), application))
            })
            .collect()
    }
}

fn registration_version(deployment: &crate::fleet::LoadedDeployment) -> &str {
    deployment
        .native
        .as_ref()
        .map(|native| native.application_version.as_str())
        .unwrap_or(&deployment.version)
}

#[derive(Clone)]
pub(super) enum Backend {
    Native(Arc<NativeApplications>),
}

impl Backend {
    pub(super) fn is_native(&self) -> bool {
        matches!(self, Self::Native(_))
    }

    pub(super) fn build(
        &self,
        id: GenerationId,
        graph: DeploymentGraph,
        options: GenerationOptions,
    ) -> anyhow::Result<Generation> {
        match self {
            Self::Native(applications) => {
                let registered = applications.build(&graph)?;
                Generation::build_native(id, graph, options, registered)
            }
        }
    }
}

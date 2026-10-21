//! Actual Fleet Workflow and mutable Queue/container/socket factories are
//! refused even if a custom Application also returns a valid Raft definition.
use super::{faults, public_node};
use anyhow::{ensure, Result};
use cellar::sdk::{
    consensus::*, Application, Cell, CellIdentity, Context, HandlerFuture, Invocation, Node,
    Output, Registry, ShutdownMode,
};
use futures_util::TryStreamExt;
use object_store::ObjectStore;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct Conflicting {
    fleet: Arc<Registry>,
    raft: Arc<RaftClass>,
}

impl Application for Conflicting {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        self.fleet.clone().invoke(context, invocation)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.fleet.supports_cell(class)
    }

    fn supports_workflow(&self, class: &str) -> bool {
        self.fleet.supports_workflow(class)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        self.fleet.create_cell(context, identity)
    }

    fn resolve_raft_class(&self, _class: &str) -> Result<Arc<RaftClass>, RegistrationError> {
        Ok(self.raft.clone())
    }
}

pub async fn native_gate() -> Result<()> {
    for kind in 0..4 {
        let calls = Arc::new(AtomicUsize::new(0));
        let invoked = calls.clone();
        let fleet = Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }));
        let fleet = if kind == 0 {
            fleet.workflow("Unsafe", move |context, _, _| {
                invoked.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    let _ = context.workflow("FLOW")?;
                    anyhow::bail!("Fleet workflow must not execute under Raft")
                })
            })?
        } else {
            fleet.cell("Unsafe", move |context, _| {
                invoked.fetch_add(1, Ordering::SeqCst);
                match kind {
                    1 => {
                        let _ = context.queue("JOBS")?;
                    }
                    2 => {
                        let _ = context.container()?;
                    }
                    _ => {
                        let _ = context.websocket_pair()?;
                    }
                }
                anyhow::bail!("mutable Fleet factory must not execute under Raft")
            })?
        };
        let raft = Arc::new(RaftClass::new("Unsafe", 1, [88; 32])?.command(
            "NoOp",
            [89; 32],
            |_, _| Ok(vec![]),
        )?);
        let application: Arc<dyn Application> = Arc::new(Conflicting {
            fleet: Arc::new(fleet),
            raft: raft.clone(),
        });
        let root = tempfile::tempdir()?;
        let node: Node = public_node::node(root.path(), application.clone()).await?;
        let store = Arc::new(faults::Store::default());
        let mut namespace = NamespaceConfig::new(
            ClusterId::new([1; 16])?,
            NamespaceId::new([2; 16])?,
            NamespaceMode::Raft,
            raft.schema_digest(),
        );
        namespace.voters = VoterPolicy::SingleVoterDevelopment;
        let registration = RaftCellRegistration {
            placement: RaftCellPlacement::Replica,
            namespace,
            class: "Unsafe".into(),
            birth: BirthProposal {
                scope: GroupScope {
                    cluster: ClusterId::new([1; 16])?,
                    namespace: NamespaceId::new([2; 16])?,
                    cell: "Unsafe:one".into(),
                    group: GroupId::new([8; 16])?,
                    incarnation: IncarnationId::new([9; 16])?,
                },
                voters: vec![InitialMember {
                    member: MemberId::new(1)?,
                    node: NodeId::new([1; 16])?,
                }],
                learners: vec![],
            },
        };
        let outcome =
            public_node::cells(&node, application, store.clone(), registration, false).await;
        ensure!(
            matches!(outcome,Err(ref error) if matches!(error.downcast_ref::<RegistrationError>(),Some(RegistrationError::UnsupportedFactory(class)) if class=="Unsafe")),
            "mutable capability {kind} did not fail with UnsupportedFactory"
        );
        ensure!(calls.load(Ordering::SeqCst) == 0 && !node.info().data_dir.join("raft").exists());
        ensure!(store.list(None).try_collect::<Vec<_>>().await?.is_empty());
        node.shutdown(ShutdownMode::Preserve).await?;
    }
    Ok(())
}

#[test]
fn fleet_bindings_cannot_name_a_raft_class() -> Result<()> {
    let registry = Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
        .raft_class(RaftClass::new("Restricted", 1, [90; 32])?)?;
    let mut options = cellar::engine_api::WorkerConfigOptions {
        src: String::new(),
        script_name: "mixed".into(),
        do_classes: vec![],
        bindings: vec![],
        blob_bindings: vec![],
        sql_bindings: vec![],
        kv_bindings: vec![],
        queue_bindings: vec![],
        stream_bindings: vec![],
        queue_consumers: vec![],
        workflow_bindings: vec![],
        vars: vec![],
        node: "mixed".into(),
        modules: vec![],
        compat: Default::default(),
    };
    // Global Fleet bindings can coexist, but a binding cannot reinterpret a
    // Raft class as a mutable Fleet cell or Workflow.
    options
        .queue_bindings
        .push(cellar::engine_api::QueueBinding {
            environment: "JOBS".into(),
            queue: "jobs".into(),
            delivery_delay: 0,
        });
    registry.validate_configuration(&options)?;
    options.bindings.push(("CELL".into(), "Restricted".into()));
    ensure!(registry.validate_configuration(&options).is_err());
    options.bindings.clear();
    options
        .workflow_bindings
        .push(cellar::engine_api::WorkflowBinding {
            environment: "FLOW".into(),
            workflow: "flow".into(),
            class: "Restricted".into(),
            settings: Default::default(),
        });
    ensure!(registry.validate_configuration(&options).is_err());
    Ok(())
}

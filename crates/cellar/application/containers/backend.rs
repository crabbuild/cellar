// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Host-selected container adapters and exact invocation observations.
use super::{
    BackendIdentity, BindingJournal, BindingLifecycleGuard, FileMetadata, FilePath, StartOptions,
};
use crate::application::{
    consensus::CommandFailure, storage::RaftStorageAuthority, Context, HandlerFuture,
};
use anyhow::{ensure, Result};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Weak,
    },
    time::Duration,
};

/// Bounded operations of the ordinary native container API.
pub enum ContainerRequest {
    /// Reconcile the original lifecycle and obtain a current running observation.
    Observe,
    Start(StartOptions),
    Destroy,
    Monitor,
    Signal(u32),
    SetInactivityTimeout(Duration),
    ReadFile {
        path: FilePath,
        max_bytes: usize,
    },
    WriteFile {
        path: FilePath,
        data: Vec<u8>,
        mode: u32,
    },
    StatFile(FilePath),
    ListFiles {
        path: FilePath,
        max_bytes: usize,
        max_entries: usize,
    },
}

/// Typed results retain the native SDK's values and byte ownership.
pub enum ContainerReply {
    Running(bool),
    Started(u64),
    Done,
    Exited(i64),
    Bytes(Vec<u8>),
    Metadata(FileMetadata),
    Entries(Vec<FileMetadata>),
}

pub type ContainerFuture<'a> = Pin<Box<dyn Future<Output = Result<ContainerReply>> + Send + 'a>>;
/// Construct external work only after its prepared metadata passes the source gate.
pub type PreparedContainerCall<'a> = Box<dyn FnOnce() -> ContainerFuture<'a> + Send + 'a>;

/// Trusted host adapter for ordinary native container calls on shared groups.
///
/// Preparation is synchronous, bounded and invocation-private. It must perform
/// no provider I/O. The framework retains the supplied lifecycle admission and
/// proves the original unfinished receipt, business writes and prepared metadata
/// before constructing the returned call. The adapter must additionally enforce
/// actual execution fencing and preserve original uncertain-operation identities.
/// Identity and body limits are immutable and participate in the class schema.
pub trait ContainerAdapter: Send + Sync {
    fn identity(&self) -> BackendIdentity;

    fn max_body_bytes(&self) -> usize;

    fn prepare<'a>(
        &'a self,
        context: &'a Context,
        request: ContainerRequest,
        lifecycle: Option<&'a BindingLifecycleGuard>,
    ) -> Result<PreparedContainerCall<'a>>;
}

pub(crate) struct Binding {
    adapter: Arc<dyn ContainerAdapter>,
    pub(crate) identity: BackendIdentity,
    pub(crate) max_body_bytes: usize,
}

impl Binding {
    pub(crate) fn new(adapter: Arc<dyn ContainerAdapter>) -> Result<Arc<Self>> {
        let identity = adapter.identity();
        crate::storage::container_bindings::validate_name(&identity.binding)?;
        ensure!(
            identity.configuration_digest.len() == 64,
            "invalid container adapter configuration digest"
        );
        crate::application::container_bindings::ProviderDeclaration::new(
            identity.provider.clone(),
            hex::decode(&identity.configuration_digest)?
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid container adapter configuration digest"))?,
        )?;
        let max_body_bytes = adapter.max_body_bytes();
        ensure!(
            (1..=16 * 1024 * 1024).contains(&max_body_bytes),
            "container adapter body limit"
        );
        Ok(Arc::new(Self {
            adapter,
            identity,
            max_body_bytes,
        }))
    }

    fn check(&self) -> Result<()> {
        ensure!(
            self.adapter.identity() == self.identity
                && self.adapter.max_body_bytes() == self.max_body_bytes,
            "container adapter configuration changed after admission"
        );
        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct Handle {
    binding: Arc<Binding>,
    resources: Weak<crate::application::storage::Resources>,
    scope: Arc<str>,
    epoch: u64,
}

impl Handle {
    pub(super) fn new(context: &Context, binding: Arc<Binding>) -> Result<Self> {
        context.check_binding_turn()?;
        let storage = context
            .raft_storage
            .as_ref()
            .ok_or(CommandFailure::UnsupportedCapability)?;
        let identity = context
            .identity()
            .ok_or(CommandFailure::UnsupportedCapability)?;
        ensure!(
            context.facet().is_none(),
            "containers must be controlled by their root cell"
        );
        binding.check()?;
        Ok(Self {
            binding,
            resources: Arc::downgrade(&storage.resources),
            scope: identity.scope.clone().into(),
            epoch: identity.epoch,
        })
    }

    pub(super) fn check(&self, context: &Context) -> Result<()> {
        context.check_binding_turn()?;
        self.binding.check()?;
        let storage = context
            .raft_storage
            .as_ref()
            .ok_or(CommandFailure::UnsupportedCapability)?;
        ensure!(
            self.resources
                .upgrade()
                .is_some_and(|original| Arc::ptr_eq(&original, &storage.resources))
                && storage
                    .resources
                    .container_adapter
                    .get()
                    .is_some_and(|installed| Arc::ptr_eq(installed, &self.binding))
                && context.facet().is_none()
                && context
                    .identity()
                    .is_some_and(|id| id.scope == self.scope.as_ref() && id.epoch == self.epoch),
            "container belongs to another or retired cell activation"
        );
        Ok(())
    }

    pub(super) fn call(
        &self,
        context: &Context,
        request: ContainerRequest,
    ) -> HandlerFuture<ContainerReply> {
        let prepared = (|| {
            self.check(context)?;
            context.container_provider_authority()
        })();
        let authority = match prepared {
            Ok(authority) => authority,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let owner = self.clone();
        let context = context.clone();
        Box::pin(async move {
            owner.check(&context)?;
            authority.check(&context)?;
            let observation = matches!(request, ContainerRequest::Monitor);
            let changes_running = matches!(
                request,
                ContainerRequest::Start(_)
                    | ContainerRequest::Destroy
                    | ContainerRequest::Signal(_)
                    | ContainerRequest::Monitor
            );
            let destroys = matches!(request, ContainerRequest::Destroy);
            let lifecycle = if observation {
                None
            } else {
                Some(
                    BindingJournal::open(&context, &owner.binding.identity.binding)?
                        .lock_lifecycle(&context)
                        .await?,
                )
            };
            let call = owner
                .binding
                .adapter
                .prepare(&context, request, lifecycle.as_ref())?;
            authority.check(&context)?;
            if changes_running {
                if let Some(prepared) = context.state.container_observation.get() {
                    prepared.check(&context)?;
                    prepared.state().store(2, Ordering::Release);
                }
            }
            authority.before_effect(&context).await?;
            owner.check(&context)?;
            let result = call().await;
            authority.check(&context)?;
            owner.check(&context)?;
            if let Some(prepared) = context.state.container_observation.get() {
                let running = match &result {
                    Ok(ContainerReply::Started(_)) => Some(true),
                    Ok(ContainerReply::Done) if destroys => Some(false),
                    Ok(ContainerReply::Exited(_)) => Some(false),
                    _ => None,
                };
                if let Some(running) = running {
                    prepared.check(&context)?;
                    prepared.state().store(u8::from(running), Ordering::Release);
                }
            }
            result
        })
    }

    pub(super) fn running(&self, context: &Context) -> Result<bool> {
        self.check(context)?;
        let prepared = context
            .state
            .container_observation
            .get()
            .ok_or(CommandFailure::Unavailable)?;
        prepared.check(context)?;
        match prepared.state().load(Ordering::Acquire) {
            0 => Ok(false),
            1 => Ok(true),
            _ => anyhow::bail!("container running state requires reconciliation"),
        }
    }
}

/// Weak event/activation references avoid retaining an invocation through itself.
/// Constructor copies permit only its snapshot; external I/O still requires the
/// real event Context and its nonempty invocation authority.
pub(crate) trait Observation: Send + Sync {
    fn check(&self, context: &Context) -> Result<()>;

    fn state(&self) -> &AtomicU8;

    fn for_constructor(&self, constructor: &Context) -> Result<Arc<dyn Observation>>;
}

struct ObservedInvocation {
    resources: Weak<crate::application::storage::Resources>,
    invocation: Weak<()>,
    event: Weak<crate::application::EventState>,
    io: Weak<crate::host_io::IoContext>,
    running: Arc<AtomicU8>,
}

impl Observation for ObservedInvocation {
    fn check(&self, context: &Context) -> Result<()> {
        context.check_binding_turn()?;
        ensure!(
            self.event
                .upgrade()
                .is_some_and(|event| !event.retired.load(Ordering::Acquire)),
            "container observation belongs to a retired invocation"
        );
        ensure!(
            self.io
                .upgrade()
                .is_some_and(|io| Arc::ptr_eq(&io, &context.io)),
            "container observation belongs to another invocation"
        );
        let resources = self
            .resources
            .upgrade()
            .ok_or(CommandFailure::Unavailable)?;
        let invocation = self
            .invocation
            .upgrade()
            .ok_or(CommandFailure::Unavailable)?;
        let supplied = context
            .raft_storage
            .as_ref()
            .ok_or(CommandFailure::UnsupportedCapability)?;
        ensure!(
            Arc::ptr_eq(&resources, &supplied.resources),
            "container observation belongs to another activation"
        );
        if let Some(supplied) = &supplied.invocation {
            ensure!(
                Arc::ptr_eq(&invocation, supplied),
                "container observation belongs to another invocation"
            );
        }
        crate::storage::require_raft_dispatch_storage(&RaftStorageAuthority {
            resources,
            invocation: Some(invocation),
        })?;
        crate::storage::capture_raft_dispatch()?
            .ok_or(CommandFailure::Unavailable)?
            .check_current()
    }

    fn state(&self) -> &AtomicU8 {
        &self.running
    }

    fn for_constructor(&self, constructor: &Context) -> Result<Arc<dyn Observation>> {
        let snapshot = Arc::new(Self {
            resources: self.resources.clone(),
            invocation: self.invocation.clone(),
            event: self.event.clone(),
            io: Arc::downgrade(&constructor.io),
            running: self.running.clone(),
        });
        snapshot.check(constructor)?;
        Ok(snapshot)
    }
}

pub(crate) async fn prepare_invocation(context: &Context) -> Result<()> {
    let Some(storage) = &context.raft_storage else {
        return Ok(());
    };
    let Some(binding) = storage.resources.container_adapter.get() else {
        return Ok(());
    };
    if let Some(prepared) = context.state.container_observation.get() {
        return prepared.check(context);
    }
    let handle = Handle::new(context, binding.clone())?;
    let journal = BindingJournal::open(context, &binding.identity.binding)?;
    // No remote resource exists for an unbound or durably retired generation.
    // Avoid an entry-only checkpoint before the first real Start preparation.
    let running = if journal
        .load(context)?
        .is_none_or(|row| row.record.is_retired())
    {
        false
    } else {
        match handle.call(context, ContainerRequest::Observe).await? {
            ContainerReply::Running(running) => running,
            _ => anyhow::bail!("container adapter returned an invalid observation"),
        }
    };
    context.container_provider_authority()?.check(context)?;
    let invocation = storage
        .invocation
        .as_ref()
        .ok_or(CommandFailure::UnsupportedCapability)?;
    let observation: Arc<dyn Observation> = Arc::new(ObservedInvocation {
        resources: Arc::downgrade(&storage.resources),
        invocation: Arc::downgrade(invocation),
        event: Arc::downgrade(&context.state),
        io: Arc::downgrade(&context.io),
        running: Arc::new(AtomicU8::new(u8::from(running))),
    });
    observation.check(context)?;
    context
        .state
        .container_observation
        .set(observation)
        .map_err(|_| anyhow::anyhow!("duplicate container invocation preparation"))?;
    Ok(())
}

pub(crate) fn prepare_constructor(context: &Context, constructor: &Context) -> Result<()> {
    if let Some(prepared) = context.state.container_observation.get() {
        prepared.check(context)?;
        let snapshot = prepared.for_constructor(constructor)?;
        constructor
            .state
            .container_observation
            .set(snapshot)
            .map_err(|_| anyhow::anyhow!("duplicate constructor container observation"))?;
    }
    Ok(())
}

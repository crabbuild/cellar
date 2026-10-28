// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Trusted single-use Linux container isolation. The deployment owns the
//! container exclusively and confines every node worker to its PID namespace.
//! Immutable IDs never authorize deletion or replacement-container discovery.
use super::*;
use crate::docker::{ContainerId, Docker};
use crate::raft::catalog::NamespaceCatalog;
use crate::raft::fleet_stop::BootBinding;
use cellar_logic::consensus::{NodeId, ProposalId};
use std::pin::Pin;

mod engine;
mod records;
use engine::Engine;
use records::{Binding, Intent, Receipt};

#[cfg(test)]
#[path = "../docker_isolation_tests.rs"]
mod tests;

/// Deployment adapter for native nodes whose workers are entirely contained in
/// one Linux Docker container. The deployment must reserve that container for a
/// single process boot, retain its volumes, and delegate exclusive lifecycle
/// control to this adapter. Other supervisors or manual restarts violate this
/// contract. Intact restart uses a new container with the retained volume.
///
/// Before activating capacity, call `bind_node` with the full immutable ID of
/// the container provisioned for the enrolled node. Discovery by name is never
/// used. Required labels are `io.cellar.fleet.owner`, `io.cellar.fleet.catalog`
/// (hex catalog ID), `io.cellar.fleet.node` (hex node ID),
/// `io.cellar.fleet.boot-mode=single-use` and
/// `io.cellar.fleet.workers=contained`. Those last two are trusted deployment
/// declarations, not facts inferred from a heartbeat or a container name.
#[derive(Clone)]
pub struct FleetDockerIsolation {
    inner: Arc<Adapter>,
}

struct Adapter {
    catalog: FleetCatalog,
    engine: Engine,
    namespaces: Vec<NamespaceCatalog>,
    turn: Mutex<()>,
}

impl FleetDockerIsolation {
    /// Explicit construction performs no Engine operation. Namespace handles
    /// must cover the fleet registry; the recovery service checks coverage.
    /// Recreate the service with complete handles when adding a namespace.
    pub fn new(
        catalog: FleetCatalog,
        docker: Docker,
        owner: &str,
        namespaces: Vec<NamespaceCatalog>,
    ) -> Result<Self> {
        if namespaces.len() > super::namespaces::MAX_NAMESPACES {
            return Err(FleetCatalogError::Limit);
        }
        Ok(Self {
            inner: Arc::new(Adapter {
                catalog,
                engine: Engine::new(docker, owner)?,
                namespaces,
                turn: Mutex::new(()),
            }),
        })
    }

    /// Persist the trusted provisioning map before failure. The current rooted
    /// boot, Engine ID, full container ID and start identity are immutable. A
    /// container cannot be rebound to a second boot. A failed response requires
    /// retrying this same binding; it does not prove the records were absent.
    pub async fn bind_node(
        &self,
        node: NodeId,
        container: &ContainerId,
        timeout: Duration,
    ) -> Result<()> {
        self.run(timeout, async {
            let catalog = &self.inner.catalog;
            let cut = catalog.read(timeout).await?;
            let host = catalog.authorize_host(&cut, node, timeout).await?;
            if !matches!(host.state(), FleetNodeState::Ready | FleetNodeState::Active) {
                return Err(FleetCatalogError::Conflict);
            }
            let boot = host.boot_binding()?;
            let binding = self.inner.engine.bind(&boot, container).await?;
            // Claim the physical container first. An interrupted binding cannot
            // allow a later process to reuse the same container identity.
            self.inner.persist_binding(&binding).await
        })
        .await
    }

    async fn run<T>(&self, timeout: Duration, work: impl Future<Output = Result<T>>) -> Result<T> {
        if timeout.is_zero() || timeout > Duration::from_secs(60) {
            return Err(FleetCatalogError::Invalid("Docker isolation timeout"));
        }
        let _turn = self
            .inner
            .turn
            .try_lock()
            .map_err(|_| FleetCatalogError::Overloaded)?;
        self.inner
            .catalog
            .inner
            .domain
            .run(crate::asyncrt::timeout(timeout, work))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
    }

    async fn isolate_boot(
        &self,
        operation: ProposalId,
        boot: &BootBinding,
    ) -> Result<FleetIsolationAttestation> {
        let key = Binding::key_for_boot(boot)?;
        let binding: Binding = self
            .inner
            .read(&key)
            .await?
            .ok_or(FleetCatalogError::Unconfirmed)?;
        binding.validate()?;
        if &binding.boot != boot {
            return Err(FleetCatalogError::Conflict);
        }
        let claim: Binding = self
            .inner
            .read(&binding.container_key())
            .await?
            .ok_or(FleetCatalogError::Unconfirmed)?;
        if claim != binding {
            return Err(FleetCatalogError::Conflict);
        }
        // The recovery service supplies this context only after permanent Root
        // exclusion. This immutable intent precedes every external mutation.
        let intent = Intent {
            format: 1,
            operation: operation.bytes(),
            binding: binding.clone(),
        };
        self.inner
            .persist(&format!("{key}/intent"), &intent)
            .await?;
        let terminal = self.inner.engine.isolate(&binding).await?;
        let receipt = Receipt {
            format: 1,
            intent,
            terminal,
        };
        let receipt_key = format!("{key}/receipt");
        self.inner.persist(&receipt_key, &receipt).await?;
        Ok(FleetIsolationAttestation {
            boot: boot.clone(),
            evidence_digest: records::digest(&receipt)?,
            evidence_reference: format!(
                "docker-isolation-1:{}",
                hex::encode(records::digest(&binding)?)
            ),
        })
    }
}

impl FleetFailureRecoveryPlatform for FleetDockerIsolation {
    fn namespaces<'a>(
        &'a self,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<NamespaceCatalog>>> + Send + 'a>> {
        Box::pin(async move {
            self.run(timeout, async { Ok(self.inner.namespaces.clone()) })
                .await
        })
    }

    fn isolate<'a>(
        &'a self,
        context: &'a FleetFailureRecoveryContext,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<FleetIsolationAttestation>> + Send + 'a>> {
        Box::pin(async move {
            self.run(
                timeout,
                self.isolate_boot(context.operation(), context.boot()),
            )
            .await
        })
    }
}

impl Adapter {
    async fn persist_binding(&self, binding: &Binding) -> Result<()> {
        binding.validate()?;
        self.persist(&binding.container_key(), binding).await?;
        self.persist(&binding.boot_key()?, binding).await
    }

    fn key(&self, suffix: &str) -> String {
        format!(
            "{}platform/docker-isolation-1/{}/{suffix}",
            self.catalog.inner.prefix,
            hex::encode(self.catalog.inner.incarnation)
        )
    }

    async fn read<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        self.catalog
            .inner
            .bucket
            .get_bounded(&self.key(key), records::LIMIT)
            .await
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map(|(bytes, _)| {
                serde_json::from_slice(&bytes)
                    .map_err(|_| FleetCatalogError::Corrupt("Docker isolation record"))
            })
            .transpose()
    }

    async fn persist<T: Serialize + DeserializeOwned + PartialEq>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<()> {
        if let Some(found) = self.read::<T>(key).await? {
            return if &found == value {
                Ok(())
            } else {
                Err(FleetCatalogError::PayloadConflict)
            };
        }
        let bytes = records::bytes(value)?;
        self.catalog
            .inner
            .bucket
            .put_cas(&self.key(key), bytes, None)
            .await
            .map_err(|_| FleetCatalogError::Unconfirmed)?;
        // Neither a successful reply nor a clean competing-writer rejection
        // substitutes for reading the exact immutable value back.
        match self.read::<T>(key).await? {
            Some(found) if &found == value => Ok(()),
            Some(_) => Err(FleetCatalogError::PayloadConflict),
            None => Err(FleetCatalogError::Unconfirmed),
        }
    }
}

// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::{
    consensus::{ClusterId, NamespaceId},
    NamespaceMode,
};

struct Definition(Arc<RaftClass>);

impl Application for Definition {
    fn supports_cell(&self, class: &str) -> bool {
        self.0.name() == class
    }

    fn invoke(self: Arc<Self>, _: Context, _: Invocation) -> HandlerFuture<Output> {
        Box::pin(async { anyhow::bail!("namespace resolution must not invoke applications") })
    }

    fn create_cell(&self, _: Context, _: &CellIdentity) -> anyhow::Result<Arc<dyn Cell>> {
        anyhow::bail!("namespace resolution must not construct cells")
    }

    fn resolve_raft_class(&self, class: &str) -> Result<Arc<RaftClass>, RegistrationError> {
        if self.supports_cell(class) {
            Ok(self.0.clone())
        } else {
            Err(RegistrationError::UnknownClass(class.into()))
        }
    }
}

#[test]
fn namespace_catalog_selects_exact_schema_and_refuses_missing_deployments() -> anyhow::Result<()> {
    let first = Arc::new(RaftClass::new("SharedName", 1, [1; 32])?);
    let second = Arc::new(RaftClass::new("SharedName", 1, [2; 32])?);
    let primary: Arc<dyn Application> = Arc::new(Definition(first.clone()));
    let other: Arc<dyn Application> = Arc::new(Definition(second.clone()));
    let namespace = NamespaceConfig::new(
        ClusterId::new([1; 16])?,
        NamespaceId::new([2; 16])?,
        NamespaceMode::Raft,
        second.schema_digest(),
    );
    assert!(matches!(
        primary.resolve_raft_namespace(&namespace, "SharedName"),
        Err(RegistrationError::ContractMismatch)
    ));
    let catalog = RaftApplications::new(primary.clone(), [other, primary.clone()])?;
    assert!(Arc::ptr_eq(
        &catalog.resolve_raft_namespace(&namespace, "SharedName")?,
        &second
    ));
    assert!(Arc::ptr_eq(
        &catalog.resolve_raft_class("SharedName")?,
        &first
    ));
    assert!(matches!(
        catalog.resolve_raft_namespace(&namespace, "Missing"),
        Err(RegistrationError::ContractMismatch)
    ));
    let missing = RaftApplications::new(primary, [])?;
    assert!(matches!(
        missing.resolve_raft_namespace(&namespace, "SharedName"),
        Err(RegistrationError::ContractMismatch)
    ));
    Ok(())
}

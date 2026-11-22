// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::{
    consensus::NativeCellConfiguration,
    containers::{
        BackendIdentity, BindingLifecycleGuard, ContainerAdapter, ContainerRequest,
        PreparedContainerCall,
    },
    Application, Context, Output, Registry,
};
use cellar_logic::{
    cell_group::CellId,
    consensus::{ClusterId, IncarnationId, NamespaceId},
};
use std::sync::Arc;

struct AdmissionOnly;

impl ContainerAdapter for AdmissionOnly {
    fn identity(&self) -> BackendIdentity {
        BackendIdentity {
            binding: "DOCKER".into(),
            provider: "sandbox-test".into(),
            configuration_digest: "11".repeat(32),
        }
    }

    fn max_body_bytes(&self) -> usize {
        4096
    }

    fn supports_bounded_sandbox(&self) -> bool {
        true
    }

    fn prepare<'a>(
        &'a self,
        _: &'a Context,
        _: ContainerRequest,
        _: Option<&'a BindingLifecycleGuard>,
    ) -> anyhow::Result<PreparedContainerCall<'a>> {
        anyhow::bail!("admission must not enter a provider")
    }
}

#[test]
fn sandbox_private_admission_authenticates_configured_application_namespace() -> anyhow::Result<()>
{
    use crate::application::{
        consensus::native_event_rpc as wire,
        sandbox_catalog::{shared, values},
    };
    use crate::runtime::sandbox_execution::{self, Action, Authority, Identity};
    let configured = |script: &str| -> anyhow::Result<Arc<RaftClass>> {
        let definition = RaftClass::new("Sandbox", 1, [41; 32])?
            .container_adapter(Arc::new(AdmissionOnly))?
            .sandbox_execution()?;
        let application =
            Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_class(definition)?;
        let mut options = NativeCellConfiguration::default().options(script);
        options.do_classes = vec!["Sandbox".into()];
        application.validate_configuration(&options)?;
        Ok(application.resolve_raft_class("Sandbox")?)
    };
    let class = configured("shop")?;
    let source = CellHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        incarnation: IncarnationId::new([3; 16])?,
        cell: CellId::new("Counter:parent")?,
    };
    let incarnation = "12".repeat(16);
    let name = values::execution_name(source.cell.as_str(), &incarnation, "job", 1)?;
    let namespace = cellar_native::cell_id::Namespace::new(
        "Sandbox",
        crate::engine_api::namespace_key("shop", "Sandbox"),
    );
    let target = CellHistory {
        cell: CellId::new(namespace.id_from_name(&name).scope())?,
        ..source.clone()
    };
    let authority = Authority::new(
        Identity {
            parent_scope: source.cell.as_str().into(),
            parent_incarnation: incarnation,
            name: "job".into(),
            generation: 1,
            execution_scope: target.cell.as_str().into(),
            parameters_digest: "13".repeat(32),
            configuration_digest: "11".repeat(32),
        },
        Action::Start,
        false,
    )?;
    let input = wire::encode(
        target.cell.as_str(),
        Some(&name),
        sandbox_execution::METHOD,
        wire::Authority::Sandbox(shared::Authority::new(&source, authority)?),
        &crate::engine_api::RpcData::Json("[]".into()),
        16 << 10,
    )?;
    let contract = class.event_contract().unwrap();
    let request = CommandRequest {
        id: CommandId::new(ClientId::new([9; 16])?, 1)?,
        operation: contract.operation.clone(),
        contract_digest: contract.digest,
        request_digest: command_request_digest(&input),
        durability: Durability::Quorum,
        retry_until_ms: 1000,
    };
    let admit = |selected: &RaftClass, admission| {
        validate_admission(
            ExecutionLimits::default(),
            selected.schema_digest(),
            &target,
            &request,
            selected,
            &input,
            admission,
        )
    };
    assert!(
        admit(&class, CommandAdmission::Event).is_ok(),
        "configured Sandbox identity must admit"
    );
    let foreign = configured("another-app")?;
    assert!(
        admit(&foreign, CommandAdmission::Event).is_err(),
        "another application must not authenticate this identity"
    );
    assert!(
        admit(&class, CommandAdmission::Public).is_err(),
        "ordinary ingress must not mint private authority"
    );
    Ok(())
}

// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::consensus::{CommandAdmission, QueryAdmission, RaftClass};

fn definition() -> event_stream::Definition {
    event_stream::Definition {
        resource: event_stream::Resource {
            tenant: "tenant".into(),
            application: "shop".into(),
            id: "00000000000000000000000000000001".into(),
            name: "orders".into(),
            family: "orders".into(),
        },
        revision: 1,
        retention: event_stream::Retention::Bounded,
        max_bytes: 4096,
        max_records: 10,
        max_age_ms: 1000,
        grants: vec![event_stream::Grant {
            id: "owner".into(),
            principal: "shop".into(),
            publish: true,
            read: true,
            subscriptions: vec![],
            administer: true,
        }],
        subscriptions: vec![],
    }
}

fn service(catalog: bool) -> Result<RaftClass> {
    let options = super::super::NativeCellConfiguration::default().options("shop");
    let configuration = super::super::AdmittedCellConfiguration::from_options(&options)?;
    Ok(super::super::native_services::event_service(
        &configuration,
        &[definition()],
        catalog,
    )?)
}

#[test]
fn configuration_probe_keeps_body_and_original_command_nonce_with_two_owned_wires() -> Result<()> {
    let definition = definition();
    let scope = event_stream::catalog_scope(&definition.resource)?;
    let name = event_stream::catalog_name(&definition.resource)?;
    let authority = Authority::Event(event_stream::Authority::deployment("shop", &definition)?);
    let args = serde_json::json!([{"operation":"a".repeat(32),"definition":"x".repeat(96 << 10)}])
        .to_string();
    let input = encode_ingress(
        &scope,
        Some(&name),
        "__catalogConfigure",
        authority,
        &RpcData::Json(args.clone()),
        256 << 10,
        [9; 16],
    )?;
    let original = input.clone();
    let (authority, query) = configuration_receipt_query(&scope, &input, input.len())?;
    assert_eq!(input, original);
    assert!(has_ingress_nonce(&input));
    assert!(!has_ingress_nonce(&query));
    assert!(query.len() <= input.len());
    assert!(configuration_receipt_query(&scope, &input, query.len() - 1).is_err());
    let decoded = decode(&scope, &query)?;
    assert_eq!(decoded.method, "__catalogConfigureReceipt");
    assert_eq!(decoded.rpc.name.as_deref(), Some(name.as_str()));
    assert_eq!(
        serde_json::to_value(decoded.authority)?,
        serde_json::to_value(authority)?
    );
    let RpcData::Json(actual) = decoded.rpc.args else {
        anyhow::bail!("changed argument kind");
    };
    assert_eq!(actual, args);
    Ok(())
}

#[test]
fn configuration_probe_refuses_other_methods_authority_and_truncated_headers() -> Result<()> {
    let definition = definition();
    let scope = event_stream::catalog_scope(&definition.resource)?;
    let name = event_stream::catalog_name(&definition.resource)?;
    let authority = Authority::Event(event_stream::Authority::deployment("shop", &definition)?);
    let input = encode_ingress(
        &scope,
        Some(&name),
        "__catalogConfigure",
        authority.clone(),
        &RpcData::Json("[{}]".into()),
        4096,
        [10; 16],
    )?;
    let header = u32::from_be_bytes(input[5..9].try_into()?) as usize;
    for end in 0..9 + header + 22 {
        assert!(
            configuration_receipt_query(&scope, &input[..end], 4096).is_err(),
            "accepted truncated header/nonce at {end}"
        );
    }
    let status = encode(
        &scope,
        Some(&name),
        "__catalogStatus",
        authority,
        &RpcData::Json("[{}]".into()),
        4096,
    )?;
    assert!(configuration_receipt_query(&scope, &status, 4096).is_err());
    let ordinary: event_stream::Authority = serde_json::from_value(serde_json::json!({
        "resource":definition.resource,"principal":"shop","grant":"owner","action":"Configure",
    }))?;
    let input = encode_ingress(
        &scope,
        Some(&name),
        "__catalogConfigure",
        Authority::Event(ordinary),
        &RpcData::Json("[{}]".into()),
        4096,
        [11; 16],
    )?;
    assert!(configuration_receipt_query(&scope, &input, 4096).is_err());
    Ok(())
}

#[tokio::test]
async fn event_catalog_enrollment_records_intent_and_work_atomically() -> Result<()> {
    use cellar_logic::{
        cell_group::{CellHistory, CellId},
        consensus::{ClusterId, IncarnationId, NamespaceId},
    };
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain.run(async {
        let definition = definition();
        let cell = CellHistory {
            cluster: ClusterId::new([1; 16])?,
            namespace: NamespaceId::new([2; 16])?,
            cell: CellId::new(event_stream::catalog_scope(&definition.resource)?)?,
            incarnation: IncarnationId::new([3; 16])?,
        };
        let class = service(true)?;
        let root = tempfile::tempdir()?;
        let mut database = crate::storage::CommandDatabase::open(
            &root.path().join("catalog.sqlite"), None, 4 << 20,
        )?;
        let inputs = crate::storage::CommandInputs {now_unix_millis:1234, random:[4;32]};
        let rejected: Result<()> = database.transaction(inputs, |tx| {
            class.invoke_initializer(&cell, tx)?;
            anyhow::bail!("creation did not commit")
        });
        assert!(rejected.is_err());
        assert_eq!(database.framework().query_row(
            "SELECT count(*) FROM sqlite_schema WHERE name IN ('__sys_event_catalog','__sys_work','__sys_raft_native_work')", [], |r| r.get::<_, u64>(0),
        )?, 0);
        let caller = event_stream::Authority::deployment("shop", &definition)?;
        use base64::Engine;
        let body = serde_json::json!({
            "operation":event_stream::raft::operation("shop",&definition)?,
            "definition":base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&definition)?),
        });
        let work = database.transaction(inputs, |tx| {
            class.invoke_initializer(&cell, tx)?;
            let storage = tx.storage()?;
            let observed = event_stream::metadata::read(
                &storage, cell.cell.as_str(), "__catalogConfigureReceipt",
                &Authority::Event(caller.clone()), &body, usize::MAX,
            )?;
            assert_eq!(observed["state"], "creating");
            assert_eq!(observed["operation"], body["operation"]);
            let args = crate::application::encode_rpc(
                &cellar_native::clone::StructuredValue::from_json(serde_json::json!([body.clone()]))?,
            )?;
            let wire = encode_ingress(
                cell.cell.as_str(), Some(&event_stream::catalog_name(&definition.resource)?),
                "__catalogConfigure", Authority::Event(caller.clone()), &args, usize::MAX, [5;16],
            )?;
            let compiled = class.event_enrollments.as_ref().unwrap()
                .get(&(cell.cell.as_str().to_owned(), definition.resource.id.clone())).unwrap();
            let (_, fingerprint) = super::super::native_event_enrollment::fingerprint(cell.cell.as_str(), &wire)?;
            assert_eq!(compiled.pending(fingerprint).unwrap(), observed,
                "compiled acceptance must describe the actual initializer's state");
            let work = storage.work_installation(cellar_logic::work::Reason::ResourceReconciliation)?.unwrap();
            assert_eq!(work.at_ms, 1234);
            Ok(work)
        })?;
        database.transaction(crate::storage::CommandInputs {now_unix_millis:2234, ..inputs}, |tx| {
            class.invoke_initializer(&cell, tx)?;
            assert_eq!(tx.storage()?.work_installation(cellar_logic::work::Reason::ResourceReconciliation)?, Some(work));
            Ok(())
        })?;
        Ok(())
    }).await?;
    domain.shutdown().await;
    result
}

#[test]
fn private_event_request_retains_authority_and_global_name() -> Result<()> {
    let definition = definition();
    let authority = Authority::Event(event_stream::Authority::deployment("shop", &definition)?);
    let scope = event_stream::cell_scope(&definition.resource)?;
    let name = event_stream::cell_name(&definition.resource)?;
    let input = encode(
        &scope,
        Some(&name),
        "__streamConfigure",
        authority.clone(),
        &RpcData::Json("[{}]".into()),
        4096,
    )?;
    let decoded = decode(&scope, &input)?;
    assert_eq!(decoded.method, "__streamConfigure");
    assert_eq!(decoded.rpc.name.as_deref(), Some(name.as_str()));
    assert!(matches!(decoded.authority, Authority::Event(_)));
    assert!(native_rpc::decode_request(&input).is_err());
    let public = native_rpc::encode_request(Some(&name), &RpcData::Json("[{}]".into()), 4096)?;
    assert!(decode(&scope, &public).is_err());
    native_rpc::validate_identity(
        "a different producer",
        crate::deploy::EVENT_STREAM_CLASS,
        &scope,
        Some(&name),
    )?;
    assert!(native_rpc::validate_identity(
        "shop",
        crate::deploy::EVENT_CATALOG_CLASS,
        &scope,
        Some(&name)
    )
    .is_err());
    assert!(encode(
        &scope,
        None,
        "__streamConfigure",
        authority.clone(),
        &RpcData::Json("[{}]".into()),
        4096
    )
    .is_err());
    assert!(encode(
        &scope,
        Some("foreign"),
        "__streamConfigure",
        authority.clone(),
        &RpcData::Json("[{}]".into()),
        4096
    )
    .is_err());
    assert!(encode(
        &scope,
        Some(&name),
        "__streamAppend",
        authority.clone(),
        &RpcData::Json("[{}]".into()),
        4096
    )
    .is_err());
    assert!(encode(
        &scope,
        Some(&name),
        "__streamConfigure",
        authority,
        &RpcData::Json("[{}]".into()),
        input.len() - 1
    )
    .is_err());
    for end in 0..input.len() {
        assert!(decode(&scope, &input[..end]).is_err());
    }
    let mut trailing = input.clone();
    trailing.push(0);
    assert!(decode(&scope, &trailing).is_err());
    let mut foreign = definition.resource;
    foreign.id = "00000000000000000000000000000002".into();
    assert!(decode(&event_stream::cell_scope(&foreign)?, &input).is_err());
    Ok(())
}

#[test]
fn changed_event_provenance_changes_the_retained_fingerprint() -> Result<()> {
    let mut definition = definition();
    let scope = event_stream::catalog_scope(&definition.resource)?;
    let name = event_stream::catalog_name(&definition.resource)?;
    let first = encode(
        &scope,
        Some(&name),
        "__catalogConfigure",
        Authority::Event(event_stream::Authority::deployment("shop", &definition)?),
        &RpcData::Json("[{}]".into()),
        4096,
    )?;
    definition.grants[0].id = "new-owner".into();
    let second = encode(
        &scope,
        Some(&name),
        "__catalogConfigure",
        Authority::Event(event_stream::Authority::deployment("shop", &definition)?),
        &RpcData::Json("[{}]".into()),
        4096,
    )?;
    assert_ne!(
        super::super::command_request_digest(&first),
        super::super::command_request_digest(&second)
    );
    let mut oversized = first;
    oversized[5..9].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(decode(&scope, &oversized).is_err());
    Ok(())
}

#[test]
fn event_contracts_are_not_public_or_maintenance_capabilities() -> Result<()> {
    for catalog in [false, true] {
        let class = RaftClass::event_service(catalog)?;
        let contract = class.event_contract().unwrap();
        assert!(class.contract(OPERATION).is_none());
        assert!(class.system_contract(OPERATION).is_none());
        assert!(class
            .validate_command_admission(&contract, CommandAdmission::Public)
            .is_err());
        assert!(class
            .validate_command_admission(&contract, CommandAdmission::System)
            .is_err());
        class.validate_command_admission(&contract, CommandAdmission::Event)?;
        let mut changed = contract;
        changed.digest[0] ^= 1;
        assert!(class
            .validate_command_admission(&changed, CommandAdmission::Event)
            .is_err());
        let query = class.event_query_contract().unwrap();
        assert!(class.contract(&query.operation).is_none());
        assert!(class.system_contract(&query.operation).is_none());
        assert!(class.validate_operation(&query).is_err());
        assert!(class
            .validate_query_admission(&query, QueryAdmission::Public)
            .is_err());
        for minimum in [
            super::super::Durability::Quorum,
            super::super::Durability::S3,
        ] {
            class.validate_query_admission(&query, QueryAdmission::Event(minimum))?;
            assert!(class
                .validate_query_admission(&changed, QueryAdmission::Event(minimum))
                .is_err());
        }
        assert!(class
            .validate_command_admission(&query, CommandAdmission::Event)
            .is_err());
        let mut foreign = query;
        foreign.digest[0] ^= 1;
        assert!(class
            .validate_query_admission(
                &foreign,
                QueryAdmission::Event(super::super::Durability::Quorum)
            )
            .is_err());
    }
    Ok(())
}

#[test]
fn event_metadata_reader_excludes_clock_lease_and_configuration_mutations() {
    use super::super::native_event_query::is_metadata;
    for method in [
        "__catalogStatus",
        "__catalogConfigureReceipt",
        "__catalogResolveName",
        "__catalogResolve",
        "__streamResolve",
        "__streamRead",
        "__streamSubscriptionStatus",
        "__streamAckReceipt",
    ] {
        assert!(is_metadata(method));
    }
    for method in [
        "__catalogConfigure",
        "__catalogRetire",
        "__streamConfigure",
        "__streamNext",
        "__streamAck",
        "__streamRetry",
        "__streamExtend",
        "__streamSubscriptionChange",
        "__streamAppend",
        "__streamStatus",
    ] {
        assert!(!is_metadata(method), "{method}");
    }
}

#[test]
fn event_service_factories_use_one_global_contract_and_keep_callbacks_private() -> Result<()> {
    for catalog in [false, true] {
        let class = service(catalog)?;
        let repeated = service(catalog)?;
        assert_eq!(class.schema_digest(), repeated.schema_digest());
        assert!(class.event_contract().is_some());
        assert!(class.has_native_callbacks());
        assert!(class.has_binding_maintenance());
        assert!(class.contract(OPERATION).is_none());
        assert!(class.system_contract(OPERATION).is_none());
        class.validate_shared_host(&Default::default())?;
    }
    Ok(())
}

#[test]
fn event_ingress_nonce_is_inside_the_authenticated_request_and_keeps_identity_checks() -> Result<()>
{
    use cellar_logic::{
        cell_group::{CellHistory, CellId},
        consensus::{ClusterId, IncarnationId, NamespaceId},
    };
    let definition = definition();
    let scope = event_stream::catalog_scope(&definition.resource)?;
    let name = event_stream::catalog_name(&definition.resource)?;
    let authority = Authority::Event(event_stream::Authority::deployment("shop", &definition)?);
    let args = RpcData::Json("[{}]".into());
    let first = encode_ingress(
        &scope,
        Some(&name),
        "__catalogConfigure",
        authority.clone(),
        &args,
        4096,
        [1; 16],
    )?;
    let second = encode_ingress(
        &scope,
        Some(&name),
        "__catalogConfigure",
        authority.clone(),
        &args,
        4096,
        [2; 16],
    )?;
    assert!(has_ingress_nonce(&first));
    assert!(decode(&scope, &first).is_ok());
    assert_ne!(
        super::super::command_request_digest(&first),
        super::super::command_request_digest(&second)
    );
    assert!(!has_ingress_nonce(&encode(
        &scope,
        Some(&name),
        "__catalogConfigure",
        authority,
        &args,
        4096
    )?));
    let class = service(true)?;
    let history = CellHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: CellId::new(scope)?,
        incarnation: IncarnationId::new([3; 16])?,
    };
    let contract = class.event_contract().unwrap();
    let ingress = super::super::CommandId::new(super::super::sql_ingress_identity::client(), 1)?;
    class.validate_command_identity(
        &history,
        &contract,
        ingress,
        super::super::Durability::Quorum,
        &first,
    )?;
    let foreign =
        super::super::CommandId::new(cellar_logic::consensus::ClientId::new([9; 16])?, 1)?;
    assert!(class
        .validate_command_identity(
            &history,
            &contract,
            foreign,
            super::super::Durability::Quorum,
            &first
        )
        .is_err());
    let mut malformed = first;
    malformed[5..9].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(!has_ingress_nonce(&malformed));
    assert!(class
        .validate_command_identity(
            &history,
            &contract,
            ingress,
            super::super::Durability::Quorum,
            &malformed
        )
        .is_err());
    Ok(())
}

fn binding() -> event_stream::Binding {
    event_stream::Binding {
        environment: "EVENTS".into(),
        resource: definition().resource,
        principal: "shop".into(),
        grant: "owner".into(),
        publish: true,
        read: true,
        subscriptions: vec![],
        administer: true,
    }
}

#[test]
fn shared_event_declarations_bind_schema_and_refuse_cross_family_aliases() -> Result<()> {
    let mut options = super::super::NativeCellConfiguration::default().options("shop");
    options.stream_bindings = vec![binding().into()];
    let first = super::super::AdmittedCellConfiguration::from_options(&options)?;
    let repeated = super::super::AdmittedCellConfiguration::from_options(&first.options())?;
    assert_eq!(first.digest(), repeated.digest());
    options.stream_bindings[0].publish = false;
    assert_ne!(
        first.digest(),
        super::super::AdmittedCellConfiguration::from_options(&options)?.digest()
    );
    options.sql_bindings = vec![("EVENTS".into(), "db".into())];
    assert!(super::super::AdmittedCellConfiguration::from_options(&options).is_err());
    options.sql_bindings.clear();
    options.stream_bindings[0].principal = "foreign".into();
    assert!(super::super::AdmittedCellConfiguration::from_options(&options).is_err());
    Ok(())
}

#[test]
fn superseded_source_activation_cannot_reopen_or_cancel_new_bindings() -> Result<()> {
    let mut options = super::super::NativeCellConfiguration::default().options("shop");
    let resolved = binding();
    let mut declaration: event_stream::BindingConfig = resolved.clone().into();
    declaration.resource.id = None;
    options.stream_bindings = vec![declaration.clone()];
    let mut config = crate::worker_config::WorkerConfig::new(options);
    config.event_readiness = std::sync::Arc::new(event_stream::readiness::Readiness::new(false));
    let source = crate::application::storage::EventSourceConfiguration::new(&config);
    let old = source.begin_activation()?;
    let current = source.begin_activation()?;
    assert!(source
        .pin_activation(old, std::slice::from_ref(&resolved))
        .is_err());
    assert!(!source.finish_activation(old, Ok(())));
    assert!(config.event_readiness.check().is_err());
    source.pin_activation(current, std::slice::from_ref(&resolved))?;
    assert!(source.finish_activation(current, Ok(())));
    assert!(!source.finish_activation(old, Err("old cancellation".into())));
    config.event_readiness.check()?;
    assert_eq!(config.event_readiness.binding(&declaration)?, resolved);
    let next = source.begin_activation()?;
    let mut recreated = resolved;
    recreated.resource.id = "00000000000000000000000000000002".into();
    assert!(source.pin_activation(next, &[recreated]).is_err());
    assert!(source.finish_activation(next, Err("retarget refused".into())));
    assert!(config.event_readiness.check().is_err());
    Ok(())
}

#[test]
fn event_services_refuse_sql_only_profiles_without_raising_their_quotas() -> Result<()> {
    let class = service(false)?;
    let mut host = crate::raft::host::RaftHostOptions::default();
    let limits = cellar_logic::event_stream::Limits::default();
    assert!(class.validate_event_host(&host, limits).is_err());
    host.effects = Some(Default::default());
    host.max_input_bytes = 1024;
    assert!(class.validate_event_host(&host, limits).is_err());
    assert_eq!(host.max_input_bytes, 1024);
    host.max_input_bytes = 16 << 10;
    class.validate_event_host(&host, limits)?;
    let mut large = definition();
    large
        .grants
        .extend((0..64).map(|index| event_stream::Grant {
            id: format!("reader-{index}"),
            principal: "x".repeat(128),
            publish: false,
            read: true,
            subscriptions: vec![],
            administer: false,
        }));
    let options = super::super::NativeCellConfiguration::default().options("shop");
    let configuration = super::super::AdmittedCellConfiguration::from_options(&options)?;
    let large = super::super::native_services::event_service(&configuration, &[large], false)?;
    assert!(large.validate_event_host(&host, limits).is_err());
    assert_eq!(host.max_input_bytes, 16 << 10);
    host.max_input_bytes = 2 << 20;
    host.execution.max_result_bytes = 2 << 20;
    class.validate_event_host(&host, limits)?;
    large.validate_event_host(&host, limits)?;
    Ok(())
}

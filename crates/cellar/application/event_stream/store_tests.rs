// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Production SQLite decisions under a synthetic activation; not fleet proof.
use super::*;
use crate::application::{storage::Resources, Activation};
use crate::engine_api::{Compat, WorkerConfigOptions};
use std::sync::Arc;

const SCOPE: &str = "__EventStream:retention-test";

fn definition() -> Definition {
    let binding = super::super::Binding {
        environment: "EVENTS".into(),
        resource: super::super::Resource {
            tenant: "tenant".into(),
            application: "shop".into(),
            id: "00000000000000000000000000000001".into(),
            name: "orders".into(),
            family: "orders".into(),
        },
        principal: "shop".into(),
        grant: "owner".into(),
        publish: true,
        read: true,
        subscriptions: vec!["projection".into()],
        administer: true,
    };
    Definition {
        resource: binding.resource.clone(),
        revision: 1,
        retention: Retention::Required,
        max_bytes: 100,
        max_records: 10,
        max_age_ms: 100,
        grants: vec![Grant::from_binding(&binding)],
        subscriptions: vec![super::super::SubscriptionDefinition {
            name: "projection".into(),
            start: Start::Earliest,
            required: true,
        }],
    }
}

async fn run_case(
    case: impl FnOnce(Storage, Definition) -> Result<()> + Send + 'static,
) -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut limits = crate::application::limits::CellLimits::default();
    limits.events.sweep_rows = 1;
    domain.services().configure_cell_limits(limits)?;
    let result=domain.run(async move {
        let cells = crate::storage::Cells::default();
        let _cells = cells.install();
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("stream.sqlite");
        crate::storage::open_at_epoch(SCOPE, path.to_str().unwrap(), 1, true, None, false)?;
        let config = crate::worker_config::WorkerConfig::new(WorkerConfigOptions {
            src: String::new(),
            script_name: "retention-test".into(),
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
            node: String::new(),
            modules: vec![],
            compat: Compat::default(),
        });
        let activation = Arc::new(Activation {
            worker_config: Arc::new(config),
            scope: SCOPE.into(),
            epoch: 1,
            delete_all_deletes_alarm: false,
            resources: Resources::default(),
            websocket_states: Default::default(),
        });
        let mut context = crate::host_io::IoContext::new();
        Arc::get_mut(&mut context).unwrap().native_activation = Some(activation.clone());
        let _context = crate::host_io::CurrentGuard::enter(context);
        let db = Storage::native(activation, None, 0);
        super::super::schema::open(&db)?;
        let definition = definition();
        db.tx(|tx| {
            tx.system_sql("INSERT INTO __sys_stream(singleton,definition,revision,lifecycle,next_offset,earliest_offset,bytes,rows,logical_ms,config_operation,config_digest,config_principal,config_grant,byte_position) VALUES(1,?,1,'active',3,1,2,2,0,'operation','digest','shop','owner',2)",&[serde_json::to_string(&definition)?.into()])?;
            tx.system_sql("INSERT INTO __sys_stream_subscriptions(id,name,generation,start,checkpoint,required) VALUES('00000000000000000000000000000002','projection','00000000000000000000000000000003','\"earliest\"',0,1)",&[])?;
            for offset in 1..=2 {
                tx.system_sql("INSERT INTO __sys_stream_events(offset,lane,sequence,envelope,size,append_ms,byte_position) VALUES(?,'lane',?,?,1,100,?)",
                    &[offset.into(),offset.into(),SqlValue::Blob(vec![b'a']),offset.into()])?;
            }
            Ok(())
        })?;
        let result = case(db,definition);
        crate::storage::close(SCOPE);
        result
    }).await?;
    domain.shutdown().await;
    result
}

fn deadline(db: &Storage) -> Result<Option<i64>> {
    Ok(db
        .work_installation(cellar_logic::work::Reason::StreamMaintenance)?
        .map(|work| work.at_ms))
}

fn status_caller(definition: &Definition) -> Result<Authority> {
    Ok(serde_json::from_value(json!({
        "resource": definition.resource,
        "principal": "shop",
        "grant": "owner",
        "source": "Counter:consumer",
        "action": Action::Consume,
    }))?)
}

fn catalog_configuration_fixture(
    db: &Storage,
    definition: &Definition,
) -> Result<(Authority, Value)> {
    use base64::Engine;
    let caller = Authority::deployment("shop", definition)?;
    let operation = super::super::raft::operation("shop", definition)?;
    let encoded = serde_json::to_string(definition)?;
    let digest = hex::encode(Sha256::digest(encoded.as_bytes()));
    let authority = serde_json::to_string(&caller)?;
    super::super::catalog_schema::open(db)?;
    db.system_sql("INSERT INTO __sys_event_catalog(id,name,lifecycle,tenant,application,family,desired_revision,pending_definition,pending_authority,operation,digest,operation_authority,retry_at_ms) VALUES(?,?,'creating',?,?,?,?,?,?,?,?,?,1000)",
        &[definition.resource.id.clone().into(),definition.resource.name.clone().into(),definition.resource.tenant.clone().into(),definition.resource.application.clone().into(),definition.resource.family.clone().into(),definition.revision.into(),encoded.clone().into(),authority.clone().into(),operation.clone().into(),digest.clone().into(),authority.into()])?;
    db.system_sql("INSERT INTO __sys_event_catalog_receipts(operation,resource,kind,digest,principal,grant_id) VALUES(?,?,'configure',?,'shop','owner')",&[operation.clone().into(),definition.resource.id.clone().into(),digest.into()])?;
    Ok((
        caller,
        json!({"operation":operation,"definition":base64::engine::general_purpose::STANDARD.encode(encoded)}),
    ))
}

#[tokio::test]
async fn catalog_configuration_join_reads_exact_intent_without_rearming_or_delivering() -> Result<()>
{
    run_case(|db, definition| {
        let (caller, body) = catalog_configuration_fixture(&db, &definition)?;
        let scope = super::super::catalog_scope(&definition.resource)?;
        let authority = crate::application::consensus::native_event_rpc::Authority::Event(caller);
        let before = db.system_sql("SELECT * FROM __sys_event_catalog", &[])?;
        let receipts = db.system_sql("SELECT * FROM __sys_event_catalog_receipts", &[])?;
        let read = || super::super::metadata::read(&db, &scope, "__catalogConfigureReceipt", &authority, &body, usize::MAX);
        for _ in 0..2 {
            let pending = read()?;
            assert_eq!(pending["state"], "creating");
            assert_eq!(pending["operation"], body["operation"]);
            assert!(pending["accepted_digest"].is_null());
        }
        assert_eq!(db.system_sql("SELECT * FROM __sys_event_catalog", &[])?, before);
        assert_eq!(db.system_sql("SELECT * FROM __sys_event_catalog_receipts", &[])?, receipts);
        db.system_sql("UPDATE __sys_event_catalog SET lifecycle='active',definition=pending_definition,pending_definition=NULL,pending_authority=NULL,accepted_revision=1,desired_revision=NULL", &[])?;
        assert_eq!(read()?["state"], "active");
        db.system_sql("UPDATE __sys_event_catalog SET lifecycle='deleted'", &[])?;
        assert_eq!(read().unwrap_err().downcast_ref::<super::super::DeliveryError>().unwrap().code, ErrorCode::DestinationDeleted);
        let mut absent = body.clone();
        absent["operation"] = json!("f".repeat(32));
        let refused = super::super::metadata::read(&db, &scope, "__catalogConfigureReceipt", &authority, &absent, usize::MAX).unwrap_err();
        assert_eq!(refused.downcast_ref::<super::super::DeliveryError>().unwrap().code, ErrorCode::DestinationDeleted);
        Ok(())
    }).await
}

#[tokio::test]
async fn catalog_configuration_join_refuses_changed_body_caller_grant_and_operation() -> Result<()>
{
    use base64::Engine;
    run_case(|db, mut definition| {
        let (caller, body) = catalog_configuration_fixture(&db, &definition)?;
        let read = |caller: &Authority, body: &Value| super::super::catalog::configuration_receipt_read(&db, caller, body);
        let mut changed = body.clone();
        definition.max_records += 1;
        changed["definition"] = json!(base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&definition)?));
        assert_eq!(read(&caller, &changed).unwrap_err().downcast_ref::<super::super::DeliveryError>().unwrap().code, ErrorCode::ConfigConflict);
        changed = body.clone();
        changed["operation"] = json!("a".repeat(32));
        assert_eq!(read(&caller, &changed).unwrap_err().downcast_ref::<super::super::DeliveryError>().unwrap().code, ErrorCode::OperationUnknown);
        let mut foreign = caller.clone();
        foreign.principal = "another".into();
        assert_eq!(read(&foreign, &body).unwrap_err().downcast_ref::<super::super::DeliveryError>().unwrap().code, ErrorCode::Forbidden);
        let ordinary: Authority = serde_json::from_value(json!({"resource":caller.resource,"principal":"shop","grant":"owner","action":Action::Configure}))?;
        assert!(read(&ordinary, &body).is_err());
        db.system_sql("UPDATE __sys_event_catalog SET operation=?", &["b".repeat(32).into()])?;
        assert_eq!(read(&caller, &body).unwrap_err().downcast_ref::<super::super::DeliveryError>().unwrap().code, ErrorCode::ConfigConflict);
        db.system_sql("UPDATE __sys_event_catalog SET definition=?", &[serde_json::to_string(&Definition {grants:vec![], ..definition})?.into()])?;
        assert_eq!(read(&caller, &body).unwrap_err().downcast_ref::<super::super::DeliveryError>().unwrap().code, ErrorCode::Forbidden);
        Ok(())
    }).await
}

#[tokio::test]
async fn retained_read_observes_one_frontier_without_mutating_stream_or_work() -> Result<()> {
    run_case(|db, definition| {
        let mut caller = status_caller(&definition)?;
        caller.action = Action::Read;
        let scope = super::super::cell_scope(&definition.resource)?;
        let read = |body: &Value| {
            super::super::metadata::read(
                &db,
                &scope,
                "__streamRead",
                &super::super::super::consensus::native_event_rpc::Authority::Event(caller.clone()),
                body,
                usize::MAX,
            )
        };
        let before = db.system_sql_read("SELECT * FROM __sys_stream", &[])?;
        let work = deadline(&db)?;
        let page = read(&json!({"max_records":1,"max_bytes":1}))?;
        assert_eq!(page["earliest"], "1");
        assert_eq!(page["end"], "3");
        assert_eq!(page["next"], "2");
        assert_eq!(page["records"], json!([{"offset":"1","envelope":"YQ=="}]));
        assert_eq!(read(&json!({"next":"2"}))?["next"], "3");
        assert_eq!(read(&json!({"next":"3"}))?["records"], json!([]));
        let invalid = read(&json!({"next":"4"})).unwrap_err();
        assert_eq!(
            invalid
                .downcast_ref::<super::super::DeliveryError>()
                .unwrap()
                .code,
            ErrorCode::InvalidCursor
        );
        assert_eq!(
            db.system_sql_read("SELECT * FROM __sys_stream", &[])?,
            before
        );
        assert_eq!(deadline(&db)?, work);
        assert_eq!(
            db.system_sql_read("SELECT offset FROM __sys_stream_events", &[])?
                .len(),
            2
        );
        // Only committed maintenance can move the frontier. A cursor that was
        // valid in an earlier image must then report the new earliest offset.
        db.system_sql("DELETE FROM __sys_stream_events WHERE offset=1", &[])?;
        db.system_sql("UPDATE __sys_stream SET earliest_offset=2", &[])?;
        let expired = read(&json!({"next":"1"})).unwrap_err();
        let expired = expired
            .downcast_ref::<super::super::DeliveryError>()
            .unwrap();
        assert_eq!(expired.code, ErrorCode::CursorExpired);
        assert_eq!(expired.data["earliest"], "2");
        db.system_sql(
            "UPDATE __sys_stream_events SET size=4,envelope=? WHERE offset=2",
            &[SqlValue::Blob(vec![b'a'; 4])],
        )?;
        let bounded = super::super::metadata::read(
            &db,
            &scope,
            "__streamRead",
            &super::super::super::consensus::native_event_rpc::Authority::Event(caller),
            &json!({}),
            4100,
        )
        .unwrap_err();
        assert_eq!(
            bounded
                .downcast_ref::<super::super::DeliveryError>()
                .unwrap()
                .code,
            ErrorCode::BatchTooSmall
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn retained_read_requires_current_read_grant_and_live_resource() -> Result<()> {
    run_case(|db, definition| {
        let mut caller = status_caller(&definition)?;
        caller.action = Action::Read;
        let scope = super::super::cell_scope(&definition.resource)?;
        let read = |scope: &str, caller: &Authority| {
            super::super::metadata::read(
                &db,
                scope,
                "__streamRead",
                &super::super::super::consensus::native_event_rpc::Authority::Event(caller.clone()),
                &json!({}),
                usize::MAX,
            )
        };
        let refusal = |failure: anyhow::Error, code| {
            assert_eq!(
                failure
                    .downcast_ref::<super::super::DeliveryError>()
                    .unwrap()
                    .code,
                code
            )
        };
        read(&scope, &caller)?;
        let mut wrong_action = caller.clone();
        wrong_action.action = Action::Consume;
        refusal(
            read(&scope, &wrong_action).unwrap_err(),
            ErrorCode::Forbidden,
        );
        assert!(read("__EventStream:foreign", &caller).is_err());
        let mut revoked = definition.clone();
        revoked.grants[0].read = false;
        db.system_sql(
            "UPDATE __sys_stream SET definition=?",
            &[serde_json::to_string(&revoked)?.into()],
        )?;
        refusal(read(&scope, &caller).unwrap_err(), ErrorCode::Forbidden);
        db.system_sql(
            "UPDATE __sys_stream SET definition=?,lifecycle='deleted'",
            &[serde_json::to_string(&definition)?.into()],
        )?;
        refusal(
            read(&scope, &caller).unwrap_err(),
            ErrorCode::DestinationDeleted,
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn subscription_status_observes_expired_lease_without_mutating_deadline_authority(
) -> Result<()> {
    run_case(|db, definition| {
        let caller = status_caller(&definition)?;
        let body = json!({"subscription":"projection"});
        db.system_sql("INSERT INTO __sys_stream_leases VALUES('00000000000000000000000000000002',1,'token','fetch',1,2,100,5000)", &[])?;
        let before = db.system_sql_read("SELECT logical_ms,earliest_offset,next_offset FROM __sys_stream", &[])?;
        let work = deadline(&db)?;
        let status: super::super::SubscriptionStatus = serde_json::from_value(
            super::super::lifecycle_store::status_read(&db, &caller, &body)?
        )?;
        assert_eq!(status.identity.id, "00000000000000000000000000000002");
        assert_eq!(status.identity.generation, "00000000000000000000000000000003");
        assert_eq!(status.checkpoint, 0);
        assert_eq!(status.observation.retained_lag_records, 2);
        assert!(!status.observation.lease_active);
        assert_eq!(status.lease.unwrap().expires_ms, 5000);
        assert_eq!(db.system_sql_read("SELECT logical_ms,earliest_offset,next_offset FROM __sys_stream", &[])?, before);
        assert_eq!(db.system_sql_read("SELECT expires_ms FROM __sys_stream_leases", &[])?.len(), 1);
        assert_eq!(db.system_sql_read("SELECT offset FROM __sys_stream_events", &[])?.len(), 2);
        assert_eq!(deadline(&db)?, work);
        let mut administrator = caller.clone();
        administrator.action = Action::Administer;
        let observed: super::super::SubscriptionStatus = serde_json::from_value(
            super::super::lifecycle_store::status_read(&db, &administrator, &body)?
        )?;
        assert_eq!(observed.identity, status.identity);
        Ok(())
    }).await
}

#[tokio::test]
async fn subscription_status_requires_current_grant_and_observes_recreation() -> Result<()> {
    run_case(|db, definition| {
        let caller = status_caller(&definition)?;
        let body = json!({"subscription":"projection"});
        let read = |caller: &Authority| super::super::lifecycle_store::status_read(&db, caller, &body);
        let refusal = |error: anyhow::Error, code| {
            assert_eq!(error.downcast_ref::<super::super::DeliveryError>().unwrap().code, code);
        };
        let original: super::super::SubscriptionStatus = serde_json::from_value(read(&caller)?)?;
        let mut wrong_action = caller.clone();
        wrong_action.action = Action::Append;
        refusal(read(&wrong_action).unwrap_err(), ErrorCode::Forbidden);
        let mut changed = definition.clone();
        changed.grants[0].subscriptions.clear();
        db.system_sql("UPDATE __sys_stream SET definition=?", &[serde_json::to_string(&changed)?.into()])?;
        refusal(read(&caller).unwrap_err(), ErrorCode::Forbidden);
        let mut administrator = caller.clone();
        administrator.action = Action::Administer;
        read(&administrator)?;
        changed.grants.clear();
        db.system_sql("UPDATE __sys_stream SET definition=?", &[serde_json::to_string(&changed)?.into()])?;
        refusal(read(&administrator).unwrap_err(), ErrorCode::Forbidden);
        db.system_sql("UPDATE __sys_stream SET definition=?", &[serde_json::to_string(&definition)?.into()])?;
        db.system_sql("UPDATE __sys_stream_subscriptions SET state='deleted'", &[])?;
        refusal(read(&caller).unwrap_err(), ErrorCode::SubscriptionReset);
        db.system_sql("UPDATE __sys_stream_subscriptions SET state='active',id='00000000000000000000000000000004',generation='00000000000000000000000000000005'", &[])?;
        let recreated: super::super::SubscriptionStatus = serde_json::from_value(read(&caller)?)?;
        assert_ne!(recreated.identity, original.identity);
        assert_eq!(recreated.identity.id, "00000000000000000000000000000004");
        db.system_sql("UPDATE __sys_stream SET lifecycle='deleted'", &[])?;
        refusal(read(&caller).unwrap_err(), ErrorCode::DestinationDeleted);
        Ok(())
    }).await
}

#[tokio::test]
async fn required_protection_quiesces_age_work_then_progress_rearms_bounded_sweeps() -> Result<()> {
    run_case(|db, definition| {
        crate::storage::set_alarm(SCOPE, 700000)?;
        for _ in 0..3 {
            db.tx(|tx| schedule_at(tx, &definition, 500))?;
            assert_eq!(deadline(&db)?, None);
        }
        assert_eq!(crate::storage::get_alarm(SCOPE), Some(700000));
        assert_eq!(
            db.system_sql("SELECT offset FROM __sys_stream_events", &[])?
                .len(),
            2
        );
        db.tx(|tx| {
            tx.system_sql("UPDATE __sys_stream_subscriptions SET checkpoint=2", &[])?;
            schedule_at(tx, &definition, 500)
        })?;
        assert_eq!(deadline(&db)?, Some(1500));
        db.tx(|tx| {
            sweep(tx, &definition, 1500, 0, 0)?;
            schedule_at(tx, &definition, 1500)
        })?;
        assert_eq!(
            db.system_sql("SELECT offset FROM __sys_stream_events", &[])?
                .len(),
            1
        );
        assert_eq!(deadline(&db)?, Some(2500));
        db.tx(|tx| {
            sweep(tx, &definition, 2500, 0, 0)?;
            schedule_at(tx, &definition, 2500)
        })?;
        assert!(db
            .system_sql("SELECT offset FROM __sys_stream_events", &[])?
            .is_empty());
        assert_eq!(deadline(&db)?, None);
        assert_eq!(crate::storage::get_alarm(SCOPE), Some(700000));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn live_lease_schedules_only_expiry_then_expired_lease_cleanup_unblocks_age() -> Result<()> {
    run_case(|db,mut definition| {
        definition.retention=Retention::Bounded;
        db.tx(|tx|{
            tx.system_sql("UPDATE __sys_stream_subscriptions SET required=0",&[])?;
            tx.system_sql("INSERT INTO __sys_stream_leases VALUES('00000000000000000000000000000002',1,'token','fetch',1,2,100,5000)",&[])?;
            schedule_at(tx,&definition,500)
        })?;
        assert_eq!(deadline(&db)?,Some(5000));
        db.tx(|tx|schedule_at(tx,&definition,5100))?;
        assert_eq!(deadline(&db)?,Some(6100));
        db.tx(|tx|{
            tx.system_sql("DELETE FROM __sys_stream_leases WHERE expires_ms<=6100",&[])?;
            sweep(tx,&definition,6100,0,0)?;
            schedule_at(tx,&definition,6100)
        })?;
        assert_eq!(db.system_sql("SELECT offset FROM __sys_stream_events",&[])?.len(),1);
        assert_eq!(deadline(&db)?,Some(7100));
        Ok(())
    }).await
}

#[tokio::test]
async fn retention_configuration_release_and_rollback_preserve_deadline_authority() -> Result<()> {
    run_case(|db, mut definition| {
        db.tx(|tx| schedule_at(tx, &definition, 500))?;
        assert_eq!(deadline(&db)?, None);
        definition.retention = Retention::Bounded;
        assert!(db
            .tx::<()>(|tx| {
                tx.system_sql("UPDATE __sys_stream_subscriptions SET required=0", &[])?;
                schedule_at(tx, &definition, 500)?;
                anyhow::bail!("abort configuration")
            })
            .is_err());
        assert_eq!(deadline(&db)?, None);
        assert_eq!(
            number(
                &db.system_sql("SELECT required FROM __sys_stream_subscriptions", &[])?[0],
                "required"
            )?,
            1
        );
        db.tx(|tx| {
            tx.system_sql("UPDATE __sys_stream_subscriptions SET required=0", &[])?;
            schedule_at(tx, &definition, 500)
        })?;
        assert_eq!(deadline(&db)?, Some(1500));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn scheduling_clamps_to_durable_clock_and_rejects_protected_deadline_overflow() -> Result<()>
{
    run_case(|db,mut definition| {
        let logical=i64::MAX-10000;
        db.tx(|tx| {
            tx.system_sql("UPDATE __sys_stream SET logical_ms=?",&[logical.into()])?;
            tx.system_sql("INSERT INTO __sys_stream_leases VALUES('00000000000000000000000000000002',1,'token','fetch',1,2,100,?)",&[(logical-1).into()])?;
            schedule(tx,&definition)
        })?;
        assert_eq!(deadline(&db)?,Some(logical+1000));
        definition.max_age_ms=i64::MAX;
        assert!(db.tx(|tx|schedule_at(tx,&definition,500)).is_err());
        assert_eq!(deadline(&db)?,Some(logical+1000));
        Ok(())
    }).await
}

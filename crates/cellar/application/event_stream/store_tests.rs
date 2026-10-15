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
        let db = Storage(activation, None, 0);
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

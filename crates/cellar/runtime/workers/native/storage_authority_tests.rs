//! Admission can close while synchronous application code is still executing.

use super::*;
use crate::application::event_stream::{Binding, Event, Resource};
use crate::application::storage::SqlValue;
use serde::Serialize;

const SCOPE: &str = "Account:storage-authority";

#[derive(Serialize)]
struct Changed(u64);

impl Event for Changed {
    const FAMILY: &'static str = "account-changed";
    const VERSION: u32 = 1;

    fn key(&self) -> &str {
        "account"
    }
}

fn configuration() -> Arc<WorkerConfig> {
    let mut configuration = config(Arc::new(Signals::default()));
    Arc::get_mut(&mut configuration).unwrap().stream_bindings = vec![Binding {
        environment: "EVENTS".into(),
        resource: Resource {
            tenant: "test".into(),
            application: "native-direct".into(),
            id: "00000000000000000000000000000001".into(),
            name: "accounts".into(),
            family: Changed::FAMILY.into(),
        },
        principal: "native-direct".into(),
        grant: "publish".into(),
        publish: true,
        read: false,
        subscriptions: Vec::new(),
        administer: false,
    }
    .into()];
    configuration
}

fn own(worker: &mut NativeWorker, path: &std::path::Path) -> Result<()> {
    worker.own_cell(
        SCOPE,
        Some(CellStorage {
            path: path.to_str().unwrap(),
            epoch: 1,
            replicated_wake: false,
            vfs: None,
        }),
    )?;
    Ok(())
}

#[tokio::test]
async fn retained_storage_refuses_wrong_and_closed_domains_inside_an_entered_turn() -> Result<()> {
    let first = asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let second = asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = first
        .run(async {
            let directory = tempfile::tempdir()?;
            let mut worker = NativeWorker::new(configuration())?;
            own(&mut worker, &directory.path().join("cell.sqlite"))?;
            let _cells = worker.cells.install();
            let context = worker.context(Some(SCOPE), None, None, false)?;
            let _turn = crate::host_io::CurrentGuard::enter(context.io.clone());
            let db = context.storage()?;
            db.put_json("business", &serde_json::json!(1))?;
            let mut cursor = db.sql_cursor("SELECT 1 UNION ALL SELECT 2", &[])?;
            second
                .run(async {
                    let failure = db.put_json("business", &serde_json::json!(2)).unwrap_err();
                    assert!(failure.to_string().contains("another execution domain"));
                })
                .await?;
            assert_eq!(db.get_json("business")?, Some(serde_json::json!(1)));
            first.close();
            assert!(db.put_json("business", &serde_json::json!(3)).is_err());
            assert!(db.sql("CREATE TABLE forbidden(value)", &[]).is_err());
            assert!(db.get_json("business").is_err());
            assert!(cursor.next_raw().is_err());
            assert!(db.tx(|_| Ok(())).is_err());
            Ok::<_, anyhow::Error>(())
        })
        .await;
    first.shutdown().await;
    second.shutdown().await;
    result?
}

#[tokio::test]
async fn closing_domain_before_commit_rolls_back_business_and_outbox_together() -> Result<()> {
    let domain = asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let directory = tempfile::tempdir()?;
            let mut worker = NativeWorker::new(configuration())?;
            own(&mut worker, &directory.path().join("cell.sqlite"))?;
            let _cells = worker.cells.install();
            let context = worker.context(Some(SCOPE), None, None, false)?;
            let _turn = crate::host_io::CurrentGuard::enter(context.io.clone());
            let db = context.storage()?;
            let events = context.stream::<Changed>("EVENTS")?;
            db.tx(|tx| {
                tx.put_json("business", &serde_json::json!(1))?;
                tx.emit(&events, Changed(1))
            })?;
            let result = db.tx(|tx| {
                tx.put_json("business", &serde_json::json!(2))?;
                tx.emit(&events, Changed(2))?;
                domain.close();
                Ok(())
            });
            assert!(result.is_err(), "closed execution domain committed a transaction");
            // Framework inspection after refusal must still work, including
            // rollback cleanup on the original connection in the closed domain.
            let state = storage::system_sql(
                SCOPE,
                "SELECT CAST(v AS TEXT) AS value FROM __sys_cell_kv WHERE scope=?1 AND k='business'",
                &[SCOPE.into()],
            )?;
            assert_eq!(state[0]["value"], SqlValue::Text("1".into()));
            let records = storage::system_sql(SCOPE, "SELECT COUNT(*) AS n FROM __sys_outbox", &[])?;
            assert_eq!(records[0]["n"], SqlValue::Integer(1));
            let lanes = storage::system_sql(
                SCOPE,
                "SELECT next_sequence,settled_through FROM __sys_outbox_lanes",
                &[],
            )?;
            assert_eq!(lanes[0]["next_sequence"], SqlValue::Integer(2));
            assert_eq!(lanes[0]["settled_through"], SqlValue::Integer(0));
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result?
}

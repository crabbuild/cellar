use super::{
    authority::{Action, Authority},
    broker,
    error::{error, ErrorCode},
    store, Definition,
};
use crate::application::storage::{SqlRow, SqlValue};
use crate::application::{Cell, Context, HandlerFuture, Invocation, Output, Result, Storage};
use base64::Engine;
use serde_json::{json, Value};
use std::sync::Arc;

struct Catalog;

pub(crate) fn create_cell(context: &Context) -> Result<Arc<dyn Cell>> {
    super::catalog_schema::open(&context.storage()?)?;
    Ok(Arc::new(Catalog))
}

fn status(row: &SqlRow) -> Result<Value> {
    let accepted = match row.get("definition") {
        Some(SqlValue::Text(encoded)) => Some(serde_json::from_str::<Definition>(encoded)?),
        _ => None,
    };
    let revision = accepted
        .as_ref()
        .map(|definition| definition.revision.to_string());
    let digest = accepted
        .as_ref()
        .map(|definition| {
            serde_json::to_vec(definition).map(|encoded| {
                use sha2::{Digest, Sha256};
                hex::encode(Sha256::digest(encoded))
            })
        })
        .transpose()?;
    Ok(
        json!({"state":store::text(row,"lifecycle")?,"operation":store::text(row,"operation")?,
        "resource":store::text(row,"id")?,"accepted_revision":revision,"accepted_digest":digest,
        "last_error":match row.get("last_error") {Some(SqlValue::Text(error))=>Some(error),_=>None}}),
    )
}

fn open(db: &Storage) -> Result<()> {
    super::catalog_schema::open(db)
}

fn authorize_resource(db: &Storage, caller: &Authority, action: Action) -> Result<SqlRow> {
    let row = db
        .system_sql(
            "SELECT * FROM __sys_event_catalog WHERE id=?",
            &[caller.resource.id.clone().into()],
        )?
        .into_iter()
        .next()
        .ok_or_else(|| error(ErrorCode::NotInitialized, "catalog resource missing"))?;
    let encoded = match row.get("definition") {
        Some(SqlValue::Text(definition)) => definition.clone(),
        _ => store::text(&row, "pending_definition")?,
    };
    let definition: Definition = serde_json::from_str(&encoded)?;
    store::authorize(&definition, caller, action, None)?;
    Ok(row)
}

fn receipt(db: &Storage, operation: &str) -> Result<Option<SqlRow>> {
    Ok(db
        .system_sql(
            "SELECT * FROM __sys_event_catalog_receipts WHERE operation=?",
            &[operation.into()],
        )?
        .into_iter()
        .next())
}

fn validate_receipt(
    row: &SqlRow,
    caller: &Authority,
    kind: &str,
    digest: Option<&str>,
) -> Result<()> {
    if store::text(row, "resource")? != caller.resource.id || store::text(row, "kind")? != kind {
        return Err(error(
            ErrorCode::ConfigConflict,
            "operation names another resource or action",
        ));
    }
    if store::text(row, "principal")? != caller.principal
        || store::text(row, "grant_id")? != caller.grant
    {
        return Err(error(
            ErrorCode::Forbidden,
            "operation belongs to another caller",
        ));
    }
    if let Some(digest) = digest {
        if store::text(row, "digest")? != digest {
            return Err(error(
                ErrorCode::ConfigConflict,
                "operation identity was reused with different arguments",
            ));
        }
    }
    Ok(())
}

fn insert_receipt(
    db: &Storage,
    caller: &Authority,
    operation: &str,
    kind: &str,
    digest: &str,
) -> Result<()> {
    let count = db.system_sql(
        "SELECT count(*) AS n FROM __sys_event_catalog_receipts",
        &[],
    )?;
    if store::number(&count[0], "n")? >= 100_000 {
        return Err(error(
            ErrorCode::OperationLimit,
            "catalog operation history exhausted",
        ));
    }
    db.system_sql("INSERT INTO __sys_event_catalog_receipts(operation,resource,kind,digest,principal,grant_id) VALUES(?,?,?,?,?,?)",
        &[operation.into(),caller.resource.id.clone().into(),kind.into(),digest.into(),caller.principal.clone().into(),caller.grant.clone().into()])?;
    Ok(())
}

fn finish_receipt(db: &Storage, id: &str, operation: &str) -> Result<()> {
    let row = db.system_sql(
        "SELECT * FROM __sys_event_catalog WHERE id=? AND operation=?",
        &[id.into(), operation.into()],
    )?;
    if let Some(row) = row.first() {
        let result = serde_json::to_string(&status(row)?)?;
        db.system_sql(
            "UPDATE __sys_event_catalog_receipts SET result=? WHERE operation=? AND result IS NULL",
            &[result.into(), operation.into()],
        )?;
    }
    Ok(())
}

fn operation_result(db: &Storage, id: &str, operation: &str) -> Result<Value> {
    let receipt = receipt(db, operation)?
        .ok_or_else(|| error(ErrorCode::NotInitialized, "catalog operation missing"))?;
    if let Some(SqlValue::Text(result)) = receipt.get("result") {
        return Ok(serde_json::from_str(result)?);
    }
    let row = db
        .system_sql(
            "SELECT * FROM __sys_event_catalog WHERE id=? AND operation=?",
            &[id.into(), operation.into()],
        )?
        .into_iter()
        .next()
        .ok_or_else(|| {
            error(
                ErrorCode::ConfigConflict,
                "pending catalog operation was superseded",
            )
        })?;
    status(&row)
}

async fn retire(context: &Context, body: &Value) -> Result<Value> {
    let caller = store::authority(context, Action::Administer)?;
    let encoded = body["request"]
        .as_str()
        .ok_or_else(|| error(ErrorCode::InvalidRequest, "retirement request missing"))?;
    let (request, digest) = super::retirement_store::decode(encoded)?;
    let db = context.storage()?;
    open(&db)?;
    let row = authorize_resource(&db, &caller, Action::Administer)?;
    if let Some(prior) = receipt(&db, &request.operation)? {
        validate_receipt(&prior, &caller, "retire", Some(&digest))?;
        if let Some(SqlValue::Text(result)) = prior.get("result") {
            return Ok(serde_json::from_str(result)?);
        }
        return reconcile(context, &caller.resource.id).await;
    }
    if store::text(&row, "lifecycle")? != "active" {
        return Err(error(
            ErrorCode::ConfigConflict,
            "resource is not active or has another pending operation",
        ));
    }
    let definition: Definition = serde_json::from_str(&store::text(&row, "definition")?)?;
    store::authorize(&definition, &caller, Action::Administer, None)?;
    if request.revision != definition.revision {
        return Err(error(
            ErrorCode::ConfigConflict,
            "retirement revision differs from catalog",
        ));
    }
    if !request.force() {
        let reply = super::call_authorized(
            context,
            caller.clone(),
            super::cell_scope(&definition.resource)?,
            Some(super::cell_name(&definition.resource)?),
            "__streamRetirementCheck",
            json!([{"request":encoded}]),
        )
        .await?;
        if reply["resource"].as_str() != Some(caller.resource.id.as_str())
            || reply["operation"].as_str() != Some(request.operation.as_str())
            || reply["digest"].as_str() != Some(digest.as_str())
        {
            return Err(error(
                ErrorCode::ConfigConflict,
                "retirement preflight identity differs",
            ));
        }
    }
    context.check_turn()?;
    db.tx(|tx| {
        if let Some(prior) = receipt(tx, &request.operation)? {
            validate_receipt(&prior, &caller, "retire", Some(&digest))?;
            authorize_resource(tx, &caller, Action::Administer)?;
            return Ok(());
        }
        let latest=tx.system_sql("SELECT * FROM __sys_event_catalog WHERE id=?",&[caller.resource.id.clone().into()])?.remove(0);
        if store::text(&latest,"lifecycle")?!="active" || store::text(&latest,"definition")?!=store::text(&row,"definition")? {
            return Err(error(ErrorCode::ConfigConflict,"resource changed during retirement preflight"));
        }
        insert_receipt(tx,&caller,&request.operation,"retire",&digest)?;
        tx.system_sql("UPDATE __sys_event_catalog SET pending_retirement=?,pending_authority=?,operation=?,digest=?,operation_authority=?,lifecycle='retiring',retry_at_ms=?,last_error=NULL WHERE id=?",
            &[serde_json::to_string(&request)?.into(),serde_json::to_string(&caller)?.into(),request.operation.clone().into(),digest.into(),serde_json::to_string(&caller)?.into(),crate::asyncrt::wall_ms().into(),caller.resource.id.clone().into()])?;
        schedule(tx)
    })?;
    reconcile(context, &caller.resource.id).await
}

fn retirement_status(context: &Context, body: &Value) -> Result<Value> {
    let caller = store::authority(context, Action::Administer)?;
    let operation = body["operation"]
        .as_str()
        .ok_or_else(|| error(ErrorCode::InvalidRequest, "operation missing"))?;
    store::validate_id(operation)?;
    let db = context.storage()?;
    authorize_resource(&db, &caller, Action::Administer)?;
    let prior = receipt(&db, operation)?
        .ok_or_else(|| error(ErrorCode::NotInitialized, "retirement operation missing"))?;
    validate_receipt(&prior, &caller, "retire", None)?;
    operation_result(&db, &caller.resource.id, operation)
}

fn configuration_status(context: &Context) -> Result<Value> {
    let caller = store::authority(context, Action::Configure)?;
    let db = context.storage()?;
    status(&authorize_resource(&db, &caller, Action::Configure)?)
}

fn resolve(context: &Context) -> Result<Value> {
    let caller = store::authority(context, Action::Resolve)?;
    let row = authorize_resource(&context.storage()?, &caller, Action::Resolve)?;
    if store::text(&row, "lifecycle")? != "active" {
        return Err(error(
            ErrorCode::Unavailable,
            "catalog resource is not active for binding adoption",
        ));
    }
    let definition: Definition = serde_json::from_str(&store::text(&row, "definition")?)?;
    if definition.resource != caller.resource {
        return Err(error(
            ErrorCode::Forbidden,
            "catalog binding identity mismatch",
        ));
    }
    Ok(
        json!({"state":"active","resource":definition.resource,"revision":definition.revision.to_string()}),
    )
}

fn schedule(db: &Storage) -> Result<()> {
    let rows=db.system_sql("SELECT min(retry_at_ms) AS at_ms FROM __sys_event_catalog WHERE (pending_definition IS NOT NULL OR pending_retirement IS NOT NULL) AND lifecycle!='deleted'",&[])?;
    match rows[0].get("at_ms") {
        Some(SqlValue::Integer(deadline)) => db.schedule_work(
            cellar_logic::work::Reason::ResourceReconciliation,
            *deadline,
        ),
        Some(SqlValue::Null) => {
            db.clear_current_work(cellar_logic::work::Reason::ResourceReconciliation)
        }
        _ => Err(anyhow::anyhow!("invalid catalog work deadline")),
    }
}

async fn reconcile(context: &Context, id: &str) -> Result<Value> {
    let db = context.storage()?;
    let row = db
        .system_sql("SELECT * FROM __sys_event_catalog WHERE id=?", &[id.into()])?
        .into_iter()
        .next()
        .ok_or_else(|| error(ErrorCode::NotInitialized, "catalog resource is absent"))?;
    let retiring = matches!(row.get("pending_retirement"), Some(SqlValue::Text(_)));
    if !retiring && !matches!(row.get("pending_definition"), Some(SqlValue::Text(_))) {
        return status(&row);
    }
    let definition: Definition = serde_json::from_str(&store::text(
        &row,
        if retiring {
            "definition"
        } else {
            "pending_definition"
        },
    )?)?;
    let authority: Authority = serde_json::from_str(&store::text(&row, "pending_authority")?)?;
    let operation = store::text(&row, "operation")?;
    db.tx(|tx| {
        tx.system_sql("UPDATE __sys_event_catalog SET retry_at_ms=?,attempts=CASE WHEN attempts<9223372036854775807 THEN attempts+1 ELSE attempts END WHERE id=? AND operation=?",
            &[crate::asyncrt::wall_ms().saturating_add(1000).into(),id.into(),operation.clone().into()])?;
        schedule(tx)
    })?;
    db.sync().await?;
    let (method, args) = if retiring {
        let request: super::Retirement =
            serde_json::from_str(&store::text(&row, "pending_retirement")?)?;
        (
            "__streamRetire",
            json!([{"request":base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&request)?)}]),
        )
    } else {
        let encoded =
            base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&definition)?);
        (
            "__streamConfigure",
            json!([{"operation":operation,"definition":encoded}]),
        )
    };
    let result = super::call_authorized(
        context,
        authority,
        super::cell_scope(&definition.resource)?,
        Some(super::cell_name(&definition.resource)?),
        method,
        args,
    )
    .await;
    context.check_turn()?;
    match result {
        Ok(reply) => {
            if reply["resource"].as_str() != Some(definition.resource.id.as_str())
                || reply["operation"].as_str() != Some(operation.as_str())
                || reply["digest"].as_str() != Some(store::text(&row, "digest")?.as_str())
            {
                return Err(error(
                    ErrorCode::ConfigConflict,
                    "configuration acceptance identity conflicts",
                ));
            }
            if retiring {
                let state = reply["state"].as_str().ok_or_else(|| {
                    error(ErrorCode::InvalidRequest, "retirement reply has no state")
                })?;
                if !matches!(state, "retiring" | "deleted") {
                    return Err(error(ErrorCode::InvalidRequest, "invalid retirement state"));
                }
                if state == "deleted" {
                    db.tx(|tx| {
                        tx.system_sql("UPDATE __sys_event_catalog SET pending_retirement=NULL,pending_authority=NULL,lifecycle='deleted',last_error=NULL WHERE id=? AND operation=?",
                            &[id.into(),operation.clone().into()])?;
                        finish_receipt(tx,id,&operation)?;schedule(tx)
                    })?;
                } else {
                    db.system_sql(
                        "UPDATE __sys_event_catalog SET last_error=NULL WHERE id=? AND operation=?",
                        &[id.into(), operation.clone().into()],
                    )?;
                }
            } else {
                let revision = super::subscription_store::integer(&reply, "revision")?;
                if revision != definition.revision {
                    return Err(error(
                        ErrorCode::ConfigConflict,
                        "configuration reply names a different revision",
                    ));
                }
                db.tx(|tx| {
                    tx.system_sql("UPDATE __sys_event_catalog SET definition=pending_definition,pending_definition=NULL,pending_authority=NULL,lifecycle='active',last_error=NULL WHERE id=? AND operation=?",
                        &[id.into(),operation.clone().into()])?;
                    finish_receipt(tx,id,&operation)?;schedule(tx)
                })?;
            }
        }
        Err(failure) => {
            let message = failure.to_string().chars().take(1024).collect::<String>();
            db.system_sql(
                "UPDATE __sys_event_catalog SET last_error=? WHERE id=? AND operation=?",
                &[message.into(), id.into(), operation.clone().into()],
            )?;
        }
    }
    operation_result(&db, id, &operation)
}

async fn configure(context: &Context, body: &Value) -> Result<Value> {
    let caller = store::authority(context, Action::Configure)?;
    let encoded = body["definition"]
        .as_str()
        .ok_or_else(|| error(ErrorCode::InvalidRequest, "catalog definition is missing"))?;
    let (desired, digest) = store::decode_definition(encoded)?;
    let operation = body["operation"]
        .as_str()
        .ok_or_else(|| error(ErrorCode::InvalidRequest, "catalog operation is missing"))?;
    store::validate_id(operation)?;
    if !desired.resource.same_identity(&caller.resource)
        || super::catalog_scope(&desired.resource)? != context.identity().unwrap().scope
    {
        return Err(error(
            ErrorCode::Forbidden,
            "catalog resource scope mismatch",
        ));
    }
    let db = context.storage()?;
    open(&db)?;
    if let Some(prior) = receipt(&db, operation)? {
        authorize_resource(&db, &caller, Action::Configure)?;
        validate_receipt(&prior, &caller, "configure", Some(&digest))?;
        if let Some(SqlValue::Text(result)) = prior.get("result") {
            return Ok(serde_json::from_str(result)?);
        }
        return reconcile(context, &desired.resource.id).await;
    }
    db.tx(|tx| {
        let rows=tx.system_sql("SELECT * FROM __sys_event_catalog WHERE id=? OR (name=? AND lifecycle!='deleted')",
            &[desired.resource.id.clone().into(),desired.resource.name.clone().into()])?;
        if let Some(prior)=rows.first() {
            if store::text(prior,"id")?!=desired.resource.id { return Err(error(ErrorCode::ConfigConflict,"catalog name belongs to a different incarnation")); }
            if store::text(prior,"lifecycle")?=="deleted" { return Err(error(ErrorCode::DestinationDeleted,"catalog incarnation is retired")); }
            if store::text(prior,"operation")?==operation && store::text(prior,"digest")?==digest {
                let original:Authority=serde_json::from_str(&store::text(prior,"operation_authority")?)?;
                if original.principal!=caller.principal || original.grant!=caller.grant {
                    return Err(error(ErrorCode::Forbidden,"configuration receipt belongs to another caller"));
                }
                return Ok(());
            }
            if matches!(prior.get("pending_definition"),Some(SqlValue::Text(_))) || matches!(prior.get("pending_retirement"),Some(SqlValue::Text(_))) {
                return Err(error(ErrorCode::ConfigConflict,"catalog configuration is already pending"));
            }
            let current:Definition=serde_json::from_str(&store::text(prior,"definition")?)?;
            store::authorize(&current,&caller,Action::Configure,None)?;
            if !current.resource.same_identity(&desired.resource) || current.resource.name!=desired.resource.name || desired.revision<=current.revision {
                return Err(error(ErrorCode::ConfigConflict,"catalog configuration revision conflicts"));
            }
            tx.system_sql("UPDATE __sys_event_catalog SET pending_definition=?,pending_authority=?,operation=?,digest=?,operation_authority=?,lifecycle='updating',retry_at_ms=? WHERE id=?",
                &[serde_json::to_string(&desired)?.into(),serde_json::to_string(&caller)?.into(),operation.into(),digest.clone().into(),serde_json::to_string(&caller)?.into(),crate::asyncrt::wall_ms().into(),desired.resource.id.clone().into()])?;
        } else {
            store::authorize(&desired,&caller,Action::Configure,None)?;
            if desired.revision!=1 { return Err(error(ErrorCode::ConfigConflict,"catalog creation requires revision one")); }
            let counts=tx.system_sql("SELECT count(*) AS n FROM __sys_event_catalog",&[])?;
            if store::number(&counts[0],"n")?>=100_000 { return Err(error(ErrorCode::ConfigConflict,"catalog identity history is full")); }
            let live=tx.system_sql("SELECT count(*) AS n FROM __sys_event_catalog WHERE lifecycle!='deleted'",&[])?;
            if store::number(&live[0],"n")?>=10_000 { return Err(error(ErrorCode::ConfigConflict,"catalog live-resource limit reached")); }
            tx.system_sql("INSERT INTO __sys_event_catalog(id,name,lifecycle,pending_definition,pending_authority,operation,digest,operation_authority,retry_at_ms)
                VALUES(?,?,'creating',?,?,?,?,?,?)",
                &[desired.resource.id.clone().into(),desired.resource.name.clone().into(),serde_json::to_string(&desired)?.into(),
                    serde_json::to_string(&caller)?.into(),operation.into(),digest.clone().into(),serde_json::to_string(&caller)?.into(),crate::asyncrt::wall_ms().into()])?;
        }
        insert_receipt(tx,&caller,operation,"configure",&digest)?;
        schedule(tx)
    })?;
    reconcile(context, &desired.resource.id).await
}

impl Cell for Catalog {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc { method, args } = invocation else {
                return Err(error(
                    ErrorCode::Forbidden,
                    "catalog accepts only authorized RPC",
                ));
            };
            let body = broker::body(&args)?;
            let result = match method.as_str() {
                "__catalogConfigure" => configure(&context, &body).await,
                "__catalogRetire" => retire(&context, &body).await,
                "__catalogRetirementStatus" => retirement_status(&context, &body),
                "__catalogStatus" => configuration_status(&context),
                "__catalogResolve" => resolve(&context),
                _ => Err(error(
                    ErrorCode::UnsupportedProtocol,
                    "unknown catalog operation",
                )),
            };
            broker::reply(result)
        })
    }
}

pub(crate) async fn maintain(
    context: Context,
    installation: cellar_logic::work::Installation,
) -> Result<Output> {
    let db = context.storage()?;
    if db.work_installation(cellar_logic::work::Reason::ResourceReconciliation)?
        != Some(installation)
    {
        return Ok(Output::Done);
    }
    let rows=db.system_sql("SELECT id FROM __sys_event_catalog WHERE (pending_definition IS NOT NULL OR pending_retirement IS NOT NULL) AND retry_at_ms<=? ORDER BY retry_at_ms,id LIMIT 1",
        &[crate::asyncrt::wall_ms().into()])?;
    if let Some(row) = rows.first() {
        reconcile(&context, &store::text(row, "id")?).await?;
    }
    db.tx(schedule)?;
    Ok(Output::Done)
}

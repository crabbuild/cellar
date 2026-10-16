//! Durable source admission closure and owner-routed retirement verification.

use crate::application::event_stream::{
    Binding, DeliveryError, ErrorCode, Resource, RetirementProof,
};
use crate::application::storage::{SqlRow, SqlValue};
use crate::application::{Context, Storage};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::atomic::Ordering;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Destroy {
    pub source: String,
    pub incarnation: String,
    pub operation: String,
    pub reason: String,
    pub data_loss_acknowledgment: String,
}

fn error(code: ErrorCode, message: &str) -> anyhow::Error {
    DeliveryError::new(code, message).into()
}

impl Destroy {
    pub(crate) fn validate(&self) -> Result<()> {
        for identity in [&self.incarnation, &self.operation] {
            anyhow::ensure!(
                identity.len() == 32
                    && identity
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
                "invalid source destruction identity"
            );
        }
        anyhow::ensure!(
            cellar_logic::cell::valid_cell_scope(&self.source)
                && !crate::application::event_stream::is_event_scope(&self.source)
                && !self.reason.trim().is_empty()
                && self.reason.len() <= 512
                && !self.data_loss_acknowledgment.trim().is_empty()
                && self.data_loss_acknowledgment.len() <= 1024,
            "invalid source destruction request"
        );
        Ok(())
    }
}

fn text(row: &SqlRow, field: &str) -> Result<String> {
    match row.get(field) {
        Some(SqlValue::Text(value)) => Ok(value.clone()),
        _ => Err(error(
            ErrorCode::UnsupportedProtocol,
            "invalid source destruction metadata",
        )),
    }
}

fn number(row: &SqlRow, field: &str) -> Result<i64> {
    match row.get(field) {
        Some(SqlValue::Integer(value)) => Ok(*value),
        _ => Err(error(
            ErrorCode::UnsupportedProtocol,
            "invalid source destruction counter",
        )),
    }
}

fn hash(value: &impl Serialize) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(value)?)))
}

fn lanes(db: &Storage) -> Result<Vec<SqlRow>> {
    let max = crate::application::limits::current()
        .events
        .max_source_lanes;
    let rows = db.system_sql("SELECT l.lane,l.binding,l.next_sequence,l.settled_through,l.dispatch_generation,l.pending_rows,l.pending_bytes,l.status,coalesce(s.state,'absent') AS seal_state FROM __sys_outbox_lanes AS l LEFT JOIN __sys_outbox_seals AS s ON s.lane=l.lane ORDER BY l.lane LIMIT ?", &[i64::try_from(max.saturating_add(1))?.into()])?;
    anyhow::ensure!(
        rows.len() <= max,
        "source lane inventory exceeds the captured host policy"
    );
    Ok(rows)
}

fn snapshot(db: &Storage) -> Result<String> {
    let captured = lanes(db)?
        .iter()
        .map(|row| -> Result<Value> {
            Ok(
                json!({"lane":text(row,"lane")?,"binding":text(row,"binding")?,
            "next_sequence":number(row,"next_sequence")?.to_string(),
            "settled_through":number(row,"settled_through")?.to_string(),
            "dispatch_generation":number(row,"dispatch_generation")?.to_string(),
            "pending_rows":number(row,"pending_rows")?.to_string(),
            "pending_bytes":number(row,"pending_bytes")?.to_string(),
            "status":text(row,"status")?,"seal_state":text(row,"seal_state")?}),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    hash(&captured)
}

fn lifecycle(db: &Storage) -> Result<Option<SqlRow>> {
    let exists = db.system_sql("SELECT 1 AS present FROM sqlite_schema WHERE type='table' AND name='__sys_outbox_source_lifecycle'",&[])?;
    if exists.is_empty() {
        return Ok(None);
    }
    Ok(db
        .system_sql(
            "SELECT * FROM __sys_outbox_source_lifecycle WHERE singleton=1",
            &[],
        )?
        .into_iter()
        .next())
}

pub(crate) fn pending(db: &Storage) -> Result<bool> {
    Ok(lifecycle(db)?.is_some_and(|row|matches!(row.get("state"),Some(SqlValue::Text(state)) if state != "live" && state != "deleted")))
}

pub(crate) fn receipt(db: &Storage, operation: &str) -> Result<Value> {
    let rows = db.system_sql(
        "SELECT receipt FROM __sys_outbox_operations WHERE operation=?",
        &[operation.into()],
    )?;
    let row = rows.first().ok_or_else(|| {
        error(
            ErrorCode::OperationUnknown,
            "source destruction receipt missing",
        )
    })?;
    Ok(serde_json::from_str(&text(row, "receipt")?)?)
}

pub(crate) fn begin(context: &Context, operator: &str, request: &Destroy) -> Result<Value> {
    request.validate()?;
    context.check_turn()?;
    anyhow::ensure!(
        context
            .identity()
            .is_some_and(|identity| identity.scope == request.source),
        "source destruction names another cell"
    );
    let db = context.storage()?;
    let digest = hash(request)?;
    let generation=db.tx(|tx| {
        crate::storage::outbox_control::ensure_schema(&request.source)?;
        let prior=tx.system_sql("SELECT digest,operator FROM __sys_outbox_operations WHERE operation=?",&[request.operation.clone().into()])?;
        if let Some(row)=prior.first() {
            if text(row,"operator")?!=operator {return Err(error(ErrorCode::Forbidden,"source destruction belongs to another operator"));}
            if text(row,"digest")?!=digest {return Err(error(ErrorCode::ConfigConflict,"source operation reused with different arguments"));}
            return Ok(None);
        }
        let count=tx.system_sql("SELECT rows FROM __sys_outbox_operation_stats WHERE singleton=1",&[])?;
        let retained=number(&count[0],"rows")?;
        if retained>=i64::try_from(crate::application::limits::current().events.max_operation_receipts.min(10_000))? {
            return Err(error(ErrorCode::OperationLimit,"source destruction receipt capacity exhausted"));
        }
        if lifecycle(tx)?.is_some_and(|row|!matches!(row.get("state"),Some(SqlValue::Text(state)) if state=="live")) {
            return Err(error(ErrorCode::ConfigConflict,"source already has a retained closure operation"));
        }
        tx.ensure_outbox_schema()?;
        let identity=tx.system_sql("SELECT incarnation FROM __sys_event_source WHERE singleton=1",&[])?;
        if identity.first().map(|row|text(row,"incarnation")).transpose()?.as_deref()!=Some(&request.incarnation) {
            return Err(error(ErrorCode::ConfigConflict,"source incarnation changed or is absent"));
        }
        let generation=context.io.native_activation.as_ref().unwrap().resources.source_generation.load(Ordering::Acquire);
        let generation=cellar_logic::event_stream::next_dispatch_generation(i64::try_from(generation)?)
            .map_err(|_|error(ErrorCode::OperationLimit,"source admission generation exhausted"))?;
        let captured=lanes(tx)?;
        for row in &captured {
            cellar_logic::event_stream::next_dispatch_generation(number(row,"dispatch_generation")?)
                .map_err(|_|error(ErrorCode::OperationLimit,"source dispatch generation exhausted"))?;
        }
        // Invalidate every begun dispatch before capturing the final lane set.
        tx.system_sql("UPDATE __sys_outbox_lanes SET dispatch_generation=dispatch_generation+1",&[])?;
        let captured=lanes(tx)?;
        let rows=captured.iter().try_fold(0i64,|sum,row|sum.checked_add(number(row,"pending_rows")?).ok_or_else(||error(ErrorCode::UnsupportedProtocol,"source count overflow")))?;
        let bytes=captured.iter().try_fold(0i64,|sum,row|sum.checked_add(number(row,"pending_bytes")?).ok_or_else(||error(ErrorCode::UnsupportedProtocol,"source bytes overflow")))?;
        let result=json!({"operation":request.operation,"operator":operator,"source":request.source,"incarnation":request.incarnation,
            "generation":generation.to_string(),"reason":request.reason,"data_loss_acknowledgment":request.data_loss_acknowledgment,
            "state":"checking","discarded_rows":"0","discarded_bytes":"0","captured_rows":rows.to_string(),"captured_bytes":bytes.to_string()});
        let encoded=serde_json::to_string(&result)?;
        anyhow::ensure!(encoded.len()<=4096,"source destruction receipt exceeds limit");
        tx.system_sql("INSERT INTO __sys_outbox_operations(operation,digest,operator,receipt,size,created_ms) VALUES(?,?,?,?,?,?)",
            &[request.operation.clone().into(),digest.clone().into(),operator.into(),encoded.clone().into(),i64::try_from(encoded.len())?.into(),crate::asyncrt::wall_ms().into()])?;
        for row in captured {
            let binding: Binding=serde_json::from_str(&text(&row,"binding")?)?;
            binding.validate()?;
            let needs=number(&row,"pending_rows")?>0 || text(&row,"seal_state")?!="accepted";
            tx.system_sql("INSERT INTO __sys_outbox_destroy_lanes(operation,lane,resource,needs_evidence) VALUES(?,?,?,?)",
                &[request.operation.clone().into(),text(&row,"lane")?.into(),serde_json::to_string(&binding.resource)?.into(),i64::from(needs).into()])?;
        }
        tx.system_sql("INSERT INTO __sys_outbox_source_lifecycle(singleton,incarnation,generation,state,operation,snapshot) VALUES(1,?,?,'checking',?,?) ON CONFLICT(singleton) DO UPDATE SET incarnation=excluded.incarnation,generation=excluded.generation,state=excluded.state,operation=excluded.operation,snapshot=excluded.snapshot",
            &[request.incarnation.clone().into(),generation.into(),request.operation.clone().into(),snapshot(tx)?.into()])?;
        tx.delete_alarm()?;
        schedule(tx, crate::asyncrt::wall_ms())?;
        Ok(Some(u64::try_from(generation)?))
    })?;
    if let Some(generation) = generation {
        context
            .io
            .native_activation
            .as_ref()
            .unwrap()
            .source_admission(generation, true);
    }
    receipt(&db, &request.operation)
}

pub(crate) async fn verify(context: &Context) -> Result<()> {
    let db = context.storage()?;
    let Some(state) = lifecycle(&db)? else {
        return Ok(());
    };
    if text(&state, "state")? != "checking" {
        return Ok(());
    }
    let operation = text(&state, "operation")?;
    let target=db.system_sql("SELECT lane,resource FROM __sys_outbox_destroy_lanes WHERE operation=? AND needs_evidence=1 AND evidence IS NULL ORDER BY lane LIMIT 1",&[operation.clone().into()])?;
    let proof = if let Some(target) = target.first() {
        let resource: Resource = serde_json::from_str(&text(target, "resource")?)?;
        let operator = receipt(&db, &operation)?["operator"]
            .as_str()
            .ok_or_else(|| {
                error(
                    ErrorCode::UnsupportedProtocol,
                    "source destruction operator missing",
                )
            })?
            .to_owned();
        let authority =
            super::rpc_authority::CatalogAuthority::retired(resource.clone(), operator)?;
        let namespace = authority.namespace();
        // Source intent and retry discovery must pass proof before network work.
        db.sync().await?;
        let args = crate::application::encode_rpc(
            &cellar_native::clone::StructuredValue::from_json(json!([{"resource":resource}]))?,
        )?;
        let result = crate::native_worker::catalog_rpc(
            context,
            authority.clone(),
            namespace.scope()?,
            namespace.name()?,
            crate::application::event_stream::retirement_proof::METHOD.into(),
            args,
        )
        .await?;
        let reply = crate::application::decode_rpc_result(&result)?.to_json()?;
        if let Some(failure) = reply.get("error") {
            return Err(serde_json::from_value::<DeliveryError>(failure.clone())?.into());
        }
        let proof: RetirementProof =
            serde_json::from_value(reply.get("ok").cloned().ok_or_else(|| {
                error(
                    ErrorCode::UnsupportedProtocol,
                    "invalid retirement evidence envelope",
                )
            })?)?;
        proof.validate(&resource)?;
        Some((text(target, "lane")?, serde_json::to_string(&proof)?))
    } else {
        None
    };
    context.check_turn()?;
    db.tx(|tx| {
        let current=lifecycle(tx)?.ok_or_else(||error(ErrorCode::ConfigConflict,"source closure was replaced"))?;
        if text(&current,"operation")?!=operation || text(&current,"incarnation")?!=text(&state,"incarnation")?
            || number(&current,"generation")?!=number(&state,"generation")? {
            return Err(error(ErrorCode::ConfigConflict,"source closure was replaced"));
        }
        // A concurrent owner turn can finish verification or re-arm cleanup
        // while this proof RPC is suspended. Its newer installation owns the
        // continuation; discard this completion without rewriting its state.
        if number(&current,"work_generation")?!=number(&state,"work_generation")?
            || text(&current,"state")?!="checking" {
            return Ok(());
        }
        if text(&current,"snapshot")?!=snapshot(tx)? {
            return Err(error(ErrorCode::ConfigConflict,"source lane set changed during retirement verification"));
        }
        if let Some((lane,evidence))=proof {
            tx.system_sql("UPDATE __sys_outbox_destroy_lanes SET evidence=? WHERE operation=? AND lane=? AND evidence IS NULL",
                &[evidence.into(),operation.clone().into(),lane.into()])?;
        }
        let unverified=tx.system_sql("SELECT 1 AS pending FROM __sys_outbox_destroy_lanes WHERE operation=? AND needs_evidence=1 AND evidence IS NULL LIMIT 1",&[operation.clone().into()])?;
        if unverified.is_empty() {
            tx.system_sql("UPDATE __sys_outbox_source_lifecycle SET state='verified' WHERE singleton=1",&[])?;
            let mut result=receipt(tx,&operation)?; result["state"]=json!("verified");
            let encoded=serde_json::to_string(&result)?;
            tx.system_sql("UPDATE __sys_outbox_operations SET receipt=?,size=? WHERE operation=?",&[encoded.clone().into(),i64::try_from(encoded.len())?.into(),operation.clone().into()])?;
        }
        schedule(tx, crate::asyncrt::wall_ms().saturating_add(1000))
    })
}

fn update_receipt(db: &Storage, operation: &str, result: &Value) -> Result<()> {
    let encoded = serde_json::to_string(result)?;
    anyhow::ensure!(
        encoded.len() <= 4096,
        "source destruction receipt exceeds limit"
    );
    db.system_sql(
        "UPDATE __sys_outbox_operations SET receipt=?,size=? WHERE operation=?",
        &[
            encoded.clone().into(),
            i64::try_from(encoded.len())?.into(),
            operation.into(),
        ],
    )?;
    Ok(())
}

fn same_closure(db: &Storage, captured: &SqlRow) -> Result<SqlRow> {
    let current = lifecycle(db)?
        .ok_or_else(|| error(ErrorCode::ConfigConflict, "source closure is absent"))?;
    if text(&current, "operation")? != text(captured, "operation")?
        || text(&current, "incarnation")? != text(captured, "incarnation")?
        || number(&current, "generation")? != number(captured, "generation")?
        || number(&current, "work_generation")? != number(captured, "work_generation")?
    {
        return Err(error(
            ErrorCode::ConfigConflict,
            "source closure was replaced",
        ));
    }
    Ok(current)
}

pub(crate) async fn clean(context: &Context) -> Result<()> {
    let db = context.storage()?;
    let Some(captured) = lifecycle(&db)? else {
        return Ok(());
    };
    let operation = text(&captured, "operation")?;
    let phase = text(&captured, "state")?;
    if phase == "verified" {
        // Verified evidence itself must be recoverable before deleting data.
        db.sync().await?;
        context.check_turn()?;
        return db.tx(|tx| {
            let current = same_closure(tx, &captured)?;
            if text(&current, "state")? != "verified" {
                return Ok(());
            }
            tx.system_sql(
                "UPDATE __sys_outbox_source_lifecycle SET state='cleaning' WHERE singleton=1",
                &[],
            )?;
            let mut result = receipt(tx, &operation)?;
            result["state"] = json!("cleaning");
            update_receipt(tx, &operation, &result)?;
            schedule(tx, crate::asyncrt::wall_ms())
        });
    }
    if phase != "cleaning" {
        return Ok(());
    }
    let scope = context.identity().unwrap().scope.clone();
    let removed = db.tx(|tx| {
        let current = same_closure(tx, &captured)?;
        if text(&current, "state")? != "cleaning" {
            return Ok(true);
        }
        let (rows, bytes) = crate::storage::outbox_control::cleanup_events(&scope)?;
        if rows == 0 {
            return Ok(false);
        }
        let mut result = receipt(tx, &operation)?;
        for (field, delta) in [("discarded_rows", rows), ("discarded_bytes", bytes)] {
            let previous = result[field]
                .as_str()
                .ok_or_else(|| {
                    error(
                        ErrorCode::UnsupportedProtocol,
                        "source discard counter missing",
                    )
                })?
                .parse::<i64>()?;
            result[field] = json!(previous
                .checked_add(delta)
                .ok_or_else(|| error(
                    ErrorCode::UnsupportedProtocol,
                    "source discard counter overflow"
                ))?
                .to_string());
        }
        update_receipt(tx, &operation, &result)?;
        schedule(tx, crate::asyncrt::wall_ms())?;
        Ok(true)
    })?;
    if removed {
        return Ok(());
    }
    let children = db.system_sql(
        "SELECT name FROM __sys_cell_facet_names ORDER BY name LIMIT 1",
        &[],
    )?;
    if let Some(child) = children.first() {
        let name = text(child, "name")?;
        db.sync().await?;
        context.check_turn()?;
        same_closure(&db, &captured)?;
        let parent = crate::storage::storage_identity(&scope)?
            .ok_or_else(|| error(ErrorCode::ConfigConflict, "source storage identity retired"))?;
        let complete = crate::host_facets::delete_facet_files_batch(parent, name.clone())
            .await
            .map_err(anyhow::Error::msg)?;
        context.check_turn()?;
        return db.tx(|tx| {
            same_closure(tx, &captured)?;
            if complete {
                tx.system_sql(
                    "DELETE FROM __sys_cell_facet_names WHERE name=?",
                    &[name.into()],
                )?;
            }
            schedule(tx, crate::asyncrt::wall_ms())
        });
    }
    // Admission remains closed throughout this existing transactional business
    // reset, including an interruption before recording the terminal result.
    // SQL table-drop cost is separate from bounded event/inbox/lane reclamation.
    same_closure(&db, &captured)?;
    db.delete_all()?;
    db.tx(|tx| {
        let current = same_closure(tx, &captured)?;
        if text(&current, "state")? != "cleaning" {
            return Ok(());
        }
        if !crate::storage::outbox_control::cleanup_metadata(&scope)? {
            return schedule(tx, crate::asyncrt::wall_ms());
        }
        let mut result = receipt(tx, &operation)?;
        if result["captured_rows"] != result["discarded_rows"]
            || result["captured_bytes"] != result["discarded_bytes"]
        {
            return Err(error(
                ErrorCode::UnsupportedProtocol,
                "source discard counts differ from captured obligations",
            ));
        }
        result["state"] = json!("deleted");
        tx.system_sql(
            "UPDATE __sys_outbox_source_lifecycle SET state='deleted' WHERE singleton=1",
            &[],
        )?;
        update_receipt(tx, &operation, &result)?;
        tx.clear_current_work(cellar_logic::work::Reason::Outbox)
    })
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Recreate {
    pub source: String,
    pub incarnation: String,
    pub operation: String,
    pub reason: String,
}

impl Recreate {
    pub(crate) fn validate(&self) -> Result<()> {
        Destroy {
            source: self.source.clone(),
            incarnation: self.incarnation.clone(),
            operation: self.operation.clone(),
            reason: self.reason.clone(),
            data_loss_acknowledgment: "recreate an already deleted source".into(),
        }
        .validate()
    }
}

pub(crate) fn recreate(context: &Context, operator: &str, request: &Recreate) -> Result<Value> {
    use rand::RngCore;
    request.validate()?;
    anyhow::ensure!(
        context
            .identity()
            .is_some_and(|identity| identity.scope == request.source),
        "source recreation names another cell"
    );
    let db = context.storage()?;
    let digest = hash(request)?;
    let generation=db.tx(|tx| {
        crate::storage::outbox_control::ensure_schema(&request.source)?;
        let prior=tx.system_sql("SELECT digest,operator FROM __sys_outbox_operations WHERE operation=?",&[request.operation.clone().into()])?;
        if let Some(row)=prior.first() {
            if text(row,"operator")?!=operator {return Err(error(ErrorCode::Forbidden,"source recreation belongs to another operator"));}
            if text(row,"digest")?!=digest {return Err(error(ErrorCode::ConfigConflict,"source operation arguments changed"));}
            return Ok(None);
        }
        let current=lifecycle(tx)?.ok_or_else(||error(ErrorCode::NotInitialized,"source has no retained destruction tombstone"))?;
        if text(&current,"state")?!="deleted" || text(&current,"incarnation")?!=request.incarnation {
            return Err(error(ErrorCode::ConfigConflict,"source recreation requires its exact deleted incarnation"));
        }
        let count=tx.system_sql("SELECT rows FROM __sys_outbox_operation_stats WHERE singleton=1",&[])?;
        if number(&count[0],"rows")?>=i64::try_from(crate::application::limits::current().events.max_operation_receipts.min(10_000))? {
            return Err(error(ErrorCode::OperationLimit,"source recreation receipt capacity exhausted"));
        }
        let generation=cellar_logic::event_stream::next_dispatch_generation(number(&current,"generation")?)
            .map_err(|_|error(ErrorCode::OperationLimit,"source admission generation exhausted"))?;
        let mut random=[0u8;16];
        crate::asyncrt::rng("event-source-recreation").try_fill_bytes(&mut random)
            .map_err(|failure|anyhow::anyhow!("source incarnation randomness failed: {failure}"))?;
        let incarnation=hex::encode(random);
        anyhow::ensure!(incarnation!=request.incarnation,"source incarnation randomness repeated a retired identity");
        tx.system_sql("UPDATE __sys_event_source SET incarnation=? WHERE singleton=1",&[incarnation.clone().into()])?;
        let result=json!({"operation":request.operation,"operator":operator,"source":request.source,
            "retired_incarnation":request.incarnation,"incarnation":incarnation,"generation":generation.to_string(),
            "reason":request.reason,"state":"live"});
        let encoded=serde_json::to_string(&result)?;
        tx.system_sql("INSERT INTO __sys_outbox_operations(operation,digest,operator,receipt,size,created_ms) VALUES(?,?,?,?,?,?)",
            &[request.operation.clone().into(),digest.into(),operator.into(),encoded.clone().into(),i64::try_from(encoded.len())?.into(),crate::asyncrt::wall_ms().into()])?;
        tx.system_sql("UPDATE __sys_outbox_source_lifecycle SET incarnation=?,generation=?,state='live',operation=?,snapshot=? WHERE singleton=1",
            &[incarnation.into(),generation.into(),request.operation.clone().into(),snapshot(tx)?.into()])?;
        Ok(Some(u64::try_from(generation)?))
    })?;
    if let Some(generation) = generation {
        let activation = context.io.native_activation.as_ref().unwrap();
        activation.resources.facets.lock().unwrap().take();
        activation.source_admission(generation, false);
    }
    receipt(&db, &request.operation)
}

pub(crate) fn schedule(db: &Storage, at_ms: i64) -> Result<()> {
    db.tx(|tx| {
        tx.schedule_work(cellar_logic::work::Reason::Outbox, at_ms)?;
        tx.system_sql("UPDATE __sys_outbox_source_lifecycle SET work_generation=(SELECT generation FROM __sys_work WHERE reason='outbox' AND scope=?) WHERE singleton=1 AND state IN ('checking','verified','cleaning')",
            &[crate::host_io::installed_context().and_then(|io|io.native_activation.as_ref().map(|activation|activation.scope.clone())).ok_or_else(||anyhow::anyhow!("source work has no activation"))?.into()])?;
        Ok(())
    })
}

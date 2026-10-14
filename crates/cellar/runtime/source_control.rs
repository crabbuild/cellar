//! Framework source administration. Runs inside the original owning cell turn.

use crate::application::event_stream::{DeliveryError, ErrorCode};
use crate::application::storage::SqlValue;
use crate::application::{Context, Invocation, OutboxOptions, OutboxRevision, Output, Storage};
use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub(crate) const METHOD: &str = "__outboxControl";
pub(crate) const MAX_REQUEST_BYTES: usize = 16 << 10;
pub(crate) const MAX_RPC_BYTES: usize = 64 << 10;
pub(crate) const MAX_REPLY_BYTES: usize = 1 << 20;
const MAX_RECEIPT_BYTES: usize = 4096;
const MAX_OPERATIONS: usize = 10_000;

fn default_limit() -> usize {
    100
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) enum Request {
    Inspect {
        #[serde(default = "default_limit")]
        limit: usize,
        #[serde(default)]
        cursor: Option<String>,
    },
    Retry {
        change: Retry,
    },
    Destroy {
        change: super::source_destruction::Destroy,
    },
    Recreate {
        change: super::source_destruction::Recreate,
    },
    Status {
        operation: String,
    },
    Audit {
        #[serde(default = "default_limit")]
        limit: usize,
        #[serde(default)]
        after: Option<String>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Retry {
    pub(crate) operation: String,
    pub(crate) reason: String,
    pub(crate) expected: OutboxRevision,
}

fn error(code: ErrorCode, message: &str) -> anyhow::Error {
    DeliveryError::new(code, message).into()
}

fn id(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn position(value: Option<&str>) -> Result<i64> {
    let value = value.unwrap_or("0");
    let n = value
        .parse::<i64>()
        .map_err(|_| error(ErrorCode::InvalidCursor, "invalid source audit position"))?;
    if n < 0 || n.to_string() != value {
        return Err(error(
            ErrorCode::InvalidCursor,
            "noncanonical source audit position",
        ));
    }
    Ok(n)
}

impl Request {
    pub(crate) fn validate(&self) -> Result<()> {
        match self {
            Self::Inspect { limit, cursor } => {
                if !(1..=100).contains(limit) || cursor.as_ref().is_some_and(|v| v.len() > 1024) {
                    return Err(error(
                        ErrorCode::InvalidRequest,
                        "invalid source diagnostic page bounds",
                    ));
                }
            }
            Self::Retry { change } => change.validate()?,
            Self::Destroy { change } => change.validate()?,
            Self::Recreate { change } => change.validate()?,
            Self::Status { operation } => {
                if !id(operation, 32) {
                    return Err(error(
                        ErrorCode::InvalidRequest,
                        "invalid source operation identity",
                    ));
                }
            }
            Self::Audit { limit, after } => {
                if !(1..=100).contains(limit) || after.as_ref().is_some_and(|v| v.len() > 19) {
                    return Err(error(
                        ErrorCode::InvalidRequest,
                        "invalid source audit page bounds",
                    ));
                }
                position(after.as_deref())?;
            }
        }
        Ok(())
    }
}

impl Retry {
    fn validate(&self) -> Result<()> {
        let r = &self.expected;
        if !id(&self.operation, 32)
            || self.reason.trim().is_empty()
            || self.reason.len() > 512
            || !cellar_logic::cell::valid_cell_scope(&r.source)
            || !id(&r.incarnation, 32)
            || !id(&r.lane, 64)
            || r.dispatch_generation < 0
            || r.next_sequence <= 0
            || r.settled_through < 0
            || r.settled_through >= r.next_sequence
        {
            return Err(error(
                ErrorCode::InvalidRequest,
                "invalid guarded source retry",
            ));
        }
        Ok(())
    }
}

fn stored_text(row: &crate::application::storage::SqlRow, key: &str) -> Result<String> {
    match row.get(key) {
        Some(SqlValue::Text(value)) => Ok(value.clone()),
        _ => anyhow::bail!("invalid source operation {key}"),
    }
}

fn stored_number(row: &crate::application::storage::SqlRow, key: &str) -> Result<i64> {
    match row.get(key) {
        Some(SqlValue::Integer(value)) => Ok(*value),
        _ => anyhow::bail!("invalid source operation {key}"),
    }
}

fn present(db: &Storage) -> Result<bool> {
    Ok(!db.system_sql("SELECT 1 AS present FROM sqlite_schema WHERE type='table' AND name='__sys_outbox_operations'",&[])?.is_empty())
}

fn retry(db: &Storage, operator: &str, change: &Retry) -> Result<Value> {
    change.validate()?;
    let digest = hex::encode(Sha256::digest(serde_json::to_vec(change)?));
    db.tx(|tx| {
        crate::storage::outbox_control::ensure_schema(&change.expected.source)?;
        let prior=tx.system_sql("SELECT digest,operator,receipt FROM __sys_outbox_operations WHERE operation=?",&[change.operation.clone().into()])?;
        let facts=if let Some(prior)=prior.first() {
            cellar_logic::event_stream::OperationReceipt::Existing {
                operator_matches:stored_text(prior,"operator")?==operator,
                arguments_match:stored_text(prior,"digest")?==digest,
            }
        } else {
            let count=tx.system_sql("SELECT rows FROM __sys_outbox_operation_stats WHERE singleton=1",&[])?;
            let count=stored_number(count.first().context("source operation statistics missing")?,"rows")?;
            let limit=crate::application::limits::current().events.max_operation_receipts.min(MAX_OPERATIONS);
            cellar_logic::event_stream::OperationReceipt::New {retained:u64::try_from(count)?,capacity:u64::try_from(limit)?}
        };
        let apply=cellar_logic::event_stream::admit_operation(facts).map_err(|refusal|match refusal {
            cellar_logic::event_stream::OperationRefusal::Forbidden=>error(ErrorCode::Forbidden,"source operation belongs to another operator"),
            cellar_logic::event_stream::OperationRefusal::Conflict=>error(ErrorCode::ConfigConflict,"source operation identity was reused with different arguments"),
            cellar_logic::event_stream::OperationRefusal::Capacity=>error(ErrorCode::OperationLimit,"source operation receipt capacity exhausted"),
        })?;
        if !apply {return Ok(serde_json::from_str(&stored_text(prior.first().context("source replay lacks receipt")?,"receipt")?)?);}
        if crate::storage::outbox_control::source_admission(&change.expected.source)?.1 {
            return Err(error(ErrorCode::ProducerSealed,"source closure prevents retrying its lanes"));
        }
        let rearmed=tx.retry_outbox_lane(&change.expected)?;
        let now=crate::asyncrt::wall_ms();
        let receipt=json!({"operation":change.operation,"operator":operator,"reason":change.reason,"expected":change.expected,"rearmed":rearmed,"applied_ms":now.to_string()});
        let encoded=serde_json::to_string(&receipt)?;
        if encoded.len()>MAX_RECEIPT_BYTES {return Err(error(ErrorCode::PayloadTooLarge,"source operation receipt is too large"));}
        tx.system_sql("INSERT INTO __sys_outbox_operations(operation,digest,operator,receipt,size,created_ms) VALUES(?,?,?,?,?,?)",
            &[change.operation.clone().into(),digest.into(),operator.into(),encoded.clone().into(),i64::try_from(encoded.len())?.into(),now.into()])?;
        Ok(receipt)
    })
}

fn run(db: &Storage, operator: &str, request: Request) -> Result<Value> {
    request.validate()?;
    match request {
        Request::Inspect { limit, cursor } => Ok(serde_json::to_value(
            db.outbox(&OutboxOptions { limit, cursor })?,
        )?),
        Request::Retry { change } => retry(db, operator, &change),
        Request::Destroy { .. } | Request::Recreate { .. } => Err(error(
            ErrorCode::InvalidRequest,
            "source destruction requires its owning context",
        )),
        Request::Status { operation } => {
            if !present(db)? {
                return Err(error(
                    ErrorCode::OperationUnknown,
                    "source operation receipt is unknown",
                ));
            }
            let rows = db.system_sql(
                "SELECT operator,receipt FROM __sys_outbox_operations WHERE operation=?",
                &[operation.into()],
            )?;
            let row = rows.first().ok_or_else(|| {
                error(
                    ErrorCode::OperationUnknown,
                    "source operation receipt is unknown",
                )
            })?;
            if stored_text(row, "operator")? != operator {
                return Err(error(
                    ErrorCode::Forbidden,
                    "source operation belongs to another operator",
                ));
            }
            Ok(serde_json::from_str(&stored_text(row, "receipt")?)?)
        }
        Request::Audit { limit, after } => {
            if !present(db)? {
                return Ok(json!({"records":[],"next":null}));
            }
            let rows=db.system_sql("SELECT sequence,receipt FROM __sys_outbox_operations WHERE sequence>? ORDER BY sequence LIMIT ?",&[position(after.as_deref())?.into(),i64::try_from(limit+1)?.into()])?;
            let records=rows.iter().take(limit).map(|row|->Result<Value>{Ok(json!({"sequence":stored_number(row,"sequence")?.to_string(),"receipt":serde_json::from_str::<Value>(&stored_text(row,"receipt")?)?}))}).collect::<Result<Vec<_>>>()?;
            let next = if rows.len() > limit {
                Some(stored_number(&rows[limit - 1], "sequence")?.to_string())
            } else {
                None
            };
            Ok(json!({"records":records,"next":next}))
        }
    }
}

pub(crate) async fn invoke(
    context: Context,
    invocation: Invocation,
    authority: super::SourceAuthority,
) -> Result<Output> {
    context.check_turn()?;
    let Invocation::CellRpc { method, args } = invocation else {
        anyhow::bail!("source operator requires RPC");
    };
    anyhow::ensure!(method == METHOD, "invalid source operator method");
    let size = match &args {
        crate::engine_api::RpcData::Json(value) => value.len(),
        crate::engine_api::RpcData::Structured(value) => value.len(),
    };
    anyhow::ensure!(
        size <= MAX_RPC_BYTES,
        "source operator arguments are too large"
    );
    let result = async {
        let request: Request =
            serde_json::from_value(crate::application::decode_rpc(&args)?.to_json()?)?;
        request.validate()?;
        if let Request::Destroy { change } = &request {
            let result = super::source_destruction::begin(&context, authority.operator(), change)?;
            if result["state"] == "deleted" {
                return Ok(result);
            }
            let db = context.storage()?;
            // An explicit replay resumes an interrupted closure without waiting
            // for its persisted outage backoff. Terminal replays above never
            // re-arm work against a recreated successor.
            super::source_destruction::schedule(&db, crate::asyncrt::wall_ms())?;
            db.sync().await?;
            super::source_destruction::verify(&context).await?;
            return super::source_destruction::receipt(
                &context.storage()?,
                change.operation.as_str(),
            );
        }
        if let Request::Recreate { change } = &request {
            return super::source_destruction::recreate(&context, authority.operator(), change);
        }
        if let Request::Retry { change } = &request {
            anyhow::ensure!(
                context
                    .identity()
                    .context("source operator has no cell")?
                    .scope
                    == change.expected.source,
                "source retry names another cell"
            );
        }
        run(&context.storage()?, authority.operator(), request)
    }
    .await;
    let mut value = match result {
        Ok(result) => json!({"result":result}),
        Err(failure) => match failure.downcast_ref::<DeliveryError>() {
            Some(error) => json!({"error":error}),
            None => return Err(failure),
        },
    };
    if crate::application::event_stream::encoding::json(&value, MAX_REPLY_BYTES).is_err() {
        value = json!({"error":DeliveryError::new(ErrorCode::PayloadTooLarge,
            "source diagnostic reply is too large; reduce the page limit")});
    }
    Ok(Output::Rpc(crate::application::encode_rpc(
        &cellar_native::clone::StructuredValue::from_json(value)?,
    )?))
}

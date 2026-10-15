use super::{authority::Action, error::error, store, subscription_store, ErrorCode};
use crate::application::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Source-owned seal obligation. Pending means emission is durably disabled
/// locally; accepted means the destination also proved its terminal watermark.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SealStatus {
    pub lane: String,
    pub operation: String,
    #[serde(with = "super::decimal")]
    pub terminal: i64,
    pub state: String,
}

pub(crate) fn accept_seal(context: &Context, body: &Value) -> Result<Value> {
    let caller = store::authority(context, Action::Append)?;
    let db = context.storage()?;
    let (definition, _) = store::load(&db)?;
    store::authorize(&definition, &caller, Action::Append, None)?;
    if body["protocol"].as_u64() != Some(1) {
        return Err(error(
            ErrorCode::UnsupportedProtocol,
            "unsupported producer seal protocol",
        ));
    }
    let binding: super::Binding = serde_json::from_value(body["binding"].clone())?;
    if !binding.resource.same_identity(&caller.resource)
        || binding.principal != caller.principal
        || binding.grant != caller.grant
    {
        return Err(error(
            ErrorCode::Forbidden,
            "seal request differs from host authority",
        ));
    }
    let operation = body["operation"]
        .as_str()
        .ok_or_else(|| error(ErrorCode::InvalidRequest, "seal operation missing"))?;
    store::validate_id(operation)?;
    let incarnation = body["incarnation"]
        .as_str()
        .ok_or_else(|| error(ErrorCode::InvalidRequest, "seal incarnation missing"))?;
    store::validate_id(incarnation)?;
    let source = caller
        .source
        .as_deref()
        .ok_or_else(|| error(ErrorCode::Forbidden, "seal source missing"))?;
    let lane = super::lane_identity(&definition.resource, source, incarnation)?;
    if body["lane"].as_str() != Some(lane.as_str()) {
        return Err(error(
            ErrorCode::InvalidRequest,
            "producer seal lane mismatch",
        ));
    }
    let terminal = subscription_store::integer(body, "terminal")?;
    db.tx(|tx| {
        let producers=tx.system_sql("SELECT * FROM __sys_stream_producers WHERE lane=?",&[lane.clone().into()])?;
        let high=producers.first().map(|row|store::number(row,"accepted_through")).transpose()?.unwrap_or(0);
        cellar_logic::event_stream::destination_seal(high,terminal).map_err(|failure|error(ErrorCode::ConfigConflict,failure.to_string()))?;
        if let Some(producer)=producers.first() {
            if store::text(producer,"source")?!=source || store::text(producer,"incarnation")?!=incarnation
                || store::text(producer,"principal")?!=caller.principal || store::text(producer,"grant_id")?!=caller.grant {
                return Err(error(ErrorCode::Forbidden,"producer seal belongs to a different authority"));
            }
        } else {
            let count=tx.system_sql("SELECT count(*) AS n FROM __sys_stream_producers",&[])?;
            if store::number(&count[0],"n")?>=crate::application::limits::current().events.max_producer_lanes as i64 {
                return Err(error(ErrorCode::ProducerLimit,"stream producer metadata is full"));
            }
        }
        tx.system_sql("INSERT INTO __sys_stream_producers(lane,source,incarnation,principal,grant_id,accepted_through,sealed) VALUES(?,?,?,?,?,?,1)
            ON CONFLICT(lane) DO UPDATE SET sealed=1",
            &[lane.clone().into(),source.into(),incarnation.into(),caller.principal.into(),caller.grant.into(),terminal.into()])?;
        Ok(json!({"protocol":1,"resource":definition.resource.id,"lane":lane,"operation":operation,"terminal":terminal.to_string(),"state":"sealed"}))
    })
}

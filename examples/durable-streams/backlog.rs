//! Opt-in measuring source: many commands share one outbox and lane.
use super::*;

pub(super) struct Backlog {
    pub(super) faults: Option<Arc<FaultControls>>,
}

impl Cell for Backlog {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Ok(Output::Done);
            };
            let controls = self
                .faults
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("backlog fixture is disabled"))?;
            let url = url::Url::parse(&request.url)?;
            let values = url
                .query_pairs()
                .collect::<std::collections::BTreeMap<_, _>>();
            let db = context.storage()?;
            let command = values.get("command").filter(|command| valid_name(command));
            if request.method == "GET" {
                let payload = command
                    .map(|command| db.get_json(&format!("command:{command}")))
                    .transpose()?
                    .flatten();
                let receipt = command
                    .map(|command| db.get_json(&format!("receipt:{command}")))
                    .transpose()?
                    .flatten();
                return reply(
                    &context,
                    200,
                    json!({"host":controls.node,"commands":db.get_json("commands")?.unwrap_or(json!(0)),
                    "order":payload,"receipt":receipt,"outbox":db.outbox(&OutboxOptions{limit:1,cursor:None})?}),
                );
            }
            if request.method != "POST" {
                return reply(&context, 405, json!({"error":"POST or GET required"}));
            }
            let command = command.ok_or_else(|| anyhow::anyhow!("bounded command required"))?;
            let amount = values
                .get("amount_cents")
                .and_then(|value| value.parse::<i64>().ok())
                .filter(|amount| *amount > 0 && *amount <= 1000000000)
                .ok_or_else(|| anyhow::anyhow!("valid amount required"))?;
            let event = OrderConfirmed {
                order_id: command.to_string(),
                amount_cents: amount,
            };
            let payload = serde_json::to_value(&event)?;
            let events = context.stream::<OrderConfirmed>("EVENTS")?;
            let outcome=db.tx(|tx| {
                let key=format!("command:{command}");
                if let Some(existing)=tx.get_json(&key)? {return Ok(if existing==payload {"duplicate"}else{"conflict"});}
                // This write must roll back when emit reaches its row quota.
                tx.put_json(&key,&payload)?;
                tx.emit(&events,event.clone())?;
                let page=tx.outbox(&OutboxOptions{limit:1,cursor:None})?;
                let lane=page.lanes.first().ok_or_else(||anyhow::anyhow!("backlog lane missing"))?;
                tx.put_json(&format!("receipt:{command}"),&json!({"payload_sha256":payload_digest(&event)?,"source":lane.revision.source,
                    "incarnation":lane.revision.incarnation,"lane":lane.revision.lane,"sequence":(lane.revision.next_sequence-1).to_string()}))?;
                let count=tx.get_json("commands")?.and_then(|value|value.as_i64()).unwrap_or(0).checked_add(1)
                    .ok_or_else(||anyhow::anyhow!("command count overflow"))?;
                tx.put_json("commands",&json!(count))?;
                Ok("created")
            });
            match outcome {
                Ok(status) => reply(
                    &context,
                    if status == "conflict" { 409 } else { 200 },
                    json!({"status":status,"order":payload,
                    "receipt":db.get_json(&format!("receipt:{command}"))?}),
                ),
                Err(error)
                    if error
                        .downcast_ref::<DeliveryError>()
                        .is_some_and(|failure| failure.code == ErrorCode::OutboxFull) =>
                {
                    reply(&context, 429, json!({"error":"OutboxFull"}))
                }
                Err(error) => Err(error),
            }
        })
    }
}

//! Private process fixture for a consumer callback that spans ownership fencing.

use super::*;
use anyhow::{ensure, Context as _};

pub(super) fn audit(context: &Context, db: &Storage, controls: &FaultControls) -> Result<Output> {
    reply(
        context,
        200,
        json!({"scope":context.identity().context("consumer identity missing")?.scope,
        "projection":projection(db)?,"receipts":db.get_json("effect_receipts")?.unwrap_or_else(||json!({})),
        "host":controls.node}),
    )
}

pub(super) async fn inbox_audit(context: &Context, db: &Storage) -> Result<Output> {
    let page = context
        .stream::<OrderConfirmed>("EVENTS")?
        .read(
            context,
            None,
            ReadOptions {
                max_records: 2,
                max_bytes: 64 << 10,
            },
        )
        .await?;
    ensure!(
        page.records.len() == 2,
        "inbox audit requires both original records"
    );
    let accepted = db.tx(|tx| {
        let accepted = page
            .records
            .iter()
            .map(|record| tx.inbox("order-projection-v1")?.accept(record))
            .collect::<Result<Vec<_>>>()?;
        // A missing receipt must fail the measuring client, never repair the
        // prefix being measured. Roll back even when all receipts are present.
        tx.rollback()?;
        Ok(accepted)
    })?;
    reply(
        context,
        200,
        json!({"accepted":accepted,"records":page.records.iter().map(event_receipt).collect::<Result<Vec<_>>>()?,"projection":projection(db)?}),
    )
}

pub(super) async fn running(
    context: &Context,
    db: &Storage,
    url: &url::Url,
    controls: &FaultControls,
) -> Result<Output> {
    let control = controls
        .owner
        .as_ref()
        .context("private consumer owner fault is not configured")?;
    let fields = url
        .query_pairs()
        .collect::<std::collections::BTreeMap<_, _>>();
    let cut = fields.get("cut").context("consumer cut missing")?.as_ref();
    ensure!(
        matches!(cut, "before_accept" | "before_commit"),
        "invalid consumer cut"
    );
    let fetch_id = fields.get("fetch_id").context("fetch ID missing")?;
    let subscription = context
        .stream::<OrderConfirmed>("EVENTS")?
        .subscription("projection")?;
    let batch = subscription
        .next_with_id(
            context,
            fetch_id,
            BatchOptions {
                max_records: 1,
                max_bytes: 64 << 10,
                wait_ms: 0,
            },
        )
        .await?
        .context("consumer delivery missing")?;
    ensure!(batch.records.len() == 1 && batch.expires_at_ms - batch.issued_at_ms == 30_000);
    let record = &batch.records[0];
    let delivery = json!({"fetch_id":fetch_id,"receipt":batch.receipt().encode()?,"issued_ms":batch.issued_at_ms.to_string(),
        "expires_ms":batch.expires_at_ms.to_string(),"records":[event_receipt(record)?]});
    let scope = context
        .identity()
        .context("consumer identity missing")?
        .scope
        .clone();
    let inbox = db.inbox("order-projection-v1")?;
    let mut acceptance = None;
    let result = db.tx(|tx| {
        if cut == "before_accept" {
            control.hold_consumer(&scope, cut, &delivery, None)?;
        }
        let accepted = inbox.accept(record);
        acceptance = Some(
            accepted
                .as_ref()
                .copied()
                .map_err(|error| format!("{error:#}")),
        );
        ensure!(accepted?, "pending event was already accepted");
        let event = record.event.decode::<OrderConfirmed>()?;
        let prior = projection(tx)?;
        let orders = prior["orders"]
            .as_i64()
            .context("order count missing")?
            .checked_add(1)
            .context("order count overflow")?;
        let total = prior["total_cents"]
            .as_i64()
            .context("projection total missing")?
            .checked_add(event.amount_cents)
            .context("projection total overflow")?;
        tx.put_json("projection", &json!({"orders":orders,"total_cents":total}))?;
        let mut receipts = tx
            .get_json("effect_receipts")?
            .context("original effect receipts missing")?;
        ensure!(
            receipts.get(&event.order_id).is_none(),
            "duplicate business effect"
        );
        receipts
            .as_object_mut()
            .context("invalid receipt map")?
            .insert(event.order_id, event_receipt(record)?);
        tx.put_json("effect_receipts", &receipts)?;
        if cut == "before_commit" {
            control.hold_consumer(&scope, cut, &delivery, Some(true))?;
        }
        Ok(())
    });
    control.attempted_consumer(
        &scope,
        cut,
        acceptance.as_ref().context("acceptance not attempted")?,
        &result,
    )?;
    // Deliberately swallow local refusal. A fenced invocation still cannot
    // expose this success-shaped output through ordinary runtime gates.
    Ok(Output::Http(cellar::engine_api::HttpResponse {
        status: 200,
        body: b"stale_success".to_vec(),
        headers: Vec::new(),
        stream: None,
        websocket: None,
        write_position: None,
        observed_position: None,
    }))
}

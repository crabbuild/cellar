use crate::support;
use anyhow::Result;
use cellar::sdk::{Cell, Context, HandlerFuture, Invocation, Output, Request};
use serde_json::json;
use std::sync::Arc;

pub async fn fetch(context: &Context, request: Request) -> Result<Output> {
    let url = url::Url::parse(&request.url)?;
    let parts: Vec<_> = url.path().split('/').collect();
    if parts.len() != 5 || parts[3] != "accounts" {
        return Err(support::reject(404, "use /tenants/TENANT/accounts/ACCOUNT"));
    }
    let tenant = support::name(parts[2])?;
    let account = support::name(parts[4])?;
    // Local teaching credentials. Replace this resolver with validated claims.
    let token = request
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
        .and_then(|(_, v)| v.strip_prefix("Bearer "))
        .ok_or_else(|| support::reject(401, "Bearer token required"))?;
    let identities: serde_json::Value =
        serde_json::from_str(context.var("TENANT_TOKENS").unwrap_or("{}"))?;
    let allowed = identities
        .get(token)
        .and_then(|v| v.as_str())
        .ok_or_else(|| support::reject(401, "invalid token"))?;
    if allowed != tenant {
        return Err(support::reject(403, "token belongs to another tenant"));
    }
    // Include the authenticated tenant in the durable business identity.
    let entity = format!("{tenant}:{account}");
    Ok(Output::Http(
        context
            .namespace("ACCOUNTS")?
            .get_by_name(&entity)
            .fetch(context, request)
            .await?,
    ))
}
pub struct Account;
impl Cell for Account {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            support::method(&request, &["GET", "POST"])?;
            let storage = context.storage()?;
            if request.method == "GET" {
                return support::json(
                    &context,
                    json!({"balance":storage.get_json("balance")?.unwrap_or(json!(0))}),
                );
            }
            let value = support::payload(&context, request).await?;
            let operation = support::name(value["id"].as_str().unwrap_or_default())?;
            let delta = value["delta"]
                .as_i64()
                .filter(|n| (-1000000..=1000000).contains(n))
                .ok_or_else(|| {
                    support::reject(400, "delta must be an integer in -1000000..1000000")
                })?;
            let result = storage.transaction_sync(|tx| {
                let receipt_key = format!("receipt:{operation}");
                if let Some(receipt) = tx.get_json(&receipt_key)? {
                    if receipt["delta"] != delta {
                        return Err(support::reject(
                            409,
                            "id already used with a different delta",
                        ));
                    }
                    return Ok(receipt);
                }
                let balance = tx
                    .get_json("balance")?
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0)
                    .checked_add(delta)
                    .ok_or_else(|| support::reject(409, "balance overflow"))?;
                if balance < 0 {
                    return Err(support::reject(409, "insufficient balance"));
                }
                let receipt = json!({"id":operation,"delta":delta,"balance":balance});
                tx.put_json("balance", &json!(balance))?;
                tx.put_json(&receipt_key, &receipt)?;
                Ok(receipt)
            })?;
            support::json(&context, result)
        })
    }
}

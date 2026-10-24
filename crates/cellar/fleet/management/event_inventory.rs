// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded root observations for the console; no background fleet scan.
use super::{FleetClient, PageRequest};
use crate::event_inventory::InventoryRequest;
use crate::event_state::StateTotals;
use anyhow::{ensure, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use futures_util::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    selector: String,
    token: String,
}

fn decode(cursor: Option<String>, selector: &str) -> Result<Option<String>> {
    cursor
        .map(|cursor| {
            ensure!(
                cursor.len() <= 16384,
                "event inventory cursor exceeds bounds"
            );
            let encoded = cursor
                .strip_prefix("events1:")
                .ok_or_else(|| anyhow::anyhow!("invalid event inventory cursor"))?;
            let bytes = URL_SAFE_NO_PAD.decode(encoded)?;
            ensure!(
                URL_SAFE_NO_PAD.encode(&bytes) == encoded,
                "noncanonical event inventory cursor"
            );
            let cursor: Cursor = serde_json::from_slice(&bytes)?;
            ensure!(
                cursor.selector == selector
                    && !cursor.token.is_empty()
                    && cursor.token.len() <= 8192,
                "event inventory cursor belongs to another fleet or prefix"
            );
            Ok(cursor.token)
        })
        .transpose()
}

impl FleetClient {
    /// One provider page, four concurrent owner probes; unavailable is never empty.
    pub async fn event_inventory_page(&self, fleet: &str, request: PageRequest) -> Result<Value> {
        let limit = request.limit()?;
        let prefix = request.prefix.unwrap_or_default();
        ensure!(
            prefix.is_empty() || cellar_logic::cell::valid_cell_scope(&prefix),
            "invalid cell prefix"
        );
        let selector = hex::encode(Sha256::digest(serde_json::to_vec(
            &json!({"fleet":fleet,"backend":format!("{:?}",self.bucket.backend),"bucket":self.bucket.name,"prefix":self.bucket.prefix,"filter":prefix}),
        )?));
        let token = decode(request.cursor, &selector)?;
        let page = self
            .bucket
            .common_prefixes_page(&format!("cells/{prefix}"), None, token.clone(), limit)
            .await?;
        ensure!(
            page.prefixes.len() <= limit && (page.page_token.is_none() || page.page_token != token),
            "provider exceeded inventory bounds or repeated its cursor"
        );
        let scanned = page.prefixes.len();
        let mut seen = std::collections::HashSet::new();
        let scopes = crate::cell_cli::cell_scopes_from_prefixes(page.prefixes, None, None)
            .filter(|scope| seen.insert(scope.clone()))
            .collect::<Vec<_>>();
        let mut totals = StateTotals::default();
        totals.failed += scanned - scopes.len();
        let mut probes = stream::iter(scopes.into_iter().map(|scope| async move {
            let observation = async {
                let descriptor = self
                    .event_inventory(&scope, InventoryRequest::Describe)
                    .await?;
                let state = self.cell_event_state(&scope).await?;
                Ok::<_, anyhow::Error>((descriptor, state))
            }
            .await;
            (scope, observation)
        }))
        .buffered(4);
        let mut records = Vec::new();
        while let Some((scope, result)) = probes.next().await {
            match result {
                Ok((descriptor, state)) => match totals.add(&state, &scope) {
                    Ok(()) => records.push(json!({"scope":scope,"status":"observed","inventory":descriptor,"state":state})),
                    Err(_) => { totals.failed += 1; records.push(json!({"scope":scope,"status":"unavailable"})); }
                },
                Err(_) => { totals.failed += 1; records.push(json!({"scope":scope,"status":"unavailable"})); }
            }
        }
        let next = page
            .page_token
            .map(|token| -> Result<String> {
                Ok(format!(
                    "events1:{}",
                    URL_SAFE_NO_PAD.encode(serde_json::to_vec(&Cursor { selector, token })?)
                ))
            })
            .transpose()?;
        Ok(
            json!({"records":records,"next":next,"scanned_prefixes":scanned,"complete_probes":totals.failed==0,"totals":totals.json(false)}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn console_inventory_continuation_pins_fleet_and_filter() -> Result<()> {
        let cursor = format!(
            "events1:{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&Cursor {
                selector: "fleet-a/order".into(),
                token: "provider-token".into()
            })?)
        );
        assert_eq!(
            decode(Some(cursor.clone()), "fleet-a/order")?,
            Some("provider-token".into())
        );
        assert!(decode(Some(cursor.clone()), "fleet-b/order").is_err());
        assert!(decode(Some(cursor), "fleet-a/account").is_err());
        assert!(decode(Some("census1:other".into()), "fleet-a/order").is_err());
        Ok(())
    }
}

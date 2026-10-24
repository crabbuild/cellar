// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use crate::support;
use cellar::sdk::{Cell, Context, HandlerFuture, Invocation, Output};
use std::sync::Arc;

pub struct Vector;

impl Cell for Vector {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            support::method(&request, &["GET"])?;
            let url = url::Url::parse(&request.url)?;
            let color = url
                .query_pairs()
                .find(|(k, _)| k == "rgb")
                .map(|(_, v)| v.into_owned())
                .unwrap_or_else(|| "250,20,20".into());
            let components = color
                .split(',')
                .map(|s| s.parse::<u8>())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| support::reject(400, "rgb must be three integers in 0..255"))?;
            if components.len() != 3 {
                return Err(support::reject(400, "rgb needs three components"));
            }
            let storage = context.storage()?;
            storage.transaction_sync(|tx| {
                tx.sql(
                    "CREATE VIRTUAL TABLE IF NOT EXISTS colors USING vec0(embedding float[3], label text)",
                    &[],
                )?;
                if tx.get_json("seeded")?.is_none() {
                    for (id, label, vector) in [
                        (1, "red", "[255,0,0]"),
                        (2, "green", "[0,255,0]"),
                        (3, "blue", "[0,0,255]"),
                    ] {
                        tx.sql(
                            "INSERT INTO colors(rowid,embedding,label) VALUES (?,vec_f32(?),?)",
                            &[
                                serde_json::json!(id),
                                serde_json::json!(vector),
                                serde_json::json!(label),
                            ],
                        )?;
                    }
                    tx.put_json("seeded", &serde_json::json!(true))?;
                }
                Ok(())
            })?;
            let vector = serde_json::to_string(&components)?;
            let (columns, rows, _) = storage.sql(
                "SELECT label,distance FROM colors WHERE embedding MATCH vec_f32(?) AND k=3 ORDER BY distance",
                &[serde_json::json!(vector)],
            )?;
            support::json(
                &context,
                serde_json::json!({
                    "columns": columns,
                    "rows": rows,
                }),
            )
        })
    }
}

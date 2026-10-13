use crate::support;
use anyhow::Result;
use cellar::sdk::{Context, Output, Request};
pub async fn fetch(context: &Context, request: Request) -> Result<Output> {
    support::method(&request, &["GET", "POST"])?;
    let database = context.sql("DB")?;
    database
        .exec(
            context,
            "CREATE TABLE IF NOT EXISTS guestbook(id INTEGER PRIMARY KEY, message TEXT NOT NULL)",
        )
        .await?;
    if request.method == "POST" {
        let value = support::payload(context, request).await?;
        let message = value["message"]
            .as_str()
            .filter(|s| !s.is_empty() && s.len() <= 4096)
            .ok_or_else(|| support::reject(400, "message must be 1..4096 bytes"))?;
        database
            .prepare("INSERT INTO guestbook(message) VALUES (?)")
            .bind_json([serde_json::json!(message)])?
            .run(context)
            .await?;
    }
    let rows = database
        .prepare("SELECT id, message FROM guestbook ORDER BY id DESC LIMIT 100")
        .all(context)
        .await?;
    support::json(context, serde_json::to_value(rows)?)
}

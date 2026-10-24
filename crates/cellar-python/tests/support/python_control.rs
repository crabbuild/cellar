// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Observations/barriers on the caller-owned test supervisor, outside SDK turns.
use super::*;

pub(super) fn call(name: &str) -> Result<Value> {
    // Fixture controls only inspect flags or release barriers. They never move
    // SDK work out of its original native execution domain or grant authority.
    pyo3::Python::attach(|py| -> Result<Value> {
        let value = py
            .import("sys")?
            .getattr("modules")?
            .get_item("_cellar_application")?
            .getattr(name)?
            .call0()?;
        let encoded: String = py
            .import("json")?
            .call_method1("dumps", (value,))?
            .extract()?;
        Ok(serde_json::from_str(&encoded)?)
    })
}

pub(super) async fn observe(check: impl Fn(&Value) -> bool) -> Result<Value> {
    let mut last = Value::Null;
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            last = call("snapshot")?;
            if check(&last) {
                return Ok::<_, anyhow::Error>(last.clone());
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .with_context(|| format!("Python fixture barrier not reached: {last}"))?
}

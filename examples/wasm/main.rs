//! Compile computation directly into the native application executable.
use crate::support;
use anyhow::Result;
use cellar::sdk::{Context, Output, Request};
pub fn fetch(context: &Context, request: Request) -> Result<Output> {
    support::method(&request, &["GET"])?;
    let url = url::Url::parse(&request.url)?;
    let samples = url
        .query_pairs()
        .find(|(k, _)| k == "samples")
        .map(|(_, v)| v.parse::<u32>())
        .transpose()
        .map_err(|_| support::reject(400, "invalid samples"))?
        .unwrap_or(1000);
    if !(1..=10000).contains(&samples) {
        return Err(support::reject(400, "samples must be 1..10000"));
    }
    let pi: f64 = (0..samples)
        .map(|i| {
            let x = (f64::from(i) + 0.5) / f64::from(samples);
            4.0 / (1.0 + x * x)
        })
        .sum::<f64>()
        / f64::from(samples);
    support::json(
        context,
        serde_json::json!({"pi":pi,"samples":samples,"execution":"native Rust"}),
    )
}

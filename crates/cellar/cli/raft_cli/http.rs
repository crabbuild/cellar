// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use serde::de::DeserializeOwned;

pub(super) struct Client {
    http: reqwest::Client,
    base: reqwest::Url,
    token: String,
}

impl Client {
    pub fn new(options: &options::Options) -> Result<Self> {
        let url = reqwest::Url::parse(&options.admin_url)
            .map_err(|_| invalid("invalid management URL"))?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(invalid(
                "management URL must be HTTP(S) without credentials, query or fragment",
            ));
        }
        let base = reqwest::Url::parse(&format!(
            "{}/api/v1/fleets/{}/raft/",
            url.as_str().trim_end_matches('/'),
            options.fleet
        ))
        .map_err(|_| invalid("invalid management URL"))?;
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|_| unavailable())?;
        Ok(Self {
            http,
            base,
            token: options.token.clone(),
        })
    }

    pub async fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        cursor: Option<&str>,
        limit: Option<usize>,
        timeout: Duration,
    ) -> Result<T> {
        let mut url = self
            .base
            .join(path)
            .map_err(|_| invalid("invalid management route"))?;
        if let Some(cursor) = cursor {
            url.query_pairs_mut().append_pair("cursor", cursor);
        }
        if let Some(limit) = limit {
            url.query_pairs_mut()
                .append_pair("limit", &limit.to_string());
        }
        self.send(self.http.get(url).timeout(timeout)).await
    }

    pub async fn post<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &impl serde::Serialize,
    ) -> Result<T> {
        let url = self
            .base
            .join(path)
            .map_err(|_| invalid("invalid management route"))?;
        self.send(self.http.post(url).json(body)).await
    }

    async fn send<T: DeserializeOwned>(&self, request: reqwest::RequestBuilder) -> Result<T> {
        let mut response = request
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|_| unavailable())?;
        let status = response.status();
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| unavailable())? {
            if bytes
                .len()
                .checked_add(chunk.len())
                .is_none_or(|n| n > 1024 * 1024)
            {
                return Err(unavailable());
            }
            bytes.extend_from_slice(&chunk);
        }
        if status.is_success() {
            return serde_json::from_slice(&bytes).map_err(|_| unavailable());
        }
        let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| unavailable())?;
        if let Some(error) = value
            .get("error")
            .and_then(|v| serde_json::from_value::<RaftFleetFailure>(v.clone()).ok())
        {
            return Err(error);
        }
        let code = match status.as_u16() {
            401 => RaftFleetFailureCode::AuthRequired,
            403 => RaftFleetFailureCode::Forbidden,
            _ => RaftFleetFailureCode::Unavailable,
        };
        Err(RaftFleetFailure { code, outcome: RaftFleetOutcome::Unknown, message: match code {
            RaftFleetFailureCode::AuthRequired => "The management endpoint rejected the credential.",
            RaftFleetFailureCode::Forbidden => "The principal lacks permission for this fleet operation.",
            _ => "The management response did not confirm the request; retain its exact ID and payload.",
        }.into() })
    }
}

pub(super) async fn wait(
    client: &Client,
    operation: &str,
    catalog_id: &str,
    timeout: Duration,
    cancelled: bool,
) -> Result<RaftFleetOperation> {
    let start = crate::asyncrt::mono_ms();
    let deadline = start
        .checked_add(timeout.as_millis() as u64)
        .ok_or_else(|| invalid("wait deadline overflow"))?;
    loop {
        let now = crate::asyncrt::mono_ms();
        if now >= deadline {
            return Err(RaftFleetFailure {
                code: RaftFleetFailureCode::WaitTimeout,
                outcome: RaftFleetOutcome::Unknown,
                message: "Observation timed out; the fleet operation remains unchanged.".into(),
            });
        }
        let result = client
            .get::<RaftFleetOperation>(
                &format!("operations/{operation}"),
                None,
                None,
                Duration::from_millis((deadline - now).min(30_000)),
            )
            .await;
        match result {
            Ok(view) if view.validate().is_err() || view.operation_id != operation => {
                return Err(unavailable())
            }
            Ok(view) if view.catalog_id != catalog_id => return Err(catalog_mismatch()),
            Ok(view) if view.terminal => {
                if view.converged != cancelled {
                    return Ok(view);
                }
                return Err(RaftFleetFailure { code: RaftFleetFailureCode::Blocked, outcome: RaftFleetOutcome::Unknown,
                    message: "The operation reached a different terminal outcome; inspect its status before further changes.".into() });
            }
            Ok(_) => {}
            Err(error)
                if matches!(
                    error.code,
                    RaftFleetFailureCode::NotFound | RaftFleetFailureCode::Busy
                ) => {}
            Err(error) => return Err(error),
        }
        let remaining = deadline.saturating_sub(crate::asyncrt::mono_ms());
        crate::asyncrt::sleep(Duration::from_millis(remaining.min(500))).await;
    }
}

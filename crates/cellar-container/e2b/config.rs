// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{ensure, Result};
use serde_json::json;
use sha2::{Digest, Sha256};
use url::Url;

/// Direct E2B does not validate Cellar ownership tokens on every request.
/// Callers must explicitly select the weaker profile; strict admission refuses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ownership {
    RequireProviderFence,
    LocalChecksAndExpiry,
}

#[derive(Clone)]
pub struct Config {
    pub(super) account: String,
    pub(super) namespace: String,
    pub(super) key: String,
    pub(super) template: String,
    pub(super) api: Url,
    pub(super) gateway: Option<Url>,
    pub(super) domain: String,
    pub(super) binding: String,
    pub(super) lifetime: u32,
    pub(super) timeout_ms: u64,
    pub(super) allow_internet: bool,
}

impl Config {
    pub fn new(
        account: impl Into<String>,
        namespace: impl Into<String>,
        api_key: impl Into<String>,
        template: impl Into<String>,
        ownership: Ownership,
    ) -> Result<Self> {
        ensure!(ownership == Ownership::LocalChecksAndExpiry, "direct E2B does not enforce Cellar ownership fences; use a fenced broker or explicitly choose LocalChecksAndExpiry");
        let value = Self {
            account: account.into(),
            namespace: namespace.into(),
            key: api_key.into(),
            template: template.into(),
            api: Url::parse("https://api.e2b.app/")?,
            gateway: None,
            domain: "e2b.app".into(),
            binding: "container".into(),
            lifetime: 300,
            timeout_ms: 30_000,
            allow_internet: false,
        };
        for value in [&value.account, &value.namespace, &value.template] {
            ensure!(
                !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control),
                "invalid E2B configuration identity"
            );
        }
        ensure!(
            !value.key.is_empty()
                && value.key.len() <= 4096
                && value.key.bytes().all(|b| (0x21..=0x7e).contains(&b)),
            "invalid E2B API key encoding"
        );
        Ok(value)
    }

    pub fn lifetime_seconds(mut self, seconds: u32) -> Result<Self> {
        ensure!(
            (1..=3600).contains(&seconds),
            "E2B lifetime must be 1..3600 seconds"
        );
        self.lifetime = seconds;
        Ok(self)
    }

    pub fn request_timeout_ms(mut self, ms: u64) -> Result<Self> {
        ensure!((1..=120_000).contains(&ms), "invalid E2B request timeout");
        self.timeout_ms = ms;
        Ok(self)
    }

    pub fn allow_internet(mut self, allow: bool) -> Self {
        self.allow_internet = allow;
        self
    }

    pub fn binding(mut self, name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        ensure!(
            !name.is_empty()
                && name.len() <= 128
                && name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.')),
            "invalid E2B binding name"
        );
        self.binding = name;
        Ok(self)
    }

    /// Host-selected control endpoint; HTTPS or an explicit loopback fixture.
    pub fn api_endpoint(mut self, endpoint: &str) -> Result<Self> {
        self.api = endpoint_url(endpoint)?;
        Ok(self)
    }

    /// A trusted envd gateway. The selected sandbox ID is supplied as a header.
    pub fn envd_gateway(mut self, endpoint: &str) -> Result<Self> {
        self.gateway = Some(endpoint_url(endpoint)?);
        Ok(self)
    }

    pub fn sandbox_domain(mut self, domain: &str) -> Result<Self> {
        ensure!(
            !domain.is_empty()
                && domain.len() <= 190
                && domain.split('.').all(|s| !s.is_empty()
                    && s.len() <= 63
                    && !s.starts_with('-')
                    && !s.ends_with('-')
                    && s.bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')),
            "invalid E2B sandbox domain"
        );
        self.domain = domain.into();
        Ok(self)
    }

    pub(super) fn digest(&self) -> String {
        digest(
            &json!({"account":self.account,"namespace":self.namespace,"template":self.template,"api":self.api.as_str(),"gateway":self.gateway.as_ref().map(Url::as_str),"domain":self.domain,"binding":self.binding,"lifetime":self.lifetime,"allow_internet":self.allow_internet,"ownership":"local_checks_and_expiry"}),
        )
    }

    pub(super) fn envd_url(&self, id: &str, path: &str) -> Result<String> {
        resource_id(id)?;
        Ok(match &self.gateway {
            Some(gateway) => gateway.join(path)?.to_string(),
            None => format!("https://49983-{id}.{}/{path}", self.domain),
        })
    }
}

fn endpoint_url(input: &str) -> Result<Url> {
    let url = Url::parse(input)?;
    ensure!(
        url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && url.path().ends_with('/'),
        "E2B endpoints require a trailing slash and no URL credentials/query/fragment"
    );
    let local = match url.host() {
        Some(url::Host::Domain("localhost")) => true,
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        _ => false,
    };
    ensure!(
        url.scheme() == "https" || (url.scheme() == "http" && local),
        "E2B endpoints require HTTPS except explicit loopback fixtures"
    );
    Ok(url)
}

pub(super) fn resource_id(id: &str) -> Result<()> {
    ensure!(
        !id.is_empty()
            && id.len() <= 56
            && !id.starts_with('-')
            && !id.ends_with('-')
            && id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
        "invalid E2B sandbox identity"
    );
    Ok(())
}

pub(super) fn digest(value: &serde_json::Value) -> String {
    format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(value).expect("JSON value encoding"))
    )
}

#[cfg(test)]
#[path = "config_tests.rs"]
mod tests;

// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

pub use crate::Ownership;
use anyhow::{ensure, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use url::Url;

#[derive(Clone)]
pub struct Config {
    pub(super) team: String,
    pub(super) project: String,
    pub(super) namespace: String,
    pub(super) token: String,
    pub(super) image: String,
    pub(super) api: Url,
    pub(super) binding: String,
    pub(super) lifetime_ms: u64,
    pub(super) timeout_ms: u64,
    pub(super) allow_internet: bool,
    pub(super) region: Option<String>,
    pub(super) vcpus: u32,
    pub(super) python: String,
}

impl Config {
    /// Direct Vercel does not enforce Cellar epochs on accepted requests.
    pub fn new(
        team: impl Into<String>,
        project: impl Into<String>,
        namespace: impl Into<String>,
        token: impl Into<String>,
        image: impl Into<String>,
        ownership: Ownership,
    ) -> Result<Self> {
        ensure!(ownership==Ownership::LocalChecksAndExpiry,"direct Vercel does not enforce Cellar ownership fences; explicitly choose LocalChecksAndExpiry or use a fenced provider");
        let config = Self {
            team: team.into(),
            project: project.into(),
            namespace: namespace.into(),
            token: token.into(),
            image: image.into(),
            api: Url::parse("https://api.vercel.com/")?,
            binding: "container".into(),
            lifetime_ms: 300_000,
            timeout_ms: 30_000,
            allow_internet: false,
            region: None,
            vcpus: 1,
            python: "/usr/bin/python3".into(),
        };
        for identity in [
            &config.team,
            &config.project,
            &config.namespace,
            &config.image,
        ] {
            ensure!(
                !identity.is_empty()
                    && identity.len() <= 512
                    && !identity.chars().any(char::is_control),
                "invalid Vercel configuration identity"
            );
        }
        ensure!(
            !config.token.is_empty()
                && config.token.len() <= 4096
                && config.token.bytes().all(|b| (0x21..=0x7e).contains(&b)),
            "invalid Vercel access token encoding"
        );
        Ok(config)
    }

    pub fn api_endpoint(mut self, endpoint: &str) -> Result<Self> {
        let url = Url::parse(endpoint)?;
        let local = match url.host() {
            Some(url::Host::Domain("localhost")) => true,
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            _ => false,
        };
        ensure!((url.scheme()=="https" || (url.scheme()=="http" && local)) && url.username().is_empty() && url.password().is_none() && url.query().is_none() && url.fragment().is_none() && url.path().ends_with('/'),"Vercel endpoint requires HTTPS (or explicit loopback HTTP), trailing slash, no credentials/query/fragment");
        self.api = url;
        Ok(self)
    }

    pub fn binding(mut self, name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        ensure!(
            !name.is_empty()
                && name.len() <= 128
                && name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.')),
            "invalid Vercel binding name"
        );
        self.binding = name;
        Ok(self)
    }

    pub fn lifetime_ms(mut self, ms: u64) -> Result<Self> {
        ensure!(
            (1_000..=2_700_000).contains(&ms),
            "Vercel lifetime must be 1 second..45 minutes"
        );
        self.lifetime_ms = ms;
        Ok(self)
    }

    pub fn request_timeout_ms(mut self, ms: u64) -> Result<Self> {
        ensure!(
            (1..=120_000).contains(&ms),
            "invalid Vercel operation timeout"
        );
        self.timeout_ms = ms;
        Ok(self)
    }

    pub fn allow_internet(mut self, allow: bool) -> Self {
        self.allow_internet = allow;
        self
    }

    pub fn region(mut self, region: impl Into<String>) -> Result<Self> {
        let region = region.into();
        ensure!(
            !region.is_empty()
                && region.len() <= 32
                && region
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()),
            "invalid Vercel region"
        );
        self.region = Some(region);
        Ok(self)
    }

    pub fn vcpus(mut self, count: u32) -> Result<Self> {
        ensure!(
            count == 1 || ((2..=32).contains(&count) && count.is_multiple_of(2)),
            "invalid Vercel CPU limit"
        );
        self.vcpus = count;
        Ok(self)
    }

    /// The host-selected image must provide Python 3 at this absolute path.
    pub fn helper_python(mut self, path: impl Into<String>) -> Result<Self> {
        let path = path.into();
        ensure!(
            path.starts_with('/') && path.len() <= 4096 && !path.chars().any(char::is_control),
            "invalid Vercel helper Python path"
        );
        self.python = path;
        Ok(self)
    }

    pub(super) fn digest(&self) -> String {
        digest(
            &json!({"team":self.team,"project":self.project,"namespace":self.namespace,"image":self.image,"api":self.api.as_str(),"binding":self.binding,"lifetime_ms":self.lifetime_ms,"allow_internet":self.allow_internet,"region":self.region,"vcpus":self.vcpus,"helper_python":self.python,"helper_sha256":format!("{:x}",Sha256::digest(include_bytes!("helper.py"))),"ownership":"local_checks_and_expiry"}),
        )
    }

    pub(super) fn url(&self, path: &str) -> Result<String> {
        let mut url = self.api.join(path)?;
        url.query_pairs_mut().append_pair("teamId", &self.team);
        Ok(url.to_string())
    }

    pub(super) fn headers(&self, content_type: &str) -> Vec<(String, String)> {
        vec![
            ("Authorization".into(), format!("Bearer {}", self.token)),
            ("Content-Type".into(), content_type.into()),
        ]
    }
}

pub(super) fn digest(value: &Value) -> String {
    format!("{:x}", Sha256::digest(value.to_string().as_bytes()))
}

pub(super) fn identifier(id: &str) -> Result<()> {
    ensure!(
        !id.is_empty()
            && id.len() <= 256
            && id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_')),
        "invalid Vercel identity"
    );
    Ok(())
}

#[cfg(test)]
#[path = "config_tests.rs"]
mod tests;

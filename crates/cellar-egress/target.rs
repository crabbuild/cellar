// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Canonical HTTP interception targets and bounded wildcard matching.

use anyhow::{bail, ensure, Result};
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, SocketAddr};

/// A validated request hostname, separate from its actual socket destination.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Host(String);

impl Host {
    pub fn parse(value: &str) -> Result<Self> {
        Ok(Self(domain(value, false)?))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Kernel-observed destination plus a validated HTTP Host name when present.
pub struct Destination<'a> {
    pub socket: SocketAddr,
    pub host: Option<&'a Host>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DnsFamily {
    V4,
    V6,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Pattern {
    Host(String),
    Address { address: IpAddr, port: Option<u16> },
    Network { address: IpAddr, prefix: u8 },
    All,
}

/// An HTTP rule. Hostname targets consume two Cloudflare-style entries; an
/// address/CIDR consumes one. A catch-all is distinct from the hostname `*`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct HttpTarget {
    key: String,
    pattern: Pattern,
}

impl HttpTarget {
    pub fn parse(value: &str) -> Result<Self> {
        ensure!(
            !value.is_empty() && value.len() <= 254 && value.trim() == value,
            "invalid HTTP intercept target"
        );
        let (key, pattern) = if let Some((address, prefix)) = value.split_once('/') {
            let address: IpAddr = address.parse()?;
            ensure!(
                !prefix.is_empty() && prefix.bytes().all(|byte| byte.is_ascii_digit()),
                "invalid CIDR prefix"
            );
            let prefix: u8 = prefix.parse()?;
            let address = network(address, prefix)?;
            (
                format!("{address}/{prefix}"),
                Pattern::Network { address, prefix },
            )
        } else if let Ok(address) = value.parse::<IpAddr>() {
            (
                address.to_string(),
                Pattern::Address {
                    address,
                    port: None,
                },
            )
        } else if let Ok(socket) = value.parse::<SocketAddr>() {
            ensure!(socket.port() != 0, "intercept port must be nonzero");
            // An explicitly selected default port is the same registration,
            // rather than an unreachable second rule behind the bare IP.
            let port = (socket.port() != 80).then_some(socket.port());
            (
                if port.is_some() {
                    socket.to_string()
                } else {
                    socket.ip().to_string()
                },
                Pattern::Address {
                    address: socket.ip(),
                    port,
                },
            )
        } else {
            let pattern = domain(value, true)?;
            (pattern.clone(), Pattern::Host(pattern))
        };
        Ok(Self { key, pattern })
    }

    pub fn all() -> Self {
        Self {
            key: "<all-http>".into(),
            pattern: Pattern::All,
        }
    }

    pub fn as_str(&self) -> &str {
        &self.key
    }

    pub fn entries(&self) -> usize {
        match self.pattern {
            Pattern::Host(_) | Pattern::All => 2,
            _ => 1,
        }
    }

    pub fn port(&self) -> u16 {
        match self.pattern {
            Pattern::Address {
                port: Some(port), ..
            } => port,
            _ => 80,
        }
    }

    pub fn matches(&self, destination: &Destination<'_>) -> bool {
        match &self.pattern {
            Pattern::Host(pattern) => {
                destination.socket.port() == 80
                    && destination
                        .host
                        .is_some_and(|host| wildcard(pattern.as_bytes(), host.0.as_bytes()))
            }
            Pattern::Address { address, port } => {
                *address == destination.socket.ip()
                    && destination.socket.port() == port.unwrap_or(80)
            }
            Pattern::Network { address, prefix } => {
                destination.socket.port() == 80
                    && network(destination.socket.ip(), *prefix)
                        .is_ok_and(|current| current == *address)
            }
            Pattern::All => destination.socket.port() == 80,
        }
    }

    /// Whether an offline resolver may answer this name. IP/CIDR entries do
    /// not grant DNS, except for the two all-address CIDR targets.
    pub fn resolves(&self, host: &Host, family: DnsFamily) -> bool {
        match &self.pattern {
            Pattern::Host(pattern) => wildcard(pattern.as_bytes(), host.0.as_bytes()),
            Pattern::All => true,
            Pattern::Network { address, prefix: 0 } => matches!(
                (address, family),
                (IpAddr::V4(_), DnsFamily::V4) | (IpAddr::V6(_), DnsFamily::V6)
            ),
            _ => false,
        }
    }
}

impl TryFrom<String> for HttpTarget {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self> {
        if value == "<all-http>" {
            Ok(Self::all())
        } else {
            Self::parse(&value)
        }
    }
}

impl From<HttpTarget> for String {
    fn from(value: HttpTarget) -> Self {
        value.key
    }
}

fn network(address: IpAddr, prefix: u8) -> Result<IpAddr> {
    Ok(match address {
        IpAddr::V4(address) => {
            ensure!(prefix <= 32, "IPv4 prefix exceeds 32");
            IpAddr::V4(
                (u32::from(address) & u32::MAX.checked_shl(u32::from(32 - prefix)).unwrap_or(0))
                    .into(),
            )
        }
        IpAddr::V6(address) => {
            ensure!(prefix <= 128, "IPv6 prefix exceeds 128");
            IpAddr::V6(
                (u128::from(address) & u128::MAX.checked_shl(u32::from(128 - prefix)).unwrap_or(0))
                    .into(),
            )
        }
    })
}

fn domain(value: &str, glob: bool) -> Result<String> {
    let value = value.strip_suffix('.').unwrap_or(value);
    ensure!(
        !value.is_empty() && value.len() <= 253 && value.is_ascii(),
        "invalid HTTP hostname"
    );
    ensure!(
        value.split('.').all(|label| !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label.bytes().all(|byte| byte.is_ascii_alphanumeric()
                || byte == b'-'
                || (glob && byte == b'*'))),
        "invalid HTTP hostname pattern"
    );
    if value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || byte == b'.')
    {
        bail!("ambiguous numeric hostname");
    }
    Ok(value.to_ascii_lowercase())
}

fn wildcard(pattern: &[u8], value: &[u8]) -> bool {
    let (mut p, mut v, mut star, mut retry) = (0, 0, None, 0);
    while v < value.len() {
        if p < pattern.len() && pattern[p] == value[v] {
            p += 1;
            v += 1;
        } else if p < pattern.len() && pattern[p] == b'*' {
            star = Some(p);
            p += 1;
            retry = v;
        } else if let Some(at) = star {
            retry += 1;
            v = retry;
            p = at + 1;
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len()
}

#[cfg(test)]
#[path = "target_tests.rs"]
mod tests;

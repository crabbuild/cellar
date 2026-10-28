// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit infrastructure for the native Linux network-helper profile.

use anyhow::{ensure, Result};
use std::{net::SocketAddr, time::Duration};

/// One immutable, operator-supplied Linux helper image. The application keeps
/// its own image and joins the helper's network namespace after readiness.
/// This profile currently requires an explicitly selected `runc` runtime.
#[derive(Clone, Debug)]
pub struct NetworkHelperOptions {
    pub image: String,
    /// Absolute executable path inside the helper image.
    pub executable: String,
    /// Explicit resolver used by marked helper sockets.
    pub upstream_dns: SocketAddr,
    pub startup_timeout: Duration,
    pub shutdown_timeout: Duration,
    /// Absolute nft executable path inside the helper image.
    pub nft_path: String,
}

impl NetworkHelperOptions {
    pub fn new(image: String, executable: String, upstream_dns: SocketAddr) -> Self {
        Self {
            image,
            executable,
            upstream_dns,
            startup_timeout: Duration::from_secs(10),
            shutdown_timeout: Duration::from_secs(5),
            nft_path: "/usr/sbin/nft".into(),
        }
    }

    pub(super) fn validate(&self) -> Result<()> {
        let digest = self
            .image
            .strip_prefix("sha256:")
            .or_else(|| self.image.strip_prefix("cellar-image:"));
        ensure!(
            digest.is_some_and(|digest| digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))),
            "network helper requires an immutable image identity"
        );
        for path in [&self.executable, &self.nft_path] {
            ensure!(
                path.starts_with('/') && path.len() <= 4096 && !path.contains(['\0', '\n', '\r']),
                "network helper executable paths must be absolute and bounded"
            );
        }
        ensure!(
            self.upstream_dns.port() != 0
                && !self.upstream_dns.ip().is_unspecified()
                && !self.upstream_dns.ip().is_multicast(),
            "network helper requires a specific DNS upstream"
        );
        for timeout in [self.startup_timeout, self.shutdown_timeout] {
            ensure!(
                !timeout.is_zero() && timeout <= Duration::from_secs(60),
                "network helper lifecycle deadlines must be within 60 seconds"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helper_configuration_requires_immutable_bounded_infrastructure() -> Result<()> {
        let valid = NetworkHelperOptions::new(
            format!("sha256:{}", "a".repeat(64)),
            "/usr/local/bin/cellar-egress".into(),
            "127.0.0.11:53".parse()?,
        );
        valid.validate()?;
        for image in ["latest", "rust:tag", "sha256:short"] {
            let mut options = valid.clone();
            options.image = image.into();
            assert!(options.validate().is_err());
        }
        for deadline in [Duration::ZERO, Duration::from_secs(61)] {
            let mut options = valid.clone();
            options.startup_timeout = deadline;
            assert!(options.validate().is_err());
            options = valid.clone();
            options.shutdown_timeout = deadline;
            assert!(options.validate().is_err());
        }
        for path in ["relative", "/bin/helper\n--argument", "/bin/helper\0"] {
            let mut options = valid.clone();
            options.executable = path.into();
            assert!(options.validate().is_err());
        }
        for upstream in ["0.0.0.0:53", "127.0.0.1:0", "224.0.0.1:53"] {
            let mut options = valid.clone();
            options.upstream_dns = upstream.parse()?;
            assert!(options.validate().is_err());
        }
        Ok(())
    }
}

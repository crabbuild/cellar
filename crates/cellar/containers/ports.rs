// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Additional image-independent TCP declarations selected by deployment policy.
use anyhow::{ensure, Result};
use serde_json::{json, Value};
use std::collections::BTreeSet;

/// Validate additional container TCP ports: at most 64 distinct nonzero ports.
/// Docker still retains ports declared by the image's own EXPOSE instructions.
pub fn validate_exposed_ports(ports: &[u16]) -> Result<()> {
    ensure!(
        ports.len() <= 64,
        "at most 64 additional container ports are permitted"
    );
    let mut unique = BTreeSet::new();
    ensure!(
        ports.iter().all(|port| *port != 0 && unique.insert(*port)),
        "container ports must be distinct integers from 1 to 65535"
    );
    Ok(())
}

pub(super) fn configure(body: &mut Value, ports: &[u16], publish: bool) -> Result<()> {
    validate_exposed_ports(ports)?;
    if ports.is_empty() {
        return Ok(());
    }
    let mut exposed = serde_json::Map::new();
    let mut bindings = serde_json::Map::new();
    for port in ports {
        let key = format!("{port}/tcp");
        exposed.insert(key.clone(), json!({}));
        if publish {
            bindings.insert(key, json!([{"HostIp":"127.0.0.1","HostPort":""}]));
        }
    }
    body["ExposedPorts"] = json!(exposed);
    if publish {
        body["HostConfig"]["PortBindings"] = json!(bindings);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publication_is_bounded_and_does_not_choose_a_public_host_port() -> Result<()> {
        for invalid in [vec![0], vec![8080, 8080], (1..=65).collect()] {
            assert!(validate_exposed_ports(&invalid).is_err());
        }
        validate_exposed_ports(&(1..=64).collect::<Vec<_>>())?;
        for publish in [false, true] {
            let mut body = json!({"Image":"pinned", "HostConfig":{"Memory":42}});
            configure(&mut body, &[8080, 65535], publish)?;
            assert_eq!(body["ExposedPorts"], json!({"8080/tcp":{},"65535/tcp":{}}));
            assert_eq!(body["HostConfig"]["Memory"], 42);
            if publish {
                assert_eq!(
                    body["HostConfig"]["PortBindings"]["8080/tcp"],
                    json!([{"HostIp":"127.0.0.1","HostPort":""}])
                );
            } else {
                assert!(body["HostConfig"].get("PortBindings").is_none());
            }
        }
        let mut unchanged = json!({"HostConfig":{}});
        configure(&mut unchanged, &[], true)?;
        assert_eq!(unchanged, json!({"HostConfig":{}}));
        Ok(())
    }
}

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

/// A VM daemon's automatic port allocator cannot see occupied host ports.
/// Hold available local candidates through container creation; release them
/// immediately before daemon startup so its forwarder can bind those ports.
pub(super) async fn reserve_host_bindings(
    body: &mut Value,
    ports: &[u16],
) -> Result<Vec<tokio::net::TcpListener>> {
    validate_exposed_ports(ports)?;
    let mut reservations = Vec::with_capacity(ports.len());
    for port in ports {
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let host = listener.local_addr()?.port();
        body["HostConfig"]["PortBindings"][format!("{port}/tcp")] =
            json!([{"HostIp":"127.0.0.1","HostPort":host.to_string()}]);
        reservations.push(listener);
    }
    Ok(reservations)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn vm_publications_select_and_hold_available_host_ports() -> Result<()> {
        let occupied = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let mut body = json!({"HostConfig":{}});
        let held = reserve_host_bindings(&mut body, &[8080, 9090]).await?;
        let mut selected = BTreeSet::new();
        for port in [8080, 9090] {
            let host: u16 = body["HostConfig"]["PortBindings"][format!("{port}/tcp")][0]
                ["HostPort"]
                .as_str()
                .unwrap()
                .parse()?;
            assert_ne!(host, occupied.local_addr()?.port());
            assert!(
                selected.insert(host),
                "two publications received one host port"
            );
            assert!(
                tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, host))
                    .await
                    .is_err(),
                "publication was not reserved through creation"
            );
        }
        drop(held);
        for host in selected {
            let _available =
                tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, host)).await?;
        }
        Ok(())
    }

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

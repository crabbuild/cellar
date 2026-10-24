// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Process-boundary parsing for platform namespace inventory.

use super::{Fleet, FleetFlags, Format, Output, Reachable, Reply, Subject, FLEET_HELP};
use crate::application::event_stream::{CatalogNamespace, Resource};
use crate::runtime::catalog_control::{Request, MAX_REQUEST_BYTES};
use anyhow::{bail, Context, Result};
use std::time::Duration;

const HELP: &str =
    "cellar stream list --tenant TENANT --application APPLICATION [--cursor TOKEN] [--limit N]

Lists catalog identity/lifecycle metadata, including retained deleted resources.
Requires fleet administration, separately from an application's stream grants.
Pages contain at most 100 records; namespace-scoped cursors observe live state.
Configuration grants, credentials and event bodies are not returned.";

pub(super) async fn run(arguments: Vec<String>, proof: bool) -> Result<()> {
    let mut args = arguments.into_iter();
    let mut tenant = None;
    let mut application = None;
    let mut cursor = None;
    let mut limit = if proof { 1 } else { 100 };
    let mut resource_file = None;
    let mut fleet = FleetFlags::default();
    let mut json = false;
    let mut unsafe_public_advertise = false;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--help" | "-h" | "help" => {
                return Output::new(Format::Text).line(format_args!("{}\n\n{FLEET_HELP}",if proof {"cellar stream retirement-proof --resource RESOURCE.json [--json]\nReturns exact terminal catalog identity and receipt metadata using fleet administration."} else {HELP}))
            }
            "--json" => {
                json = true;
                continue;
            }
            "--unsafe-public-advertise" => {
                unsafe_public_advertise = true;
                continue;
            }
            _ => {}
        }
        let mut value = |flag: &str| {
            args.next()
                .with_context(|| format!("{flag} requires a value"))
        };
        if fleet.consume(&flag, &mut value)? {
            continue;
        }
        match flag.as_str() {
            "--resource" if proof => resource_file = Some(value(&flag)?),
            "--tenant" if !proof => tenant = Some(value(&flag)?),
            "--application" if !proof => application = Some(value(&flag)?),
            "--cursor" if !proof => cursor = Some(value(&flag)?),
            "--limit" if !proof => {
                limit = value(&flag)?.parse().context("invalid catalog limit")?
            }
            _ => bail!(
                "invalid catalog {} option {flag}",
                if proof { "retirement-proof" } else { "list" }
            ),
        }
    }
    let retired_resource = resource_file
        .map(|path| super::file::<Resource>(&path, 4096))
        .transpose()?;
    anyhow::ensure!(
        !proof || retired_resource.is_some(),
        "retirement-proof requires --resource RESOURCE.json"
    );
    let namespace = if let Some(resource) = &retired_resource {
        resource.validate()?;
        CatalogNamespace {
            tenant: resource.tenant.clone(),
            application: resource.application.clone(),
        }
    } else {
        CatalogNamespace {
            tenant: tenant.context("catalog list requires --tenant")?,
            application: application.context("catalog list requires --application")?,
        }
    };
    let request = Request {
        namespace,
        cursor,
        limit,
        retired_resource,
    };
    request.prepare("catalogs-operator".into())?;
    anyhow::ensure!(
        serde_json::to_vec(&request)?.len() <= MAX_REQUEST_BYTES,
        "catalog operator request exceeds limit"
    );
    let storage = fleet.resolve(if proof {
        "cellar stream retirement-proof"
    } else {
        "cellar stream list"
    })?;
    let target = Reachable::open_catalog(
        Fleet {
            bucket: &storage.bucket,
            endpoint: storage.endpoint.as_deref(),
            region: &storage.region,
            unsafe_public_advertise,
        },
        Subject {
            noun: "catalogs",
            source: "catalogs-operator",
            timeout: Duration::from_secs(60),
        },
        request.scope()?,
    )
    .await?;
    let reply = target.call(serde_json::to_value(request)?).await?;
    let mut output = Output::new(if json { Format::Json } else { Format::Text });
    output.row(&Reply(reply))?;
    output.finish()
}

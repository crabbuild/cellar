//! Publish the same FleetCare bindings used by the local performance host.
//!
//! This one-shot process owns package files and bootstrap I/O, outside cells.
#![allow(clippy::disallowed_methods)]

use anyhow::Result;
use cellar::native_deploy::{executable_identity, ApplicationConfig, Package};
use cellar_fleetcare::{config::AppConfig, deployment};
use serde_json::json;
use std::path::Path;

fn application_config(config: &AppConfig) -> Result<ApplicationConfig> {
    let graph = deployment::graph(config);
    let options = &graph.primary.options;
    let mut bindings = Vec::new();
    for (name, class) in &options.bindings {
        bindings.push(json!({"type":"durable_object_namespace", "name":name, "class_name":class}));
    }
    for (name, database) in &options.sql_bindings {
        bindings.push(json!({"type":"sql", "name":name, "database_id":database}));
    }
    for (name, namespace) in &options.kv_bindings {
        bindings.push(json!({"type":"kv", "name":name, "id":namespace}));
    }
    for binding in &options.queue_bindings {
        bindings.push(json!({"type":"queue", "name":binding.environment,
                             "queue":binding.queue, "delivery_delay":binding.delivery_delay}));
    }
    for binding in &options.workflow_bindings {
        bindings.push(json!({"type":"workflow", "name":binding.environment,
                             "workflow_name":binding.workflow, "class_name":binding.class}));
    }
    for (name, bucket) in &options.blob_bindings {
        bindings.push(json!({"type":"blob_bucket", "name":name, "bucket_name":bucket}));
    }
    for (name, service, entrypoint) in &graph.primary.services {
        bindings.push(
            json!({"type":"service", "name":name, "service":service, "entrypoint":entrypoint}),
        );
    }
    for name in &graph.primary.loader_bindings {
        bindings.push(json!({"type":"worker_loader", "name":name}));
    }
    serde_json::from_value(json!({
        "script_name": graph.primary.script_name,
        "application_version": graph.primary.version,
        "cells": options.do_classes,
        "bindings": bindings,
        "crons": graph.primary.crons,
        "queue_consumers": options.queue_consumers,
        "assets": {"directory":"manual", "binding":"ASSETS",
                   "run_worker_first":["/api/*", "/diagnostics/*"]}
    }))
    .map_err(Into::into)
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::from_env()?;
    anyhow::ensure!(
        config.container_image.is_none(),
        "console lab does not provision cell containers"
    );
    let directory = Path::new("/data/package");
    let manual = directory.join("manual/assets");
    std::fs::create_dir_all(&manual)?;
    std::fs::write(manual.join("manual.txt"), cellar_fleetcare::assets::MANUAL)?;
    let executable = Path::new("/usr/local/bin/fleetcare-daemon");
    let artifact = executable_identity(executable, cellar::native_deploy::current_target())?;
    std::fs::copy(executable, directory.join(&artifact.sha256))?;
    let manifest =
        application_config(&config)?.stage_manifest(vec![artifact], directory, directory)?;
    let package = Package {
        primary: manifest,
        cohosted: Vec::new(),
    };
    let domain = cellar::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let bucket = cellar::fleet::bucket_client(
                &std::env::var("FLEETCARE_BUCKET")?,
                Some(&std::env::var("FLEETCARE_ENDPOINT")?),
                "us-east-1",
            )?;
            package.publish_staged(&bucket, directory).await
        })
        .await;
    domain.shutdown().await;
    result??;
    cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(format_args!(
        "Published FleetCare deployment {}",
        package.primary.version
    ))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cellar_fleetcare::config::Profile;
    use std::sync::Arc;

    #[test]
    fn published_bindings_match_each_profile() {
        for profile in [Profile::Smoke, Profile::Full] {
            let config = AppConfig {
                profile,
                node: Arc::from("node1"),
                token: Arc::from("fixture-token-123456"),
                asynchronous: true,
                simulator: None,
                container_image: None,
                fence_image: None,
                offline_ms: 60_000,
                reservation_ms: 60_000,
            };
            let graph = deployment::graph(&config);
            let published = application_config(&config).unwrap();
            let count = |kind| {
                published
                    .bindings
                    .iter()
                    .filter(|binding| binding["type"] == kind)
                    .count()
            };
            assert_eq!(count("sql"), profile.counts().sql);
            assert_eq!(count("kv"), profile.counts().kv);
            assert_eq!(count("queue"), profile.counts().queues);
            assert_eq!(count("workflow"), 1);
            assert_eq!(published.crons, graph.primary.crons);
            assert_eq!(published.application_version, graph.primary.version);
            let directory = tempfile::tempdir().unwrap();
            std::fs::create_dir_all(directory.path().join("manual/assets")).unwrap();
            std::fs::write(
                directory.path().join("manual/assets/manual.txt"),
                cellar_fleetcare::assets::MANUAL,
            )
            .unwrap();
            let manifest = published
                .stage_manifest(
                    vec![cellar::protocol::NativeArtifact {
                        target: cellar::native_deploy::current_target().into(),
                        sha256: "a".repeat(64),
                        bytes: 1,
                    }],
                    directory.path(),
                    directory.path(),
                )
                .unwrap();
            Package {
                primary: manifest,
                cohosted: Vec::new(),
            }
            .validate()
            .unwrap();
        }
    }
}

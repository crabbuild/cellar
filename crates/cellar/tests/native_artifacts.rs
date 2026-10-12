// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Native publication, install, manifest refusal and dependency boundaries.
#![allow(clippy::disallowed_methods)]

use anyhow::Result;
use cellar::bucket::Bucket;
use cellar::generation::{DeploymentGraph, Generation, GenerationOptions};
use cellar::native_deploy::{
    current_target, executable_identity, verify_native_dependencies, ApplicationConfig, Package,
};
use cellar::protocol::{validate_execution_manifest, FEATURE_NATIVE_V1};
use serde_json::json;
use std::collections::BTreeMap;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_artifact_publication_install_and_source_refusal() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    let directory = tempfile::tempdir()?;
    let executable = directory.path().join("application");
    std::fs::write(&executable, b"native executable fixture\0payload")?;
    let artifact = executable_identity(&executable, current_target())?;
    let config: ApplicationConfig = serde_json::from_value(json!({
        "script_name":"accounts", "application_version":"code-v1", "cells":["Account"],
        "bindings":[{"type":"durable_object_namespace","name":"ACCOUNTS","class_name":"Account"}],
    }))?;
    let primary = config.manifest(vec![artifact.clone()])?;
    assert!(primary.main_module.is_none());
    assert!(primary.modules.is_empty());
    let mut component = config.clone();
    component.script_name = "audit".into();
    component.cells = vec!["Audit".into()];
    component.bindings.clear();
    let package = Package {
        primary,
        cohosted: vec![component.manifest(vec![artifact.clone()])?],
    };
    let bucket = Bucket::open_dev(&directory.path().join("bucket.sqlite"))?;
    let paths = BTreeMap::from([(artifact.sha256.clone(), executable.clone())]);
    let previous = Bucket::open_dev(&directory.path().join("previous.sqlite"))?;
    previous
        .put("cells/Account:old/owner.json", b"{}".to_vec())
        .await?;
    assert!(package.publish(&previous, &paths).await.is_err());
    assert!(previous.get("deploy/current.json").await?.is_none());
    assert!(previous.get("wake/format.json").await?.is_none());
    package.publish(&bucket, &paths).await?;
    let marker = bucket.get("wake/format.json").await?.unwrap().0;
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&marker)?["system"],
        "cellar"
    );
    assert_eq!(
        cellar::fleet::read_current_pointer(&bucket)
            .await?
            .script_name
            .as_deref(),
        Some("accounts")
    );
    assert_eq!(
        cellar::fleet::load_named_worker(&bucket, "audit", "node".into())
            .await?
            .script_name,
        "audit"
    );
    let graph = DeploymentGraph::load(&bucket, "node".into()).await?;
    assert!(graph.primary.native.is_some());
    assert!(graph.primary.options.src.is_empty());
    let error = Generation::build_native(
        1,
        graph,
        GenerationOptions {
            node: "node".into(),
            region: "test".into(),
        },
        Default::default(),
    )
    .err()
    .unwrap();
    assert!(error
        .to_string()
        .contains("no native application registered for script"));
    let installed = directory.path().join("installed");
    package
        .install(&bucket, current_target(), &installed)
        .await?;
    assert_eq!(std::fs::read(&installed)?, std::fs::read(&executable)?);
    assert!(
        package
            .install(&bucket, current_target(), &installed)
            .await
            .is_err(),
        "install must not overwrite an executable"
    );
    assert!(package
        .install(
            &bucket,
            "different-target",
            &directory.path().join("wrong-target")
        )
        .await
        .is_err());
    bucket
        .put(&artifact.key()?, b"corrupted artifact".to_vec())
        .await?;
    assert!(package
        .install(
            &bucket,
            current_target(),
            &directory.path().join("corrupted")
        )
        .await
        .is_err());
    assert!(!directory.path().join("corrupted").exists());
    assert!(
        package.publish(&bucket, &paths).await.is_err(),
        "existing blobs must be verified"
    );
    let mut altered = package.primary.clone();
    altered
        .required_features
        .retain(|feature| feature != FEATURE_NATIVE_V1);
    assert!(validate_execution_manifest(&altered).is_err());
    altered = package.primary.clone();
    altered.main_module = Some("main.js".into());
    assert!(validate_execution_manifest(&altered).is_err());
    altered = package.primary.clone();
    altered.native.as_mut().unwrap().host_protocol += 1;
    assert!(validate_execution_manifest(&altered).is_err());
    let mut altered_package = package.clone();
    altered_package.primary.crons.push("* * * * *".into());
    assert!(
        altered_package.validate().is_err(),
        "configuration mutations change deployment identity"
    );
    // Host channels have one process-wide runtime. Exercise both publication
    // fixtures under that runtime instead of installing competing test handles.
    staged_assets_preserve_shared_protocol_and_refuse_incomplete_publication().await?;
    Ok(())
}

#[test]
fn native_dependencies_refuse_runtime_engines_but_retain_dev_oracles() -> Result<()> {
    let graph = |kind: serde_json::Value| {
        json!({
            "packages":[{"id":"app","name":"app"},{"id":"engine","name":"v8"}],
            "resolve":{"nodes":[
                {"id":"app","features":[],"deps":[{"pkg":"engine","dep_kinds":[{"kind":kind}]}]},
                {"id":"engine","features":[],"deps":[]}
            ]}
        })
    };
    assert!(verify_native_dependencies(&graph(serde_json::Value::Null), "app").is_err());
    assert!(verify_native_dependencies(&graph(json!("build")), "app").is_err());
    verify_native_dependencies(&graph(json!("dev")), "app")?;
    Ok(())
}

#[test]
fn native_queue_configuration_installs_its_reserved_class_and_consumer() -> Result<()> {
    let config: ApplicationConfig = serde_json::from_value(json!({
        "script_name":"jobs", "application_version":"v1",
        "bindings":[{"type":"queue","name":"JOBS","queue":"jobs"}],
        "queue_consumers":[{"queue":"jobs","max_batch_size":10,"max_batch_timeout":1,"max_retries":3}]
    }))?;
    let artifact = cellar::protocol::NativeArtifact {
        target: current_target().into(),
        sha256: "a".repeat(64),
        bytes: 10,
    };
    let manifest = config.manifest(vec![artifact])?;
    assert!(manifest
        .required_features
        .iter()
        .any(|f| f == cellar::protocol::FEATURE_QUEUES_V1));
    assert!(manifest
        .do_classes
        .iter()
        .any(|c| c == cellar::deploy::QUEUE_CLASS));
    cellar::protocol::validate_queue_manifest(&manifest)?;
    Ok(())
}

async fn staged_assets_preserve_shared_protocol_and_refuse_incomplete_publication() -> Result<()> {
    use cellar::protocol::AssetIndex;
    use http_body_util::BodyExt;
    let root = tempfile::tempdir()?;
    let assets = root.path().join("public");
    std::fs::create_dir(&assets)?;
    std::fs::write(assets.join("index.html"), b"native asset body")?;
    std::fs::write(assets.join("same.txt"), b"native asset body")?;
    std::fs::write(assets.join("_headers"), "/*\n  X-Native-Asset: retained\n")?;
    std::fs::write(assets.join("_redirects"), "/old / 302\n")?;
    let executable = root.path().join("application");
    std::fs::write(&executable, b"native fixture")?;
    let artifact = executable_identity(&executable, current_target())?;
    let config: ApplicationConfig = serde_json::from_value(json!({
        "script_name":"site", "application_version":"v1", "compatibility_date":"2026-01-01",
        "assets":{"directory":"public", "binding":"ASSETS", "run_worker_first":["/api/*"]}
    }))?;
    assert!(config.manifest(vec![artifact.clone()]).is_err());
    let directory = root.path().join("package");
    let primary = config.stage_manifest(vec![artifact.clone()], root.path(), &directory)?;
    let repeated = config.stage_manifest(vec![artifact.clone()], root.path(), &directory)?;
    assert_eq!(primary.version, repeated.version);
    let reference = primary.assets.as_ref().unwrap();
    assert_eq!(
        reference.file_count, 2,
        "directive files are not response bodies"
    );
    assert_eq!(reference.total_bytes, 2 * b"native asset body".len() as u64);
    let index_path = directory.join("assets").join(&reference.sha256);
    let original_index = std::fs::read(&index_path)?;
    let index: AssetIndex = serde_json::from_slice(&original_index)?;
    assert_eq!(
        index.config.compatibility_date.as_deref(),
        Some("2026-01-01")
    );
    assert_eq!(
        index.entries["/index.html"].sha256,
        index.entries["/same.txt"].sha256
    );
    let blob = directory
        .join("assets")
        .join(&index.entries["/index.html"].sha256);
    assert_eq!(std::fs::read_dir(directory.join("assets"))?.count(), 2);
    let mut component = config.clone();
    component.script_name = "static-component".into();
    let component_assets = root.path().join("component-public");
    std::fs::create_dir(&component_assets)?;
    std::fs::write(component_assets.join("index.html"), b"component body")?;
    component.assets.as_mut().unwrap().directory = "component-public".into();
    let component = component.stage_manifest(vec![artifact.clone()], root.path(), &directory)?;
    let component_index: AssetIndex = serde_json::from_slice(&std::fs::read(
        directory
            .join("assets")
            .join(&component.assets.as_ref().unwrap().sha256),
    )?)?;
    let component_blob = directory
        .join("assets")
        .join(&component_index.entries["/index.html"].sha256);
    let package = Package {
        primary,
        cohosted: vec![component],
    };
    let bucket = Bucket::open_dev(&root.path().join("bucket.sqlite"))?;
    let paths = BTreeMap::from([(artifact.sha256.clone(), executable.clone())]);
    assert!(
        package.publish(&bucket, &paths).await.is_err(),
        "assets cannot be dropped by executable-only publication"
    );
    assert!(
        package.publish_staged(&bucket, &directory).await.is_err(),
        "missing executable must refuse publication"
    );
    std::fs::copy(&executable, directory.join(&artifact.sha256))?;
    std::fs::write(&blob, b"corrupt same len!")?;
    assert!(package.publish_staged(&bucket, &directory).await.is_err());
    assert!(bucket.get("deploy/current.json").await?.is_none());
    assert!(bucket
        .get("deploy/static-component/current.json")
        .await?
        .is_none());
    assert!(
        config
            .stage_manifest(vec![artifact.clone()], root.path(), &directory)
            .is_err(),
        "build must not overwrite corrupted staged content"
    );
    std::fs::write(&blob, b"native asset body")?;
    std::fs::write(&index_path, b"corrupted index")?;
    assert!(package.publish_staged(&bucket, &directory).await.is_err());
    std::fs::write(&index_path, &original_index)?;
    std::fs::write(&component_blob, b"bad component!")?;
    assert!(
        package.publish_staged(&bucket, &directory).await.is_err(),
        "cohosted payloads must also be verified before publication"
    );
    assert!(bucket.get("deploy/current.json").await?.is_none());
    assert!(bucket
        .get("deploy/static-component/current.json")
        .await?
        .is_none());
    std::fs::write(&component_blob, b"component body")?;
    #[cfg(unix)]
    {
        std::fs::remove_file(&blob)?;
        std::os::unix::fs::symlink(assets.join("index.html"), &blob)?;
        assert!(
            package.publish_staged(&bucket, &directory).await.is_err(),
            "staged links are not immutable payloads"
        );
        std::fs::remove_file(&blob)?;
        std::fs::write(&blob, b"native asset body")?;
    }
    package.publish_staged(&bucket, &directory).await?;
    // Read immutable publication objects directly as well: a warm host cache
    // must not hide a missing body or index in the fleet bucket.
    for (digest, expected) in [
        (
            &index.entries["/index.html"].sha256,
            &b"native asset body"[..],
        ),
        (
            &component_index.entries["/index.html"].sha256,
            &b"component body"[..],
        ),
    ] {
        let key = cellar::protocol::asset_blob_key(digest).unwrap();
        assert_eq!(bucket.get(&key).await?.unwrap().0, expected);
    }
    let index_key = format!(
        "deploy/{}/{}/assets.json",
        package.primary.script_name, package.primary.version
    );
    assert_eq!(bucket.get(&index_key).await?.unwrap().0, original_index);
    let graph = DeploymentGraph::load(&bucket, "node".into()).await?;
    let resolver = graph.primary.assets.as_ref().unwrap();
    assert!(
        !resolver.asset_only(),
        "native main_module=None must still dispatch application misses"
    );
    assert_eq!(resolver.binding_name(), Some("ASSETS"));
    assert!(resolver.should_run_worker_first("/api/one"));
    let response = resolver
        .ingress_response("/", None, false, &axum::http::HeaderMap::new())
        .await?
        .unwrap();
    assert_eq!(response.headers()["x-native-asset"], "retained");
    assert_eq!(
        response.into_body().collect().await?.to_bytes(),
        &b"native asset body"[..]
    );
    let response = resolver
        .ingress_response("/old", None, false, &axum::http::HeaderMap::new())
        .await?
        .unwrap();
    assert_eq!(response.status(), 302);
    assert_eq!(response.headers()["location"], "/");
    let component =
        cellar::fleet::load_named_worker(&bucket, "static-component", "node".into()).await?;
    let resolver = component.assets.unwrap();
    assert_eq!(resolver.binding_name(), Some("ASSETS"));
    let response = resolver
        .ingress_response("/", None, false, &axum::http::HeaderMap::new())
        .await?
        .unwrap();
    assert_eq!(
        response.into_body().collect().await?.to_bytes(),
        &b"component body"[..]
    );
    let mut collision = config.clone();
    collision
        .bindings
        .push(json!({"type":"plain_text","name":"ASSETS","text":"bad"}));
    assert!(collision
        .stage_manifest(vec![artifact.clone()], root.path(), &directory)
        .is_err());
    let mut outside = config.clone();
    outside.assets.as_mut().unwrap().directory = "../public".into();
    assert!(outside
        .stage_manifest(vec![artifact.clone()], root.path(), &directory)
        .is_err());
    std::fs::write(assets.join("_headers"), "  orphan: header\n")?;
    assert!(
        config
            .stage_manifest(vec![artifact.clone()], root.path(), &directory)
            .is_err(),
        "invalid directives must fail before publishing a deployment"
    );
    std::fs::write(assets.join("_headers"), "/*\n  X-Native-Asset: retained\n")?;
    std::fs::write(assets.join("index.html"), "new asset body")?;
    let changed = config.stage_manifest(vec![artifact], root.path(), &directory)?;
    assert_ne!(
        package.primary.version, changed.version,
        "asset changes must change the native revision"
    );
    Ok(())
}

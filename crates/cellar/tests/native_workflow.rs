//! Binding preflight through native application turns, without engine startup.

use anyhow::Result;
use cellar::application::workflow::{BatchCreateOptions, CreateOptions, Duration, Retention};
use cellar::application::{Context, Invocation, Output, Registry};
use cellar::engine_api::{
    Compat, HttpResponse, RuntimeOptions, WorkerConfigOptions, WorkflowBinding,
};
use cellar::fleet::LoadedDeployment;
use cellar::generation::{DeploymentGraph, Generation, GenerationOptions};
use cellar::native::clone::{Object, StructuredValue, Value};
use cellar::runtime::RuntimeManager;
use std::sync::Arc;

#[test]
fn workflow_durations_keep_existing_units_fractions_and_whitespace() {
    for (text, expected) in [
        ("1 second", 1000.0),
        ("1.25 minutes", 75000.0),
        ("01 hours", 3600000.0),
        ("1 day", 86400000.0),
        ("2 weeks", 1209600000.0),
        ("1 month", 2592000000.0),
        ("1 year", 31536000000.0),
        ("\u{feff} 0.5\u{a0}seconds\u{2029}", 500.0),
    ] {
        assert_eq!(Duration::from(text).milliseconds().unwrap(), expected);
    }
    assert!(Duration::Milliseconds(-0.0)
        .milliseconds()
        .unwrap()
        .is_sign_negative());
    for text in [
        ".5 seconds",
        "1. seconds",
        "1e3 seconds",
        "-1 seconds",
        "1seconds",
        "1 SECOND",
        "1 seconds extra",
        "1\u{85}second",
    ] {
        assert!(Duration::from(text).milliseconds().is_err(), "{text}");
    }
    for number in [-1.0, f64::NAN, f64::INFINITY] {
        assert!(Duration::Milliseconds(number).milliseconds().is_err());
    }
}

async fn preflight(context: Context, _: Invocation) -> Result<Output> {
    let workflow = context.workflow("WF")?;
    assert_eq!(workflow.name(), "orders");
    assert!(context.workflow("MISSING").is_err());
    for options in [
        CreateOptions {
            id: Some("-id".into()),
            ..Default::default()
        },
        CreateOptions {
            id: Some("x".repeat(101)),
            ..Default::default()
        },
        CreateOptions {
            location_hint: Some("unknown".into()),
            ..Default::default()
        },
        CreateOptions {
            retention: Retention {
                success: Some(Duration::from("31 days")),
                ..Default::default()
            },
            ..Default::default()
        },
        CreateOptions {
            retention: Retention {
                error: Some(Duration::Milliseconds(f64::INFINITY)),
                ..Default::default()
            },
            ..Default::default()
        },
        CreateOptions {
            params: Some(StructuredValue {
                root: Value::Reference(0),
                objects: vec![Object::ArrayBuffer {
                    bytes: vec![0; 1 << 20],
                    max_length: None,
                    immutable: false,
                }],
            }),
            ..Default::default()
        },
    ] {
        assert!(workflow
            .create(&context, options)
            .await
            .unwrap_err()
            .to_string()
            .starts_with("WORKFLOW_ERROR:"));
    }
    assert!(workflow.create_batch(&context, Vec::new()).await.is_err());
    for count in [0, 101, usize::MAX] {
        assert!(workflow
            .create_batch_with(&context, BatchCreateOptions::count(count))
            .await
            .is_err());
    }
    assert!(workflow
        .create_batch_with(
            &context,
            BatchCreateOptions::instances(vec![
                CreateOptions {
                    id: Some("valid-modern".into()),
                    ..Default::default()
                },
                CreateOptions {
                    id: Some("bad/name".into()),
                    ..Default::default()
                },
            ])
        )
        .await
        .is_err());
    assert!(workflow
        .create_batch(&context, vec![CreateOptions::default(); 101])
        .await
        .is_err());
    assert!(workflow
        .create_batch(
            &context,
            vec![
                CreateOptions {
                    id: Some("valid".into()),
                    ..Default::default()
                },
                CreateOptions {
                    id: Some("bad/name".into()),
                    ..Default::default()
                }
            ]
        )
        .await
        .is_err());
    let filtered = workflow
        .create_batch(
            &context,
            vec![CreateOptions {
                id: Some("filtered".into()),
                params: Some(StructuredValue {
                    root: Value::Reference(123),
                    objects: Vec::new(),
                }),
                ..Default::default()
            }],
        )
        .await?;
    assert!(filtered.is_empty());
    assert!(workflow.delete_batch(&context, vec![]).await.is_err());
    assert!(workflow
        .delete_batch(&context, vec!["valid".into(), "".into()])
        .await
        .is_err());
    Ok(Output::Http(HttpResponse {
        status: 200,
        body: b"checked".to_vec(),
        stream: None,
        headers: Vec::new(),
        websocket: None,
        write_position: None,
        observed_position: None,
    }))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_workflow_preflight_refuses_bad_batches_before_any_cell_call() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    let graph = DeploymentGraph::single(LoadedDeployment {
        native: None,
        options: WorkerConfigOptions {
            src: String::new(),
            script_name: "workflow-policy".into(),
            do_classes: Vec::new(),
            bindings: Vec::new(),
            blob_bindings: Vec::new(),
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: vec![WorkflowBinding {
                environment: "WF".into(),
                workflow: "orders".into(),
                class: "Orders".into(),
                settings: Default::default(),
            }],
            vars: Vec::new(),
            node: "test".into(),
            modules: Vec::new(),
            compat: Compat::default(),
        },
        script_name: "workflow-policy".into(),
        version: "test".into(),
        prefix: "test/".into(),
        asset_binding: None,
        loader_bindings: Vec::new(),
        assets: None,
        services: Vec::new(),
        crons: Vec::new(),
        containers: Vec::new(),
        fence_image: None,
        container_images: Vec::new(),
    });
    let generation = Generation::build_native(
        1,
        graph,
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([(
            "workflow-policy".into(),
            Arc::new(
                Registry::new(|context, invocation| Box::pin(preflight(context, invocation)))
                    .workflow("Orders", |_, _, _| {
                        Box::pin(async { Err(anyhow::anyhow!("preflight must not run workflows")) })
                    })?,
            ) as Arc<dyn cellar::application::Application>,
        )]),
    )?;
    let directory = tempfile::tempdir()?;
    let runtime = RuntimeManager::start(
        generation,
        RuntimeOptions {
            data_dir: directory.path().into(),
            replication: None,
            wake: None,
            alarm_observer: Arc::new(|_, _| {}),
            node: "test".into(),
            region: "test".into(),
            bucket: None,
        },
    )?;
    let (tx, mut calls) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_rpc_call_tx(tx);
    let reply = cellar::asyncrt::timeout(
        std::time::Duration::from_secs(10),
        runtime.fetch_worker(
            "http://worker/".into(),
            "GET".into(),
            Vec::new(),
            Vec::new(),
        ),
    )
    .await??;
    assert_eq!(reply.body, b"checked");
    assert!(matches!(
        calls.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    Ok(())
}

//! Cancellation can close admission while an already-entered poll is running.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

fn graph() -> DeploymentGraph {
    DeploymentGraph::single(crate::fleet::LoadedDeployment {
        event_streams: vec![],
        native: None,
        script_name: "cancel-admission".into(),
        version: "test".into(),
        prefix: "test/".into(),
        options: crate::engine_api::WorkerConfigOptions {
            src: String::new(),
            script_name: "cancel-admission".into(),
            do_classes: vec![],
            bindings: vec![],
            blob_bindings: vec![],
            sql_bindings: vec![],
            kv_bindings: vec![],
            queue_bindings: vec![],
            stream_bindings: vec![],
            queue_consumers: vec![],
            workflow_bindings: vec![],
            vars: vec![],
            node: String::new(),
            modules: vec![],
            compat: Default::default(),
        },
        asset_binding: None,
        loader_bindings: vec![],
        assets: None,
        services: vec![],
        crons: vec![],
        containers: vec![],
        fence_image: None,
        container_images: vec![],
    })
}

#[tokio::test]
async fn closure_during_entered_poll_refuses_build_and_metrics_without_panicking(
) -> anyhow::Result<()> {
    let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
    let samples = Arc::new(AtomicUsize::new(0));
    let sample_count = samples.clone();
    domain.services().configure_metrics(Arc::new(move || {
        sample_count.fetch_add(1, Ordering::SeqCst);
        Default::default()
    }))?;
    let factories = Arc::new(AtomicUsize::new(0));
    let factory_count = factories.clone();
    let apps = CompiledApplications::default().register("cancel-admission", "test", move || {
        factory_count.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(crate::application::Registry::new(|_, _| {
            Box::pin(async { Ok(crate::application::Output::Done) })
        })))
    })?;
    let closing = domain.clone();
    let result = domain
        .run(async move {
            // run() admitted this poll. Closure can occur before a later effect
            // in that same poll, so its own admission must remain fallible.
            closing.close();
            let build = std::panic::AssertUnwindSafe(build_generation(
                Backend::Native(Arc::new(apps)),
                1,
                graph(),
                GenerationOptions {
                    node: "test".into(),
                    region: "test".into(),
                },
            ))
            .catch_unwind()
            .await;
            let sample = std::panic::AssertUnwindSafe(sample_load())
                .catch_unwind()
                .await;
            assert!(build.is_ok(), "generation admission panicked after closure");
            assert!(sample.is_ok(), "metrics admission panicked after closure");
            assert!(build
                .unwrap()
                .err()
                .expect("closed generation admission succeeded")
                .downcast_ref::<crate::asyncrt::DomainClosed>()
                .is_some());
            assert!(sample
                .unwrap()
                .unwrap_err()
                .downcast_ref::<crate::asyncrt::DomainClosed>()
                .is_some());
            anyhow::Ok(())
        })
        .await?;
    domain.shutdown().await;
    result?;
    assert_eq!(
        factories.load(Ordering::SeqCst),
        0,
        "closed admission invoked an application factory"
    );
    assert_eq!(
        samples.load(Ordering::SeqCst),
        0,
        "closed admission invoked a metrics sampler"
    );
    Ok(())
}

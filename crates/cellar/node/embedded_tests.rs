// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

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
        cron_revision: None,
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

#[tokio::test]
async fn event_startup_observes_supervisor_exit_before_activation_is_polled() {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    // prepare() begins readiness before the driver polls activation. No Pending
    // guard exists yet to finish the latch if the driver exits at that cut.
    let readiness = crate::application::event_stream::readiness::Readiness::new(false);
    let supervisor = tokio::spawn(async { anyhow::bail!("driver exited before activation") });
    while !supervisor.is_finished() {
        tokio::task::yield_now().await;
    }
    let result = wait_for_event_activation(&readiness, &domain, supervisor).now_or_never();
    domain.shutdown().await;
    let error = result
        .expect("startup waited on readiness after its supervisor had exited")
        .expect_err("startup accepted an exited supervisor");
    assert!(format!("{error:#}").contains("driver exited before activation"));
}

#[tokio::test]
async fn event_startup_rejects_completed_outcomes_even_when_readiness_is_ready() {
    for ready in [false, true] {
        for outcome in [
            NodeOutcome::Cancelled,
            NodeOutcome::Fenced {
                code: 3,
                reason: crate::node_lifecycle::FenceReason::NodeLeaseMissing,
            },
            NodeOutcome::CoreFailed {
                error: "core exited".into(),
            },
        ] {
            let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
            let readiness = crate::application::event_stream::readiness::Readiness::new(ready);
            let expected = format!("{outcome:?}");
            let supervisor = tokio::spawn(async move { Ok(outcome) });
            while !supervisor.is_finished() {
                tokio::task::yield_now().await;
            }
            let result = wait_for_event_activation(&readiness, &domain, supervisor).now_or_never();
            domain.shutdown().await;
            let error = result
                .expect("startup ignored completed supervisor")
                .expect_err("startup admitted a completed supervisor");
            assert!(format!("{error:#}").contains(&expected));
        }
    }
}

#[tokio::test]
async fn event_startup_waits_for_activation_and_preserves_live_supervision() -> anyhow::Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let readiness = crate::application::event_stream::readiness::Readiness::new(false);
    let (stop, stopped) = oneshot::channel();
    let supervisor = tokio::spawn(async move {
        stopped.await?;
        Ok(NodeOutcome::Cancelled)
    });
    let mut startup = Box::pin(wait_for_event_activation(&readiness, &domain, supervisor));
    assert!(startup.as_mut().now_or_never().is_none());
    assert!(!domain.is_closed());
    readiness.finish(Ok(()));
    let supervisor = startup.await?;
    assert!(!supervisor.is_finished());
    assert!(!domain.is_closed());
    stop.send(()).unwrap();
    assert_eq!(supervisor.await??, NodeOutcome::Cancelled);
    domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn event_startup_activation_failure_closes_and_joins_before_returning() {
    let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
    let readiness = crate::application::event_stream::readiness::Readiness::new(false);
    readiness.finish(Err("catalog activation refused".into()));
    let closing = domain.clone();
    let joined = Arc::new(AtomicUsize::new(0));
    let observed = joined.clone();
    let supervisor = tokio::spawn(async move {
        while !closing.is_closed() {
            tokio::task::yield_now().await;
        }
        observed.fetch_add(1, Ordering::SeqCst);
        Ok(NodeOutcome::Cancelled)
    });
    let error = wait_for_event_activation(&readiness, &domain, supervisor)
        .await
        .expect_err("failed activation admitted a node");
    assert!(format!("{error:#}").contains("catalog activation refused"));
    assert!(domain.is_closed());
    assert_eq!(joined.load(Ordering::SeqCst), 1);
    domain.shutdown().await;
}

async fn activation_failure_with_outcome(outcome: anyhow::Result<NodeOutcome>) -> anyhow::Error {
    let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
    let readiness = crate::application::event_stream::readiness::Readiness::new(false);
    readiness.finish(Err("catalog activation refused".into()));
    let closing = domain.clone();
    let joined = Arc::new(AtomicUsize::new(0));
    let observed = joined.clone();
    let supervisor = tokio::spawn(async move {
        // Completion stays pending until the readiness-error path closes the
        // domain. A simultaneous-ready select cannot bypass the measured path.
        while !closing.is_closed() {
            tokio::task::yield_now().await;
        }
        observed.fetch_add(1, Ordering::SeqCst);
        outcome
    });
    let error = wait_for_event_activation(&readiness, &domain, supervisor)
        .await
        .expect_err("failed activation admitted a node");
    assert!(domain.is_closed());
    assert_eq!(joined.load(Ordering::SeqCst), 1);
    domain.shutdown().await;
    error
}

#[tokio::test]
async fn event_startup_activation_failure_preserves_the_supervisor_outcome() {
    for outcome in [
        NodeOutcome::Fenced {
            code: 3,
            reason: crate::node_lifecycle::FenceReason::NodeLeaseExpired,
        },
        NodeOutcome::Fenced {
            code: 3,
            reason: crate::node_lifecycle::FenceReason::NodeLeaseMissing,
        },
        NodeOutcome::Fenced {
            code: 3,
            reason: crate::node_lifecycle::FenceReason::NodeLeaseMismatch,
        },
        NodeOutcome::CoreFailed {
            error: "core failed before activation".into(),
        },
        NodeOutcome::ReplicationFailed {
            error: "replication failed before activation".into(),
        },
        NodeOutcome::Cancelled,
    ] {
        let expected = format!("{outcome:?}");
        let error = activation_failure_with_outcome(Ok(outcome)).await;
        let message = format!("{error:#}");
        assert!(message.contains("catalog activation refused"));
        assert!(
            message.contains(&expected),
            "lost supervisor outcome: {message}"
        );
    }
}

#[tokio::test]
async fn event_startup_activation_failure_preserves_the_supervisor_error_chain() {
    let error =
        activation_failure_with_outcome(Err(anyhow::Error::new(crate::asyncrt::DomainClosed)
            .context("supervisor stopped before activation")))
        .await;
    let message = format!("{error:#}");
    assert!(message.contains("catalog activation refused"));
    assert!(message.contains("supervisor stopped before activation"));
    assert!(error
        .downcast_ref::<crate::asyncrt::DomainClosed>()
        .is_some());
}

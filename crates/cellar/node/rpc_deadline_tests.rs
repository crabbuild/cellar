use super::*;
use crate::application::rpc::EffectOutcome;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn headers(value: &'static str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        BUDGET_HEADER,
        hyper::header::HeaderValue::from_static(value),
    );
    headers
}

#[test]
fn received_durations_use_the_receiver_clock_and_host_cap() -> Result<()> {
    assert_eq!(from_headers(&HeaderMap::new(), 50_000, 500)?, None);
    assert_eq!(from_headers(&headers("100"), 50_000, 500)?, Some(50_100));
    assert_eq!(from_headers(&headers("10000"), 50_000, 500)?, Some(50_500));
    assert_eq!(from_headers(&headers("0"), 50_000, 500)?, Some(50_000));
    assert_eq!(
        from_headers(&headers("18446744073709551615"), u64::MAX - 1, 500)?,
        Some(u64::MAX)
    );
    Ok(())
}

#[test]
fn malformed_and_ambiguous_budget_headers_are_refused() {
    for value in [
        "",
        "-1",
        "+1",
        " 1",
        "1 ",
        "1,2",
        "01",
        "00",
        "1.5",
        "18446744073709551616",
        "999999999999999999999",
    ] {
        assert!(
            from_headers(&headers(value), 10, 500).is_err(),
            "accepted {value:?}"
        );
    }
    let mut duplicate = headers("100");
    duplicate.append(
        BUDGET_HEADER,
        hyper::header::HeaderValue::from_static("200"),
    );
    assert!(from_headers(&duplicate, 10, 500).is_err());
}

#[test]
fn peer_timeout_requires_a_paired_host_marker_and_never_proves_rollback() {
    let mut headers = HeaderMap::new();
    assert!(remote_failure(StatusCode::GATEWAY_TIMEOUT, &headers).is_none());
    headers.insert(
        FAILURE_HEADER,
        hyper::header::HeaderValue::from_static(DEADLINE),
    );
    assert!(remote_failure(StatusCode::OK, &headers).is_none());
    let failure = remote_failure(StatusCode::GATEWAY_TIMEOUT, &headers).unwrap();
    assert_eq!(failure.code(), Code::DeadlineExceeded);
    assert_eq!(failure.outcome(), EffectOutcome::Unknown);
    let failure = anyhow::Error::new(failure);
    assert_eq!(
        super::super::classify_remote_attempt(&failure),
        cellar_logic::routing::Attempt::Ambiguous
    );
    headers.append(
        FAILURE_HEADER,
        hyper::header::HeaderValue::from_static(DEADLINE),
    );
    assert!(remote_failure(StatusCode::GATEWAY_TIMEOUT, &headers).is_none());
}

struct Dropped(Arc<AtomicUsize>);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn assert_expired(error: anyhow::Error) {
    let failure = error.downcast_ref::<Failure>().unwrap();
    assert_eq!(failure.code(), Code::DeadlineExceeded);
    assert_eq!(failure.outcome(), EffectOutcome::Unknown);
}

#[tokio::test]
async fn expired_queued_work_is_dropped_without_being_polled() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let polls = AtomicUsize::new(0);
            let drops = Arc::new(AtomicUsize::new(0));
            let owned = Dropped(drops.clone());
            let operation = async {
                let _owned = owned;
                polls.fetch_add(1, Ordering::SeqCst);
                Ok(7)
            };
            assert_expired(receive(Some(0), operation).await.unwrap_err());
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn independent_timer_drops_a_pending_route_or_proof_wait() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let drops = Arc::new(AtomicUsize::new(0));
            let owned = Dropped(drops.clone());
            let operation = async {
                let _owned = owned;
                std::future::pending::<Result<()>>().await
            };
            let deadline = crate::asyncrt::mono_ms() + 20;
            assert_expired(receive(Some(deadline), operation).await.unwrap_err());
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn synchronous_overrun_cannot_release_a_ready_result() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let deadline = crate::asyncrt::mono_ms() + 20;
            let operation = async {
                std::thread::sleep(Duration::from_millis(50));
                Ok(7)
            };
            assert_expired(receive(Some(deadline), operation).await.unwrap_err());
            assert_eq!(receive(None, async { Ok(9) }).await?, 9);
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

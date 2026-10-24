// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Reply-task admission may close during an already-entered alarm turn.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn closed_poll_refuses_alarm_gate_reply_without_panic_or_output() -> Result<()> {
    let domain = Arc::new(asyncrt::ExecutionDomain::new(
        tokio::runtime::Handle::current(),
    ));
    let closing = domain.clone();
    let result = domain
        .run(async move {
            let (reply, receiver) = tokio::sync::oneshot::channel();
            let (gate, waiting) = tokio::sync::oneshot::channel();
            closing.close();
            let completion = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                Answer::Alarm(reply).fail_with_arm_gates(anyhow!("alarm cancelled"), vec![waiting])
            }));
            assert!(
                completion.is_ok(),
                "alarm completion panicked after task admission closed"
            );
            assert!(
                completion
                    .unwrap()
                    .expect("gated reply must remain observable")
                    .await
                    .is_err(),
                "closed admission reported gate completion"
            );
            assert!(
                receiver.await.is_err(),
                "an unproven alarm error became visible"
            );
            assert!(
                gate.is_closed(),
                "refused reply retained its discovery gate"
            );
            Ok::<(), anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn closed_poll_discards_success_value_and_never_bypasses_gate() -> Result<()> {
    struct Value(Arc<AtomicUsize>);

    impl Drop for Value {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let domain = Arc::new(asyncrt::ExecutionDomain::new(
        tokio::runtime::Handle::current(),
    ));
    let closing = domain.clone();
    let dropped = Arc::new(AtomicUsize::new(0));
    let counted = dropped.clone();
    let result = domain
        .run(async move {
            let (reply, receiver) = tokio::sync::oneshot::channel();
            let (gate, waiting) = tokio::sync::oneshot::channel();
            closing.close();
            let completion = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                send_answer_after_arm_gates(reply, Ok(Value(counted)), vec![waiting])
            }));
            assert!(
                completion.is_ok(),
                "successful reply panicked after task admission closed"
            );
            assert!(completion
                .unwrap()
                .expect("gated reply must remain observable")
                .await
                .is_err());
            assert!(
                receiver.await.is_err(),
                "closed admission published unproven success"
            );
            assert!(gate.is_closed());
            Ok::<(), anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result?;
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn admitted_reply_still_waits_for_discovery_and_reports_completion() -> Result<()> {
    let domain = Arc::new(asyncrt::ExecutionDomain::new(
        tokio::runtime::Handle::current(),
    ));
    let result = domain
        .run(async {
            let (reply, mut receiver) = tokio::sync::oneshot::channel();
            let (gate, waiting) = tokio::sync::oneshot::channel();
            let completion = send_answer_after_arm_gates(reply, Ok(7), vec![waiting]).unwrap();
            assert!(matches!(
                receiver.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));
            gate.send(Ok(())).unwrap();
            assert!(matches!(
                completion.await?,
                GatedReplyCompletion::Sent { failure: None }
            ));
            assert_eq!(receiver.await??, 7);
            Ok::<(), anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

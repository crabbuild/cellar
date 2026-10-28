// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::asyncrt::ExecutionDomain;
use std::time::Duration;

#[tokio::test]
async fn stalled_dial_reports_unreachable_before_the_call_expires() -> anyhow::Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let token = crate::asyncrt::services()
                .domain_token()
                .ok_or(crate::asyncrt::DomainClosed)?;
            let deadline = token.mono_ms()? + 400;
            let stage = AtomicU8::new(0);
            let result = crate::asyncrt::timeout(
                Duration::from_millis(400),
                dial(
                    &token,
                    deadline,
                    &stage,
                    std::future::pending::<std::io::Result<()>>(),
                ),
            )
            .await;
            anyhow::ensure!(
                matches!(result, Ok(Err(TransportError::Timeout))),
                "connect timeout did not reach the live caller: {result:?}"
            );
            anyhow::ensure!(token.mono_ms()? < deadline, "outer call already expired");
            assert_eq!(
                classify(TransportError::Timeout, stage.load(Ordering::Acquire)),
                InspectionError::Unreachable
            );
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result?
}

#[tokio::test]
async fn only_an_attempted_dial_can_report_unreachable() -> anyhow::Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let token = crate::asyncrt::services()
                .domain_token()
                .ok_or(crate::asyncrt::DomainClosed)?;
            let stage = AtomicU8::new(0);
            let polled = std::sync::atomic::AtomicBool::new(false);
            let expired = dial(&token, 0, &stage, async {
                polled.store(true, Ordering::Release);
                Ok(())
            })
            .await;
            assert!(!polled.load(Ordering::Acquire));
            assert_eq!(
                classify(expired.unwrap_err(), stage.load(Ordering::Acquire)),
                InspectionError::Unavailable
            );

            dial(&token, token.mono_ms()? + 400, &stage, async { Ok(()) }).await?;
            // A TCP connection followed by a stalled TLS/metadata exchange
            // must not become failed-node evidence.
            assert_eq!(
                classify(TransportError::Timeout, stage.load(Ordering::Acquire)),
                InspectionError::Unavailable
            );

            let stage = AtomicU8::new(0);
            let refused = dial(&token, token.mono_ms()? + 400, &stage, async {
                Err::<(), _>(std::io::Error::from(std::io::ErrorKind::ConnectionRefused))
            })
            .await;
            assert_eq!(
                classify(refused.unwrap_err(), stage.load(Ordering::Acquire)),
                InspectionError::Unreachable
            );
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result?
}

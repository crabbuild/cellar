// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A ready completion cannot escape the seed operation's original deadline.
use super::super::shared_fixture::Fixture;
use super::super::*;

#[tokio::test]
#[allow(clippy::disallowed_methods)] // Intentionally model an uncooperative poll.
async fn seed_deadline_refuses_success_and_io_error_after_poll_overrun() -> anyhow::Result<()> {
    let fixture = Fixture::start(TransportOptions::default(), vec![]).await?;
    let result = fixture
        .domain
        .run(async {
            let state = &fixture.transport.handle.0;
            let mut results = Vec::new();
            for io in [false, true] {
                for fail in [false, true] {
                    let deadline = state.domain.mono_ms()? + 100;
                    let mut entered = None;
                    let work = async {
                        entered = Some(state.domain.mono_ms().unwrap());
                        // The timer is still pending at entry. A synchronous
                        // poll crosses expiry, then returns a ready outcome.
                        // This cannot be reproduced by pre-expiring a timer.
                        std::thread::sleep(std::time::Duration::from_millis(200));
                        if fail {
                            Err(TransportError::Io("late socket outcome".into()))
                        } else {
                            Ok(())
                        }
                    };
                    let outcome = if io {
                        state.seed_io(deadline, work).await
                    } else {
                        state.seed_until(deadline, work).await
                    };
                    anyhow::ensure!(
                        entered.is_some_and(|at| at < deadline),
                        "deadline fixture did not enter before expiry"
                    );
                    results.push((io, fail, outcome));
                }
            }
            Ok::<_, anyhow::Error>(results)
        })
        .await;
    let closed = fixture.close().await;
    let results = result??;
    closed?;
    anyhow::ensure!(
        results
            .iter()
            .all(|(_, _, outcome)| matches!(outcome, Err(TransportError::Timeout))),
        "expired seed completions escaped their original deadline: {results:?}"
    );
    Ok(())
}

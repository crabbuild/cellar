// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Stream intake is part of the public KV operation's original deadline.
use super::*;
use futures_util::{stream, StreamExt};

struct Source(Arc<std::sync::atomic::AtomicBool>);

impl Drop for Source {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

pub(super) async fn deadline(context: &Context) -> Result<()> {
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let source = Source(dropped.clone());
    let input = stream::once(async { Ok(vec![7; 4096]) })
        .chain(stream::pending::<std::result::Result<Vec<u8>, String>>())
        .map(move |chunk| {
            let _source = &source;
            chunk
        });
    let namespace = context.kv("KV")?;
    // The fixture's operation budget is three seconds. This outer observer
    // only prevents a broken implementation from hanging the qualification.
    let outcome = cellar::asyncrt::timeout(
        Duration::from_millis(3500),
        namespace.put_stream(
            context,
            "stream-deadline",
            Box::pin(input),
            Default::default(),
        ),
    )
    .await;
    ensure!(
        dropped.load(Ordering::SeqCst),
        "expired stream source remained owned"
    );
    let error = outcome
        .context("KV stream intake exceeded its original three-second budget")?
        .err()
        .context("unending KV stream unexpectedly committed")?;
    ensure!(
        matches!(
            error.downcast_ref::<RuntimeError>(),
            Some(RuntimeError::DeadlineExceeded)
        ),
        "stream deadline discarded its typed outcome: {error:#}"
    );
    ensure!(
        namespace
            .get(context, "stream-deadline", ReadType::Bytes)
            .await?
            .is_none(),
        "expired stream published a value"
    );
    Ok(())
}

pub(super) async fn publication_deadline(context: &Context) -> Result<()> {
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let source = Source(dropped.clone());
    let input = stream::once(async {
        cellar::asyncrt::sleep(Duration::from_secs(1)).await;
        Ok(vec![9; cellar_logic::kv::MAX_INLINE_VALUE_BYTES + 1])
    })
    .map(move |chunk| {
        let _source = &source;
        chunk
    });
    let namespace = context.kv("KV")?;
    // The fixture fragments read-back into 512-byte chunks with a 1ms delay.
    // Input consumes one second; publication must inherit the remaining two.
    let outcome = cellar::asyncrt::timeout(
        Duration::from_millis(3500),
        namespace.put_stream(
            context,
            "stream-publication-deadline",
            Box::pin(input),
            Default::default(),
        ),
    )
    .await;
    ensure!(
        dropped.load(Ordering::SeqCst),
        "completed stream retained its source"
    );
    let error = outcome
        .context("KV publication restarted the stream operation budget")?
        .err()
        .context("delayed read-back unexpectedly completed")?;
    if let Some(failure) = error.downcast_ref::<cellar::sdk::CellBindingCallError>() {
        ensure!(
            failure.target.is_some() && failure.ambiguous,
            "submitted streaming failure lost its target or ambiguity: {failure:?}"
        );
    } else {
        use cellar::sdk::consensus::verification::cell_groups::CellGroupPayloadError;
        ensure!(
            matches!(
                error.downcast_ref::<RuntimeError>(),
                Some(RuntimeError::DeadlineExceeded)
            ) || matches!(
                error.downcast_ref::<CellGroupPayloadError>(),
                Some(CellGroupPayloadError::DeadlineExceeded)
            ),
            "stream publication discarded its typed deadline: {error:#}"
        );
    }
    ensure!(
        namespace
            .get(context, "stream-publication-deadline", ReadType::Bytes)
            .await?
            .is_none(),
        "incompletely verified streaming value was installed"
    );
    Ok(())
}

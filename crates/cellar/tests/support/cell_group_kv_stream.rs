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
        namespace.put_stream(context, "stream-deadline", Box::pin(input), Default::default()),
    )
    .await;
    ensure!(dropped.load(Ordering::SeqCst), "expired stream source remained owned");
    let error = outcome.context("KV stream intake exceeded its original three-second budget")?
        .err().context("unending KV stream unexpectedly committed")?;
    ensure!(
        matches!(error.downcast_ref::<RuntimeError>(), Some(RuntimeError::DeadlineExceeded)),
        "stream deadline discarded its typed outcome: {error:#}"
    );
    ensure!(namespace.get(context, "stream-deadline", ReadType::Bytes).await?.is_none(),
        "expired stream published a value");
    Ok(())
}

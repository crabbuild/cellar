// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Dense bounded accumulation; fragmented chunks never accumulate list nodes.
use bytes::Bytes;
use http_body_util::BodyExt;

pub(super) enum Error {
    TooLarge,
    Exhausted,
    Invalid,
}

pub(super) async fn receive<B>(
    body: B,
    limit: usize,
    reservation: &crate::inbound_rpc::Request,
) -> Result<Bytes, Error>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
{
    if body.size_hint().lower() > limit as u64 {
        return Err(Error::TooLarge);
    }
    let mut buffer = Vec::new();
    if let Some(exact) = body.size_hint().exact() {
        let capacity = usize::try_from(exact).map_err(|_| Error::TooLarge)?;
        grow(&mut buffer, capacity, reservation)?;
    }
    let mut body = Box::pin(body);
    let mut frames = 0_usize;
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| Error::Invalid)?;
        if let Ok(data) = frame.into_data() {
            let needed = buffer
                .len()
                .checked_add(data.len())
                .filter(|size| *size <= limit)
                .ok_or(Error::TooLarge)?;
            if needed > buffer.capacity() {
                let capacity = needed
                    .max(buffer.capacity().saturating_mul(2))
                    .max(4_096)
                    .min(limit);
                grow(&mut buffer, capacity, reservation)?;
            }
            buffer.extend_from_slice(&data);
        }
        frames += 1;
        if frames == 64 {
            // A peer continuously providing tiny/empty chunks cannot monopolize
            // this poll and prevent its independent timer or replies progressing.
            tokio::task::yield_now().await;
            frames = 0;
        }
    }
    Ok(Bytes::from(buffer))
}

fn grow(
    buffer: &mut Vec<u8>,
    capacity: usize,
    reservation: &crate::inbound_rpc::Request,
) -> Result<(), Error> {
    reservation.buffer(capacity).ok_or(Error::Exhausted)?;
    buffer
        .try_reserve_exact(capacity - buffer.len())
        .map_err(|_| Error::Exhausted)?;
    // Account for visible spare capacity too if the allocator supplied more.
    reservation
        .buffer(buffer.capacity())
        .ok_or(Error::Exhausted)
}

#[cfg(test)]
#[path = "rpc_upload_tests.rs"]
mod tests;

// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exact physical image publication observations, independent of a dispatcher.
use super::*;
use crate::raft::cache::CachedGroup;

fn valid_published_position(cache: &CachedGroup, image: &ImageView) -> bool {
    let position = image.position;
    if position.index > cache.applied.position.index {
        return false;
    }
    match cache.term(position.index.get()) {
        Some(term) => term == position.term.get(),
        // Protocol-only progress can checkpoint beyond the last SQL image.
        // The exact durable marker proves that this already verified image
        // still represents applied state, even after its log entry is gone.
        // A retained conflicting term must never use this fallback.
        None => {
            position.index.get() < cache.snapshot.get_metadata().index
                && cache.coherent_image_position(&image.image, position)
                    == Some(cache.applied.position)
        }
    }
}

#[cfg(test)]
#[path = "host_inspection_tests.rs"]
mod tests;

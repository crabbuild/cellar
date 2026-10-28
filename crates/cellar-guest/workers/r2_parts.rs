// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Temporary part identities never cross an invocation or native authority.

use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{blob, Context};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
};

#[derive(Default)]
pub(in crate::workers) struct Parts {
    next: AtomicU64,
    entries: Mutex<HashMap<String, Arc<Entry>>>,
}

struct Entry {
    part: tokio::sync::Mutex<Option<blob::ResumablePart>>,
    cancelled: AtomicBool,
}

struct Admission<'a> {
    owner: &'a Parts,
    id: String,
    entry: Arc<Entry>,
    accepted: bool,
}

impl Drop for Admission<'_> {
    fn drop(&mut self) {
        if !self.accepted {
            self.entry.cancelled.store(true, Ordering::Release);
            self.owner.entries.lock().unwrap().remove(&self.id);
        }
    }
}

impl Parts {
    pub async fn begin(
        &self,
        context: &Context,
        upload: blob::ResumableMultipart,
        number: u32,
        length: u64,
        maximum: usize,
    ) -> Result<String> {
        let entry = Arc::new(Entry {
            part: Default::default(),
            cancelled: AtomicBool::new(false),
        });
        let id = {
            let mut entries = self.entries.lock().unwrap();
            ensure!(entries.len() < maximum, "too many staged R2 parts");
            let id = self.next.fetch_add(1, Ordering::Relaxed).to_string();
            entries.insert(id.clone(), entry.clone());
            id
        };
        let mut admission = Admission {
            owner: self,
            id: id.clone(),
            entry: entry.clone(),
            accepted: false,
        };
        let part = upload.begin_part(context, number, length).await?;
        let mut slot = entry.part.lock().await;
        ensure!(
            !entry.cancelled.load(Ordering::Acquire),
            "part stage cancelled"
        );
        *slot = Some(part);
        admission.accepted = true;
        Ok(id)
    }

    pub async fn write(&self, context: &Context, id: &str, bytes: bytes::Bytes) -> Result<()> {
        let entry = self
            .entries
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .context("unknown R2 part stage")?;
        let mut slot = entry.part.lock().await;
        ensure!(
            !entry.cancelled.load(Ordering::Acquire),
            "part stage cancelled"
        );
        slot.as_mut()
            .context("R2 part stage is not writable")?
            .write(context, bytes)
            .await?;
        ensure!(
            !entry.cancelled.load(Ordering::Acquire),
            "part stage cancelled"
        );
        Ok(())
    }

    pub async fn complete(&self, context: &Context, id: &str) -> Result<blob::UploadedPart> {
        let entry = self
            .entries
            .lock()
            .unwrap()
            .remove(id)
            .context("unknown R2 part stage")?;
        let part = entry
            .part
            .lock()
            .await
            .take()
            .context("R2 part stage is not writable")?;
        ensure!(
            !entry.cancelled.load(Ordering::Acquire),
            "part stage cancelled"
        );
        part.complete(context).await
    }

    pub async fn cancel(&self, id: &str) {
        let entry = self.entries.lock().unwrap().remove(id);
        if let Some(entry) = entry {
            entry.cancelled.store(true, Ordering::Release);
            entry.part.lock().await.take();
        }
    }
}

// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Hold an entered synchronous SQL callback until the test releases it.
use anyhow::Result;
use std::sync::Arc;

pub struct Gate {
    open: std::sync::Mutex<bool>,
    changed: std::sync::Condvar,
    entered: tokio::sync::mpsc::UnboundedSender<()>,
}

impl Gate {
    pub fn new() -> (Arc<Self>, tokio::sync::mpsc::UnboundedReceiver<()>) {
        let (entered, receive) = tokio::sync::mpsc::unbounded_channel();
        (
            Arc::new(Self {
                open: std::sync::Mutex::new(false),
                changed: std::sync::Condvar::new(),
                entered,
            }),
            receive,
        )
    }

    pub fn wait(&self) -> Result<()> {
        self.entered.send(())?;
        let mut open = self.open.lock().unwrap_or_else(|e| e.into_inner());
        while !*open {
            open = self.changed.wait(open).unwrap_or_else(|e| e.into_inner());
        }
        Ok(())
    }

    pub fn release(&self) {
        *self.open.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.changed.notify_all();
    }
}

pub struct Release(pub Arc<Gate>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

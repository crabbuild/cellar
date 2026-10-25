// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Scalar accounting owned by open image stores. Observers retain no directory,
//! file, pin or store. Failed accounting stays unavailable instead of becoming zero.
use crate::generation::{GenerationLimits, GenerationUsage};
use std::sync::{Arc, Mutex};

/// Reservations in observed, currently open image directories. This is not a
/// disk inventory: closed directories can still contain durable image files.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GenerationResources {
    pub stores: usize,
    pub generations: usize,
    pub bytes: u64,
    pub retained: usize,
    pub busy: usize,
    pub max_generations: usize,
    pub max_bytes: u64,
}

impl GenerationResources {
    fn replace(self, old: Self, new: Self) -> Option<Self> {
        Some(Self {
            stores: self
                .stores
                .checked_sub(old.stores)?
                .checked_add(new.stores)?,
            generations: self
                .generations
                .checked_sub(old.generations)?
                .checked_add(new.generations)?,
            bytes: self.bytes.checked_sub(old.bytes)?.checked_add(new.bytes)?,
            retained: self
                .retained
                .checked_sub(old.retained)?
                .checked_add(new.retained)?,
            busy: self.busy.checked_sub(old.busy)?.checked_add(new.busy)?,
            max_generations: self
                .max_generations
                .checked_sub(old.max_generations)?
                .checked_add(new.max_generations)?,
            max_bytes: self
                .max_bytes
                .checked_sub(old.max_bytes)?
                .checked_add(new.max_bytes)?,
        })
    }
}

/// Constant-size passive observer, shared by any number of image stores.
#[derive(Clone)]
pub struct GenerationAccounting(Arc<Mutex<Option<GenerationResources>>>);

impl Default for GenerationAccounting {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(Some(GenerationResources::default()))))
    }
}

impl GenerationAccounting {
    /// No I/O, directory locks or enumeration. Quarantine, poison or arithmetic
    /// failure makes the source unavailable for the observer's remaining lifetime.
    pub fn snapshot(&self) -> Option<GenerationResources> {
        *self.0.lock().ok()?
    }

    fn replace(&self, old: GenerationResources, new: GenerationResources) {
        if let Ok(mut total) = self.0.lock() {
            *total = total.and_then(|total| total.replace(old, new));
        }
    }

    pub(crate) fn register(
        &self,
        limits: GenerationLimits,
        usage: GenerationUsage,
    ) -> Registration {
        let current = GenerationResources {
            stores: 1,
            generations: usage.generations,
            bytes: usage.bytes,
            retained: usage.retained,
            busy: usage.busy,
            max_generations: limits.max_generations,
            max_bytes: limits.max_bytes,
        };
        self.replace(GenerationResources::default(), current);
        Registration {
            accounting: self.clone(),
            current,
        }
    }
}

pub(crate) struct Registration {
    accounting: GenerationAccounting,
    current: GenerationResources,
}

impl Registration {
    pub(crate) fn update(&mut self, usage: GenerationUsage) {
        let next = GenerationResources {
            generations: usage.generations,
            bytes: usage.bytes,
            retained: usage.retained,
            busy: usage.busy,
            ..self.current
        };
        self.accounting.replace(self.current, next);
        self.current = next;
    }

    pub(crate) fn fail(&self) {
        if let Ok(mut total) = self.accounting.0.lock() {
            *total = None;
        }
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.accounting
            .replace(self.current, GenerationResources::default());
    }
}

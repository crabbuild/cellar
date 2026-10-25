// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared admission for anonymous snapshot files. Reserve a complete operation
//! before creating files; charging growth incrementally can deadlock when all
//! partially written encoders consume the remaining capacity.

use super::Error;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug)]
pub struct ScratchLimits {
    pub max_bytes: u64,
    pub max_files: usize,
}

impl Default for ScratchLimits {
    fn default() -> Self {
        Self {
            max_bytes: 2 * 1024 * 1024 * 1024,
            max_files: 64,
        }
    }
}

impl ScratchLimits {
    pub fn validate(self) -> Result<(), Error> {
        if !(1..=256 * 1024 * 1024 * 1024).contains(&self.max_bytes)
            || !(1..=1024).contains(&self.max_files)
        {
            return Err(Error::Limit);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScratchUsage {
    pub reserved_bytes: u64,
    pub reserved_files: usize,
}

#[derive(Debug)]
struct Inner {
    limits: ScratchLimits,
    usage: Mutex<ScratchUsage>,
}

/// Clone this owner across all snapshot producers and receivers sharing a node
/// volume. Memory, immutable generations, WAL and logs have separate admission.
/// Reservations follow physical file owners through cancellation and cloning.
#[derive(Clone, Debug)]
pub struct ScratchPool(Arc<Inner>);

impl Default for ScratchPool {
    fn default() -> Self {
        Self(Arc::new(Inner {
            limits: ScratchLimits::default(),
            usage: Mutex::default(),
        }))
    }
}

impl ScratchPool {
    pub fn new(limits: ScratchLimits) -> Result<Self, Error> {
        limits.validate()?;
        Ok(Self(Arc::new(Inner {
            limits,
            usage: Mutex::new(ScratchUsage::default()),
        })))
    }

    pub fn usage(&self) -> Result<ScratchUsage, Error> {
        self.0
            .usage
            .lock()
            .map(|usage| *usage)
            .map_err(|_| Error::Invalid("poisoned snapshot scratch admission"))
    }

    pub(super) fn acquire(&self, bytes: u64, files: usize) -> Result<Reservation, Error> {
        if bytes == 0 || files == 0 {
            return Err(Error::Limit);
        }
        let mut usage = self
            .0
            .usage
            .lock()
            .map_err(|_| Error::Invalid("poisoned snapshot scratch admission"))?;
        let reserved_bytes = usage
            .reserved_bytes
            .checked_add(bytes)
            .filter(|&total| total <= self.0.limits.max_bytes)
            .ok_or(Error::ScratchFull)?;
        let reserved_files = usage
            .reserved_files
            .checked_add(files)
            .filter(|&total| total <= self.0.limits.max_files)
            .ok_or(Error::ScratchFull)?;
        *usage = ScratchUsage {
            reserved_bytes,
            reserved_files,
        };
        Ok(Reservation {
            pool: self.clone(),
            bytes,
            files,
        })
    }
}

pub(super) struct Reservation {
    pool: ScratchPool,
    bytes: u64,
    files: usize,
}

impl Reservation {
    /// Only refund bytes/handles after the index file has physically closed.
    pub(super) fn shrink(&mut self, bytes: u64, files: usize) -> Result<(), Error> {
        if bytes > self.bytes || files > self.files {
            return Err(Error::Invalid("scratch reservation cannot grow"));
        }
        let mut usage = self
            .pool
            .0
            .usage
            .lock()
            .map_err(|_| Error::Invalid("poisoned snapshot scratch admission"))?;
        usage.reserved_bytes -= self.bytes - bytes;
        usage.reserved_files -= self.files - files;
        self.bytes = bytes;
        self.files = files;
        Ok(())
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let mut usage = self.pool.0.usage.lock().unwrap_or_else(|p| p.into_inner());
        usage.reserved_bytes -= self.bytes;
        usage.reserved_files -= self.files;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_is_atomic_bounded_and_released_by_its_owner() {
        let pool = ScratchPool::new(ScratchLimits {
            max_bytes: 100,
            max_files: 2,
        })
        .unwrap();
        let mut first = pool.acquire(80, 2).unwrap();
        assert!(matches!(pool.acquire(1, 1), Err(Error::ScratchFull)));
        assert_eq!(pool.usage().unwrap().reserved_bytes, 80);
        first.shrink(60, 1).unwrap();
        assert!(matches!(pool.acquire(41, 1), Err(Error::ScratchFull)));
        let second = pool.clone().acquire(40, 1).unwrap();
        assert_eq!(pool.usage().unwrap().reserved_bytes, 100);
        drop(first);
        assert_eq!(pool.usage().unwrap().reserved_bytes, 40);
        drop(second);
        assert_eq!(pool.usage().unwrap(), ScratchUsage::default());
    }
}

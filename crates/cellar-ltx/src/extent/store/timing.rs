// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Request-local exclusive wall times. This ledger is telemetry only: no
//! admission or persistence decision may depend on its ambient clock values.

use std::cell::RefCell;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum Phase {
    Sql,
    InventoryWait,
    LtxDecode,
    ExtentRead,
    ExtentHash,
    ExtentWrite,
    FileSync,
    DirectorySync,
    BarrierWait,
    Other,
}

pub const PHASES: [Phase; 10] = [
    Phase::Sql,
    Phase::InventoryWait,
    Phase::LtxDecode,
    Phase::ExtentRead,
    Phase::ExtentHash,
    Phase::ExtentWrite,
    Phase::FileSync,
    Phase::DirectorySync,
    Phase::BarrierWait,
    Phase::Other,
];

#[derive(Clone, Copy, Debug, Default)]
pub struct Sample {
    pub calls: u64,
    pub micros: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, Default)]
pub struct Timings {
    pub phases: [Sample; PHASES.len()],
    pub unavailable: bool,
}

struct Context {
    timings: Timings,
    active: Option<Phase>,
    since: u64,
    depth: usize,
    generation: u64,
}

thread_local! {
    static CURRENT: RefCell<Option<Context>> = const { RefCell::new(None) };
    static GENERATION: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn add(slot: &mut u64, n: u64, unavailable: &mut bool) {
    *slot = slot.checked_add(n).unwrap_or_else(|| {
        *unavailable = true;
        u64::MAX
    });
}

impl Context {
    fn elapsed(&mut self, now: u64) {
        if let Some(phase) = self.active {
            match now.checked_sub(self.since) {
                Some(us) => add(
                    &mut self.timings.phases[phase as usize].micros,
                    us,
                    &mut self.timings.unavailable,
                ),
                None => self.timings.unavailable = true,
            }
        }
        self.since = now;
    }
}

struct Scope(Option<Context>);

impl Drop for Scope {
    fn drop(&mut self) {
        CURRENT.with(|cell| {
            let mut parent = self.0.take();
            if let Some(parent) = parent.as_mut() {
                parent.since = crate::host::telemetry_us();
            }
            cell.replace(parent);
        });
    }
}

/// Collect on the calling blocking worker. Nested scopes and panics restore
/// their parent's ledger; no observer or clock affects storage behavior.
pub fn measure<T>(work: impl FnOnce() -> T) -> (T, Timings) {
    let generation = GENERATION.with(|n| {
        let next = n.get().wrapping_add(1);
        n.set(next);
        next
    });
    let scope = Scope(CURRENT.with(|cell| {
        let now = crate::host::telemetry_us();
        let mut current = cell.borrow_mut();
        if let Some(parent) = current.as_mut() {
            parent.elapsed(now);
        }
        let mut timings = Timings::default();
        timings.phases[Phase::Other as usize].calls = 1;
        current.replace(Context {
            timings,
            active: Some(Phase::Other),
            since: now,
            depth: 0,
            generation,
        })
    }));
    let result = work();
    let timings = CURRENT.with(|cell| {
        cell.borrow_mut()
            .take()
            .map(|mut c| {
                c.elapsed(crate::host::telemetry_us());
                if c.depth != 0 {
                    c.timings.unavailable = true;
                }
                c.timings
            })
            .unwrap_or_default()
    });
    drop(scope);
    (result, timings)
}

pub struct Span {
    generation: u64,
    parent: Option<Phase>,
    depth: usize,
}

/// Nested phases pause their parent's clock, avoiding double-counting SQL,
/// extent reads/hashes and syncs. Keep spans lexically nested inside `measure`.
pub fn span(phase: Phase) -> Span {
    CURRENT.with(|cell| {
        let mut current = cell.borrow_mut();
        let Some(context) = current.as_mut() else {
            return Span {
                generation: 0,
                parent: None,
                depth: 0,
            };
        };
        context.elapsed(crate::host::telemetry_us());
        let parent = context.active.replace(phase);
        context.depth += 1;
        if parent != Some(phase) {
            add(
                &mut context.timings.phases[phase as usize].calls,
                1,
                &mut context.timings.unavailable,
            );
        }
        Span {
            generation: context.generation,
            parent,
            depth: context.depth,
        }
    })
}

impl Drop for Span {
    fn drop(&mut self) {
        if self.depth == 0 {
            return;
        }
        CURRENT.with(|cell| {
            let mut current = cell.borrow_mut();
            let Some(context) = current.as_mut().filter(|c| c.generation == self.generation) else {
                return;
            };
            if context.depth != self.depth {
                context.timings.unavailable = true;
                return;
            }
            context.elapsed(crate::host::telemetry_us());
            context.active = self.parent;
            context.depth -= 1;
        });
    }
}

pub(crate) fn bytes(phase: Phase, n: u64) {
    CURRENT.with(|cell| {
        if let Some(c) = cell.borrow_mut().as_mut() {
            add(
                &mut c.timings.phases[phase as usize].bytes,
                n,
                &mut c.timings.unavailable,
            );
        }
    });
}

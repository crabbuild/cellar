// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Check actual decoder allocation layouts against its adapter reservation.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

#[derive(Clone, Copy, Default)]
struct Observation {
    active: bool,
    live: isize,
    peak: usize,
}

thread_local! {
    // Constant TLS needs no heap allocation inside allocator callbacks. Other
    // harness threads and allocations outside the decode are excluded.
    static USAGE: Cell<Observation> = const { Cell::new(Observation {
        active: false, live: 0, peak: 0,
    }) };
}

fn record(delta: isize) {
    // Allocator callbacks must also tolerate thread-local teardown.
    let _ = USAGE.try_with(|usage| {
        let mut observation = usage.get();
        if observation.active {
            observation.live += delta;
            observation.peak = observation.peak.max(observation.live.max(0) as usize);
            usage.set(observation);
        }
    });
}

struct Measured;

// SAFETY: every allocation is forwarded to System with its original layout;
// thread-local counters do not allocate or change allocation ownership.
unsafe impl GlobalAlloc for Measured {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forward the caller's valid allocation layout unchanged.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record(layout.size() as isize);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        record(-(layout.size() as isize));
        // SAFETY: this pointer was allocated by System with this layout.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: preserve the original allocation's pointer/layout and the
        // caller's new size. Failed reallocations keep their old ownership.
        let next = unsafe { System.realloc(pointer, layout, size) };
        if !next.is_null() {
            record(size as isize - layout.size() as isize);
        }
        next
    }
}

#[global_allocator]
static ALLOCATOR: Measured = Measured;

#[test]
fn decoder_reservation_covers_escaped_string_scratch_growth() {
    for length in [1000, 4000, 8000, 8192, 12_000, 16_000, 16_232] {
        for escaped in [false, true] {
            let name = "x".repeat(length) + if escaped { "\n" } else { "" };
            let payload = serde_json::json!({
                "binding":"COUNTERS", "name":name, "method":"test.Counter/GetCount",
                "contract_digest":"a".repeat(64),
            })
            .to_string();
            let reserved = Cell::new(0);
            USAGE.with(|usage| {
                usage.set(Observation {
                    active: true,
                    ..Observation::default()
                })
            });
            let decoded =
                cellar_guest::operations::decode_metadata_with("cell.call", &payload, |bytes| {
                    reserved.set(bytes);
                    Ok(())
                });
            let peak = USAGE.with(|usage| {
                let observation = usage.get();
                usage.set(Observation::default());
                observation.peak
            });
            let (value, _lease) = decoded.unwrap();
            assert_eq!(value["name"], name);
            assert!(peak <= reserved.get(),
                "length={length}, escaped={escaped}: peak allocation layouts {peak} exceed reservation {}",
                reserved.get());
        }
    }
}

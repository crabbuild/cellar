// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native layout admission supplements the portable decoded-slot profile.
use super::{contract::Field, Code, Contract, Failure};
use prost_types::field_descriptor_proto::Type;
use std::collections::{BTreeMap, BTreeSet};

/// Native codec support. Keep admission alive through decoding and the handler
/// which owns the resulting request, or until the decoded response is returned
/// to application code. This does not grant invocation authority.
#[doc(hidden)]
pub struct DecodeReservation(crate::native_rpc_decode::Admission);

impl DecodeReservation {
    pub fn new(contract: &Contract, message: &str, bytes: &[u8]) -> Result<Self, Failure> {
        let services = crate::asyncrt::services();
        if !services.accepts_work() {
            return Err(Failure::new(Code::Unavailable));
        }
        let maximum = services.runtime_policy().max_native_rpc_decode_bytes;
        let weight = super::wire::native_decode_bytes(
            contract,
            message.trim_start_matches('.'),
            bytes,
            maximum,
            services.runtime_policy().max_native_rpc_stack_bytes,
        )
        .map_err(|error| {
            Failure::new(if error.is::<super::wire::ResourceLimit>() {
                Code::ResourceExhausted
            } else {
                Code::InvalidArgument
            })
        })?;
        services
            .reserve_native_rpc_decode(weight)
            .map(Self)
            .ok_or_else(|| {
                Failure::new(if services.accepts_work() {
                    Code::ResourceExhausted
                } else {
                    Code::Unavailable
                })
            })
    }

    pub fn reserved_bytes(&self) -> usize {
        self.0.weight
    }
}

// A generated native field/container fits this layout unit. Repeated/map
// capacity growth and codec scratch have a separate multiplier in the walker.
// These are conservative reservations, not allocator-size or RSS measurements.
pub(super) const LAYOUT_BYTES: usize = 64;
pub(super) const CAPACITY_FACTOR: usize = 4;
pub(super) const FIXED_BYTES: usize = 64 * 1024;

// Additional per-message decoder bookkeeping, separate from its native value's
// inline layout. Stack accounting follows active nesting, not sibling totals.
pub(super) const FRAME_BYTES: usize = 4096;

/// Include the transitive inline layout of absent ordinary message fields.
/// Prost boxes singular cycles; a DFS back edge therefore needs only its own
/// field slot. Counting other already-computed cyclic edges overestimates safely.
/// Iterative traversal also handles long, non-nested descriptor reference chains.
pub(super) fn layouts(
    messages: &BTreeMap<String, BTreeMap<u32, Field>>,
) -> BTreeMap<String, usize> {
    let mut output = BTreeMap::<String, usize>::new();
    let mut active = BTreeSet::new();
    for root in messages.keys() {
        let mut pending = vec![(root.as_str(), false)];
        while let Some((name, finish)) = pending.pop() {
            if finish {
                let fields = &messages[name];
                let mut slots = fields.len().saturating_add(1);
                for field in fields
                    .values()
                    .filter(|field| field.kind == Type::Message && !field.repeated)
                {
                    if let Some(child) = output.get(&field.type_name) {
                        slots = slots.saturating_add(*child);
                    }
                }
                output.insert(name.to_string(), slots);
                active.remove(name);
            } else if !output.contains_key(name) && active.insert(name) {
                pending.push((name, true));
                for field in messages[name]
                    .values()
                    .filter(|field| field.kind == Type::Message && !field.repeated)
                {
                    if !output.contains_key(&field.type_name)
                        && !active.contains(field.type_name.as_str())
                    {
                        pending.push((field.type_name.as_str(), false));
                    }
                }
            }
        }
    }
    output
}

#[cfg(test)]
#[path = "native_decode_tests.rs"]
mod tests;

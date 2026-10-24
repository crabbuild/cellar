// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Pre-serialization checks for generated native messages. Incoming bytes use
//! the descriptor wire walker; owned Rust outputs need this separate guard
//! before Prost's recursive encoded_len and allocating writer run.
use super::{Code, Failure, Limits};
use prost::Message;

/// Implemented by Cellar's generated messages and oneofs. Applications normally
/// use their generated clients rather than implementing this codec contract.
pub trait MessageGraph: 'static {
    #[doc(hidden)]
    fn layout_slots(&self) -> usize {
        1
    }

    #[doc(hidden)]
    fn visit(&self, budget: &mut GraphBudget, depth: usize) -> Result<(), Failure>;

    /// Move recursive children into an iterative cleanup stack. A rejected deep
    /// message must not recursively overflow the stack while being destroyed.
    #[doc(hidden)]
    fn discard(self: Box<Self>, pending: &mut GraphDiscard);
}

/// Internal support surface for generated visitors. Counts actual serialized
/// occurrences (including packed containers and synthetic map entries).
#[doc(hidden)]
pub struct GraphBudget {
    limits: Limits,
    remaining: usize,
    payload: usize,
    slots: usize,
}

impl GraphBudget {
    pub fn layout(&mut self, count: usize) -> Result<(), Failure> {
        self.slots = self
            .slots
            .checked_sub(count)
            .ok_or_else(|| Failure::new(Code::ResourceExhausted))?;
        Ok(())
    }

    pub fn message(&mut self, depth: usize, slots: usize) -> Result<(), Failure> {
        self.depth(depth)?;
        self.layout(slots)
    }

    pub fn is_default<T: Default + PartialEq>(value: &T) -> bool {
        value == &T::default()
    }

    pub fn depth(&self, depth: usize) -> Result<(), Failure> {
        if depth > self.limits.max_depth {
            return Err(Failure::new(Code::ResourceExhausted));
        }
        Ok(())
    }

    pub fn elements(&mut self, count: usize) -> Result<(), Failure> {
        self.layout(count)?;
        self.remaining = self
            .remaining
            .checked_sub(count)
            .ok_or_else(|| Failure::new(Code::ResourceExhausted))?;
        Ok(())
    }

    pub fn bytes(&mut self, count: usize) -> Result<(), Failure> {
        self.payload = self
            .payload
            .checked_add(count)
            .filter(|n| *n <= self.limits.max_message_bytes)
            .ok_or_else(|| Failure::new(Code::ResourceExhausted))?;
        Ok(())
    }
}

type Children = Box<dyn Iterator<Item = Box<dyn MessageGraph>>>;

/// Each frame owns an iterator, rather than boxing every wide-list child at
/// once. Deep messages use heap frames and never recurse through Drop.
#[doc(hidden)]
#[derive(Default)]
pub struct GraphDiscard {
    frames: Vec<Children>,
}

impl GraphDiscard {
    pub fn children<T, I>(&mut self, children: I)
    where
        T: MessageGraph,
        I: IntoIterator<Item = T>,
        I::IntoIter: 'static,
    {
        let children = children.into_iter();
        if children.size_hint().1 == Some(0) {
            return;
        }
        self.frames.push(Box::new(
            children.map(|child| Box::new(child) as Box<dyn MessageGraph>),
        ));
    }

    fn run(mut self) {
        while let Some(frame) = self.frames.last_mut() {
            if let Some(child) = frame.next() {
                child.discard(&mut self);
            } else {
                self.frames.pop();
            }
        }
    }
}

/// Bound graph work before asking Prost for the complete encoded size. Borrowed
/// validation leaves ownership with the caller; use encode_message for owned
/// values so failed validation also performs iterative cleanup.
pub fn validate_graph<T: Message + MessageGraph>(
    message: &T,
    limits: Limits,
) -> Result<usize, Failure> {
    let limits = limits
        .validate()
        .map_err(|_| Failure::new(Code::InvalidArgument))?;
    message.visit(
        &mut GraphBudget {
            limits,
            remaining: limits.max_elements,
            payload: 0,
            slots: limits.max_decoded_slots,
        },
        0,
    )?;
    let size = message.encoded_len();
    if size > limits.max_message_bytes {
        return Err(Failure::new(Code::ResourceExhausted));
    }
    Ok(size)
}

/// Encode an owned generated message after bounded graph/sizing checks. Cleanup
/// is iterative on both paths, including oversized handler-created responses.
pub fn encode_message<T: Message + MessageGraph>(
    message: T,
    limits: Limits,
) -> Result<Vec<u8>, Failure> {
    let result = validate_graph(&message, limits).map(|_| message.encode_to_vec());
    discard_message(message);
    result
}

/// Release an owned message without recursive destruction, including a request
/// refused because its invocation expired before serialization began.
pub fn discard_message<T: MessageGraph>(message: T) {
    let mut pending = GraphDiscard::default();
    pending.children([message]);
    pending.run();
}

impl<T: MessageGraph> MessageGraph for Box<T> {
    fn layout_slots(&self) -> usize {
        (**self).layout_slots()
    }

    fn visit(&self, budget: &mut GraphBudget, depth: usize) -> Result<(), Failure> {
        (**self).visit(budget, depth)
    }

    fn discard(self: Box<Self>, pending: &mut GraphDiscard) {
        <T as MessageGraph>::discard(*self, pending);
    }
}

// Prost represents scalar wrapper messages directly with these Rust values.
macro_rules! wrapper {
    ($($ty:ty => $zero:expr),* $(,)?) => {$ (
        impl MessageGraph for $ty {
            fn layout_slots(&self) -> usize { 2 }
            fn visit(&self, budget: &mut GraphBudget, depth: usize) -> Result<(), Failure> {
                budget.message(depth, 2)?;
                if *self != $zero { budget.elements(1)?; }
                Ok(())
            }
            fn discard(self: Box<Self>, _: &mut GraphDiscard) {}
        }
    )*};
}
wrapper!(bool => false, i32 => 0, u32 => 0, i64 => 0, u64 => 0, f32 => 0.0, f64 => 0.0);

impl MessageGraph for () {
    fn visit(&self, budget: &mut GraphBudget, depth: usize) -> Result<(), Failure> {
        budget.message(depth, 1)
    }

    fn discard(self: Box<Self>, _: &mut GraphDiscard) {}
}

macro_rules! buffer_wrapper {
    ($ty:ty) => {
        impl MessageGraph for $ty {
            fn layout_slots(&self) -> usize {
                2
            }
            fn visit(&self, budget: &mut GraphBudget, depth: usize) -> Result<(), Failure> {
                budget.message(depth, 2)?;
                if !self.is_empty() {
                    budget.elements(1)?;
                    budget.bytes(self.len())?;
                }
                Ok(())
            }
            fn discard(self: Box<Self>, _: &mut GraphDiscard) {}
        }
    };
}
buffer_wrapper!(String);
buffer_wrapper!(Vec<u8>);
buffer_wrapper!(prost::bytes::Bytes);

mod wkt;

#[cfg(test)]
mod tests;

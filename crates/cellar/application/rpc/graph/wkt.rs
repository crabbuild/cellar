//! Graph guards for Prost's externally mapped proto3 well-known messages.
use super::*;
use prost_types::{
    value::Kind, Duration, FieldMask, ListValue, SourceContext, Struct, Timestamp, Value,
};

macro_rules! time {
    ($ty:ty) => {
        impl MessageGraph for $ty {
            fn layout_slots(&self) -> usize {
                3
            }
            fn visit(&self, budget: &mut GraphBudget, depth: usize) -> Result<(), Failure> {
                budget.message(depth, 3)?;
                budget.elements(usize::from(self.seconds != 0) + usize::from(self.nanos != 0))
            }
            fn discard(self: Box<Self>, _: &mut GraphDiscard) {}
        }
    };
}
time!(Timestamp);
time!(Duration);

impl MessageGraph for FieldMask {
    fn layout_slots(&self) -> usize {
        2
    }

    fn visit(&self, budget: &mut GraphBudget, depth: usize) -> Result<(), Failure> {
        budget.message(depth, 2)?;
        budget.elements(self.paths.len())?;
        for path in &self.paths {
            budget.bytes(path.len())?;
        }
        Ok(())
    }

    fn discard(self: Box<Self>, _: &mut GraphDiscard) {}
}

impl MessageGraph for SourceContext {
    fn layout_slots(&self) -> usize {
        2
    }

    fn visit(&self, budget: &mut GraphBudget, depth: usize) -> Result<(), Failure> {
        budget.message(depth, 2)?;
        if !self.file_name.is_empty() {
            budget.elements(1)?;
            budget.bytes(self.file_name.len())?;
        }
        Ok(())
    }

    fn discard(self: Box<Self>, _: &mut GraphDiscard) {}
}

impl MessageGraph for Struct {
    fn layout_slots(&self) -> usize {
        2
    }

    fn visit(&self, budget: &mut GraphBudget, depth: usize) -> Result<(), Failure> {
        budget.message(depth, 2)?;
        budget.elements(self.fields.len())?;
        for (key, value) in &self.fields {
            budget.message(depth + 1, 3)?;
            if !key.is_empty() {
                budget.elements(1)?;
                budget.bytes(key.len())?;
            }
            if value != &Value::default() {
                budget.elements(1)?;
                value.visit(budget, depth + 2)?;
            } else {
                budget.layout(value.layout_slots())?;
            }
        }
        Ok(())
    }

    fn discard(mut self: Box<Self>, pending: &mut GraphDiscard) {
        pending.children(std::mem::take(&mut self.fields).into_values());
    }
}

impl MessageGraph for ListValue {
    fn layout_slots(&self) -> usize {
        2
    }

    fn visit(&self, budget: &mut GraphBudget, depth: usize) -> Result<(), Failure> {
        budget.message(depth, 2)?;
        budget.elements(self.values.len())?;
        for value in &self.values {
            value.visit(budget, depth + 1)?;
        }
        Ok(())
    }

    fn discard(mut self: Box<Self>, pending: &mut GraphDiscard) {
        pending.children(std::mem::take(&mut self.values));
    }
}

impl MessageGraph for Value {
    fn layout_slots(&self) -> usize {
        7
    }

    fn visit(&self, budget: &mut GraphBudget, depth: usize) -> Result<(), Failure> {
        budget.message(depth, 7)?;
        if let Some(value) = &self.kind {
            value.visit(budget, depth)?;
        }
        Ok(())
    }

    fn discard(mut self: Box<Self>, pending: &mut GraphDiscard) {
        pending.children(self.kind.take());
    }
}

impl MessageGraph for Kind {
    fn visit(&self, budget: &mut GraphBudget, depth: usize) -> Result<(), Failure> {
        budget.elements(1)?;
        match self {
            Self::StructValue(value) => value.visit(budget, depth + 1),
            Self::ListValue(value) => value.visit(budget, depth + 1),
            Self::StringValue(value) => budget.bytes(value.len()),
            _ => Ok(()),
        }
    }

    fn discard(self: Box<Self>, pending: &mut GraphDiscard) {
        match *self {
            Self::StructValue(value) => pending.children([value]),
            Self::ListValue(value) => pending.children([value]),
            _ => {}
        }
    }
}

// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Event-local nesting, background registration and response-boundary gates.
//!
//! Drivers supply their own promise and gate types. No engine, executor or
//! ambient event determines which invocation owns a registration.

struct Frame<P, G> {
    background: Vec<P>,
    gates: Vec<G>,
}

/// Registrations returned when a frame ends. A host drops discarded gates
/// after releasing its event-state lock, since cleanup can wake other work.
pub struct EndedFrame<P, G> {
    pub background: Vec<P>,
    pub discarded_gates: Vec<G>,
}

/// One invocation's event frames, innermost last.
pub struct EventFrames<P, G> {
    frames: Vec<Frame<P, G>>,
    late: Vec<P>,
    active: bool,
    ended_gates: Vec<G>,
    gates_sealed: bool,
}

impl<P, G> Default for EventFrames<P, G> {
    fn default() -> Self {
        Self {
            frames: Vec::new(),
            late: Vec::new(),
            active: false,
            ended_gates: Vec::new(),
            gates_sealed: false,
        }
    }
}

impl<P, G> EventFrames<P, G> {
    pub fn begin(&mut self) {
        self.frames.push(Frame {
            background: Vec::new(),
            gates: Vec::new(),
        });
    }

    /// End the innermost frame. Gates remain owned by this invocation until
    /// its response boundary is sealed. Background work extends registration
    /// authority only when the outermost frame returns a nonempty batch.
    pub fn end(&mut self) -> Option<EndedFrame<P, G>> {
        let mut frame = self.frames.pop()?;
        let mut discarded_gates = Vec::new();
        if self.gates_sealed {
            discarded_gates = std::mem::take(&mut frame.gates);
        } else if let Some(parent) = self.frames.last_mut() {
            parent.gates.append(&mut frame.gates);
        } else {
            self.ended_gates.append(&mut frame.gates);
        }
        if self.frames.is_empty() && !frame.background.is_empty() {
            self.active = true;
        }
        Some(EndedFrame {
            background: frame.background,
            discarded_gates,
        })
    }

    /// Return a refused registration to its owner for cleanup.
    pub fn register_background(&mut self, work: P) -> Result<(), P> {
        if let Some(frame) = self.frames.last_mut() {
            frame.background.push(work);
        } else if self.active {
            self.late.push(work);
        } else {
            return Err(work);
        }
        Ok(())
    }

    pub fn accepts_background(&self) -> bool {
        !self.frames.is_empty() || self.active
    }

    /// An empty drain ends background registration authority. A nonempty
    /// batch extends it while the driver waits for that batch to settle.
    pub fn take_late_background(&mut self) -> Vec<P> {
        let late = std::mem::take(&mut self.late);
        if late.is_empty() {
            self.active = false;
        }
        late
    }

    /// End background authority and return abandoned late work for cleanup.
    pub fn seal_background(&mut self) -> Vec<P> {
        self.active = false;
        std::mem::take(&mut self.late)
    }

    pub fn register_gate(&mut self, gate: G) -> Result<(), G> {
        if self.gates_sealed {
            return Err(gate);
        }
        if let Some(frame) = self.frames.last_mut() {
            frame.gates.push(gate);
        } else {
            self.ended_gates.push(gate);
        }
        Ok(())
    }

    /// Seal and take gates from both ended and still-active frames, covering
    /// failures whose driver cannot enter application code again.
    pub fn take_gates(&mut self) -> Vec<G> {
        if self.gates_sealed {
            return Vec::new();
        }
        self.gates_sealed = true;
        let mut gates = std::mem::take(&mut self.ended_gates);
        for frame in &mut self.frames {
            gates.append(&mut frame.gates);
        }
        gates
    }

    pub fn depth(&self) -> usize {
        self.frames.len()
    }
}

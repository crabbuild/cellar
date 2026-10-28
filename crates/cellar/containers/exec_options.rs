// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native execution options and validated terminal dimensions.

use anyhow::{ensure, Result};

/// Nonzero terminal dimensions, in character cells.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TerminalSize {
    cols: u16,
    rows: u16,
}

impl TerminalSize {
    pub fn new(cols: u16, rows: u16) -> Result<Self> {
        ensure!(cols > 0 && rows > 0, "terminal dimensions must be nonzero");
        Ok(Self { cols, rows })
    }

    pub fn cols(self) -> u16 {
        self.cols
    }

    pub fn rows(self) -> u16 {
        self.rows
    }
}

pub struct ExecParams {
    pub cmd: Vec<String>,
    pub env: Vec<(String, String)>,
    pub cwd: Option<String>,
    pub user: Option<String>,
    /// stderr folded into stdout.
    pub combined: bool,
    pub pty: Option<TerminalSize>,
}

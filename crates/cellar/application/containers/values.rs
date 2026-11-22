// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::TerminalSize;

/// Options for the container's root process. Internet access defaults to off.
#[derive(Clone, Debug, Default)]
pub struct StartOptions {
    pub entrypoint: Option<Vec<String>>,
    pub env: Vec<(String, String)>,
    pub enable_internet: bool,
    pub labels: Vec<(String, String)>,
}

#[derive(Clone, Debug, Default)]
pub struct ExecOptions {
    pub cmd: Vec<String>,
    pub env: Vec<(String, String)>,
    pub cwd: Option<String>,
    pub user: Option<String>,
    pub combined: bool,
    /// Allocate a terminal; stdout includes both output channels.
    pub pty: Option<TerminalSize>,
}

#[derive(Debug)]
pub struct ExecOutput {
    pub exit_code: i64,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

/// Allocation metadata. The PID belongs to the container namespace; images
/// without the shell identity handshake report None, never a daemon-host PID.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecInfo {
    pub pid: Option<u32>,
    pub is_pty: bool,
}

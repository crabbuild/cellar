// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Complete already-admitted original voters; never replace a missing engine.
mod materialize;
mod receive;

use super::*;
pub(super) use materialize::next;

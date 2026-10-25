// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Python handlers may suppress cancellation without retaining native authority.
mod case;
mod probes;
mod process;
mod recovery;

pub use process::{child, finish, run as supervision, smoke as supervision_smoke};

use crate::application::Composite;
use anyhow::{ensure, Result};
use cellar_python::PythonApplication;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;

const OWNER: &str = "python-owner";
const SOURCE: &str = "python-source";
const DELTA: i64 = 9_007_199_254_740_993;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Trigger {
    Cancel,
    Shutdown,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Case {
    caller: String,
    peer: bool,
    after: bool,
    stubborn: bool,
    trigger: Trigger,
}

impl Case {
    fn validate(&self) -> Result<()> {
        ensure!(
            ["rust", "python", "v8", "node"].contains(&self.caller.as_str()),
            "unknown Python lifecycle caller"
        );
        ensure!(
            !self.stubborn || self.trigger == Trigger::Shutdown,
            "persistent suppression requires owner shutdown"
        );
        Ok(())
    }

    fn operation(&self) -> &'static str {
        if self.after {
            "worker-after"
        } else {
            "worker-before"
        }
    }
}

pub async fn run(base: Arc<Composite>, python: Arc<PythonApplication>) -> Result<Value> {
    let mut cases = Vec::new();
    for trigger in [Trigger::Cancel, Trigger::Shutdown] {
        for caller in ["rust", "python", "v8", "node"] {
            for peer in [false, true] {
                for after in [false, true] {
                    let spec = Case {
                        caller: caller.into(),
                        peer,
                        after,
                        stubborn: false,
                        trigger,
                    };
                    base.audit.reset_after_node_drain();
                    eprintln!("Python suppression: {spec:?}");
                    let root = tempfile::tempdir()?;
                    cases.push(case::run(base.clone(), python.clone(), root.path(), spec).await?);
                }
            }
        }
    }
    Ok(
        json!({"format":1,"suite":"python-cancellation","case_count":cases.len(),
        "cases":cases,"remote_fleet_qualified":false}),
    )
}

pub async fn smoke(base: Arc<Composite>, python: Arc<PythonApplication>) -> Result<Value> {
    let root = tempfile::tempdir()?;
    let spec = Case {
        caller: "rust".into(),
        peer: false,
        after: true,
        stubborn: false,
        trigger: Trigger::Cancel,
    };
    let case = case::run(base, python, root.path(), spec).await?;
    Ok(
        json!({"format":1,"suite":"python-cancellation-smoke","case_count":1,
        "cases":[case],"remote_fleet_qualified":false}),
    )
}

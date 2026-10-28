// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
#[path = "../../../cellar-container/tests/support/named_language.rs"]
mod qualification;

pub(super) fn child() -> Result<()> {
    let source = Source::new(include_str!("sandboxes.py"), "sandboxes.py")
        .events([EventKind::Fetch])
        .cell("Counter")
        .sandbox_executor(
            "Sandbox",
            Arc::new(cellar_container::ChildExecutor::new(
                Arc::new(cellar_container::Native),
                "c".repeat(64),
            )?),
        )?;
    let report = run(Options::default(), source, |application, _| {
        qualification::exercise(application)
    })?;
    ensure!(report.complete());
    Ok(())
}

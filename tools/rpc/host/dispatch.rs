// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Keep suite construction off the stack used to poll native startup/cleanup.
//! The Python host keeps its normal stack; no suite gets another task or budget.
use crate::{
    admission, application::Composite, crash, cycles, deadline, peer_faults, proof, provenance,
    qualify, remote, worker, Suite,
};
use anyhow::Result;
use cellar_python::PythonApplication;
use futures_util::future::LocalBoxFuture;
use serde_json::{json, Value};
use std::{path::PathBuf, sync::Arc};

pub fn run(
    suite: Suite,
    app: Arc<Composite>,
    python: Arc<PythonApplication>,
    server: Option<crash::Server>,
    child: Option<PathBuf>,
) -> LocalBoxFuture<'static, Result<Value>> {
    if let Some(server) = server {
        return Box::pin(crash::serve(app, server));
    }
    if let Some(path) = child {
        return Box::pin(worker::python_child(app, python, path));
    }
    selected(suite, app, python)
}

fn selected(
    suite: Suite,
    app: Arc<Composite>,
    python: Arc<PythonApplication>,
) -> LocalBoxFuture<'static, Result<Value>> {
    match suite {
        Suite::Local => Box::pin(qualify::local(app)),
        Suite::Remote => Box::pin(remote::run(app)),
        Suite::PeerFaults => Box::pin(peer_faults::run(app)),
        Suite::Crash => Box::pin(crash::run(app)),
        Suite::Proof => Box::pin(proof::run(app)),
        Suite::ProofWait => Box::pin(proof::waits(app)),
        Suite::ProofWaitSmoke => Box::pin(proof::wait_smoke(app)),
        Suite::ProofWaitV8Repro => Box::pin(proof::wait_v8_repro(app)),
        Suite::ProofWaitNodeRepro => Box::pin(proof::wait_node_repro(app)),
        Suite::Fence => Box::pin(proof::fences(app)),
        Suite::Recruitment => Box::pin(proof::recruitment(app)),
        Suite::Recovery => Box::pin(admission::run(app)),
        Suite::RecoveryProbe => Box::pin(admission::probe(app)),
        Suite::Deadline => Box::pin(deadline::run(app)),
        Suite::Cancellation => Box::pin(deadline::cancellation::run(app)),
        Suite::Cycles => Box::pin(cycles::run(app)),
        Suite::Provenance => Box::pin(provenance::run(app)),
        Suite::WorkerFailure => Box::pin(worker::run(app)),
        Suite::WorkerFailureSmoke => Box::pin(worker::smoke(app)),
        Suite::WorkerShutdown => Box::pin(worker::shutdown(app)),
        Suite::WorkerShutdownSmoke => Box::pin(worker::shutdown_smoke(app)),
        Suite::V8Termination => Box::pin(worker::v8_termination(app)),
        Suite::V8TerminationSmoke => Box::pin(worker::v8_termination_smoke(app)),
        Suite::V8Shutdown => Box::pin(worker::v8_shutdown(app)),
        Suite::V8ShutdownSmoke => Box::pin(worker::v8_shutdown_smoke(app)),
        Suite::V8ForcedPeerRepro => Box::pin(worker::v8_forced_peer_repro(app)),
        Suite::V8ForcedPeerDeadlineRepro => Box::pin(worker::v8_forced_peer_deadline_repro(app)),
        Suite::Lifecycle => Box::pin(worker::lifecycle(app)),
        Suite::PythonCancellation => Box::pin(worker::python_cancellation(app, python)),
        Suite::PythonCancellationSmoke => Box::pin(worker::python_cancellation_smoke(app, python)),
        Suite::PythonSupervision => Box::pin(worker::python_supervision(app, python)),
        Suite::PythonSupervisionSmoke => Box::pin(worker::python_supervision_smoke(app, python)),
        Suite::All => Box::pin(all(app, python)),
    }
}

async fn all(app: Arc<Composite>, python: Arc<PythonApplication>) -> Result<Value> {
    let mut report = json!({"format":1,"suite":"all"});
    // One indirect suite future at a time avoids debug-build temporaries for
    // every suite sharing the root poll frame with deeply nested node startup.
    for (name, suite) in [
        ("local", Suite::Local),
        ("remote", Suite::Remote),
        ("peer_faults", Suite::PeerFaults),
        ("deadline", Suite::Deadline),
        ("cancellation", Suite::Cancellation),
        ("cycles", Suite::Cycles),
        ("provenance", Suite::Provenance),
        ("proof_wait", Suite::ProofWait),
        ("recovery", Suite::Recovery),
        ("crash", Suite::Crash),
        ("proof", Suite::Proof),
        ("worker_failure", Suite::WorkerFailure),
        ("worker_shutdown", Suite::WorkerShutdown),
        ("v8_termination", Suite::V8Termination),
        ("v8_shutdown", Suite::V8Shutdown),
        ("python_cancellation", Suite::PythonCancellation),
        ("python_supervision", Suite::PythonSupervision),
    ] {
        report[name] = selected(suite, app.clone(), python.clone()).await?;
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;

    fn future_size<F: Future>(_: fn(Arc<Composite>, Arc<PythonApplication>) -> F) -> usize {
        std::mem::size_of::<F>()
    }

    #[test]
    fn combined_driver_does_not_embed_every_suite_state() {
        let bytes = future_size(all);
        assert!(
            bytes <= 16 * 1024,
            "combined driver embeds {bytes} bytes of suite state"
        );
    }
}

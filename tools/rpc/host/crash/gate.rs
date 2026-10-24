//! Qualification-only barriers at explicitly observed invocation boundaries.
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{rpc, Context, Invocation, Output};
use rpc::prost::Message;
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Point {
    BeforeHandler,
    AfterCountBeforeReceipt,
    AfterSyncedHandler,
}

impl Point {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "before_handler" => Ok(Self::BeforeHandler),
            "after_count_before_receipt" => Ok(Self::AfterCountBeforeReceipt),
            "after_synced_handler" => Ok(Self::AfterSyncedHandler),
            _ => anyhow::bail!("unknown crash point"),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::BeforeHandler => "before_handler",
            Self::AfterCountBeforeReceipt => "after_count_before_receipt",
            Self::AfterSyncedHandler => "after_synced_handler",
        }
    }
}

#[derive(Clone)]
struct Arm {
    point: Point,
    class: String,
    operation_id: String,
}

#[derive(Default)]
struct State {
    arm: Option<Arm>,
    evidence: Option<Value>,
    evidence_path: Option<PathBuf>,
}

#[derive(Default)]
pub struct Gate(Mutex<State>);

impl Gate {
    pub fn evidence_file(&self, path: PathBuf) {
        self.0.lock().unwrap().evidence_path = Some(path);
    }

    pub(super) fn publish(&self, evidence: Value) -> Result<()> {
        let mut state = self.0.lock().unwrap();
        if let Some(path) = &state.evidence_path {
            let temporary = path.with_extension("pending");
            std::fs::write(&temporary, serde_json::to_vec(&evidence)?)?;
            std::fs::rename(temporary, path)?;
        }
        state.evidence = Some(evidence);
        Ok(())
    }

    pub fn armed(&self) -> bool {
        self.0.lock().unwrap().arm.is_some()
    }

    pub fn arm(&self, point: Point, class: &str, operation_id: &str) -> Result<()> {
        ensure!(
            super::super::fleet::CLASSES
                .iter()
                .any(|(_, _, candidate)| *candidate == class),
            "unknown crash provider"
        );
        ensure!(
            !operation_id.is_empty() && operation_id.len() <= 128,
            "invalid crash operation ID"
        );
        let mut state = self.0.lock().unwrap();
        ensure!(state.arm.is_none(), "crash gate already armed");
        state.arm = Some(Arm {
            point,
            class: class.into(),
            operation_id: operation_id.into(),
        });
        Ok(())
    }

    pub fn snapshot(&self) -> Value {
        self.0
            .lock()
            .unwrap()
            .evidence
            .clone()
            .unwrap_or_else(|| json!({"ready":false}))
    }

    pub fn sqlite_watch(
        self: &Arc<Self>,
        context: &Context,
        invocation: &Invocation,
    ) -> Result<Option<Arc<super::sqlite::Watch>>> {
        let arm = self.0.lock().unwrap().arm.clone();
        let Some(arm) = arm.filter(|arm| {
            arm.point == Point::AfterCountBeforeReceipt && arm.class != "RustCounter"
        }) else {
            return Ok(None);
        };
        if context.identity().map(|identity| &identity.class) != Some(&arm.class) {
            return Ok(None);
        }
        let Invocation::CellRpc { method, args } = invocation else {
            return Ok(None);
        };
        if method != "example.counter.v1.Counter/Increment" {
            return Ok(None);
        }
        let (_, bytes) = rpc::decode_envelope(args, rpc::Limits::default().max_message_bytes)?;
        let request = super::super::generated::IncrementRequest::decode(bytes.as_slice())?;
        if request.operation_id != arm.operation_id {
            return Ok(None);
        }
        Ok(Some(super::sqlite::Watch::new(
            self.clone(),
            context,
            &request.operation_id,
            request
                .delta
                .context("transaction checkpoint requires a delta")?,
        )?))
    }

    /// This synchronous checkpoint deliberately holds the real transaction's
    /// poll. Evidence crosses an atomic file rename because this owner cannot
    /// process another SDK turn while that poll is stopped. The coordinator
    /// SIGKILLs the process; the finite failure path never commits partial work.
    pub fn transaction(&self, context: &Context, operation_id: &str) -> Result<()> {
        let arm = self.0.lock().unwrap().arm.clone();
        let Some(arm) = arm.filter(|arm| {
            arm.point == Point::AfterCountBeforeReceipt && arm.operation_id == operation_id
        }) else {
            return Ok(());
        };
        let identity = context
            .identity()
            .context("transaction checkpoint requires an identity")?;
        if identity.class != arm.class {
            return Ok(());
        }
        let sample = cellar::storage::sync_sample(&identity.scope)?
            .context("transaction connection is not installed")?;
        ensure!(
            sample.in_transaction && sample.epoch == identity.epoch,
            "checkpoint is outside the admitted transaction"
        );
        let storage = context.storage()?;
        let count = storage
            .get_json("count")?
            .context("count was not written before checkpoint")?;
        let receipt = storage.get_json(&format!("receipt:{operation_id}"))?;
        ensure!(
            receipt.is_none(),
            "receipt was written before the transaction checkpoint"
        );
        self.publish(json!({"ready":true,"point":arm.point.name(),
            "node":context.var("DOMAIN_MARKER").context("missing domain marker")?,
            "class":identity.class,"scope":identity.scope,"epoch":identity.epoch,
            "count":count,"receipt":receipt,"typed_reply_held":false,
            "transaction_open":sample.in_transaction,
            "transaction_observer":"native_callback"}))?;
        std::thread::sleep(Duration::from_secs(10));
        anyhow::bail!("coordinator did not kill the owner at its transaction checkpoint")
    }

    /// Observe synchronously, then let the invocation owner hold its own future.
    /// In particular, an HTTP body's non-Sync stream is never borrowed by an
    /// async barrier when only a typed RPC reply needs inspection.
    pub fn boundary(
        &self,
        context: &Context,
        invocation: &Invocation,
        point: Point,
        output: Option<&Output>,
    ) -> Result<bool> {
        let arm = self.0.lock().unwrap().arm.clone();
        let Some(arm) = arm.filter(|arm| arm.point == point) else {
            return Ok(false);
        };
        let identity = context
            .identity()
            .context("crash barrier requires a cell identity")?;
        if identity.class != arm.class {
            return Ok(false);
        }
        let Invocation::CellRpc { method, args } = invocation else {
            return Ok(false);
        };
        if method != "example.counter.v1.Counter/Increment" {
            return Ok(false);
        }
        // This is a tool-owned, already admitted request. Match the command we
        // armed so an unrelated read or housekeeping operation cannot certify
        // the requested crash point.
        let (digest, bytes) = rpc::decode_envelope(args, rpc::Limits::default().max_message_bytes)?;
        let request = super::super::generated::IncrementRequest::decode(bytes.as_slice())?;
        if request.operation_id != arm.operation_id {
            return Ok(false);
        }
        let storage = context.storage()?;
        let count = storage.get_json("count")?.unwrap_or_else(|| json!("0"));
        let receipt = storage.get_json(&format!("receipt:{}", arm.operation_id))?;
        let sample = cellar::storage::sync_sample(&identity.scope)?
            .context("barrier connection is not installed")?;
        ensure!(
            !sample.in_transaction && sample.epoch == identity.epoch,
            "handler barrier is inside a transaction or stale epoch"
        );
        if point == Point::AfterSyncedHandler {
            let Some(Output::Rpc(reply)) = output else {
                anyhow::bail!("synced Counter did not produce an RPC reply");
            };
            let bytes = rpc::decode_reply(&digest, reply, rpc::Limits::default())?;
            let response = super::super::generated::IncrementResponse::decode(bytes.as_slice())?;
            let Some(super::super::generated::increment_response::Result::Count(value)) =
                response.result
            else {
                anyhow::bail!("synced Counter did not produce a successful business result");
            };
            ensure!(
                count.as_str() == Some(value.value.to_string().as_str()),
                "held RPC response differs from committed state"
            );
        }
        let evidence = json!({
            "ready":true, "point":point.name(),
            "node":context.var("DOMAIN_MARKER").context("missing crash domain marker")?,
            "class":identity.class, "scope":identity.scope, "epoch":identity.epoch,
            "count":count, "receipt":receipt,
            "typed_reply_held":point == Point::AfterSyncedHandler,
            "transaction_open":sample.in_transaction,
        });
        self.publish(evidence)?;
        // The coordinator terminates the entire owner process with SIGKILL.
        // No fake error, cancellation or graceful shutdown substitutes for loss.
        Ok(true)
    }
}

/// Same Counter implementation as ordinary qualification, with an observation
/// inside its synchronous SQLite transaction rather than before/after a handler.
pub struct Counter {
    pub crash: Arc<Gate>,
    pub live: Arc<super::super::proof::Live>,
    pub provenance: Arc<super::super::provenance::Native>,
}

impl super::super::generated::CounterHandler for Counter {
    fn increment(
        self: Arc<Self>,
        context: Context,
        request: super::super::generated::IncrementRequest,
    ) -> rpc::HandlerFuture<super::super::generated::IncrementResponse> {
        let gate = self.crash.clone();
        let live = self.live.clone();
        let guard = live.begin(&context, &request.operation_id);
        let id = request.operation_id.clone();
        let post_commit = if id.starts_with("t06:") {
            context
                .storage()
                .and_then(|storage| storage.get_json(&format!("receipt:{id}")))
                .map(|saved| saved.is_none().then(|| context.clone()))
        } else {
            Ok(None)
        };
        let provenance = self.provenance.clone();
        let handler = super::super::counter::increment_with_checkpoint(
            context,
            request,
            move |context, id, stage| match stage {
                super::super::counter::Checkpoint::AfterCount => gate.transaction(context, id),
                super::super::counter::Checkpoint::AfterCommit => live.committed(context, id),
            },
        );
        Box::pin(async move {
            let mut guard = guard.map_err(|_| rpc::Failure::new(rpc::Code::Internal))?;
            let result = handler.await;
            if let Some(guard) = guard.as_mut() {
                guard.finish(result.is_ok());
            }
            if let Some(context) =
                post_commit.map_err(|_| rpc::Failure::new(rpc::Code::Internal))?
            {
                if result.is_ok() {
                    provenance.after_commit(&context, &id).await?;
                }
            }
            result
        })
    }

    fn get_count(
        self: Arc<Self>,
        context: Context,
        request: super::super::generated::GetCountRequest,
    ) -> rpc::HandlerFuture<super::super::generated::GetCountResponse> {
        Arc::new(super::super::counter::Counter).get_count(context, request)
    }
}

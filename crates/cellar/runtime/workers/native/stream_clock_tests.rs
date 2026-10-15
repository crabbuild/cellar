//! Production native contexts and SQLite handlers, without fleet proof or
//! background maintenance that could hide a mutation's lost time observation.

use super::*;
use crate::application::event_stream::{
    self as streams,
    authority::{Action, Authority},
    broker, Binding, Definition, DeliveryError, Envelope, ErrorCode, Grant, Resource, Retention,
    Start, SubscriptionDefinition,
};
use base64::Engine;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicI64};

fn rpc(context: &Context, method: &str, body: &Value) -> Result<Value> {
    let cell = broker::create_cell(context)?;
    let args = crate::application::encode_rpc(&cellar_native::clone::StructuredValue::from_json(
        json!([body]),
    )?)?;
    let output = cell
        .invoke(
            context.clone(),
            Invocation::CellRpc {
                method: method.into(),
                args,
            },
        )
        .now_or_never()
        .context("non-polling stream operation suspended")??;
    let Output::Rpc(data) = output else {
        anyhow::bail!("stream returned non-RPC output");
    };
    let reply = crate::application::decode_rpc(&data)?.to_json()?;
    if let Some(failure) = reply.get("error") {
        return Err(serde_json::from_value::<DeliveryError>(failure.clone())?.into());
    }
    Ok(reply
        .get("ok")
        .context("stream success body missing")?
        .clone())
}

struct Calls {
    source: Context,
    destination: Context,
    binding: Binding,
}

impl Calls {
    fn call<T>(&self, action: Action, handler: impl FnOnce(&Context) -> Result<T>) -> Result<T> {
        let authority = {
            let _source = crate::host_io::CurrentGuard::enter(self.source.io.clone());
            Authority::mint(&self.source, &self.binding, action)?
        };
        let mut context = self.destination.clone();
        context.event_authority = Some(authority);
        let _turn = crate::host_io::CurrentGuard::enter(context.io.clone());
        handler(&context)
    }

    fn packet(&self, sequences: &[i64]) -> Result<Value> {
        let source = self.source.identity().unwrap().scope.clone();
        let incarnation = "00000000000000000000000000000004";
        let lane = streams::lane_identity(&self.binding.resource, &source, incarnation)?;
        let records = sequences
            .iter()
            .map(|sequence| {
                let event = Envelope {
                    id: format!("v1:{lane}:{sequence}"),
                    lane: lane.clone(),
                    sequence: *sequence,
                    source: source.clone(),
                    source_incarnation: incarnation.into(),
                    family: self.binding.resource.family.clone(),
                    version: 1,
                    key: "order".into(),
                    created_at_ms: 100_000,
                    payload: json!({"value": sequence}),
                };
                Ok(base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&event)?))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(json!({"protocol":1,"binding":self.binding,"lane":lane,"records":records}))
    }

    fn status(&self) -> Result<Value> {
        self.call(Action::Consume, |context| {
            rpc(
                context,
                "__streamSubscriptionStatus",
                &json!({"subscription":"projection"}),
            )
        })
    }
}

fn definition(retention: Retention) -> (Binding, Definition) {
    let binding = Binding {
        environment: "EVENTS".into(),
        resource: Resource {
            tenant: "test".into(),
            application: "native-direct".into(),
            id: "00000000000000000000000000000001".into(),
            name: "clock-orders".into(),
            family: "orders".into(),
        },
        principal: "native-direct".into(),
        grant: "owner".into(),
        publish: true,
        read: true,
        administer: true,
        subscriptions: vec!["projection".into()],
    };
    let definition = Definition {
        resource: binding.resource.clone(),
        revision: 1,
        retention,
        max_bytes: 16_384,
        max_records: 1,
        max_age_ms: 60_000,
        grants: vec![Grant::from_binding(&binding)],
        subscriptions: vec![SubscriptionDefinition {
            name: "projection".into(),
            start: Start::Earliest,
            required: retention == Retention::Required,
        }],
    };
    (binding, definition)
}

fn configuration(definition: &Definition) -> Result<Value> {
    Ok(json!({"operation":format!("{:032x}",definition.revision),
        "definition":base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(definition)?)}))
}

#[derive(Clone, Copy)]
enum Mutation {
    RejectedBatch,
    ProtectedCapacity,
    Duplicate,
    FailedConfiguration,
    Reconfigured,
    RefusedRetirement,
    RotatedGrant,
    Forbidden(Action),
    AdvancingClock,
}

async fn run(mutation: Mutation) -> Result<()> {
    for reopen in [false, true] {
        run_once(mutation, reopen).await?;
    }
    Ok(())
}

async fn run_once(mutation: Mutation, reopen: bool) -> Result<()> {
    let sample = Arc::new(AtomicI64::new(100_000));
    let selected = sample.clone();
    let stepping = Arc::new(AtomicBool::new(false));
    let advance = stepping.clone();
    let observed = Arc::new(AtomicI64::new(100_000));
    let record = observed.clone();
    let domain = asyncrt::ExecutionDomain::with_wall_clock(
        tokio::runtime::Handle::current(),
        Arc::new(move || {
            let value = if advance.load(Ordering::SeqCst) {
                selected.fetch_add(30_001, Ordering::SeqCst)
            } else {
                selected.load(Ordering::SeqCst)
            };
            record.store(value, Ordering::SeqCst);
            value
        }),
    );
    let result = domain.run(async {
        let directory = tempfile::tempdir()?;
        let mut worker = NativeWorker::new(config(Arc::new(Signals::default())))?;
        let retention = if matches!(mutation, Mutation::ProtectedCapacity) {
            Retention::Required
        } else {
            Retention::Bounded
        };
        let (binding, mut definition) = definition(retention);
        let destination = streams::cell_scope(&definition.resource)?;
        let source = "Order:clock-source";
        let destination_path = directory.path().join("stream.sqlite");
        let source_path = directory.path().join("source.sqlite");
        for (scope, path) in [(destination.as_str(), &destination_path), (source, &source_path)] {
            worker.own_cell(scope, Some(CellStorage {
                path: path.to_str().unwrap(), epoch: 1, replicated_wake: false, vfs: None,
            }))?;
        }
        let _cells = worker.cells.install();
        let mut calls = Calls {
            source: worker.context(Some(source), None, None, false)?,
            destination: worker.context(Some(&destination), None, None, false)?,
            binding,
        };
        {
            let mut context = calls.destination.clone();
            context.event_authority = Some(Authority::deployment("native-direct", &definition)?);
            let _turn = crate::host_io::CurrentGuard::enter(context.io.clone());
            rpc(&context, "__streamConfigure", &configuration(&definition)?)?;
        }
        let original_packet = calls.packet(&[1])?;
        calls.call(Action::Append, |context| rpc(context, "__streamAppend", &original_packet))?;
        let identity = calls.status()?["identity"].clone();
        let batch = calls.call(Action::Consume, |context| {
            rpc(context, "__streamNext", &json!({"subscription":"projection","wait_ms":"0",
                "expected":identity,"fetch_id":"00000000000000000000000000000005",
                "max_records":1,"max_bytes":16_384}))
        })?;
        assert_eq!(batch["issued_ms"], "100000");
        assert_eq!(batch["expires_ms"], "130000");
        let initial = calls.status()?;
        assert_eq!(initial["observation"]["lease_active"], true);
        sample.store(if matches!(mutation, Mutation::AdvancingClock) { 129_999 } else { 130_001 }, Ordering::SeqCst);
        stepping.store(matches!(mutation, Mutation::AdvancingClock), Ordering::SeqCst);
        match mutation {
            Mutation::RejectedBatch | Mutation::ProtectedCapacity => {
                let sequences = if matches!(mutation, Mutation::RejectedBatch) {vec![2,3]} else {vec![2]};
                let packet = calls.packet(&sequences)?;
                let failure = calls.call(Action::Append, |context| rpc(context, "__streamAppend", &packet)).unwrap_err();
                assert_eq!(failure.downcast_ref::<DeliveryError>().unwrap().code, ErrorCode::StreamFull);
            }
            Mutation::Duplicate | Mutation::AdvancingClock => {
                let replay = calls.call(Action::Append, |context| rpc(context, "__streamAppend", &original_packet))?;
                assert_eq!(replay["accepted_through"], "1");
            }
            Mutation::FailedConfiguration | Mutation::Reconfigured | Mutation::RotatedGrant => {
                definition.revision = 2;
                definition.max_age_ms = if matches!(mutation, Mutation::FailedConfiguration) { i64::MAX } else { 120_000 };
                if matches!(mutation, Mutation::RotatedGrant) {
                    definition.grants[0].id = "successor".into();
                }
                let result = calls.call(Action::Configure, |context| rpc(context, "__streamConfigure", &configuration(&definition)?));
                if matches!(mutation, Mutation::FailedConfiguration) {
                    let failure = result.unwrap_err();
                    assert!(failure.to_string().contains("stream age deadline overflow"), "{failure:#}");
                } else {
                    let result = result?;
                    assert_eq!(result["revision"], "2");
                    if matches!(mutation, Mutation::RotatedGrant) {
                        let replay = calls.call(Action::Configure, |context| rpc(context, "__streamConfigure", &configuration(&definition)?))?;
                        assert_eq!(replay, result);
                        calls.binding.grant = "successor".into();
                    }
                }
            }
            Mutation::RefusedRetirement => {
                let request = json!({"operation":"00000000000000000000000000000006","revision":"1",
                    "reason":"close only after all work settles","mode":{"mode":"coordinated","procedure":"stop all producers"}});
                let packet = json!({"request":base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&request)?)});
                let failure = calls.call(Action::Administer, |context| rpc(context, "__streamRetire", &packet)).unwrap_err();
                assert_eq!(failure.downcast_ref::<DeliveryError>().unwrap().code, ErrorCode::PendingDelivery);
            }
            Mutation::Forbidden(action) => {
                calls.binding.grant = "absent".into();
                let (method, body) = match action {
                    Action::Append => ("__streamAppend", calls.packet(&[2])?),
                    Action::Configure => {
                        definition.revision = 2;
                        ("__streamConfigure", configuration(&definition)?)
                    }
                    Action::Administer => ("__streamRetire", json!({"request":base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&json!({
                        "operation":"00000000000000000000000000000006","revision":"1","reason":"denied",
                        "mode":{"mode":"coordinated","procedure":"stop all producers"}}))?)})),
                    _ => unreachable!(),
                };
                let failure = calls.call(action, |context| rpc(context, method, &body)).unwrap_err();
                assert_eq!(failure.downcast_ref::<DeliveryError>().unwrap().code, ErrorCode::Forbidden);
                calls.binding.grant = "owner".into();
            }
        }
        // Capture only the mutation's samples, before read-only diagnostics.
        let mutation_observed = observed.load(Ordering::SeqCst);
        stepping.store(false, Ordering::SeqCst);
        if matches!(mutation, Mutation::AdvancingClock) {
            sample.store(mutation_observed, Ordering::SeqCst);
        }
        let expired = calls.status()?;
        assert_eq!(expired["observation"]["lease_active"], matches!(mutation, Mutation::AdvancingClock) && mutation_observed < 130_000);
        assert_eq!(expired["checkpoint"], "0");
        assert_eq!(expired["lease"], initial["lease"]);
        sample.store(100_000, Ordering::SeqCst);
        if reopen {
            worker.own_cell(&destination, None)?;
            worker.own_cell(&destination, Some(CellStorage {
                path: destination_path.to_str().unwrap(), epoch: 2, replicated_wake: false, vfs: None,
            }))?;
            calls.destination = worker.context(Some(&destination), None, None, false)?;
        }
        let rolled_back = calls.status()?;
        assert_eq!(rolled_back["identity"], initial["identity"]);
        assert_eq!(rolled_back["lease"], initial["lease"]);
        let described = calls.call(Action::Administer, |context| rpc(context, "__streamDescribe", &json!({})))?;
        assert_eq!(described["revision"], if matches!(mutation, Mutation::Reconfigured | Mutation::RotatedGrant) { "2" } else { "1" });
        let acknowledgment = calls.call(Action::Consume, |context| {
            rpc(context, "__streamAck", &json!({"subscription":"projection","receipt":batch["receipt"]}))
        });
        if matches!(mutation, Mutation::Forbidden(_)) || (matches!(mutation, Mutation::AdvancingClock) && mutation_observed < 130_000) {
            // Denied requests and read-only status cannot acquire durable time
            // authority. A captured sample before expiry keeps its live lease.
            assert_eq!(rolled_back["observation"]["observed_ms"], if matches!(mutation, Mutation::Forbidden(_)) { "100000".to_owned() } else { mutation_observed.to_string() });
            assert_eq!(rolled_back["observation"]["lease_active"], true);
            assert_eq!(acknowledgment?["status"], "acknowledged");
            assert_eq!(calls.status()?["checkpoint"], "1");
            return Ok(());
        }
        assert!(acknowledgment.is_err(), "observed expiry was lost and old acknowledgment succeeded: {acknowledgment:?}; rolled-back status: {rolled_back}");
        assert_eq!(acknowledgment.unwrap_err().downcast_ref::<DeliveryError>().unwrap().code, ErrorCode::LeaseExpired);
        assert_eq!(rolled_back["observation"]["observed_ms"], mutation_observed.to_string());
        assert_eq!(rolled_back["observation"]["lease_active"], false);
        assert_eq!(calls.status()?["checkpoint"], "0");
        calls.call(Action::Read, |context| {
            let records = rpc(context, "__streamRead", &json!({"max_records":10,"max_bytes":16_384}))?;
            assert_eq!(records["records"], batch["records"]);
            Ok(())
        })?;
        Ok::<_, anyhow::Error>(())
    }).await;
    domain.shutdown().await;
    result?
}

#[tokio::test]
async fn rejected_batch_preserves_observed_expiry_across_clock_rollback() -> Result<()> {
    run(Mutation::RejectedBatch).await
}

#[tokio::test]
async fn protected_capacity_refusal_preserves_observed_expiry_across_clock_rollback() -> Result<()>
{
    run(Mutation::ProtectedCapacity).await
}

#[tokio::test]
async fn duplicate_append_preserves_observed_expiry_across_clock_rollback() -> Result<()> {
    run(Mutation::Duplicate).await
}

#[tokio::test]
async fn failed_configuration_preserves_observed_expiry_across_clock_rollback() -> Result<()> {
    run(Mutation::FailedConfiguration).await
}

#[tokio::test]
async fn reconfiguration_preserves_observed_expiry_across_clock_rollback() -> Result<()> {
    run(Mutation::Reconfigured).await
}

#[tokio::test]
async fn retirement_refusal_preserves_observed_expiry_across_clock_rollback() -> Result<()> {
    run(Mutation::RefusedRetirement).await
}

#[tokio::test]
async fn configuration_receipt_survives_own_grant_rotation_with_durable_expiry() -> Result<()> {
    run(Mutation::RotatedGrant).await
}

#[tokio::test]
async fn unauthorized_mutations_cannot_advance_the_durable_stream_clock() -> Result<()> {
    for action in [Action::Append, Action::Configure, Action::Administer] {
        run(Mutation::Forbidden(action)).await?;
    }
    Ok(())
}

#[tokio::test]
async fn scheduling_cannot_forget_a_later_expiry_sample_within_one_mutation() -> Result<()> {
    run(Mutation::AdvancingClock).await
}

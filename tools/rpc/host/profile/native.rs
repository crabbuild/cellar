// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Generated native authoring and independent semantic checks of observed bytes.
use super::*;
use rpc::prost::Message;

#[allow(dead_code)]
#[path = "../../fixtures/profile-generated/rust/cellar.rpc.conformance.v1.rs"]
mod generated;
use generated::{all_types, AllTypes, ProfileClient, ProfileHandler, RuntimeMappings};

struct Echo;

impl ProfileHandler for Echo {
    fn echo(self: Arc<Self>, _: Context, request: AllTypes) -> rpc::HandlerFuture<AllTypes> {
        Box::pin(async move { Ok(request) })
    }

    fn ping(self: Arc<Self>, _: Context, _: ()) -> rpc::HandlerFuture<()> {
        Box::pin(async { Ok(()) })
    }

    fn echo_child(
        self: Arc<Self>,
        _: Context,
        request: all_types::Child,
    ) -> rpc::HandlerFuture<all_types::Child> {
        Box::pin(async move { Ok(request) })
    }

    fn echo_timestamp(
        self: Arc<Self>,
        _: Context,
        request: rpc::prost_types::Timestamp,
    ) -> rpc::HandlerFuture<rpc::prost_types::Timestamp> {
        Box::pin(async move { Ok(request) })
    }

    fn echo_duration(
        self: Arc<Self>,
        _: Context,
        request: rpc::prost_types::Duration,
    ) -> rpc::HandlerFuture<rpc::prost_types::Duration> {
        Box::pin(async move { Ok(request) })
    }

    fn echo_mappings(
        self: Arc<Self>,
        _: Context,
        request: RuntimeMappings,
    ) -> rpc::HandlerFuture<RuntimeMappings> {
        Box::pin(async move { Ok(request) })
    }
}

pub(super) fn application(contract: Arc<rpc::Contract>) -> Result<Arc<dyn Application>> {
    Ok(Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
            .cell("RustCounter", |_, _| {
                Ok(Arc::new(generated::ProfileRegistration::register(
                    rpc::Router::new(),
                    Arc::new(Echo),
                )?))
            })?
            .rpc_service("RustCounter", SERVICE, contract)?,
    ))
}

pub(super) async fn call(context: &Context, binding: &str, vector: &Vector) -> Result<()> {
    let client = ProfileClient::bind(context, binding, &format!("profile-{}", vector.id))?;
    let limits = rpc::Limits::default();
    let bytes = vector.bytes.as_slice();
    let reply = match vector.method.as_str() {
        "Echo" => rpc::encode_message(client.echo(AllTypes::decode(bytes)?).await?, limits)?,
        "Ping" => rpc::encode_message(client.ping(<()>::decode(bytes)?).await?, limits)?,
        "EchoChild" => rpc::encode_message(
            client.echo_child(all_types::Child::decode(bytes)?).await?,
            limits,
        )?,
        "EchoTimestamp" => rpc::encode_message(
            client
                .echo_timestamp(rpc::prost_types::Timestamp::decode(bytes)?)
                .await?,
            limits,
        )?,
        "EchoDuration" => rpc::encode_message(
            client
                .echo_duration(rpc::prost_types::Duration::decode(bytes)?)
                .await?,
            limits,
        )?,
        "EchoMappings" => rpc::encode_message(
            client
                .echo_mappings(RuntimeMappings::decode(bytes)?)
                .await?,
            limits,
        )?,
        _ => anyhow::bail!("unknown profile method"),
    };
    equivalent(vector, &reply)
}

fn same<T: Message + Default + PartialEq>(expected: &[u8], actual: &[u8]) -> Result<()> {
    ensure!(
        T::decode(expected)? == T::decode(actual)?,
        "profile message semantics changed"
    );
    Ok(())
}

pub(super) fn equivalent(vector: &Vector, actual: &[u8]) -> Result<()> {
    let bytes = vector.bytes.as_slice();
    match vector.method.as_str() {
        "Echo" => {
            let mut expected = AllTypes::decode(bytes)?;
            let mut actual = AllTypes::decode(actual)?;
            // These two corpus fields deliberately include NaN. Compare their
            // meaning before PartialEq; NaN payload bits are not an API contract.
            ensure!(
                expected.float32 == actual.float32
                    || (expected.float32.is_nan() && actual.float32.is_nan()),
                "float32 changed"
            );
            ensure!(
                expected.float64 == actual.float64
                    || (expected.float64.is_nan() && actual.float64.is_nan()),
                "float64 changed"
            );
            expected.float32 = 0.0;
            actual.float32 = 0.0;
            expected.float64 = 0.0;
            actual.float64 = 0.0;
            ensure!(expected == actual, "profile AllTypes semantics changed");
            Ok(())
        }
        "Ping" => same::<()>(bytes, actual),
        "EchoChild" => same::<all_types::Child>(bytes, actual),
        "EchoTimestamp" => same::<rpc::prost_types::Timestamp>(bytes, actual),
        "EchoDuration" => same::<rpc::prost_types::Duration>(bytes, actual),
        "EchoMappings" => same::<RuntimeMappings>(bytes, actual),
        _ => anyhow::bail!("unknown profile method"),
    }
    .with_context(|| format!("profile vector {}", vector.id))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vector(name: &str) -> Vector {
        let vectors: Vec<Vector> =
            serde_json::from_str(include_str!("../../fixtures/profile-vectors.json")).unwrap();
        vectors
            .into_iter()
            .find(|vector| vector.id == name)
            .unwrap()
    }

    #[test]
    fn native_profile_observer_detects_lost_presence_and_selected_defaults() {
        let expected = vector("present-zero");
        let changed = AllTypes::default().encode_to_vec();
        assert!(equivalent(&expected, &changed).is_err());
    }

    #[test]
    fn native_profile_observer_detects_a_lost_nan() {
        let expected = vector("nan-floats");
        let mut changed = AllTypes::decode(expected.bytes.as_slice()).unwrap();
        changed.float32 = 0.0;
        assert!(equivalent(&expected, &changed.encode_to_vec()).is_err());
    }

    #[test]
    fn native_profile_observer_detects_a_lost_zero_wrapper() {
        let expected = vector("wrapper-presence");
        let mut changed = RuntimeMappings::decode(expected.bytes.as_slice()).unwrap();
        changed.wrapped64 = None;
        assert!(equivalent(&expected, &changed.encode_to_vec()).is_err());
    }
}

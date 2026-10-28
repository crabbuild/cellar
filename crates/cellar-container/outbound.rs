// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The wire selects declared bindings or local exports, never a target script.

use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::{
    native::clone,
    sdk::{service::Fetcher, Context},
};
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ServiceTarget {
    Binding { binding: String },
    Export { name: String, props: Option<String> },
}

impl ServiceTarget {
    fn properties(&self, maximum: usize) -> Result<Option<clone::StructuredValue>> {
        let Self::Export {
            props: Some(props), ..
        } = self
        else {
            return Ok(None);
        };
        ensure!(
            props.len() <= maximum.div_ceil(3).saturating_mul(4),
            "service props exceed the bridge budget"
        );
        Ok(Some(clone::decode_with_limits(
            &STANDARD.decode(props)?,
            clone::Limits {
                bytes: maximum,
                objects: 100_000,
                values: 200_000,
                depth: 64,
            },
        )?))
    }

    fn validate(&self, maximum: usize) -> Result<()> {
        let name = match self {
            Self::Binding { binding } => binding,
            Self::Export { name, .. } => name,
        };
        ensure!(
            !name.is_empty() && name.len() <= 512 && !name.contains('\0'),
            "invalid declared service name"
        );
        self.properties(maximum)?;
        Ok(())
    }

    /// Resolve against the executing application's declared authority. Props
    /// are copied clone data; application JSON cannot choose a deployment.
    pub fn resolve(&self, context: &Context, maximum: usize) -> Result<Fetcher> {
        self.validate(maximum)?;
        match self {
            Self::Binding { binding } => context.service(binding),
            Self::Export { name, .. } => {
                let route = context.exported_entrypoint(name.clone())?;
                match self.properties(maximum)? {
                    Some(props) => route.with_props(&props),
                    None => Ok(route),
                }
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Intercept {
    target: Value,
    service: ServiceTarget,
}

fn input(value: Value, maximum: usize) -> Result<Intercept> {
    let input: Intercept = serde_json::from_value(value)?;
    if !input.target.is_null() {
        let target = input
            .target
            .as_str()
            .context("intercept target must be a string or null")?;
        cellar::container::HttpInterceptTarget::parse(target)?;
    }
    input.service.validate(maximum)?;
    Ok(input)
}

pub(crate) fn validate(value: &Value, maximum: usize) -> Result<()> {
    input(value.clone(), maximum).map(|_| ())
}

pub(crate) async fn execute(context: &Context, value: Value, maximum: usize) -> Result<Value> {
    let (input, container, route) = (|| -> Result<_> {
        let input = input(value, maximum)?;
        let container = context.container()?;
        ensure!(
            container.supports_outbound_http(context)?,
            "native HTTP interception profile is unavailable"
        );
        let route = input.service.resolve(context, maximum)?;
        Ok((input, container, route))
    })()
    .context(crate::Validation)?;
    if let Some(target) = input.target.as_str() {
        container
            .intercept_outbound_http(context, target, &route)
            .await?;
    } else {
        container
            .intercept_all_outbound_http(context, &route)
            .await?;
    }
    Ok(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn interception_accepts_only_bounded_declared_routes_and_explicit_targets() {
        for value in [
            json!({"service":{"kind":"binding","binding":"API"}}),
            json!({"target":[],"service":{"kind":"binding","binding":"API"}}),
            json!({"target":"http://bad/path","service":{"kind":"binding","binding":"API"}}),
            json!({"target":"good.test","service":{"kind":"binding","binding":"API","script":"forged"}}),
            json!({"target":"good.test","service":{"kind":"script","script":"forged","generation":1}}),
            json!({"target":"good.test","service":{"kind":"export","name":"Gateway","props":"invalid clone bytes"}}),
        ] {
            assert!(
                validate(&value, 4096).is_err(),
                "accepted a forged or invalid route"
            );
        }
        validate(
            &json!({"target":null,"service":{"kind":"binding","binding":"API"}}),
            4096,
        )
        .unwrap();
        validate(
            &json!({"target":"*.example.test","service":{"kind":"export","name":"Gateway"}}),
            4096,
        )
        .unwrap();
    }
}

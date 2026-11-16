// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::consensus::CellDispatchError;
use crate::raft::completion::RuntimeError;

#[test]
fn configuration_probe_does_not_hide_authority_availability_or_unknown_outcomes() {
    for error in [
        anyhow::Error::from(RuntimeError::NotFound),
        anyhow::Error::from(CellDispatchError::Runtime(RuntimeError::NotFound)),
        anyhow::Error::from(std::sync::Arc::new(CellDispatchError::Runtime(
            RuntimeError::NotFound,
        ))),
        super::super::error::error(super::super::ErrorCode::NotInitialized, "absent"),
    ] {
        assert!(missing_configuration(&error.context("current catalog")));
    }
    for error in [
        anyhow::Error::from(RuntimeError::DeadlineExceeded),
        anyhow::Error::from(RuntimeError::NotReady),
        super::super::error::error(super::super::ErrorCode::Forbidden, "revoked"),
        super::super::error::error(super::super::ErrorCode::Unavailable, "no quorum"),
        super::super::error::error(super::super::ErrorCode::Ambiguous, "unknown"),
        super::super::error::error(super::super::ErrorCode::DestinationDeleted, "retired"),
    ] {
        assert!(!missing_configuration(&error.context("current catalog")));
    }
}

struct Host {
    known: bool,
    replies: std::sync::Mutex<std::collections::VecDeque<(&'static str, Value)>>,
}

impl super::Host for Host {
    fn configuration_known(&self, _principal: &str, _authority: &Authority) -> Result<bool> {
        Ok(self.known)
    }

    fn rpc(
        &self,
        principal: &str,
        authority: native_event_rpc::Authority,
        scope: String,
        name: String,
        method: &'static str,
        _args: Value,
    ) -> HandlerFuture<Value> {
        let reply = (|| -> Result<_> {
            anyhow::ensure!(principal == "app");
            authority.validate_target(&scope, Some(&name), method)?;
            let mut replies = self.replies.lock().unwrap();
            let expected = replies
                .front()
                .map(|(method, _)| *method)
                .ok_or_else(|| anyhow::anyhow!("unexpected activation call {method}"))?;
            anyhow::ensure!(method == expected, "expected {expected}, observed {method}");
            let (_, value) = replies
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("activation reply disappeared"))?;
            Ok(value)
        })();
        Box::pin(async move { reply })
    }
}

fn definition() -> Definition {
    Definition {
        resource: super::super::Resource {
            tenant: "tenant".into(),
            application: "app".into(),
            id: "01010101010101010101010101010101".into(),
            name: "events".into(),
            family: "events".into(),
        },
        revision: 1,
        retention: super::super::Retention::Bounded,
        max_bytes: 1024,
        max_records: 10,
        max_age_ms: 1000,
        grants: vec![super::super::Grant {
            id: "owner".into(),
            principal: "app".into(),
            publish: false,
            read: false,
            subscriptions: vec![],
            administer: true,
        }],
        subscriptions: vec![],
    }
}

fn binding_declaration(named: bool) -> (super::super::Binding, BindingConfig) {
    let binding = super::super::Binding {
        environment: "EVENTS".into(),
        resource: definition().resource,
        principal: "app".into(),
        grant: "owner".into(),
        publish: false,
        read: true,
        subscriptions: vec![],
        administer: false,
    };
    let mut declaration = BindingConfig::from(binding.clone());
    if named {
        declaration.resource.id = None;
    }
    (binding, declaration)
}

#[tokio::test]
async fn named_adoption_uses_its_current_catalog_proof_but_still_checks_the_partition() -> Result<()>
{
    let (binding, named) = binding_declaration(true);
    let active = json!({"state":"active","resource":binding.resource,"revision":"1"});
    // Name resolution already proved current catalog lifecycle and all captured
    // grant capabilities. A second catalog read is unavailable in this host.
    let host = Host {
        known: false,
        replies: std::sync::Mutex::new(
            vec![
                (super::super::name_resolution::METHOD, active.clone()),
                ("__streamResolve", active.clone()),
            ]
            .into(),
        ),
    };
    assert_eq!(
        configure(&host, &[], &[named]).await?,
        vec![binding.clone()]
    );
    assert!(host.replies.lock().unwrap().is_empty());
    // An explicit UUID supplies no catalog proof. It cannot use the same
    // shortcut; both independently authorized authorities must still be read.
    let (_, explicit) = binding_declaration(false);
    let missing_catalog = Host {
        known: false,
        replies: std::sync::Mutex::new(vec![("__streamResolve", active.clone())].into()),
    };
    assert!(configure(&missing_catalog, &[], &[explicit.clone()])
        .await
        .is_err());
    let explicit_host = Host {
        known: false,
        replies: std::sync::Mutex::new(
            vec![
                ("__catalogResolve", active.clone()),
                ("__streamResolve", active),
            ]
            .into(),
        ),
    };
    assert_eq!(
        configure(&explicit_host, &[], &[explicit]).await?,
        vec![binding]
    );
    assert!(explicit_host.replies.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn catalog_adoption_never_replaces_partition_lifecycle_or_resource_identity() -> Result<()> {
    for named in [false, true] {
        let (binding, declaration) = binding_declaration(named);
        let active = json!({"state":"active","resource":binding.resource,"revision":"1"});
        for field in ["state", "resource"] {
            let mut refused = active.clone();
            if field == "state" {
                refused["state"] = json!("retiring");
            } else {
                refused["resource"]["id"] = json!("f".repeat(32));
            }
            let catalog_method = if named {
                super::super::name_resolution::METHOD
            } else {
                "__catalogResolve"
            };
            let host = Host {
                known: false,
                replies: std::sync::Mutex::new(
                    vec![
                        (catalog_method, active.clone()),
                        ("__streamResolve", refused),
                    ]
                    .into(),
                ),
            };
            assert!(configure(&host, &[], &[declaration.clone()]).await.is_err());
            assert!(
                host.replies.lock().unwrap().is_empty(),
                "partition check was skipped"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn configuration_hint_changes_cost_without_replacing_current_proof() -> Result<()> {
    let definition = definition();
    let active = json!({"state":"active", "operation":super::super::raft::operation("app", &definition)?, "resource":definition.resource.id, "accepted_revision":"1", "accepted_digest":hex::encode(Sha256::digest(serde_json::to_vec(&definition)?))});
    let mut retired = active.clone();
    retired["state"] = json!("deleted");
    let mut superseded = active.clone();
    superseded["accepted_revision"] = json!("2");
    for (known, replies, accepted) in [
        (
            false,
            vec![
                ("__catalogConfigure", active.clone()),
                ("__catalogStatus", active.clone()),
            ],
            true,
        ),
        (
            false,
            vec![
                ("__catalogConfigure", active.clone()),
                ("__catalogStatus", retired.clone()),
            ],
            false,
        ),
        (true, vec![("__catalogStatus", active.clone())], true),
        (
            true,
            vec![
                ("__catalogStatus", retired.clone()),
                ("__catalogConfigure", active.clone()),
                ("__catalogStatus", retired),
            ],
            false,
        ),
        (
            true,
            vec![
                ("__catalogStatus", superseded.clone()),
                ("__catalogConfigure", active.clone()),
                ("__catalogStatus", superseded),
            ],
            false,
        ),
    ] {
        let host = Host {
            known,
            replies: std::sync::Mutex::new(replies.into()),
        };
        let result = configure(&host, &[("app".into(), definition.clone())], &[]).await;
        assert_eq!(result.is_ok(), accepted, "{result:?}");
        if !accepted {
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("event resource does not match the current deployment definition"));
        }
        assert!(host.replies.lock().unwrap().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn pending_configuration_polls_current_state_without_reissuing_mutation() -> Result<()> {
    let definition = definition();
    let operation = super::super::raft::operation("app", &definition)?;
    let active = json!({"state":"active", "operation":operation, "resource":definition.resource.id, "accepted_revision":"1", "accepted_digest":hex::encode(Sha256::digest(serde_json::to_vec(&definition)?))});
    let mut pending = active.clone();
    pending["state"] = json!("creating");
    pending["accepted_revision"] = Value::Null;
    pending["accepted_digest"] = Value::Null;
    let mut foreign = pending.clone();
    foreign["operation"] = json!("02020202020202020202020202020202");
    for (status, accepted) in [(pending.clone(), true), (foreign, false)] {
        let mut replies = vec![
            ("__catalogConfigure", pending.clone()),
            ("__catalogStatus", status),
        ];
        if accepted {
            replies.push(("__catalogStatus", active.clone()));
        }
        let host = Host {
            known: false,
            replies: std::sync::Mutex::new(replies.into()),
        };
        let domain = std::sync::Arc::new(crate::asyncrt::ExecutionDomain::new(
            tokio::runtime::Handle::current(),
        ));
        let definitions = [("app".into(), definition.clone())];
        let result = domain.run(configure(&host, &definitions, &[])).await;
        domain.shutdown().await;
        let result = result?;
        assert_eq!(result.is_ok(), accepted, "{result:?}");
        if !accepted {
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("event resource does not match the current deployment definition"));
        }
        assert!(host.replies.lock().unwrap().is_empty());
    }
    Ok(())
}

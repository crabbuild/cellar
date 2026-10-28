// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
};
use cellar_admin::{AuditStore, Console, Fleet, FleetInfo, Permission, Principal};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use tower::ServiceExt;

const TOKEN: &str = "test-console-token-with-32-or-more-bytes";
const CONFIGURE_TOKEN: &str = "configure-test-token-with-32-or-more-bytes";

#[tokio::test]
async fn filesystem_metrics_require_data_read_and_preserve_unavailability() {
    let (_dir, app, domain) = fixture().await;
    let endpoint = "/api/v1/fleets/alpha/cells/Customer:one/filesystem";
    for (token, path, expected) in [
        (None, endpoint, StatusCode::UNAUTHORIZED),
        (Some(TOKEN), endpoint, StatusCode::FORBIDDEN),
        (Some(CONFIGURE_TOKEN), endpoint, StatusCode::FORBIDDEN),
        (
            Some("write-only-test-token-with-32-or-more-bytes"),
            endpoint,
            StatusCode::FORBIDDEN,
        ),
        (
            Some("editor-test-token-with-32-or-more-bytes"),
            "/api/v1/fleets/beta/cells/Customer:one/filesystem",
            StatusCode::FORBIDDEN,
        ),
        (
            Some("editor-test-token-with-32-or-more-bytes"),
            "/api/v1/fleets/alpha/cells/Customer/filesystem",
            StatusCode::BAD_REQUEST,
        ),
        (
            Some("editor-test-token-with-32-or-more-bytes"),
            endpoint,
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    ] {
        let mut request = Request::builder().uri(path);
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        let reply = app
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(reply.status(), expected);
        if expected == StatusCode::SERVICE_UNAVAILABLE {
            let bytes = reply.into_body().collect().await.unwrap().to_bytes();
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(value, json!({"error":"Filesystem observation unavailable"}));
        }
    }
    domain.shutdown().await;
}

async fn fixture() -> (
    tempfile::TempDir,
    Router,
    std::sync::Arc<cellar::sdk::ExecutionDomain>,
) {
    fixture_with_cells(5).await
}

async fn fixture_with_cells(
    cell_count: usize,
) -> (
    tempfile::TempDir,
    Router,
    std::sync::Arc<cellar::sdk::ExecutionDomain>,
) {
    let domain = std::sync::Arc::new(cellar::sdk::ExecutionDomain::new(
        tokio::runtime::Handle::current(),
    ));
    let (dir, router) = domain
        .run(async {
            let dir = tempfile::tempdir().unwrap();
            let mut fleets = BTreeMap::new();
            for id in ["alpha", "beta"] {
                let store =
                    cellar::dev::open_local_bucket(&dir.path().join(format!("{id}.sqlite")))
                        .unwrap();
                for index in 0..if id == "alpha" { cell_count } else { 5 } {
                    store
                        .put(
                            &format!("cells/Customer:{index}/own.json"),
                            serde_json::to_vec(&json!({"node":format!("{id}-node"),"epoch":2}))
                                .unwrap(),
                        )
                        .await
                        .unwrap();
                }
                fleets.insert(
                    id.into(),
                    Fleet {
                        info: FleetInfo {
                            id: id.into(),
                            name: id.into(),
                            environment: "test".into(),
                        },
                        client: cellar::management::FleetClient::new(store, false),
                    },
                );
            }
            let principal = Principal::new(
                "viewer".into(),
                TOKEN,
                BTreeMap::from([("alpha".into(), vec![Permission::MetadataRead])]),
            )
            .unwrap();
            let router = cellar_admin::router(Console {
                domain: domain.clone(),
                fleets,
                principals: vec![
                    principal,
                    Principal::new(
                        "operator".into(),
                        "operator-test-token-with-32-or-more-bytes",
                        BTreeMap::from([(
                            "alpha".into(),
                            vec![
                                Permission::MetadataRead,
                                Permission::DataRead,
                                Permission::Operate,
                            ],
                        )]),
                    )
                    .unwrap(),
                    Principal::new(
                        "editor".into(),
                        "editor-test-token-with-32-or-more-bytes",
                        BTreeMap::from([(
                            "alpha".into(),
                            vec![Permission::DataRead, Permission::DataWrite],
                        )]),
                    )
                    .unwrap(),
                    Principal::new(
                        "configurer".into(),
                        CONFIGURE_TOKEN,
                        BTreeMap::from([("alpha".into(), vec![Permission::Configure])]),
                    )
                    .unwrap(),
                    Principal::new(
                        "writer".into(),
                        "write-only-test-token-with-32-or-more-bytes",
                        BTreeMap::from([("alpha".into(), vec![Permission::DataWrite])]),
                    )
                    .unwrap(),
                ],
                audit: AuditStore::open(&dir.path().join("audit.sqlite")).unwrap(),
                assets: dir.path().join("assets"),
                origins: vec!["http://localhost:4174".into()],
            })
            .unwrap();
            (dir, router)
        })
        .await
        .unwrap();
    (dir, router, domain)
}

async fn get(app: &Router, path: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(path)
                .header("authorization", format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn inventory_is_paged_and_fleet_permissions_apply_before_lookup() {
    let (_dir, app, domain) = fixture().await;
    let (status, page) = get(&app, "/api/v1/fleets/alpha/cells?limit=2").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["items"].as_array().unwrap().len(), 2);
    assert_eq!(page["items"][0]["owner"], "alpha-node");
    assert_eq!(page["coverage"], "partial");
    assert!(page["next_cursor"].is_string());
    let mut count = 2;
    let mut cursor = page["next_cursor"].as_str().map(str::to_string);
    while let Some(token) = cursor {
        // The local provider token is an integer offset, not a cell scope.
        let (_, page) = get(
            &app,
            &format!("/api/v1/fleets/alpha/cells?limit=2&cursor={token}"),
        )
        .await;
        count += page["items"].as_array().unwrap().len();
        cursor = page["next_cursor"].as_str().map(str::to_string);
    }
    assert_eq!(count, 5);
    assert_eq!(
        get(&app, "/api/v1/fleets/beta/cells").await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        get(&app, "/api/v1/fleets/unknown/cells").await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        get(&app, "/api/v1/fleets/alpha/cells?limit=100000").await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        get(&app, "/api/v1/fleets/alpha/cells?prefix=../").await.0,
        StatusCode::BAD_REQUEST
    );
    domain.shutdown().await;
}

#[tokio::test]
async fn authentication_origin_and_data_permissions_are_independent() {
    let (_dir, app, domain) = fixture().await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/session")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/session")
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("origin", "https://untrusted.example")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let (_, session) = get(&app, "/api/v1/session").await;
    assert_eq!(session["fleets"].as_array().unwrap().len(), 1);
    assert!(!session.to_string().contains(TOKEN));
    let request = json!({"application":"app","binding":"DB","operation_id":"read-1","command":{"action":"sql","sql":"SELECT 1"}});
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/fleets/alpha/resources/action")
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(request.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    domain.shutdown().await;
}

#[test]
fn audit_intent_survives_restart_and_is_scoped_to_actor_and_fleet() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("audit.sqlite");
    {
        let store = AuditStore::open(&path).unwrap();
        store
            .begin("request-1", "alpha", "builder", "app/DB", "sql", "hash-a")
            .unwrap();
    }
    let store = AuditStore::open(&path).unwrap();
    assert_eq!(store.list("alpha").unwrap()[0].status, "outcome_unknown");
    store
        .begin("request-1", "alpha", "builder", "app/DB", "sql", "hash-a")
        .unwrap();
    assert_eq!(store.list("alpha").unwrap().len(), 1);
    assert!(store
        .begin(
            "request-1",
            "alpha",
            "builder",
            "app/OTHER",
            "sql",
            "hash-a"
        )
        .is_err());
    assert!(store
        .begin(
            "request-1",
            "alpha",
            "builder",
            "app/DB",
            "delete",
            "hash-a"
        )
        .is_err());
    assert!(store
        .begin("request-1", "alpha", "builder", "app/DB", "sql", "hash-b")
        .is_err());
    store
        .begin("request-1", "beta", "builder", "app/DB", "sql", "hash-b")
        .unwrap();
    assert_eq!(store.list("beta").unwrap().len(), 1);
}

#[tokio::test]
async fn cron_preview_uses_the_cellar_dialect_and_does_not_accept_invalid_weekdays() {
    let (_dir, app, domain) = fixture().await;
    for (expression, expected, satisfiable) in [
        ("*/5 * * * *", StatusCode::OK, Some(true)),
        ("0 0 30 2 *", StatusCode::OK, Some(false)),
        ("0 0 * * 0", StatusCode::BAD_REQUEST, None),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/fleets/alpha/cron/preview")
                    .header("authorization", format!("Bearer {TOKEN}"))
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"expression":expression}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        let value: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        if let Some(satisfiable) = satisfiable {
            assert_eq!(value["satisfiable"], satisfiable);
            let occurrences = value["occurrences"].as_array().unwrap();
            assert_eq!(occurrences.len(), if satisfiable { 5 } else { 0 });
            if satisfiable {
                assert!(occurrences[0].as_u64().unwrap() > value["after"].as_u64().unwrap());
            }
        }
    }
    domain.shutdown().await;
    assert_eq!(
        get(&app, "/api/v1/session").await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn ten_thousand_cells_use_bounded_continuations() {
    let (_dir, app, domain) = fixture_with_cells(10_000).await;
    let started = domain
        .run(async { cellar::asyncrt::mono_ms() })
        .await
        .unwrap();
    let (status, first) = get(&app, "/api/v1/fleets/alpha/cells?limit=100").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["items"].as_array().unwrap().len(), 100);
    assert!(serde_json::to_vec(&first).unwrap().len() < 64 * 1024);
    eprintln!(
        "10,000-cell inventory: first page returned in {} ms",
        domain
            .run(async { cellar::asyncrt::mono_ms() })
            .await
            .unwrap()
            .saturating_sub(started)
    );
    let mut scopes = std::collections::BTreeSet::new();
    let mut page = first;
    let mut pages = 0;
    loop {
        pages += 1;
        assert!(pages <= 100, "continuation loop did not advance");
        let items = page["items"].as_array().unwrap();
        assert!(!items.is_empty() && items.len() <= 100);
        for item in items {
            assert!(
                scopes.insert(item["scope"].as_str().unwrap().to_string()),
                "duplicate cell across pages"
            );
        }
        let Some(cursor) = page["next_cursor"].as_str() else {
            break;
        };
        let (status, next) = get(
            &app,
            &format!("/api/v1/fleets/alpha/cells?limit=100&cursor={cursor}"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        page = next;
    }
    assert_eq!(scopes.len(), 10_000);
    assert_eq!(pages, 100);
    domain.shutdown().await;
}

#[tokio::test]
async fn writable_sql_also_requires_data_read_permission() {
    let (_dir, app, domain) = fixture().await;
    let response=app.oneshot(Request::builder().method("POST").uri("/api/v1/fleets/alpha/resources/action").header("authorization","Bearer write-only-test-token-with-32-or-more-bytes").header("content-type","application/json").body(Body::from(json!({"application":"app","binding":"DB","operation_id":"write-only","command":{"action":"sql","sql":"SELECT * FROM private","readonly":false}}).to_string())).unwrap()).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    domain.shutdown().await;
}

#[tokio::test]
async fn cron_writes_require_configuration_permission_before_application_lookup() {
    let (_dir, app, domain) = fixture().await;
    let response = app.oneshot(Request::builder().method("POST")
        .uri("/api/v1/fleets/alpha/applications/missing/cron")
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .body(Body::from(json!({"operation_id":"cron-1","deployment_revision":"missing","expected_revision":null,"expressions":[]}).to_string())).unwrap()).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    domain.shutdown().await;
}

#[tokio::test]
async fn cron_api_persists_acceptance_and_deduplicates_without_repeating_the_write() {
    let (dir, app, domain) = fixture().await;
    domain.run(async {
        let store = cellar::dev::open_local_bucket(&dir.path().join("alpha.sqlite")).unwrap();
        let config: cellar::native_deploy::ApplicationConfig = serde_json::from_value(json!({"script_name":"scheduler","application_version":"v1","crons":["0 * * * *"]})).unwrap();
        let manifest = config.manifest(vec![cellar::protocol::NativeArtifact {
            target: cellar::native_deploy::current_target().into(), sha256: "a".repeat(64), bytes: 1,
        }]).unwrap();
        let prefix = format!("deploy/scheduler/{}", manifest.version);
        store.put(&format!("{prefix}/manifest.json"), serde_json::to_vec(&manifest).unwrap()).await.unwrap();
        let pointer = json!({"script_name":"scheduler","version":manifest.version,"prefix":prefix,"rollout":{"percent":100}});
        for key in ["deploy/scheduler/current.json", "deploy/current.json"] {
            store.put(key,serde_json::to_vec(&pointer).unwrap()).await.unwrap();
        }
    }).await.unwrap();
    let path = "/api/v1/fleets/alpha/applications/scheduler/cron";
    let (_, before) = get(&app, path).await;
    let mut body = json!({"operation_id":"save-cron","deployment_revision":before["deployment_revision"],"expected_revision":null,"expressions":[]});
    for replayed in [false, true] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("authorization", format!("Bearer {CONFIGURE_TOKEN}"))
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let result: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(result["operation"]["status"], "accepted");
        assert_eq!(result["replayed"] == true, replayed);
        if !replayed {
            assert_eq!(result["propagation"], "pending");
        }
    }
    let (_, after) = get(&app, path).await;
    assert_eq!(after["expressions"], json!([]));
    assert!(after["revision"].is_string());
    body["operation_id"] = json!("stale-cron");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("authorization", format!("Bearer {CONFIGURE_TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let rejected: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert!(rejected["error"]
        .as_str()
        .unwrap()
        .contains("configuration changed"));
    assert_eq!(get(&app, path).await.1["revision"], after["revision"]);
    let (_, operations) = get(&app, "/api/v1/fleets/alpha/operations").await;
    assert_eq!(operations["items"].as_array().unwrap().len(), 2);
    domain.shutdown().await;
}

#[tokio::test]
async fn stop_receipt_reconciles_only_its_actor_target_and_joined_proof() {
    use cellar::management::{StopMode, StopProof, StopReceipt};
    use sha2::{Digest, Sha256};
    let (dir, app, domain) = fixture().await;
    let audit = AuditStore::open(&dir.path().join("audit.sqlite")).unwrap();
    audit
        .begin(
            "stop-1",
            "alpha",
            "viewer",
            "node/node-1@generation-1",
            "stop",
            "request",
        )
        .unwrap();
    let mut operation = audit.list("alpha").unwrap().remove(0);
    audit
        .begin(
            "stop-1",
            "alpha",
            "another-actor",
            "node/node-1@generation-1",
            "stop",
            "request",
        )
        .unwrap();
    audit
        .begin(
            "stop-2",
            "alpha",
            "viewer",
            "node/node-2@generation-1",
            "stop",
            "request",
        )
        .unwrap();
    let receipt_id = format!("{:x}", Sha256::digest(b"alpha\0viewer\0stop-1"));
    let key = format!("management/stops/node-1/generation-1/{receipt_id}.json");
    let mut receipt = StopReceipt {
        version: 1,
        node: "node-1".into(),
        generation: "generation-1".into(),
        operation_id: receipt_id,
        requested_mode: StopMode::Handoff,
        phase: "joining_domain".into(),
        sequence: 2,
        requested_at: 1,
        observed_at: 2,
        domain_joined: false,
        error: None,
        proof: Some(StopProof {
            raft: cellar::management::RaftStopProof::NotRegistered,
            mode: StopMode::Handoff,
            ingress_drained: true,
            application_work_drained: true,
            pending_application_events: 0,
            cells_drained: true,
            pending_dispatches: 0,
            log_quiesced: true,
            local_durability_closed: true,
            clean_reload_prepared: false,
            drain_timed_out: false,
            deadline_elapsed: false,
        }),
    };
    let bucket = domain
        .run(async { cellar::dev::open_local_bucket(&dir.path().join("alpha.sqlite")).unwrap() })
        .await
        .unwrap();
    domain
        .run(bucket.put(&key, serde_json::to_vec(&receipt).unwrap()))
        .await
        .unwrap()
        .unwrap();
    let path = "/api/v1/fleets/alpha/nodes/node-1/stops/generation-1/viewer/stop-1";
    assert_eq!(get(&app, path).await.0, StatusCode::OK);
    assert_eq!(
        audit
            .list("alpha")
            .unwrap()
            .iter()
            .find(|op| op.actor == "viewer" && op.id == "stop-1")
            .unwrap()
            .status,
        "outcome_unknown"
    );
    assert_eq!(
        get(
            &app,
            "/api/v1/fleets/alpha/nodes/node-1/stops/generation-1/another-actor/stop-1"
        )
        .await
        .1["receipt"],
        Value::Null
    );
    assert_eq!(
        get(
            &app,
            "/api/v1/fleets/beta/nodes/node-1/stops/generation-1/viewer/stop-1"
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    receipt.domain_joined = true;
    receipt.phase = "complete".into();
    receipt.sequence = u64::MAX;
    domain
        .run(bucket.put(&key, serde_json::to_vec(&receipt).unwrap()))
        .await
        .unwrap()
        .unwrap();
    let (status, result) = get(&app, path).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["receipt"]["domain_joined"], true);
    // Late HTTP acceptance cannot overwrite proof in the row or response.
    audit.finish(&mut operation, "accepted").unwrap();
    assert_eq!(operation.status, "succeeded");
    let entries = audit.list("alpha").unwrap();
    assert_eq!(
        entries.iter().filter(|op| op.status == "succeeded").count(),
        1
    );
    assert_eq!(
        entries
            .iter()
            .filter(|op| op.status == "outcome_unknown")
            .count(),
        2
    );
    // An inconsistent terminal document cannot be used to manufacture proof.
    receipt.proof.as_mut().unwrap().application_work_drained = false;
    domain
        .run(bucket.put(&key, serde_json::to_vec(&receipt).unwrap()))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(get(&app, path).await.0, StatusCode::OK);
    domain.shutdown().await;
}

#[tokio::test]
async fn conditional_kv_edits_require_read_and_write_permissions_before_lookup() {
    let (_dir, app, domain) = fixture().await;
    for command in [
        json!({"action":"kv_edit","key":"key","edit":{"value_base64":"AA==","tag":"bytes","metadata":null,"expiration":{"mode":"preserve"},"condition":{"revision":"1"}}}),
        json!({"action":"kv_delete_if","key":"key","condition":{"revision":"1"}}),
    ] {
        for token in [TOKEN, "write-only-test-token-with-32-or-more-bytes"] {
            let response = app.clone().oneshot(Request::builder().method("POST")
                .uri("/api/v1/fleets/alpha/resources/action")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(json!({"application":"missing","binding":"KV","operation_id":"conditional","command":command}).to_string())).unwrap()).await.unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        }
    }
    domain.shutdown().await;
}

#[tokio::test]
async fn invalid_conditional_kv_inputs_are_rejected_before_dispatch_or_audit() {
    let (_dir, app, domain) = fixture().await;
    let edit = json!({"value_base64":"AA==","tag":"bytes","metadata":null,"expiration":{"mode":"preserve"},"condition":{"revision":"1"}});
    let mut invalid_metadata = edit.clone();
    invalid_metadata["metadata"] = json!("not JSON");
    let mut invalid_bytes = edit.clone();
    invalid_bytes["value_base64"] = json!("!invalid!");
    for command in [
        json!({"action":"kv_edit","key":"","edit":edit}),
        json!({"action":"kv_edit","key":"key","edit":invalid_metadata}),
        json!({"action":"kv_edit","key":"key","edit":invalid_bytes}),
        json!({"action":"kv_delete_if","key":"key","condition":{"revision":"01"}}),
        json!({"action":"kv_delete_if","key":"","condition":{"revision":"1"}}),
    ] {
        let response=app.clone().oneshot(Request::builder().method("POST").uri("/api/v1/fleets/alpha/resources/action")
            .header("authorization","Bearer editor-test-token-with-32-or-more-bytes")
            .header("content-type","application/json")
            .body(Body::from(json!({"application":"missing","binding":"KV","operation_id":"invalid-edit","expected_identity":"namespace-a","command":command}).to_string())).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    let (_, operations) = get(&app, "/api/v1/fleets/alpha/operations").await;
    assert_eq!(operations["items"], json!([]));
    domain.shutdown().await;
}

#[tokio::test]
async fn conditional_kv_drafts_cannot_cross_a_rebound_namespace() {
    let (dir, app, domain) = fixture().await;
    domain.run(async {
        let store = cellar::dev::open_local_bucket(&dir.path().join("alpha.sqlite")).unwrap();
        let config: cellar::native_deploy::ApplicationConfig = serde_json::from_value(json!({
            "script_name":"app", "application_version":"v1",
            "bindings":[{"type":"kv","name":"KV","id":"namespace-b"}]
        })).unwrap();
        let manifest = config.manifest(vec![cellar::protocol::NativeArtifact {
            target: cellar::native_deploy::current_target().into(), sha256:"a".repeat(64), bytes:1,
        }]).unwrap();
        let prefix = format!("deploy/app/{}", manifest.version);
        store.put(&format!("{prefix}/manifest.json"), serde_json::to_vec(&manifest).unwrap()).await.unwrap();
        store.put("deploy/app/current.json", serde_json::to_vec(&json!({"script_name":"app","version":manifest.version,"prefix":prefix,"rollout":{"percent":100}})).unwrap()).await.unwrap();
    }).await.unwrap();
    for command in [
        json!({"action":"kv_get","key":"key"}),
        json!({"action":"kv_edit","key":"key","edit":{"value_base64":"AA==","tag":"bytes","metadata":null,"expiration":{"mode":"never"},"condition":{"revision":"1"}}}),
        json!({"action":"kv_delete_if","key":"key","condition":{"revision":"1"}}),
    ] {
        let response=app.clone().oneshot(Request::builder().method("POST").uri("/api/v1/fleets/alpha/resources/action")
            .header("authorization","Bearer editor-test-token-with-32-or-more-bytes")
            .header("content-type","application/json")
            .body(Body::from(json!({"application":"app","binding":"KV","operation_id":"rebound","expected_identity":"namespace-a","command":command}).to_string())).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert!(serde_json::from_slice::<Value>(&body).unwrap()["error"]
            .as_str()
            .unwrap()
            .contains("another resource"));
    }
    let (_, operations) = get(&app, "/api/v1/fleets/alpha/operations").await;
    assert_eq!(operations["items"], json!([]));
    domain.shutdown().await;
}

#[tokio::test]
async fn execution_inspection_requires_data_read_before_lookup() {
    let (_dir, app, domain) = fixture().await;
    for fleet in ["alpha", "beta", "missing"] {
        let (status, _) = get(
            &app,
            &format!("/api/v1/fleets/{fleet}/applications/missing/cron/executions"),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }
    let body = json!({"application":"missing","binding":"WF","operation_id":"search","command":{"action":"workflow","instance":"run","command":{"operation":"history_search","query":"step","cursor":0}}});
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/fleets/alpha/resources/action")
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    // Write-only operators cannot use history search to read step inputs.
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/fleets/alpha/resources/action")
                .header(
                    "authorization",
                    "Bearer write-only-test-token-with-32-or-more-bytes",
                )
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    domain.shutdown().await;
}

async fn event_request(app: &Router, path: &str, token: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    // Axum's extractor rejections are plain text; handler responses are JSON.
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| json!({"error": String::from_utf8_lossy(&bytes)}));
    (status, body)
}

#[tokio::test]
async fn event_permissions_and_bounds_apply_before_dispatch() {
    let (_dir, app, domain) = fixture().await;
    let operation = "a".repeat(32);
    let outbox = "/api/v1/fleets/alpha/cells/Customer:one/outbox";
    let stream = "/api/v1/fleets/alpha/streams/missing/EVENTS/action";
    let inspect =
        json!({"operation_id":operation,"action":{"kind":"inspect","limit":25,"cursor":null}});
    assert_eq!(
        event_request(&app, outbox, TOKEN, inspect.clone()).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        event_request(
            &app,
            outbox,
            "editor-test-token-with-32-or-more-bytes",
            json!({"operation_id":operation,"action":{"kind":"inspect","limit":101,"cursor":null}})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let expected = json!({"resource":"b".repeat(32),"name":"projection","id":"c".repeat(32),"generation":"d".repeat(32)});
    let change = json!({"operation_id":operation,"resource_id":"b".repeat(32),"action":{"kind":"change","expected":expected,"action":{"kind":"pause"},"reason":"pause for maintenance"}});
    assert_eq!(
        event_request(
            &app,
            stream,
            "editor-test-token-with-32-or-more-bytes",
            change.clone()
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        event_request(
            &app,
            "/api/v1/fleets/beta/streams/missing/EVENTS/action",
            "operator-test-token-with-32-or-more-bytes",
            change.clone()
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    // A browser may never submit a delegated grant or choose its signed operator.
    let mut forged = change.clone();
    forged["operator"] = json!("administrator");
    assert_eq!(
        event_request(
            &app,
            stream,
            "operator-test-token-with-32-or-more-bytes",
            forged
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let mut reset = change.clone();
    reset["action"]["action"] =
        json!({"kind":"reset","cursor":{"resource":"b".repeat(32),"partition":0,"next":"1"}});
    assert_eq!(
        event_request(
            &app,
            stream,
            "operator-test-token-with-32-or-more-bytes",
            reset
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    // Valid pause intent is persisted before a missing deployment is looked up.
    let (status, first) = event_request(
        &app,
        stream,
        "operator-test-token-with-32-or-more-bytes",
        change.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["operation"]["status"], "outcome_unknown");
    assert!(first["failure"]
        .as_str()
        .unwrap()
        .contains("application not found"));
    let (_, repeated) = event_request(
        &app,
        stream,
        "operator-test-token-with-32-or-more-bytes",
        change.clone(),
    )
    .await;
    assert_eq!(repeated["replayed"], true);
    assert!(repeated.get("failure").is_none());
    let mut changed = change;
    changed["action"]["reason"] = json!("different request");
    assert_eq!(
        event_request(
            &app,
            stream,
            "operator-test-token-with-32-or-more-bytes",
            changed
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    domain.shutdown().await;
}

#[tokio::test]
async fn event_state_and_host_metrics_require_data_read_before_lookup() {
    let (_dir, app, domain) = fixture().await;
    for path in [
        "/api/v1/fleets/alpha/cells/missing:one/event-state",
        "/api/v1/fleets/alpha/nodes/missing/event-metrics?generation=old",
        "/api/v1/fleets/alpha/streams/missing/EVENTS/resources?limit=50",
    ] {
        assert_eq!(get(&app, path).await.0, StatusCode::FORBIDDEN);
    }
    domain.shutdown().await;
}

#[tokio::test]
async fn event_lifecycle_requires_both_operate_and_data_write() {
    let (_dir, app, domain) = fixture().await;
    let id = "a".repeat(32);
    let actions = [
        (
            "/api/v1/fleets/alpha/cells/Customer:one/outbox",
            json!({"operation_id":id,"action":{"kind":"destroy","incarnation":"b".repeat(32),"reason":"remove test source","data_loss_acknowledgment":"discard test data"}}),
        ),
        (
            "/api/v1/fleets/alpha/cells/Customer:one/outbox",
            json!({"operation_id":id,"action":{"kind":"recreate","incarnation":"b".repeat(32),"reason":"start empty"}}),
        ),
        (
            "/api/v1/fleets/alpha/streams/missing/EVENTS/action",
            json!({"operation_id":id,"resource_id":"b".repeat(32),"action":{"kind":"retire","revision":"1","reason":"retire test stream","mode":{"mode":"coordinated","procedure":"producers stopped"}}}),
        ),
    ];
    for (path, body) in actions {
        for token in [
            "operator-test-token-with-32-or-more-bytes",
            "editor-test-token-with-32-or-more-bytes",
        ] {
            assert_eq!(
                event_request(&app, path, token, body.clone()).await.0,
                StatusCode::FORBIDDEN
            );
        }
    }
    assert_eq!(
        get(&app, "/api/v1/fleets/alpha/events/inventory?limit=25")
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        event_request(
            &app,
            "/api/v1/fleets/alpha/cells/Customer:one/event-inventory",
            TOKEN,
            json!({"op":"history","limit":25,"cursor":null})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    domain.shutdown().await;
}

// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::verify_dependencies;
use serde_json::{json, Value};

fn graph(adapter: bool, engine: &str) -> Value {
    json!({"packages":[{"id":"app","name":"app"},{"id":"adapter","name":if adapter {"cellar-javascript"} else {"other"}},{"id":"engine","name":engine}],
    "resolve":{"nodes":[
      {"id":"app","deps":[{"pkg":"adapter","dep_kinds":[{"kind":null}]}]},
      {"id":"adapter","deps":[{"pkg":"engine","dep_kinds":[{"kind":null}]}]},
      {"id":"engine","deps":[]}
    ]}})
}

#[test]
fn javascript_requires_explicit_opt_in_and_adapter() {
    assert!(verify_dependencies(&graph(true, "v8"), "app", false).is_err());
    assert!(verify_dependencies(&graph(true, "v8"), "app", true).is_ok());
    assert!(verify_dependencies(&graph(false, "v8"), "app", true).is_err());
    assert!(verify_dependencies(&graph(true, "deno_core"), "app", true).is_err());
    assert!(verify_dependencies(&graph(true, "serde"), "app", true).is_err());
}

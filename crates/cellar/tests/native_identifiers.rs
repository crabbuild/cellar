//! Cellar identifiers and third-party endpoint recognition.
use cellar::container::{image_key, image_reference};
use cellar_ltx::replica_url::{is_cloudflare_blob_endpoint, is_minio_endpoint};

#[test]
fn provider_endpoints_and_cellar_container_images() {
    let endpoint = "https://account.r2.cloudflarestorage.com";
    assert!(is_cloudflare_blob_endpoint(endpoint));
    assert!(!is_minio_endpoint(&format!("{endpoint}:443")));
    assert!(!is_cloudflare_blob_endpoint(
        "https://account.r2.cloudflarestorage.com.example.net"
    ));
    let digest = "ab".repeat(32);
    let current = image_reference(&digest);
    assert!(current.starts_with("cellar-image:"));
    assert_eq!(image_key(&current), format!("deploy/images/{digest}.tar"));
}

#[test]
fn sql_database_scope_uses_the_cellar_sql_namespace() {
    let scope = cellar::engine_api::sql_cell_scope("ledger");
    assert!(scope.starts_with("__SQLDatabase:"));
    assert_eq!(scope, cellar::engine_api::sql_cell_scope("ledger"));
    assert_ne!(scope, cellar::engine_api::sql_cell_scope("other"));
}

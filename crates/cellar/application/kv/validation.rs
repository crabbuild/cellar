//! Validate KV requests using the caller's captured resource budget and clock.
use super::error;
use crate::application::limits::KvLimits;
use anyhow::{ensure, Result};
use cellar_logic::kv as policy;

pub(super) fn list_limit(requested: Option<i64>, limits: KvLimits) -> usize {
    let maximum = limits.max_list_limit;
    requested
        .filter(|n| *n > 0)
        .map_or(policy::MAX_LIST_LIMIT, |n| n as usize)
        .min(maximum)
}

pub(super) fn check_key(key: &str, limits: KvLimits) -> Result<()> {
    ensure!(!key.is_empty(), "KV_ERROR: a key must not be empty");
    ensure!(
        key.len() <= limits.max_key_bytes,
        "KV_ERROR: a key is at most {} bytes, got {}",
        limits.max_key_bytes,
        key.len()
    );
    Ok(())
}

pub(super) fn check_value(size: usize, limits: KvLimits) -> Result<()> {
    ensure!(
        size <= limits.max_value_bytes,
        "KV_ERROR: a value is at most {} bytes, got {size}",
        limits.max_value_bytes
    );
    Ok(())
}

pub(super) fn check_metadata(metadata: &str, limits: KvLimits) -> Result<()> {
    ensure!(
        metadata.len() <= limits.max_metadata_bytes,
        "KV_ERROR: metadata is at most {} bytes, got {}",
        limits.max_metadata_bytes,
        metadata.len()
    );
    Ok(())
}

pub(super) fn check_bulk(keys: &[String], limits: KvLimits) -> Result<()> {
    ensure!(
        !keys.is_empty(),
        "KV_ERROR: a bulk get needs at least one key"
    );
    ensure!(
        keys.len() <= limits.max_bulk_keys,
        "KV_ERROR: a bulk get takes at most {} keys, got {}",
        limits.max_bulk_keys,
        keys.len()
    );
    for key in keys {
        check_key(key, limits)?;
    }
    Ok(())
}

pub(super) fn expiry(
    now: i64,
    expiration: Option<f64>,
    ttl: Option<f64>,
    floor: i64,
) -> Result<Option<i64>> {
    if let Some(ttl) = ttl {
        let ms = (ttl * 1000.0).floor();
        ensure!(
            ms.is_finite() && ms >= floor as f64 && ms < i64::MAX as f64,
            "KV_ERROR: expirationTtl is at least {} seconds",
            floor as f64 / 1000.0
        );
        return Ok(Some(
            now.checked_add(ms as i64)
                .ok_or_else(|| error("expiration is outside timestamp range"))?,
        ));
    }
    if let Some(expiration) = expiration {
        let at = (expiration * 1000.0).floor();
        ensure!(
            at.is_finite() && at > now as f64 && at < i64::MAX as f64,
            "KV_ERROR: expiration {expiration} is in the past"
        );
        return Ok(Some(at as i64));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use crate::application::{kv, limits};

    #[test]
    fn kv_validation_uses_the_supplied_clock_and_expiry_floor() -> anyhow::Result<()> {
        let limits = limits::KvLimits {
            min_expiration_ttl_ms: 2_000,
            ..Default::default()
        };
        assert!(
            kv::validation::expiry(10_000, None, Some(1.0), limits.min_expiration_ttl_ms).is_err()
        );
        assert_eq!(
            kv::validation::expiry(10_000, None, Some(2.0), limits.min_expiration_ttl_ms)?,
            Some(12_000)
        );
        assert!(kv::validation::expiry(
            i64::MAX - 1,
            None,
            Some(2.0),
            limits.min_expiration_ttl_ms
        )
        .is_err());
        assert!(kv::RevisionCondition {
            revision: Some("01".into())
        }
        .validate()
        .is_err());
        Ok(())
    }
}

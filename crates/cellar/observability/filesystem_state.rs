// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded owner observations. File names, contents and host paths are absent.
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilesystemQuotas {
    #[serde(with = "number")]
    pub max_file_bytes: u64,
    #[serde(with = "number")]
    pub max_logical_bytes: u64,
    #[serde(with = "number")]
    pub max_allocated_bytes: u64,
    #[serde(with = "number")]
    pub max_inodes: u64,
    #[serde(with = "number")]
    pub max_handles: u64,
}

impl From<crate::application::filesystem::FilesystemLimits> for FilesystemQuotas {
    fn from(limits: crate::application::filesystem::FilesystemLimits) -> Self {
        Self {
            max_file_bytes: limits.max_file_bytes,
            max_logical_bytes: limits.max_logical_bytes,
            max_allocated_bytes: limits.max_allocated_bytes,
            max_inodes: limits.max_inodes,
            max_handles: limits.max_handles as u64,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilesystemUsage {
    #[serde(with = "number")]
    pub revision: u64,
    #[serde(with = "number")]
    pub logical_bytes: u64,
    #[serde(with = "number")]
    pub allocated_bytes: u64,
    #[serde(with = "number")]
    pub inodes: u64,
    #[serde(with = "number")]
    pub unlinked_inodes: u64,
    #[serde(with = "number")]
    pub unlinked_logical_bytes: u64,
    /// Retained unlinked contents may still be pinned by an open descriptor.
    #[serde(with = "number")]
    pub unlinked_allocated_bytes: u64,
    #[serde(with = "number")]
    pub discarded_ranges: u64,
    /// Eligible truncated chunks, deduplicated across overlapping ranges.
    #[serde(with = "number")]
    pub discarded_allocated_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilesystemState {
    pub format: u8,
    pub scope: String,
    #[serde(with = "number")]
    pub epoch: u64,
    #[serde(with = "number")]
    pub sampled_ms: u64,
    /// Class policy at this owner; persisted files can remain after disabling it.
    pub enabled: bool,
    pub quotas: FilesystemQuotas,
    /// None means the filesystem has never been initialized in this database.
    #[serde(deserialize_with = "present_usage")]
    pub usage: Option<FilesystemUsage>,
    #[serde(with = "number")]
    pub live_handles: u64,
    #[serde(with = "number")]
    pub pinned_inodes: u64,
}

fn present_usage<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<FilesystemUsage>, D::Error> {
    Option::deserialize(deserializer)
}

impl FilesystemState {
    /// Validate identity and internal accounting before displaying a peer reply.
    pub fn validate(&self, scope: &str) -> Result<()> {
        ensure!(
            self.format == 1
                && self.scope == scope
                && cellar_logic::cell::valid_cell_scope(scope)
                && scope.contains(':')
                && self.epoch > 0
                && self.sampled_ms <= i64::MAX as u64,
            "filesystem observation identity conflicts"
        );
        let quotas = &self.quotas;
        ensure!(
            [
                quotas.max_file_bytes,
                quotas.max_logical_bytes,
                quotas.max_allocated_bytes,
                quotas.max_inodes,
                quotas.max_handles
            ]
            .into_iter()
            .all(|n| n > 0 && n <= i64::MAX as u64)
                && quotas.max_allocated_bytes >= crate::application::filesystem::CHUNK_BYTES as u64
                && quotas.max_handles <= i32::MAX as u64
                && self.pinned_inodes <= self.live_handles
                && self.live_handles <= i32::MAX as u64,
            "invalid filesystem observation limits/handles"
        );
        if let Some(usage) = &self.usage {
            ensure!(
                usage.revision > 0
                    && usage.inodes > 0
                    && [
                        usage.revision,
                        usage.logical_bytes,
                        usage.allocated_bytes,
                        usage.inodes,
                        usage.unlinked_inodes,
                        usage.unlinked_logical_bytes,
                        usage.unlinked_allocated_bytes,
                        usage.discarded_ranges,
                        usage.discarded_allocated_bytes
                    ]
                    .into_iter()
                    .all(|n| n <= i64::MAX as u64)
                    && usage.unlinked_inodes < usage.inodes
                    && self.pinned_inodes <= usage.inodes
                    && usage.unlinked_logical_bytes <= usage.logical_bytes
                    && usage
                        .unlinked_allocated_bytes
                        .checked_add(usage.discarded_allocated_bytes)
                        .is_some_and(|n| n <= usage.allocated_bytes),
                "invalid filesystem observation accounting"
            );
        } else {
            ensure!(
                self.live_handles == 0 && self.pinned_inodes == 0,
                "uninitialized filesystem has live handles"
            );
        }
        // A lowered quota can legitimately be below persisted usage. Report
        // that pressure rather than turning an over-quota cell into zero use.
        Ok(())
    }
}

mod number {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(value)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        let value = String::deserialize(deserializer)?;
        let number: u64 = value.parse().map_err(serde::de::Error::custom)?;
        if value != number.to_string() {
            return Err(serde::de::Error::custom(
                "noncanonical filesystem observation integer",
            ));
        }
        Ok(number)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> FilesystemState {
        FilesystemState {
            format: 1,
            scope: "Account:one".into(),
            epoch: 9_007_199_254_740_993,
            sampled_ms: 100,
            enabled: true,
            quotas: crate::application::filesystem::FilesystemLimits::default().into(),
            usage: None,
            live_handles: 0,
            pinned_inodes: 0,
        }
    }

    #[test]
    fn observations_preserve_exact_integers_and_require_explicit_absence() -> Result<()> {
        let original = sample();
        let value = serde_json::to_value(&original)?;
        assert_eq!(value["epoch"], "9007199254740993");
        let decoded: FilesystemState = serde_json::from_value(value.clone())?;
        decoded.validate("Account:one")?;
        assert_eq!(decoded.epoch, original.epoch);
        let mut missing = value.clone();
        missing.as_object_mut().unwrap().remove("usage");
        assert!(serde_json::from_value::<FilesystemState>(missing).is_err());
        for epoch in [
            serde_json::json!(2),
            serde_json::json!("02"),
            serde_json::json!("-1"),
        ] {
            let mut invalid = value.clone();
            invalid["epoch"] = epoch;
            assert!(serde_json::from_value::<FilesystemState>(invalid).is_err());
        }
        assert!(decoded.validate("Account:other").is_err());
        Ok(())
    }

    #[test]
    fn over_quota_is_reportable_but_inconsistent_retention_is_not() -> Result<()> {
        let mut sample = sample();
        sample.quotas.max_logical_bytes = 1;
        sample.usage = Some(FilesystemUsage {
            revision: 1,
            logical_bytes: 2,
            inodes: 2,
            ..Default::default()
        });
        sample.validate("Account:one")?;
        sample.usage.as_mut().unwrap().discarded_allocated_bytes = 65536;
        assert!(sample.validate("Account:one").is_err());
        sample.usage = None;
        sample.live_handles = 1;
        assert!(sample.validate("Account:one").is_err());
        Ok(())
    }
}

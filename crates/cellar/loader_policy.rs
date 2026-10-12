//! Immutable admission and copied-value budgets for one compiled Cellar host.

/// Configure through `domain.services().configure_loader(...)` before building
/// a generation. Workers share the host limit; instances of the same script
/// and generation also share a principal limit. Loaded environments include
/// both encoded values and supplied service-route metadata. Props budgets apply
/// to loaded entrypoints, service handles, and exported cell classes.
/// Both encoded-value budgets are at most the structured-value decoder's 64 MiB
/// ceiling. Reading the host policy pins library defaults if none was configured.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LoaderPolicy {
    pub max_workers: usize,
    pub max_workers_per_principal: usize,
    pub max_environment_bytes: usize,
    pub max_props_bytes: usize,
}

impl Default for LoaderPolicy {
    /// Library defaults are bounded and never inspect the environment.
    fn default() -> Self {
        Self {
            max_workers: 256,
            max_workers_per_principal: 255,
            max_environment_bytes: 1024 * 1024,
            max_props_bytes: 1024 * 1024,
        }
    }
}

impl LoaderPolicy {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.max_workers > 0, "loader max_workers must be positive");
        anyhow::ensure!(
            self.max_workers_per_principal > 0
                && self.max_workers_per_principal <= self.max_workers,
            "loader max_workers_per_principal must be positive and at most max_workers"
        );
        let ceiling = cellar_native::clone::Limits::default().bytes;
        for (name, bytes) in [
            ("max_environment_bytes", self.max_environment_bytes),
            ("max_props_bytes", self.max_props_bytes),
        ] {
            anyhow::ensure!(
                bytes > 0 && bytes <= ceiling,
                "loader {name} must be between 1 and {ceiling}"
            );
        }
        Ok(())
    }

    /// Explicit CLI adapter. Captures all settings before workers start.
    pub fn from_env() -> anyhow::Result<Self> {
        let defaults = Self::default();
        let max_workers =
            crate::env_vars::positive_or("CELLAR_LOADER_MAX_WORKERS", defaults.max_workers)?;
        let policy = Self {
            max_workers,
            max_workers_per_principal: crate::env_vars::positive_or(
                "CELLAR_LOADER_MAX_WORKERS_PER_PRINCIPAL",
                max_workers.saturating_sub(1).max(1),
            )?,
            max_environment_bytes: crate::env_vars::positive_or(
                "CELLAR_LOADER_MAX_ENV_BYTES",
                defaults.max_environment_bytes,
            )?,
            max_props_bytes: crate::env_vars::positive_or(
                "CELLAR_LOADER_MAX_PROPS_BYTES",
                defaults.max_props_bytes,
            )?,
        };
        policy.validate()?;
        Ok(policy)
    }
}

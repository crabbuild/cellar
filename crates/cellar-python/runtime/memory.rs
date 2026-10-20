//! Reuse host memory probes outside node execution; no Python heap attribution.

/// Whole-process observations. `None` means that probe is unavailable. Jemalloc
/// covers its own allocations; libc excludes other private/native allocators.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct MemorySample {
    pub process_rss_bytes: Option<u64>,
    pub jemalloc_allocated_bytes: Option<u64>,
    pub jemalloc_active_bytes: Option<u64>,
    pub jemalloc_resident_bytes: Option<u64>,
    pub libc_in_use_bytes: Option<u64>,
    pub libc_mmap_bytes: Option<u64>,
    pub libc_free_bytes: Option<u64>,
}

impl MemorySample {
    /// Bootstrap and post-join only: probes can read kernel files and take
    /// allocator locks, so never sample here from a core or interpreter tick.
    pub(crate) fn capture() -> Self {
        let rss = cellar::memory::resident_bytes();
        let allocator = cellar::memory::allocator_stats();
        let libc = cellar::memory::libc_malloc_stats();
        Self {
            process_rss_bytes: (rss > 0).then_some(rss),
            jemalloc_allocated_bytes: allocator.map(|stats| stats.allocated_bytes),
            jemalloc_active_bytes: allocator.map(|stats| stats.active_bytes),
            jemalloc_resident_bytes: allocator.map(|stats| stats.resident_bytes),
            libc_in_use_bytes: libc.map(|stats| stats.in_use_bytes),
            libc_mmap_bytes: libc.map(|stats| stats.mmap_bytes),
            libc_free_bytes: libc.map(|stats| stats.free_bytes),
        }
    }
}

/// Growth can be computed between these observations, but includes native host,
/// interpreter initialization, imports and caches. CPython remains initialized
/// after joined cleanup; this is neither per-cell accounting nor a heap limit.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct MemoryReport {
    pub before_interpreter: MemorySample,
    pub after_join: MemorySample,
}

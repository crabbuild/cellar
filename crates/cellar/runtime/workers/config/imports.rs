// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Adapted for Cellar from Celld: crates/celld/js/modules.rs.

use crate::engine_api::BARE_NODE_BUILTINS;

pub(crate) const PYTHON_HOST_SPECIFIER: &str = "cellar:python";

pub(crate) fn is_python_host(spec: &str) -> bool {
    spec == PYTHON_HOST_SPECIFIER
}

pub(crate) fn is_external(spec: &str) -> bool {
    if spec.starts_with("node:") || spec.starts_with("cloudflare:") || is_python_host(spec) {
        return true;
    }
    // bare builtin, or a builtin submodule like `stream/promises`
    BARE_NODE_BUILTINS.contains(&spec)
        || BARE_NODE_BUILTINS.contains(&spec.split('/').next().unwrap_or(""))
}

/// The import statement, and the brace group inside its clause.
///
/// `Regex::new` compiles the pattern to a program and builds the matcher
/// scaffolding around it, so it is far more expensive than the scan itself on
/// a small bundle. Both patterns were function locals, so an isolate paid that
/// cost once for the main module and once more for each sibling module of a
/// Worker Loader bundle — on every isolate build.
static IMPORT_RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    // A minifier can join `import` to `{` or `*`, but a default binding must
    // keep whitespace. Preserve that token boundary so an identifier that
    // starts with `import` cannot look like an import statement.
    regex::Regex::new(r#"\bimport(\s+[^;]*?|\s*[{*][^;]*?)\s*from\s*["']([^"']+)["']"#).unwrap()
});
static IMPORT_BRACES_RE: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| regex::Regex::new(r"\{([^}]*)\}").unwrap());

/// Counts scans of a source carrying [`SCAN_PROBE_MARKER`].
///
/// The counter is filtered by the marker rather than counting every scan, so a
/// bare global counter cannot move under an unrelated worker build.
#[cfg(cellar_internal_tests)]
static IMPORT_SCANS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// A source puts this in a comment to have its scans counted.
#[cfg(cellar_internal_tests)]
pub(crate) const SCAN_PROBE_MARKER: &str = "cellar-import-scan-probe";

/// How many times a source carrying [`SCAN_PROBE_MARKER`] has been scanned.
#[cfg(cellar_internal_tests)]
#[doc(hidden)]
pub fn import_scans() -> usize {
    IMPORT_SCANS.load(std::sync::atomic::Ordering::Relaxed)
}

/// External specifier -> the bindings a bundle pulls from it.
pub(crate) type ExternalImports =
    std::collections::BTreeMap<String, std::collections::BTreeSet<String>>;

/// Scan a bundle for its external (`cloudflare:*` / `node:*`) imports and the
/// named/default bindings pulled from each. A source bundler links these, so a stub
/// must provide exactly these names; namespace imports (`* as x`) need none.
pub(crate) fn scan_external_imports(src: &str) -> ExternalImports {
    #[cfg(cellar_internal_tests)]
    if src.contains(SCAN_PROBE_MARKER) {
        IMPORT_SCANS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    let mut map = ExternalImports::default();
    for cap in IMPORT_RE.captures_iter(src) {
        if !is_external(&cap[2]) {
            continue;
        }
        let clause = cap[1].trim();
        let set = map.entry(cap[2].to_string()).or_default();
        if let Some(b) = IMPORT_BRACES_RE.captures(clause) {
            for part in b[1].split(',') {
                let name = part.split_whitespace().next().unwrap_or("");
                // A commented-out binding inside the braces is not a name.
                if !name.is_empty() && !name.starts_with("//") {
                    set.insert(name.to_string());
                }
            }
        }
        let head = clause
            .split(['{', '*'])
            .next()
            .unwrap_or("")
            .trim()
            .trim_end_matches(',')
            .trim();
        if !head.is_empty() {
            set.insert("default".into());
        } // default import
          // `import * as x` consumes the whole namespace; the stub must then
          // export the module's full surface, not just the scanned names.
        if clause.contains('*') {
            set.insert("*".into());
        }
    }
    map
}

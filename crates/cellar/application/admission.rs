//! Application adapter admission failures, independent of node/cell capacity.

/// An application adapter cannot admit another invocation within its budget.
///
/// Return this before entering the refused handler. HTTP ingress classifies it
/// as overload (503), rather than an application fault. Propagation through a
/// caller does not prove that caller made no writes: the mutation outcome stays
/// unknown and the host does not automatically replay the invocation.
#[derive(Debug)]
pub struct ApplicationOverloaded;

// Service and peer handler failures can retain only a string error chain.
// This marker preserves classification, never authority or retry certainty.
const MARKER: &str = "cellar-application-overload-v1-87e24f0c";

impl ApplicationOverloaded {
    /// Recognize a typed or forwarded application refusal. A native output-gate
    /// verdict takes precedence over the handler failure retained beneath it.
    pub fn matches(error: &anyhow::Error) -> bool {
        if error
            .downcast_ref::<crate::cell_dispatch::RoutedRequestError>()
            .is_some()
        {
            return false;
        }
        error.chain().any(|cause| {
            cause.downcast_ref::<Self>().is_some() || cause.to_string().contains(MARKER)
        })
    }
}

impl std::fmt::Display for ApplicationOverloaded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "application admission refused ({MARKER})")
    }
}

impl std::error::Error for ApplicationOverloaded {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_and_forwarded_refusals_preserve_only_classification() {
        let error = anyhow::Error::new(ApplicationOverloaded).context("nested service call");
        assert!(ApplicationOverloaded::matches(&error));
        assert!(ApplicationOverloaded::matches(&anyhow::anyhow!(
            "{error:#}"
        )));
        assert!(!ApplicationOverloaded::matches(&anyhow::anyhow!(
            "application admission refused"
        )));
    }

    #[test]
    fn output_gate_verdict_wins_over_retained_admission_failure() {
        for verdict in [
            cellar_logic::RequestError::NodeFenced,
            cellar_logic::RequestError::DurabilityUnproven,
        ] {
            let error = anyhow::Error::new(ApplicationOverloaded)
                .context(crate::cell_dispatch::RoutedRequestError(verdict));
            assert!(!ApplicationOverloaded::matches(&error));
        }
    }
}

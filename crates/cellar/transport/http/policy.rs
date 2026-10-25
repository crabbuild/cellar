// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Outbound HTTP policy and connection pools owned by one execution domain.

use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Captured outbound request deadline, including response-body consumption.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HttpPolicy {
    /// Positive duration, at most `u32::MAX` seconds to fit transport timers.
    pub request_timeout: Duration,
}

impl Default for HttpPolicy {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(120),
        }
    }
}

impl HttpPolicy {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.request_timeout.is_zero(),
            "HTTP request timeout must be positive"
        );
        anyhow::ensure!(
            self.request_timeout <= Duration::from_secs(u32::MAX.into()),
            "HTTP request timeout exceeds the timer range"
        );
        Ok(())
    }

    /// The CLI resolves process settings once; library defaults read none.
    pub fn from_env() -> anyhow::Result<Self> {
        let policy = Self {
            request_timeout: Duration::from_secs(crate::env_vars::positive_or(
                "CELLAR_FETCH_TIMEOUT_S",
                120,
            )?),
        };
        policy.validate()?;
        Ok(policy)
    }
}

/// Application-owned HTTP client configuration. Supply TLS, DNS, proxies,
/// credentials and connection-pool settings here. Cellar sets redirect policy
/// after invoking the factory so error mode and subrequest budgets cannot be
/// bypassed. Each ordinary mode builds one pool per host; a redirect-limited event
/// builds its own client to retain that event's accounting callback.
#[derive(Clone)]
pub struct HttpClientFactory(Arc<dyn Fn() -> reqwest::ClientBuilder + Send + Sync>);

impl HttpClientFactory {
    pub fn new(factory: impl Fn() -> reqwest::ClientBuilder + Send + Sync + 'static) -> Self {
        Self(Arc::new(factory))
    }
}

pub(crate) struct HttpService {
    pub(crate) policy: HttpPolicy,
    factory: Option<HttpClientFactory>,
    clients: Mutex<Clients>,
}

#[derive(Default)]
struct Clients {
    closed: bool,
    modes: [Option<Result<reqwest::Client, String>>; 3],
}

impl Default for HttpService {
    fn default() -> Self {
        Self::new(HttpPolicy::default(), None)
    }
}

impl HttpService {
    pub(crate) fn new(policy: HttpPolicy, factory: Option<HttpClientFactory>) -> Self {
        Self {
            policy,
            factory,
            clients: Mutex::new(Clients::default()),
        }
    }

    pub(crate) fn matches(&self, policy: HttpPolicy, factory: Option<&HttpClientFactory>) -> bool {
        self.policy == policy
            && match (&self.factory, factory) {
                (None, None) => true,
                (Some(existing), Some(candidate)) => Arc::ptr_eq(&existing.0, &candidate.0),
                _ => false,
            }
    }

    pub(crate) fn factory(&self) -> Option<&HttpClientFactory> {
        self.factory.as_ref()
    }

    pub(crate) fn build(
        &self,
        redirect: reqwest::redirect::Policy,
    ) -> Result<reqwest::Client, String> {
        self.check_open()?;
        let client = self.build_client(redirect)?;
        self.check_open()?;
        Ok(client)
    }

    fn build_client(&self, redirect: reqwest::redirect::Policy) -> Result<reqwest::Client, String> {
        let builder = match &self.factory {
            Some(factory) => (factory.0)(),
            // Ambient process proxies are an explicit application/CLI choice.
            None => reqwest::Client::builder().no_proxy(),
        };
        builder
            .redirect(redirect)
            .build()
            .map_err(|error| format!("fetch: HTTP client construction failed: {error}"))
    }

    pub(crate) fn client(&self, mode: &str) -> Result<reqwest::Client, String> {
        let (index, redirect) = match mode {
            "follow" => (0, reqwest::redirect::Policy::default()),
            "manual" => (1, reqwest::redirect::Policy::none()),
            "error" => (
                2,
                reqwest::redirect::Policy::custom(|attempt| {
                    attempt.error("fetch redirect mode is error")
                }),
            ),
            _ => unreachable!("validated HTTP redirect mode"),
        };
        let mut clients = self.clients.lock().unwrap();
        if clients.closed {
            return Err("fetch: HTTP execution domain is closed".into());
        }
        clients.modes[index]
            .get_or_insert_with(|| self.build_client(redirect))
            .clone()
    }

    fn check_open(&self) -> Result<(), String> {
        if self.clients.lock().unwrap().closed {
            Err("fetch: HTTP execution domain is closed".into())
        } else {
            Ok(())
        }
    }

    pub(crate) fn close(&self) {
        let pools = {
            let mut clients = self.clients.lock().unwrap();
            clients.closed = true;
            std::mem::take(&mut clients.modes)
        };
        drop(pools);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn ordinary_pools_are_reused_and_closed_without_reconstruction() {
        let builds = Arc::new(AtomicUsize::new(0));
        let count = builds.clone();
        let http = HttpService::new(
            HttpPolicy::default(),
            Some(HttpClientFactory::new(move || {
                count.fetch_add(1, Ordering::SeqCst);
                reqwest::Client::builder().no_proxy()
            })),
        );
        for _ in 0..3 {
            for mode in ["follow", "manual", "error"] {
                assert!(http.client(mode).is_ok());
            }
        }
        assert_eq!(builds.load(Ordering::SeqCst), 3);
        http.close();
        for mode in ["follow", "manual", "error"] {
            assert!(http.client(mode).is_err());
        }
        assert!(http.build(reqwest::redirect::Policy::none()).is_err());
        assert_eq!(builds.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn client_construction_failure_returns_an_error_without_panicking() {
        let http = HttpService::new(
            HttpPolicy::default(),
            Some(HttpClientFactory::new(|| {
                reqwest::Client::builder().user_agent("invalid\nheader")
            })),
        );
        assert!(http
            .client("follow")
            .unwrap_err()
            .contains("HTTP client construction failed"));
    }

    #[tokio::test]
    async fn retained_host_services_cannot_rebuild_http_after_closure() {
        let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let services = domain.services();
        let builds = Arc::new(AtomicUsize::new(0));
        let count = builds.clone();
        services
            .configure_http(
                HttpPolicy::default(),
                Some(HttpClientFactory::new(move || {
                    count.fetch_add(1, Ordering::SeqCst);
                    reqwest::Client::builder().no_proxy()
                })),
            )
            .unwrap();
        assert!(services.http().client("follow").is_ok());
        domain.shutdown().await;
        for mode in ["follow", "manual", "error"] {
            assert!(services.http().client(mode).is_err());
        }
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        let unopened = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        unopened.shutdown().await;
        assert!(unopened.services().http().client("follow").is_err());
    }
}

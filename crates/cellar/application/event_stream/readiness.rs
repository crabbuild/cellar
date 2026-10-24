// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Generation-local admission. Catalog/partition state remains authoritative;
//! this latch only prevents staged bindings from admitting application work.

use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    OnceLock,
};

pub(crate) struct Readiness {
    bindings: OnceLock<BTreeMap<(String, String), super::Binding>>,
    ready: AtomicBool,
    result: tokio::sync::watch::Sender<Option<Result<(), String>>>,
}

impl Readiness {
    pub(crate) fn new(ready: bool) -> Self {
        let (result, _) = tokio::sync::watch::channel(ready.then_some(Ok(())));
        Self {
            bindings: OnceLock::new(),
            ready: AtomicBool::new(ready),
            result,
        }
    }

    pub(crate) fn pin(&self, bindings: Vec<super::Binding>) -> anyhow::Result<()> {
        let mut resolved = BTreeMap::new();
        for binding in bindings {
            binding.validate()?;
            anyhow::ensure!(
                resolved
                    .insert(
                        (binding.principal.clone(), binding.environment.clone()),
                        binding
                    )
                    .is_none(),
                "duplicate resolved binding"
            );
        }
        let prior = self.bindings.get_or_init(|| resolved.clone());
        anyhow::ensure!(
            *prior == resolved,
            "generation bindings cannot be retargeted"
        );
        Ok(())
    }

    pub(crate) fn binding(
        &self,
        declaration: &super::BindingConfig,
    ) -> anyhow::Result<super::Binding> {
        declaration.validate()?;
        if let Some(bindings) = self.bindings.get() {
            let binding = bindings
                .get(&(
                    declaration.principal.clone(),
                    declaration.environment.clone(),
                ))
                .ok_or_else(|| {
                    super::error::error(
                        super::ErrorCode::Unavailable,
                        "binding is absent from activated generation",
                    )
                })?;
            anyhow::ensure!(
                declaration.resolve(binding.resource.clone())? == *binding,
                "binding differs from captured deployment"
            );
            return Ok(binding.clone());
        }
        // Runtime-only hosts may supply already resolved bindings. A name never
        // becomes an ID, nor triggers lookup, outside owner activation.
        declaration.explicit()
    }

    pub(crate) fn begin(&self) {
        self.ready.store(false, Ordering::Release);
        self.result.send_replace(None);
    }

    pub(crate) fn finish(&self, result: Result<(), String>) {
        self.ready.store(result.is_ok(), Ordering::Release);
        self.result.send_replace(Some(result));
    }

    pub(crate) fn check(&self) -> crate::application::Result<()> {
        if !self.ready.load(Ordering::Acquire) {
            return Err(super::error::error(
                super::ErrorCode::Unavailable,
                "event bindings have not completed resource activation",
            ));
        }
        Ok(())
    }

    pub(crate) async fn wait(&self) -> anyhow::Result<()> {
        let mut result = self.result.subscribe();
        loop {
            if let Some(result) = result.borrow_and_update().clone() {
                return result.map_err(anyhow::Error::msg);
            }
            result
                .changed()
                .await
                .map_err(|_| anyhow::anyhow!("event activation observer closed"))?;
        }
    }
}

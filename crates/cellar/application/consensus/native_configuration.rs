// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Immutable deployment values admitted before ordinary Raft cell construction.

use super::RegistrationError;
use crate::engine_api::{Compat, WorkerConfigOptions};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Provisioning carries the same values as the daemon's deployment. Node-local
/// identity is deliberately excluded from the namespace's immutable definition.
#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct NativeCellConfiguration {
    pub(crate) vars: Vec<(String, String)>,
    pub(crate) compat: Compat,
    pub(crate) do_classes: Vec<String>,
    pub(crate) bindings: Vec<(String, String)>,
}

impl NativeCellConfiguration {
    pub(crate) fn options(&self, script: &str) -> WorkerConfigOptions {
        WorkerConfigOptions {
            script_name: script.into(),
            src: String::new(),
            do_classes: self.do_classes.clone(),
            bindings: self.bindings.clone(),
            blob_bindings: Vec::new(),
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            stream_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: self.vars.clone(),
            node: String::new(),
            modules: Vec::new(),
            compat: self.compat,
        }
    }
}

#[derive(Clone)]
pub(in crate::application) struct AdmittedCellConfiguration {
    pub(in crate::application) script: String,
    pub(in crate::application) vars: Arc<BTreeMap<String, String>>,
    pub(in crate::application) compat: Compat,
    pub(in crate::application) classes: Arc<BTreeSet<String>>,
    pub(in crate::application) bindings: Arc<BTreeMap<String, String>>,
}

impl AdmittedCellConfiguration {
    pub(in crate::application) fn from_options(
        options: &WorkerConfigOptions,
    ) -> Result<Self, RegistrationError> {
        // Do not turn a declared provider into an empty binding map. Each
        // adapter must first acquire shared execution and settlement authority.
        if !options.src.is_empty()
            || !options.modules.is_empty()
            || !options.sql_bindings.is_empty()
            || !options.kv_bindings.is_empty()
            || !options.blob_bindings.is_empty()
            || !options.queue_bindings.is_empty()
            || !options.stream_bindings.is_empty()
            || !options.queue_consumers.is_empty()
            || !options.workflow_bindings.is_empty()
        {
            return Err(RegistrationError::Invalid(
                "unimplemented Raft cell bindings",
            ));
        }
        if options.compat.sqlite_vec {
            return Err(RegistrationError::Invalid(
                "unimplemented Raft SQLite extension",
            ));
        }
        if options.script_name.is_empty() || options.script_name.chars().any(char::is_control) {
            return Err(RegistrationError::Invalid("Raft cell script"));
        }
        if options.vars.len() > 256 {
            return Err(RegistrationError::Limit);
        }
        let mut bytes = options.script_name.len();
        let mut vars = BTreeMap::new();
        for (name, value) in &options.vars {
            if name.is_empty() || name.len() > 256 || name.chars().any(char::is_control) {
                return Err(RegistrationError::Invalid("Raft cell variable name"));
            }
            bytes = bytes
                .checked_add(name.len())
                .and_then(|bytes| bytes.checked_add(value.len()))
                .ok_or(RegistrationError::Limit)?;
            if bytes > 64 * 1024 {
                return Err(RegistrationError::Limit);
            }
            if vars.insert(name.clone(), value.clone()).is_some() {
                // Variable values may contain secrets. Never print them in
                // admission errors or retain a second, ambiguous definition.
                return Err(RegistrationError::Invalid("duplicate Raft cell variable"));
            }
        }
        if bytes > 64 * 1024 {
            return Err(RegistrationError::Limit);
        }
        if options.do_classes.len() > 256 || options.bindings.len() > 256 {
            return Err(RegistrationError::Limit);
        }
        let mut classes = BTreeSet::new();
        let mut bindings = BTreeMap::new();
        for class in &options.do_classes {
            super::class::validate_name(class)?;
            bytes = bytes
                .checked_add(class.len())
                .ok_or(RegistrationError::Limit)?;
            if !classes.insert(class.clone()) {
                return Err(RegistrationError::Invalid("duplicate Raft exported class"));
            }
        }
        for (name, class) in &options.bindings {
            super::class::validate_name(class)?;
            if name.is_empty() || name.len() > 256 || name.chars().any(char::is_control) {
                return Err(RegistrationError::Invalid("Raft cell binding name"));
            }
            bytes = bytes
                .checked_add(name.len())
                .and_then(|n| n.checked_add(class.len()))
                .ok_or(RegistrationError::Limit)?;
            if bindings.insert(name.clone(), class.clone()).is_some() {
                return Err(RegistrationError::Invalid("duplicate Raft cell binding"));
            }
        }
        if bytes > 64 * 1024 {
            return Err(RegistrationError::Limit);
        }
        Ok(Self {
            script: options.script_name.clone(),
            vars: Arc::new(vars),
            compat: options.compat,
            classes: Arc::new(classes),
            bindings: Arc::new(bindings),
        })
    }

    pub(in crate::application) fn digest(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"cellar-raft-cell-configuration-1");
        // BTree order and all compatibility switches participate, independent
        // of declaration order. Serialization of these bounded values is total.
        hash.update(
            serde_json::to_vec(&(&self.script, self.vars.as_ref(), self.compat))
                .expect("serializable cell configuration"),
        );
        if self.has_cell_bindings() {
            hash.update(b"cell-bindings-origin-and-sealing-1");
            hash.update(
                serde_json::to_vec(&(self.classes.as_ref(), self.bindings.as_ref()))
                    .expect("serializable cell bindings"),
            );
        }
        hash.finalize().into()
    }

    pub(in crate::application) fn has_cell_bindings(&self) -> bool {
        !self.classes.is_empty() || !self.bindings.is_empty()
    }

    pub(in crate::application) fn options(&self) -> WorkerConfigOptions {
        NativeCellConfiguration {
            vars: self
                .vars
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            compat: self.compat,
            do_classes: self.classes.iter().cloned().collect(),
            bindings: self
                .bindings
                .iter()
                .map(|(name, class)| (name.clone(), class.clone()))
                .collect(),
        }
        .options(&self.script)
    }
}

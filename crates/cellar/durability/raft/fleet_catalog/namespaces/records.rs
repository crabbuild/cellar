use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Declaration {
    pub namespace: [u8; 16],
    pub root: [u8; 32],
    pub schema: [u8; 32],
    pub class: String,
}

impl Declaration {
    pub fn new(catalog: &FleetCatalog, namespace: &NamespaceCatalog, class: &str) -> Result<Self> {
        let cfg = namespace.config();
        if cfg.cluster.bytes() != catalog.inner.cluster
            || cfg.mode != cellar_logic::NamespaceMode::Raft
            || cfg.voters != VoterPolicy::ThreeVoters
            || cfg.image_storage != crate::raft::catalog::RaftImageStorage::Materialized
        {
            return Err(FleetCatalogError::Invalid("fleet namespace"));
        }
        if cfg.managed_fleet.is_some() && catalog.bind_namespace(cfg.clone())? != *cfg {
            return Err(FleetCatalogError::Conflict);
        }
        let value = Self {
            namespace: cfg.namespace.bytes(),
            root: namespace.root_digest(),
            schema: cfg.schema_digest,
            class: class.into(),
        };
        value
            .validate()
            .map_err(|_| FleetCatalogError::Invalid("namespace declaration"))?;
        Ok(value)
    }

    pub fn key(&self) -> String {
        hex::encode(self.namespace)
    }

    pub fn validate(&self) -> Result<()> {
        if NamespaceId::new(self.namespace).is_err()
            || self.root == [0; 32]
            || self.schema == [0; 32]
            || self.class.is_empty()
            || self.class.len() > 128
            || self.class.chars().any(|c| c.is_control() || c == ':')
        {
            return Err(FleetCatalogError::Corrupt("namespace declaration"));
        }
        Ok(())
    }

    pub fn check(
        &self,
        catalog: &FleetCatalog,
        namespace: &NamespaceCatalog,
        class: Option<&str>,
    ) -> Result<()> {
        self.validate()?;
        let expected = Self::new(catalog, namespace, class.unwrap_or(&self.class))?;
        if *self != expected {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Registry {
    pub count: usize,
    #[serde(deserialize_with = "required_nullable")]
    pub index: Option<[u8; 32]>,
}

fn required_nullable<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::<T>::deserialize(d)
}

impl Registry {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Self = decode(REGISTRY_KIND, bytes, 4096)?;
        if !(1..=MAX_NAMESPACES).contains(&value.count) || value.index.is_none() {
            return Err(FleetCatalogError::Corrupt("namespace registry"));
        }
        Ok(value)
    }
}

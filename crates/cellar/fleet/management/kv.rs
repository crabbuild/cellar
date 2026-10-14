//! Typed console edits; preconditions are evaluated by the namespace owner.
use super::*;
pub use crate::application::kv::{EditExpiration, RevisionCondition};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct KvEdit {
    pub value_base64: String,
    pub tag: String,
    pub metadata: Option<String>,
    pub expiration: EditExpiration,
    pub condition: RevisionCondition,
}

impl KvEdit {
    pub fn validate(&self) -> Result<()> {
        use base64::Engine;
        self.condition.validate()?;
        ensure!(
            matches!(self.tag.as_str(), "bytes" | "text" | "json"),
            "Unsupported KV storage type"
        );
        let bytes = base64::engine::general_purpose::STANDARD.decode(&self.value_base64)?;
        ensure!(
            bytes.len() <= 1024 * 1024,
            "dashboard values are limited to 1 MiB"
        );
        if let Some(metadata) = &self.metadata {
            serde_json::from_str::<Value>(metadata)?;
            ensure!(metadata.len() <= 1024, "KV metadata is limited to 1 KiB");
        }
        Ok(())
    }
}

impl FleetClient {
    /// Bounded console inspection. Larger values retain metadata but omit bytes.
    pub async fn kv_inspect(&self, namespace: &str, key: &str) -> Result<Value> {
        ensure!(
            !key.is_empty() && key.len() <= 512,
            "key must contain 1–512 bytes"
        );
        self.call(
            crate::engine_api::kv_cell_scope(
                namespace,
                cellar_logic::kv::shard_of(key, cellar_logic::kv::SHARDS),
            ),
            None,
            json!({"op":"get","key":key,"max_bytes":1024*1024}),
        )
        .await
    }

    pub async fn kv_edit(&self, namespace: &str, key: &str, edit: KvEdit) -> Result<Value> {
        edit.validate()?;
        ensure!(
            !key.is_empty() && key.len() <= 512,
            "key must contain 1–512 bytes"
        );
        self.call(
            crate::engine_api::kv_cell_scope(namespace, cellar_logic::kv::shard_of(key, cellar_logic::kv::SHARDS)),
            None,
            json!({"op":"edit","key":key,"value":edit.value_base64,"tag":edit.tag,"metadata":edit.metadata,"condition":edit.condition,"expiration_mode":edit.expiration}),
        ).await
    }

    pub async fn kv_delete_if(
        &self,
        namespace: &str,
        key: &str,
        condition: RevisionCondition,
    ) -> Result<Value> {
        condition.validate()?;
        ensure!(
            !key.is_empty() && key.len() <= 512,
            "key must contain 1–512 bytes"
        );
        self.call(
            crate::engine_api::kv_cell_scope(
                namespace,
                cellar_logic::kv::shard_of(key, cellar_logic::kv::SHARDS),
            ),
            None,
            json!({"op":"delete","keys":[key],"condition":condition}),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn console_edits_validate_encoding_revision_and_json_metadata() {
        let edit = KvEdit {
            value_base64: "AP8B".into(),
            tag: "bytes".into(),
            metadata: Some("null".into()),
            expiration: EditExpiration::Preserve,
            condition: RevisionCondition {
                revision: Some("9223372036854775807".into()),
            },
        };
        assert!(edit.validate().is_ok());
        let encoded = serde_json::to_value(&edit).unwrap();
        assert_eq!(encoded["metadata"], "null");
        assert!(
            serde_json::from_value::<KvEdit>(
                json!({"value_base64":"AA==","tag":"bytes","expiration":{"mode":"never"}})
            )
            .is_err(),
            "edit must carry a condition"
        );
        for revision in ["-1", "01", "+1", "1.0", "9223372036854775808"] {
            let mut invalid = edit.clone();
            invalid.condition.revision = Some(revision.into());
            assert!(invalid.validate().is_err());
        }
        let mut invalid = edit.clone();
        invalid.value_base64 = "!invalid!".into();
        assert!(invalid.validate().is_err());
        invalid = edit.clone();
        invalid.metadata = Some("not JSON".into());
        assert!(invalid.validate().is_err());
        invalid = edit.clone();
        invalid.tag = "unsupported".into();
        assert!(invalid.validate().is_err());
    }
}

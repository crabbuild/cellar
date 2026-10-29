// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeAction {
    State,
    Cells {
        after: Option<String>,
        limit: usize,
    },
    PauseBalancing,
    ResumeBalancing,
    Reload,
    Stop {
        operation_id: String,
        mode: super::StopMode,
    },
    FleetStop {
        request: Box<crate::application::consensus::FleetStopRequest>,
    },
    Evict {
        scope: String,
        epoch: u64,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NodeRequest {
    pub generation: String,
    pub action: NodeAction,
}

impl FleetClient {
    pub async fn node_action(
        &self,
        node: &str,
        generation: &str,
        action: NodeAction,
    ) -> Result<Value> {
        ensure!(valid_node_id(node), "invalid node ID");
        let lease = crate::fleet::live_node_lease(&self.bucket, node)
            .await?
            .ok_or_else(|| anyhow::anyhow!("node has no current lease"))?;
        ensure!(
            lease.generation == generation,
            "node generation changed; refresh before continuing"
        );
        let address = crate::startup::parse_advertise(&lease.addr)?;
        ensure!(
            self.allow_public_peers || !address.is_public_ip(),
            "public peer addresses are disabled"
        );
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(35))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let auth = crate::peer_auth::PeerAuth::new(
            crate::peer_auth::load_existing(&self.bucket).await?,
            "dashboard",
        )?;
        crate::peer_probe::probe(&http, &lease, &auth).await?;
        let path = "/peer/management";
        let payload = serde_json::to_vec(&NodeRequest {
            generation: generation.into(),
            action,
        })?;
        let response = auth
            .sign(
                http.post(format!("http://{}{path}", lease.addr))
                    .body(payload.clone()),
                "POST",
                path,
                &payload,
                node,
            )?
            .send()
            .await?;
        crate::peer_auth::validate_response(response.headers())?;
        let status = response.status();
        let text = crate::operator_cell::bounded_reply(response, 1024 * 1024).await?;
        let value: Value = serde_json::from_str(&text)?;
        ensure!(
            status.is_success(),
            "{}",
            value["error"]
                .as_str()
                .unwrap_or("node management request failed")
        );
        value
            .get("result")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("node response has no result"))
    }
}

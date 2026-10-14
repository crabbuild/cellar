//! Bounded entrance discovery. A known owner is preferred; any current fleet
//! member can forward. Exhausting the sample is an unavailable observation.
use super::*;

impl FleetClient {
    pub(super) async fn routing_entrances(&self, scope: &str) -> Result<Vec<(String, String)>> {
        let mut entrances = Vec::new();
        if let Ok(cell) = self.cell(scope).await {
            if let Some(owner) = cell.owner {
                if let Ok(node) = self.node(&owner).await {
                    self.admit_entrance(node, &mut entrances);
                }
            }
        }
        let mut cursor = None;
        for _ in 0..4 {
            let page = self
                .nodes(PageRequest {
                    cursor,
                    prefix: None,
                    limit: Some(32),
                })
                .await?;
            for node in page.items {
                self.admit_entrance(node, &mut entrances);
                if entrances.len() == 4 {
                    return Ok(entrances);
                }
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        ensure!(!entrances.is_empty(), "No current routing entrance found among the owner and first 128 node records; fleet availability is unknown");
        Ok(entrances)
    }

    fn admit_entrance(&self, node: NodeView, entrances: &mut Vec<(String, String)>) {
        if node.protocol != crate::peer_auth::PROTOCOL_VERSION
            || !node.lease_current
            || entrances.iter().any(|(id, _)| id == &node.id)
        {
            return;
        }
        if crate::startup::parse_advertise(&node.address)
            .is_ok_and(|address| self.allow_public_peers || !address.is_public_ip())
        {
            entrances.push((node.id, node.address));
        }
    }
}

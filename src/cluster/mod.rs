use std::collections::HashSet;

use bytes::Bytes;

use crate::error::ConfigError;

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
const MAX_CLUSTER_NODES: usize = 256;
const MAX_CLIENT_ADDRESS_LENGTH: usize = 255;
const MAX_VIRTUAL_NODES: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterNode {
    id: String,
    address: String,
}

impl ClusterNode {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn address(&self) -> &str {
        &self.address
    }
}

#[derive(Clone, Debug)]
pub struct ClusterTopology {
    local_node_index: usize,
    nodes: Vec<ClusterNode>,
    ring: Vec<(u64, usize)>,
    virtual_nodes: usize,
}

impl ClusterTopology {
    pub fn new(
        local_node_id: &str,
        membership: &str,
        virtual_nodes: usize,
    ) -> Result<Self, ConfigError> {
        if !(1..=MAX_VIRTUAL_NODES).contains(&virtual_nodes) {
            return Err(ConfigError::InvalidCombination(
                format!("cluster virtual node count must be in 1..={MAX_VIRTUAL_NODES}"),
            ));
        }
        validate_node_id(local_node_id)?;

        let mut nodes = Vec::new();
        let mut identifiers = HashSet::new();
        let mut addresses = HashSet::new();
        for member in membership.split(',') {
            if nodes.len() >= MAX_CLUSTER_NODES {
                return Err(ConfigError::InvalidCombination(format!(
                    "FORGEKV_CLUSTER_NODES supports at most {MAX_CLUSTER_NODES} nodes"
                )));
            }
            let member = member.trim();
            let (id, address) = member.split_once('@').ok_or_else(|| {
                ConfigError::InvalidCombination(
                    "FORGEKV_CLUSTER_NODES entries must use node-id@host:port".to_owned(),
                )
            })?;
            validate_node_id(id)?;
            validate_address(address)?;
            if !identifiers.insert(id.to_owned()) {
                return Err(ConfigError::InvalidCombination(format!(
                    "duplicate cluster node id {id:?}"
                )));
            }
            if !addresses.insert(address.to_owned()) {
                return Err(ConfigError::InvalidCombination(format!(
                    "duplicate cluster client address {address:?}"
                )));
            }
            nodes.push(ClusterNode {
                id: id.to_owned(),
                address: address.to_owned(),
            });
        }
        if nodes.is_empty() {
            return Err(ConfigError::InvalidCombination(
                "FORGEKV_CLUSTER_NODES must contain at least one node".to_owned(),
            ));
        }
        nodes.sort_by(|left, right| left.id.cmp(&right.id));
        let local_node_index = nodes
            .iter()
            .position(|node| node.id == local_node_id)
            .ok_or_else(|| {
                ConfigError::InvalidCombination(
                    "FORGEKV_CLUSTER_NODE_ID must appear in FORGEKV_CLUSTER_NODES".to_owned(),
                )
            })?;

        let capacity = nodes.len().checked_mul(virtual_nodes).ok_or_else(|| {
            ConfigError::InvalidCombination("cluster ring size exceeds supported range".to_owned())
        })?;
        let mut ring = Vec::with_capacity(capacity);
        for (node_index, node) in nodes.iter().enumerate() {
            for replica in 0..virtual_nodes {
                let point = format!("{}#{replica}", node.id);
                ring.push((stable_hash(point.as_bytes()), node_index));
            }
        }
        ring.sort_unstable();

        Ok(Self {
            local_node_index,
            nodes,
            ring,
            virtual_nodes,
        })
    }

    pub fn owner(&self, key: &Bytes) -> &ClusterNode {
        let hash = stable_hash(key);
        let point = self
            .ring
            .partition_point(|(ring_hash, _)| *ring_hash < hash);
        let ring_index = if point == self.ring.len() { 0 } else { point };
        &self.nodes[self.ring[ring_index].1]
    }

    pub fn is_local(&self, key: &Bytes) -> bool {
        let owner = self.owner(key);
        owner == &self.nodes[self.local_node_index]
    }

    pub fn local_node(&self) -> &ClusterNode {
        &self.nodes[self.local_node_index]
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn virtual_nodes(&self) -> usize {
        self.virtual_nodes
    }
}

fn stable_hash(value: &[u8]) -> u64 {
    value.iter().fold(FNV_OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

fn validate_node_id(id: &str) -> Result<(), ConfigError> {
    if id.is_empty()
        || id.len() > 64
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(ConfigError::InvalidCombination(format!(
            "invalid cluster node id {id:?}; use 1-64 ASCII letters, digits, '.', '-' or '_'"
        )));
    }
    Ok(())
}

fn validate_address(address: &str) -> Result<(), ConfigError> {
    if address.len() > MAX_CLIENT_ADDRESS_LENGTH
        || !address.is_ascii()
        || address.bytes().any(|byte| byte.is_ascii_whitespace())
    {
        return Err(ConfigError::InvalidCombination(format!(
            "cluster client address must be at most {MAX_CLIENT_ADDRESS_LENGTH} ASCII bytes without whitespace"
        )));
    }
    let (host, port) = address.rsplit_once(':').ok_or_else(|| {
        ConfigError::InvalidCombination(format!(
            "invalid cluster client address {address:?}; expected host:port"
        ))
    })?;
    if host.trim().is_empty() || port.parse::<u16>().ok().filter(|port| *port > 0).is_none() {
        return Err(ConfigError::InvalidCombination(format!(
            "invalid cluster client address {address:?}; expected host:port"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::ClusterTopology;

    #[test]
    fn topology_is_independent_of_membership_order() {
        let first = ClusterTopology::new("node-a", "node-a@a:6380,node-b@b:6380", 32)
            .expect("topology should be valid");
        let second = ClusterTopology::new("node-a", "node-b@b:6380,node-a@a:6380", 32)
            .expect("topology should be valid");
        for key in ["alpha", "beta", "gamma", "delta"] {
            let key = Bytes::copy_from_slice(key.as_bytes());
            assert_eq!(first.owner(&key), second.owner(&key));
        }
    }

    #[test]
    fn every_configured_node_owns_keys() {
        let topology = ClusterTopology::new(
            "node-a",
            "node-a@a:6380,node-b@b:6380,node-c@c:6380",
            128,
        )
        .expect("topology should be valid");
        let mut owners = std::collections::HashSet::new();
        for index in 0..10_000 {
            let key = Bytes::from(format!("key-{index}"));
            owners.insert(topology.owner(&key).id().to_owned());
        }
        assert_eq!(owners.len(), 3);
    }

    #[test]
    fn rejects_duplicate_nodes_and_missing_local_node() {
        assert!(ClusterTopology::new("node-a", "node-a@a:6380,node-a@b:6380", 32).is_err());
        assert!(ClusterTopology::new("node-c", "node-a@a:6380,node-b@b:6380", 32).is_err());
    }

    #[test]
    fn adding_a_node_only_moves_keys_to_the_new_node() {
        let before = ClusterTopology::new("node-a", "node-a@a:6380,node-b@b:6380", 128)
            .expect("topology should be valid");
        let after = ClusterTopology::new(
            "node-a",
            "node-a@a:6380,node-b@b:6380,node-c@c:6380",
            128,
        )
        .expect("topology should be valid");
        let mut moved = 0;
        for index in 0..10_000 {
            let key = Bytes::from(format!("migration-key-{index}"));
            if before.owner(&key).id() != after.owner(&key).id() {
                moved += 1;
                assert_eq!(after.owner(&key).id(), "node-c");
            }
        }
        assert!(moved > 0);
    }
}

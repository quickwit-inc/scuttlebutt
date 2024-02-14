use std::collections::BTreeMap;

use crate::serialize::*;
use crate::{ChitchatId, Heartbeat, Version};

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub(crate) struct NodeDigest {
    pub(crate) heartbeat: Heartbeat,
    pub(crate) max_version: Version,
}

impl NodeDigest {
    pub(crate) fn new(heartbeat: Heartbeat, max_version: Version) -> Self {
        Self {
            heartbeat,
            max_version,
        }
    }
}

/// A digest represents is a piece of information summarizing
/// the staleness of one peer's data.
///
/// It is equivalent to a map
/// peer -> (heartbeat, max version).
#[derive(Debug, Default, Eq, PartialEq)]
pub struct Digest {
    pub(crate) node_digests: BTreeMap<ChitchatId, NodeDigest>,
}

#[cfg(test)]
impl Digest {
    pub fn add_node(&mut self, node: ChitchatId, heartbeat: Heartbeat, max_version: Version) {
        let node_digest = NodeDigest::new(heartbeat, max_version);
        self.node_digests.insert(node, node_digest);
    }
}

impl Serializable for Digest {
    fn serialize(&self, buf: &mut Vec<u8>) {
        (self.node_digests.len() as u16).serialize(buf);
        for (chitchat_id, node_digest) in &self.node_digests {
            chitchat_id.serialize(buf);
            node_digest.heartbeat.serialize(buf);
            node_digest.max_version.serialize(buf);
        }
    }
    fn serialized_len(&self) -> usize {
        let mut len = (self.node_digests.len() as u16).serialized_len();
        for (chitchat_id, node_digest) in &self.node_digests {
            len += chitchat_id.serialized_len();
            len += node_digest.heartbeat.serialized_len();
            len += node_digest.max_version.serialized_len();
        }
        len
    }
}

impl Deserializable for Digest {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let num_nodes = u16::deserialize(buf)?;
        let mut node_digests: BTreeMap<ChitchatId, NodeDigest> = Default::default();

        for _ in 0..num_nodes {
            let chitchat_id = ChitchatId::deserialize(buf)?;
            let heartbeat = Heartbeat::deserialize(buf)?;
            let max_version = u64::deserialize(buf)?;
            let node_digest = NodeDigest::new(heartbeat, max_version);
            node_digests.insert(chitchat_id, node_digest);
        }
        Ok(Digest { node_digests })
    }
}

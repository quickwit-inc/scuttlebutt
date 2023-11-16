use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;
use std::ops::Bound;
use std::time::Instant;

use itertools::Itertools;
use rand::prelude::SliceRandom;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tracing::warn;

use crate::delta::{Delta, DeltaWriter};
use crate::digest::{Digest, NodeDigest};
use crate::{ChitchatId, Heartbeat, MaxVersion, Version, VersionedValue};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeState {
    heartbeat: Heartbeat,
    key_values: BTreeMap<String, VersionedValue>,
    max_version: MaxVersion,
    #[serde(skip)]
    #[serde(default = "Instant::now")]
    last_heartbeat: Instant,
}

impl Default for NodeState {
    fn default() -> Self {
        Self {
            heartbeat: Heartbeat(0),
            key_values: Default::default(),
            max_version: Default::default(),
            last_heartbeat: Instant::now(),
        }
    }
}

impl NodeState {
    /// Returns the node's last heartbeat value.
    pub fn heartbeat(&self) -> Heartbeat {
        self.heartbeat
    }

    /// Returns the node's max version.
    pub fn max_version(&self) -> MaxVersion {
        self.max_version
    }

    /// Returns an iterator over the keys matching the given predicate, excluding keys marked for
    /// deletion.
    pub fn key_values(&self) -> impl Iterator<Item = (&str, &VersionedValue)> {
        self.internal_iter_key_values()
            .filter(|&(_, versioned_value)| versioned_value.tombstone.is_none())
    }

    /// Returns key values matching a prefix
    pub fn iter_prefix<'a>(
        &'a self,
        prefix: &'a str,
    ) -> impl Iterator<Item = (&'a str, &'a VersionedValue)> + 'a {
        let range = (Bound::Included(prefix), Bound::Unbounded);
        self.key_values
            .range::<str, _>(range)
            .take_while(move |(key, _)| key.starts_with(prefix))
            .map(|(key, record)| (key.as_str(), record))
            .filter(|&(_, versioned_value)| versioned_value.tombstone.is_none())
    }

    /// Returns the number of key-value pairs, excluding keys marked for deletion.
    pub fn num_key_values(&self) -> usize {
        self.key_values
            .values()
            .filter(|versioned_value| versioned_value.tombstone.is_none())
            .count()
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.get_versioned(key)
            .map(|versioned_value| versioned_value.value.as_str())
    }

    pub fn get_versioned(&self, key: &str) -> Option<&VersionedValue> {
        self.key_values.get(key)
    }

    /// Sets a new value for a given key.
    ///
    /// Setting a new value automatically increments the
    /// version of the entire NodeState regardless of whether the
    /// value is really changed or not.
    pub fn set(&mut self, key: impl ToString, value: impl ToString) {
        let new_version = self.max_version + 1;
        self.set_with_version(key.to_string(), value.to_string(), new_version);
    }

    /// Marks key for deletion and sets the value to an empty string.
    pub fn mark_for_deletion(&mut self, key: &str) {
        let Some(versioned_value) = self.key_values.get_mut(key) else {
            warn!(
                "Key `{key}` does not exist in the node's state and could not be marked for \
                 deletion.",
            );
            return;
        };
        self.max_version += 1;
        versioned_value.tombstone = Some(self.heartbeat.0);
        versioned_value.version = self.max_version;
        versioned_value.value = "".to_string();
    }

    pub(crate) fn update_heartbeat(&mut self) {
        self.heartbeat.inc();
    }

    fn digest(&self) -> NodeDigest {
        NodeDigest {
            heartbeat: self.heartbeat,
            max_version: self.max_version,
        }
    }

    /// Returns an iterator over keys matching the given predicate.
    /// Not public as it returns also keys marked for deletion.
    fn internal_iter_key_values(&self) -> impl Iterator<Item = (&str, &VersionedValue)> {
        self.key_values
            .iter()
            .map(|(key, record)| (key.as_str(), record))
    }

    /// Removes the keys marked for deletion such that `tombstone + grace_period > heartbeat`.
    fn gc_keys_marked_for_deletion(&mut self, grace_period: u64) {
        let heartbeat = self.heartbeat.0;
        self.key_values
            .retain(|_, versioned_value: &mut VersionedValue| {
                versioned_value
                    .tombstone
                    .map(|tombstone: u64| tombstone + grace_period >= heartbeat)
                    .unwrap_or(true)
            })
    }

    /// Returns an iterator over the versioned values that are strictly greater than
    /// `floor_version`. The floor version typically comes from the max version of a digest.
    ///
    /// This includes keys marked for deletion.
    fn stale_key_values(
        &self,
        floor_version: u64,
    ) -> impl Iterator<Item = (&str, &VersionedValue)> {
        // TODO optimize by checking the max version.
        self.internal_iter_key_values()
            .filter(move |(_key, versioned_value)| versioned_value.version > floor_version)
    }

    /// Sets a new versioned value to associate to a given key.
    /// This operation is ignored if  the key value inserted has a version that is obsolete.
    ///
    /// This method also update the max_version if necessary.
    fn set_versioned_value(&mut self, key: String, versioned_value_update: VersionedValue) {
        self.max_version = versioned_value_update.version.max(self.max_version);
        match self.key_values.entry(key) {
            Entry::Occupied(mut occupied) => {
                let occupied_versioned_value = occupied.get_mut();
                // The current version is more recent than the newer version.
                if occupied_versioned_value.version >= versioned_value_update.version {
                    return;
                }
                *occupied_versioned_value = versioned_value_update;
            }
            Entry::Vacant(vacant) => {
                vacant.insert(versioned_value_update);
            }
        };
    }

    fn set_with_version(&mut self, key: String, value: String, version: Version) {
        assert!(version > self.max_version);
        self.set_versioned_value(
            key,
            VersionedValue {
                value,
                version,
                tombstone: None,
            },
        );
    }
}

#[derive(Debug)]
pub(crate) struct ClusterState {
    pub(crate) node_states: BTreeMap<ChitchatId, NodeState>,
    seed_addrs: watch::Receiver<HashSet<SocketAddr>>,
}

#[cfg(test)]
impl Default for ClusterState {
    fn default() -> Self {
        let (_seed_addrs_tx, seed_addrs_rx) = watch::channel(Default::default());
        Self {
            node_states: Default::default(),
            seed_addrs: seed_addrs_rx,
        }
    }
}

impl ClusterState {
    pub fn with_seed_addrs(seed_addrs: watch::Receiver<HashSet<SocketAddr>>) -> ClusterState {
        ClusterState {
            seed_addrs,
            node_states: BTreeMap::new(),
        }
    }

    pub(crate) fn node_state_mut(&mut self, chitchat_id: &ChitchatId) -> &mut NodeState {
        // TODO use the `hash_raw_entry` feature once it gets stabilized.
        self.node_states.entry(chitchat_id.clone()).or_default()
    }

    pub fn node_state(&self, chitchat_id: &ChitchatId) -> Option<&NodeState> {
        self.node_states.get(chitchat_id)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &ChitchatId> {
        self.node_states.keys()
    }

    pub fn seed_addrs(&self) -> HashSet<SocketAddr> {
        self.seed_addrs.borrow().clone()
    }

    pub(crate) fn remove_node(&mut self, chitchat_id: &ChitchatId) {
        self.node_states.remove(chitchat_id);
    }

    pub(crate) fn apply_delta(&mut self, delta: Delta) {
        // Remove nodes to reset.
        self.node_states
            .retain(|chitchat_id, _| !delta.nodes_to_reset.contains(chitchat_id));

        // Apply delta.
        for (chitchat_id, node_delta) in delta.node_deltas {
            let node_state = self.node_states.entry(chitchat_id).or_default();
            if node_state.heartbeat < node_delta.heartbeat {
                node_state.heartbeat = node_delta.heartbeat;
                node_state.last_heartbeat = Instant::now();
            }
            node_state.max_version = node_state.max_version.max(node_delta.max_version);
            for (key, versioned_value) in node_delta.key_values {
                node_state.max_version = node_state.max_version.max(versioned_value.version);
                node_state.set_versioned_value(key, versioned_value);
            }
        }
    }

    pub fn compute_digest(&self, dead_nodes: &HashSet<&ChitchatId>) -> Digest {
        Digest {
            node_digests: self
                .node_states
                .iter()
                .filter(|(chitchat_id, _)| !dead_nodes.contains(chitchat_id))
                .map(|(chitchat_id, node_state)| (chitchat_id.clone(), node_state.digest()))
                .collect(),
        }
    }

    pub fn gc_keys_marked_for_deletion(
        &mut self,
        marked_for_deletion_grace_period: u64,
        dead_nodes: &HashSet<ChitchatId>,
    ) {
        for (chitchat_id, node_state) in &mut self.node_states {
            if dead_nodes.contains(chitchat_id) {
                continue;
            }
            node_state.gc_keys_marked_for_deletion(marked_for_deletion_grace_period);
        }
    }

    /// Implements the Scuttlebutt reconciliation with the scuttle-depth ordering.
    pub fn compute_delta(
        &self,
        digest: &Digest,
        mtu: usize,
        dead_nodes: &HashSet<&ChitchatId>,
        marked_for_deletion_grace_period: u64,
    ) -> Delta {
        let mut stale_nodes = SortedStaleNodes::default();
        let mut nodes_to_reset = Vec::new();

        for (chitchat_id, node_state) in &self.node_states {
            if dead_nodes.contains(chitchat_id) {
                continue;
            }
            let Some(node_digest) = digest.node_digests.get(chitchat_id) else {
                stale_nodes.insert(chitchat_id, node_state);
                continue;
            };
            if node_digest.heartbeat.0 + marked_for_deletion_grace_period < node_state.heartbeat.0 {
                warn!("Node to reset {chitchat_id:?}");
                nodes_to_reset.push(chitchat_id);
                stale_nodes.insert(chitchat_id, node_state);
                continue;
            }
            stale_nodes.offer(chitchat_id, node_state, node_digest);
        }
        let mut delta_writer = DeltaWriter::with_mtu(mtu);

        for chitchat_id in nodes_to_reset {
            if !delta_writer.add_node_to_reset(chitchat_id.clone()) {
                break;
            }
        }
        for stale_node in stale_nodes.into_iter() {
            if !delta_writer.add_node(stale_node.chitchat_id.clone(), stale_node.heartbeat) {
                break;
            }
            for (key, versioned_value) in stale_node.stale_key_values() {
                if !delta_writer.add_kv(key, versioned_value.clone()) {
                    let delta: Delta = delta_writer.into();
                    return delta;
                }
            }
        }
        delta_writer.into()
    }
}

/// Number of stale key-value pairs carried by the node. A key-value is considered stale if its
/// local version is higher than the max version of the digest, also called "floor version".
type Staleness = usize;

/// Sorts the stale nodes in decreasing order of staleness.
#[derive(Debug, Default)]
struct SortedStaleNodes<'a> {
    stale_nodes: BTreeMap<Staleness, Vec<StaleNode<'a>>>,
}

impl<'a> SortedStaleNodes<'a> {
    /// Adds a node to the list of stale nodes.
    fn insert(&mut self, chitchat_id: &'a ChitchatId, node_state: &'a NodeState) {
        let staleness = node_state.num_key_values() + 1; // +1 for the heartbeat.
        let heartbeat = node_state.heartbeat;
        let floor_version = 0;

        let stale_node = StaleNode {
            chitchat_id,
            heartbeat,
            node_state,
            floor_version,
        };
        self.stale_nodes
            .entry(staleness)
            .or_default()
            .push(stale_node);
    }

    /// Evaluates whether the node should be added to the list of stale nodes.
    fn offer(
        &mut self,
        chitchat_id: &'a ChitchatId,
        node_state: &'a NodeState,
        node_digest: &NodeDigest,
    ) {
        let heartbeat: Heartbeat = node_digest.heartbeat.max(node_state.heartbeat);
        let floor_version = node_digest.max_version;
        let mut staleness = node_state.stale_key_values(floor_version).count();
        if heartbeat > node_digest.heartbeat {
            staleness += 1;
        }
        if staleness > 0 {
            let stale_node = StaleNode {
                chitchat_id,
                heartbeat,
                node_state,
                floor_version,
            };
            self.stale_nodes
                .entry(staleness)
                .or_default()
                .push(stale_node);
        }
    }

    /// Returns an iterator over the stale nodes sorted in decreasing order of staleness.
    /// Nodes with the same level of staleness are shuffled to give them an equal opportunity to be
    /// written into the delta.
    fn into_iter(self) -> impl Iterator<Item = StaleNode<'a>> {
        let mut rng = random_generator();
        self.stale_nodes
            .into_values()
            .rev()
            .flat_map(move |mut stale_nodes| {
                stale_nodes.shuffle(&mut rng);
                stale_nodes.into_iter()
            })
    }
}

/// A stale node, i.e. a node with a stale heartbeat or at least one stale key-value pair.
#[derive(Debug)]
struct StaleNode<'a> {
    chitchat_id: &'a ChitchatId,
    heartbeat: Heartbeat,
    node_state: &'a NodeState,
    floor_version: u64,
}

impl<'a> StaleNode<'a> {
    /// Iterates over the stale key-value pairs in decreasing order of staleness.
    fn stale_key_values(&self) -> impl Iterator<Item = (&str, &VersionedValue)> {
        self.node_state
            .stale_key_values(self.floor_version)
            .sorted_unstable_by_key(|(_, versioned_value)| versioned_value.version)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeStateSnapshot {
    pub chitchat_id: ChitchatId,
    pub node_state: NodeState,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterStateSnapshot {
    pub node_state_snapshots: Vec<NodeStateSnapshot>,
    pub seed_addrs: HashSet<SocketAddr>,
}

impl From<&ClusterState> for ClusterStateSnapshot {
    fn from(cluster_state: &ClusterState) -> Self {
        let node_state_snapshots = cluster_state
            .node_states
            .iter()
            .map(|(chitchat_id, node_state)| NodeStateSnapshot {
                chitchat_id: chitchat_id.clone(),
                node_state: node_state.clone(),
            })
            .collect();
        Self {
            node_state_snapshots,
            seed_addrs: cluster_state.seed_addrs(),
        }
    }
}

#[cfg(not(test))]
fn random_generator() -> impl Rng {
    rand::thread_rng()
}

// We use a deterministic random generator in tests.
#[cfg(test)]
fn random_generator() -> impl Rng {
    use rand::prelude::StdRng;
    use rand::SeedableRng;
    StdRng::seed_from_u64(9u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serialize::Serializable;
    use crate::MAX_UDP_DATAGRAM_PAYLOAD_SIZE;

    #[test]
    fn test_stale_node_iter_stale_key_values() {
        {
            let node = ChitchatId::for_local_test(10_001);
            let node_state = NodeState::default();
            let stale_node = StaleNode {
                chitchat_id: &node,
                heartbeat: Heartbeat(0),
                node_state: &node_state,
                floor_version: 0,
            };
            assert!(stale_node.stale_key_values().next().is_none());
        }
        {
            let node = ChitchatId::for_local_test(10_001);
            let mut node_state = NodeState::default();
            node_state
                .key_values
                .insert("key_a".to_string(), VersionedValue::for_test("value_a", 3));
            node_state
                .key_values
                .insert("key_b".to_string(), VersionedValue::for_test("value_b", 2));
            node_state
                .key_values
                .insert("key_c".to_string(), VersionedValue::for_test("value_c", 1));

            let stale_node = StaleNode {
                chitchat_id: &node,
                heartbeat: Heartbeat(0),
                node_state: &node_state,
                floor_version: 1,
            };
            assert_eq!(
                stale_node.stale_key_values().collect::<Vec<_>>(),
                vec![
                    ("key_b", &VersionedValue::for_test("value_b", 2)),
                    ("key_a", &VersionedValue::for_test("value_a", 3))
                ]
            );
        }
    }

    #[test]
    fn test_sorted_stale_nodes_empty() {
        let stale_nodes = SortedStaleNodes::default();
        assert!(stale_nodes.into_iter().next().is_none());
    }

    #[test]
    fn test_sorted_stale_nodes_insert() {
        let mut stale_nodes = SortedStaleNodes::default();

        let node1 = ChitchatId::for_local_test(10_001);
        let node1_state = NodeState::default();
        stale_nodes.insert(&node1, &node1_state);

        let expected_staleness = 1;
        assert_eq!(stale_nodes.stale_nodes[&expected_staleness].len(), 1);

        let node2 = ChitchatId::for_local_test(10_002);
        let mut node2_state = NodeState::default();
        node2_state
            .key_values
            .insert("key_a".to_string(), VersionedValue::for_test("value_a", 1));
        stale_nodes.insert(&node2, &node2_state);

        let expected_staleness = 2;
        assert_eq!(stale_nodes.stale_nodes[&expected_staleness].len(), 1);

        let node3 = ChitchatId::for_local_test(10_003);
        let mut node3_state = NodeState::default();
        node3_state
            .key_values
            .insert("key_b".to_string(), VersionedValue::for_test("value_b", 2));
        stale_nodes.insert(&node3, &node3_state);

        let expected_staleness = 2;
        assert_eq!(stale_nodes.stale_nodes[&expected_staleness].len(), 2);

        let num_nodes = stale_nodes
            .stale_nodes
            .values()
            .map(|nodes| nodes.len())
            .sum::<usize>();
        assert_eq!(num_nodes, 3);
    }

    #[test]
    fn test_sorted_stale_nodes_offer() {
        let mut stale_nodes = SortedStaleNodes::default();

        let node1 = ChitchatId::for_local_test(10_001);
        let node1_state = NodeState::default();
        stale_nodes.offer(&node1, &node1_state, &NodeDigest::new(Heartbeat(0), 0));

        assert_eq!(stale_nodes.stale_nodes.len(), 0);

        let node2 = ChitchatId::for_local_test(10_002);
        let node2_state = NodeState {
            heartbeat: Heartbeat(1),
            ..Default::default()
        };
        stale_nodes.offer(&node2, &node2_state, &NodeDigest::new(Heartbeat(0), 0));

        let expected_staleness = 1;
        assert_eq!(stale_nodes.stale_nodes[&expected_staleness].len(), 1);

        let node3 = ChitchatId::for_local_test(10_003);
        let mut node3_state = NodeState::default();
        node3_state
            .key_values
            .insert("key_a".to_string(), VersionedValue::for_test("value_a", 1));
        stale_nodes.offer(&node3, &node3_state, &NodeDigest::new(Heartbeat(0), 0));

        let expected_staleness = 1;
        assert_eq!(stale_nodes.stale_nodes[&expected_staleness].len(), 2);

        let node4 = ChitchatId::for_local_test(10_004);
        let mut node4_state = NodeState {
            heartbeat: Heartbeat(1),
            ..Default::default()
        };
        node4_state
            .key_values
            .insert("key_a".to_string(), VersionedValue::for_test("value_a", 1));
        stale_nodes.offer(&node4, &node4_state, &NodeDigest::new(Heartbeat(0), 0));

        let expected_staleness = 2;
        assert_eq!(stale_nodes.stale_nodes[&expected_staleness].len(), 1);
    }

    #[test]
    fn test_sorted_stale_nodes_into_iter() {
        let mut stale_nodes = SortedStaleNodes::default();
        let stale_node1 = StaleNode {
            chitchat_id: &ChitchatId::for_local_test(10_001),
            heartbeat: Heartbeat(0),
            node_state: &NodeState::default(),
            floor_version: 0,
        };
        stale_nodes.stale_nodes.insert(1, vec![stale_node1]);

        let stale_node2 = StaleNode {
            chitchat_id: &ChitchatId::for_local_test(10_002),
            heartbeat: Heartbeat(0),
            node_state: &NodeState::default(),
            floor_version: 0,
        };
        let stale_node3 = StaleNode {
            chitchat_id: &ChitchatId::for_local_test(10_003),
            heartbeat: Heartbeat(0),
            node_state: &NodeState::default(),
            floor_version: 0,
        };
        let stale_node4 = StaleNode {
            chitchat_id: &ChitchatId::for_local_test(10_004),
            heartbeat: Heartbeat(0),
            node_state: &NodeState::default(),
            floor_version: 0,
        };
        stale_nodes
            .stale_nodes
            .insert(2, vec![stale_node2, stale_node3, stale_node4]);

        assert_eq!(
            stale_nodes
                .into_iter()
                .map(|stale_node| stale_node.chitchat_id.gossip_advertise_addr.port())
                .collect::<Vec<_>>(),
            vec![10_003, 10_002, 10_004, 10_001]
        );
    }

    #[test]
    fn test_cluster_state_missing_node() {
        let cluster_state = ClusterState::default();
        let node_state = cluster_state.node_state(&ChitchatId::for_local_test(10_001));
        assert!(node_state.is_none());
    }

    #[test]
    fn test_cluster_state_first_version_is_one() {
        let mut cluster_state = ClusterState::default();
        let node_state = cluster_state.node_state_mut(&ChitchatId::for_local_test(10_001));
        node_state.set("key_a", "");
        assert_eq!(
            node_state.get_versioned("key_a").unwrap(),
            &VersionedValue {
                value: "".to_string(),
                version: 1,
                tombstone: None,
            }
        );
    }

    #[test]
    fn test_cluster_state_set() {
        let mut cluster_state = ClusterState::default();
        let node_state = cluster_state.node_state_mut(&ChitchatId::for_local_test(10_001));
        node_state.set("key_a", "1");
        assert_eq!(
            node_state.get_versioned("key_a").unwrap(),
            &VersionedValue {
                value: "1".to_string(),
                version: 1,
                tombstone: None,
            }
        );
        node_state.set("key_b", "2");
        assert_eq!(
            node_state.get_versioned("key_a").unwrap(),
            &VersionedValue {
                value: "1".to_string(),
                version: 1,
                tombstone: None,
            }
        );
        assert_eq!(
            node_state.get_versioned("key_b").unwrap(),
            &VersionedValue {
                value: "2".to_string(),
                version: 2,
                tombstone: None,
            }
        );
        node_state.set("key_a", "3");
        assert_eq!(
            node_state.get_versioned("key_a").unwrap(),
            &VersionedValue {
                value: "3".to_string(),
                version: 3,
                tombstone: None,
            }
        );
    }

    #[test]
    fn test_cluster_state_set_with_same_value_updates_version() {
        let mut cluster_state = ClusterState::default();
        let node_state = cluster_state.node_state_mut(&ChitchatId::for_local_test(10_001));
        node_state.set("key", "1");
        assert_eq!(
            node_state.get_versioned("key").unwrap(),
            &VersionedValue {
                value: "1".to_string(),
                version: 1,
                tombstone: None,
            }
        );
        node_state.set("key", "1");
        assert_eq!(
            node_state.get_versioned("key").unwrap(),
            &VersionedValue {
                value: "1".to_string(),
                version: 2,
                tombstone: None,
            }
        );
    }

    #[test]
    fn test_cluster_state_set_and_mark_for_deletion() {
        let mut cluster_state = ClusterState::default();
        let node_state = cluster_state.node_state_mut(&ChitchatId::for_local_test(10_001));
        node_state.heartbeat = Heartbeat(10);
        node_state.set("key", "1");
        node_state.mark_for_deletion("key");
        assert_eq!(
            node_state.get_versioned("key").unwrap(),
            &VersionedValue {
                value: "".to_string(),
                version: 2,
                tombstone: Some(10),
            }
        );
        node_state.set("key", "2");
        assert_eq!(
            node_state.get_versioned("key").unwrap(),
            &VersionedValue {
                value: "2".to_string(),
                version: 3,
                tombstone: None,
            }
        );
    }

    #[test]
    fn test_cluster_state_compute_digest() {
        let mut cluster_state = ClusterState::default();
        let node1 = ChitchatId::for_local_test(10_001);
        let node1_state = cluster_state.node_state_mut(&node1);
        node1_state.set("key_a", "");

        let node2 = ChitchatId::for_local_test(10_002);
        let node2_state = cluster_state.node_state_mut(&node2);
        node2_state.set("key_a", "");
        node2_state.set("key_b", "");

        let dead_nodes = HashSet::new();
        let digest = cluster_state.compute_digest(&dead_nodes);

        let mut expected_node_digests = Digest::default();
        expected_node_digests.add_node(node1.clone(), Heartbeat(0), 1);
        expected_node_digests.add_node(node2.clone(), Heartbeat(0), 2);

        assert_eq!(&digest, &expected_node_digests);

        // Consider node 1 dead:
        let dead_nodes = HashSet::from_iter([&node1]);
        let digest = cluster_state.compute_digest(&dead_nodes);

        let mut expected_node_digests = Digest::default();
        expected_node_digests.add_node(node2.clone(), Heartbeat(0), 2);

        assert_eq!(&digest, &expected_node_digests);
    }

    #[test]
    fn test_cluster_state_gc_keys_marked_for_deletion() {
        let mut cluster_state = ClusterState::default();
        let node1 = ChitchatId::for_local_test(10_001);
        let node1_state = cluster_state.node_state_mut(&node1);
        node1_state.heartbeat = Heartbeat(100);
        node1_state.set_with_version("key_a".to_string(), "1".to_string(), 1); // 1
        node1_state.mark_for_deletion("key_a"); // Version 2. Tombstone set to heartbeat 100.
        node1_state.set_with_version("key_b".to_string(), "3".to_string(), 13); // 3
        node1_state.heartbeat = Heartbeat(110);
        // No GC if tombstone (=100) + grace_period <= heartbeat (=110).
        cluster_state.gc_keys_marked_for_deletion(10, &HashSet::new());
        cluster_state
            .node_state(&node1)
            .unwrap()
            .key_values
            .get("key_a")
            .unwrap();
        cluster_state
            .node_state(&node1)
            .unwrap()
            .key_values
            .get("key_b")
            .unwrap();
        // GC if tombstone (=100) + grace_period > heartbeat (=110).
        cluster_state.gc_keys_marked_for_deletion(9, &HashSet::new());
        assert!(cluster_state
            .node_state(&node1)
            .unwrap()
            .key_values
            .get("key_a")
            .is_none());
        cluster_state
            .node_state(&node1)
            .unwrap()
            .key_values
            .get("key_b")
            .unwrap();
    }

    #[test]
    fn test_cluster_state_apply_delta() {
        let mut cluster_state = ClusterState::default();

        let node1 = ChitchatId::for_local_test(10_001);
        let node1_state = cluster_state.node_state_mut(&node1);
        node1_state.set_with_version("key_a".to_string(), "1".to_string(), 1); // 1
        node1_state.set_with_version("key_b".to_string(), "3".to_string(), 3); // 2

        let node2 = ChitchatId::for_local_test(10_002);
        let node2_state = cluster_state.node_state_mut(&node2);
        node2_state.set_with_version("key_c".to_string(), "3".to_string(), 1); // 1

        let mut delta = Delta::default();
        delta.add_node(node1.clone(), Heartbeat(0));
        delta.add_kv(&node1, "key_a", "4", 4, None);
        delta.add_kv(&node1, "key_b", "2", 2, None);

        // Reset node 2.
        delta.add_node_to_reset(node2.clone());
        delta.add_node(node2.clone(), Heartbeat(0));
        delta.add_kv(&node2, "key_d", "4", 4, None);
        cluster_state.apply_delta(delta);

        let node1_state = cluster_state.node_state(&node1).unwrap();
        assert_eq!(
            node1_state.get_versioned("key_a").unwrap(),
            &VersionedValue {
                value: "4".to_string(),
                version: 4,
                tombstone: None,
            }
        );
        // We ignore stale values.
        assert_eq!(
            node1_state.get_versioned("key_b").unwrap(),
            &VersionedValue {
                value: "3".to_string(),
                version: 3,
                tombstone: None,
            }
        );
        // Check node 2 is reset and is only populated with the new `key_d`.
        let node2_state = cluster_state.node_state(&node2).unwrap();
        assert_eq!(node2_state.key_values.len(), 1);
        assert_eq!(
            node2_state.get_versioned("key_d").unwrap(),
            &VersionedValue {
                value: "4".to_string(),
                version: 4,
                tombstone: None,
            }
        );
    }

    // This helper test function will test all possible mtu version, and check that the resulting
    // delta matches the expectation.
    fn test_with_varying_max_transmitted_kv_helper(
        cluster_state: &ClusterState,
        digest: &Digest,
        dead_nodes: &HashSet<&ChitchatId>,
        expected_delta_atoms: &[(&ChitchatId, &str, &str, Version, Option<u64>)],
    ) {
        let max_delta = cluster_state.compute_delta(digest, usize::MAX, dead_nodes, 10_000);
        let mut buf = Vec::new();
        max_delta.serialize(&mut buf);
        let mut mtu_per_num_entries = Vec::new();
        for mtu in 2..buf.len() {
            let delta = cluster_state.compute_delta(digest, mtu, dead_nodes, 10_000);
            let num_tuples = delta.num_tuples();
            if mtu_per_num_entries.len() == num_tuples + 1 {
                continue;
            }
            buf.clear();
            delta.serialize(&mut buf);
            mtu_per_num_entries.push(buf.len());
        }
        for (num_entries, &mtu) in mtu_per_num_entries.iter().enumerate() {
            let mut expected_delta = Delta::default();
            for &(node, key, val, version, tombstone) in &expected_delta_atoms[..num_entries] {
                expected_delta.add_node(node.clone(), Heartbeat(0));
                expected_delta.add_kv(node, key, val, version, tombstone);
            }
            {
                let delta = cluster_state.compute_delta(digest, mtu, dead_nodes, 10_000);
                assert_eq!(&delta, &expected_delta);
            }
            {
                let delta = cluster_state.compute_delta(digest, mtu + 1, dead_nodes, 10_000);
                assert_eq!(&delta, &expected_delta);
            }
        }
    }

    fn test_cluster_state() -> ClusterState {
        let mut cluster_state = ClusterState::default();

        let node1 = ChitchatId::for_local_test(10_001);
        let node1_state = cluster_state.node_state_mut(&node1);
        node1_state.set_with_version("key_a".to_string(), "1".to_string(), 1); // 1
        node1_state.set_with_version("key_b".to_string(), "2".to_string(), 2); // 2

        let node2 = ChitchatId::for_local_test(10_002);
        let node2_state = cluster_state.node_state_mut(&node2);
        node2_state.set_with_version("key_a".to_string(), "1".to_string(), 1); // 1
        node2_state.set_with_version("key_b".to_string(), "2".to_string(), 2); // 2
        node2_state.set_with_version("key_c".to_string(), "3".to_string(), 3); // 3
        node2_state.set_with_version("key_d".to_string(), "4".to_string(), 4); // 4
        node2_state.mark_for_deletion("key_d"); // 5

        cluster_state
    }

    #[test]
    fn test_cluster_state_compute_delta_depth_first_single_node() {
        let cluster_state = test_cluster_state();

        let mut digest = Digest::default();
        let node1 = ChitchatId::for_local_test(10_001);
        let node2 = ChitchatId::for_local_test(10_002);
        digest.add_node(node1.clone(), Heartbeat(0), 1);
        digest.add_node(node2.clone(), Heartbeat(0), 2);

        test_with_varying_max_transmitted_kv_helper(
            &cluster_state,
            &digest,
            &HashSet::new(),
            &[
                (&node2, "key_c", "3", 3, None),
                (&node2, "key_d", "", 5, Some(0)),
                (&node1, "key_b", "2", 2, None),
            ],
        );
    }

    #[test]
    fn test_cluster_state_compute_delta_depth_first_chitchat() {
        let cluster_state = test_cluster_state();

        let mut digest = Digest::default();
        let node1 = ChitchatId::for_local_test(10_001);
        let node2 = ChitchatId::for_local_test(10_002);
        digest.add_node(node1.clone(), Heartbeat(0), 1);
        digest.add_node(node2.clone(), Heartbeat(0), 2);

        test_with_varying_max_transmitted_kv_helper(
            &cluster_state,
            &digest,
            &HashSet::new(),
            &[
                (&node2, "key_c", "3", 3, None),
                (&node2, "key_d", "", 5, Some(0)),
                (&node1, "key_b", "2", 2, None),
            ],
        );
    }

    #[test]
    fn test_cluster_state_compute_delta_missing_node() {
        let cluster_state = test_cluster_state();

        let mut digest = Digest::default();
        let node1 = ChitchatId::for_local_test(10_001);
        let node2 = ChitchatId::for_local_test(10_002);
        digest.add_node(node2.clone(), Heartbeat(0), 3);

        test_with_varying_max_transmitted_kv_helper(
            &cluster_state,
            &digest,
            &HashSet::new(),
            &[
                (&node1, "key_a", "1", 1, None),
                (&node1, "key_b", "2", 2, None),
                (&node2, "key_d", "4", 4, None),
            ],
        );
    }

    #[test]
    fn test_cluster_state_compute_delta_should_ignore_dead_nodes() {
        let cluster_state = test_cluster_state();

        let digest = Digest::default();
        let node1 = ChitchatId::for_local_test(10_001);
        let node2 = ChitchatId::for_local_test(10_002);

        let dead_nodes = HashSet::from_iter([&node2]);

        test_with_varying_max_transmitted_kv_helper(
            &cluster_state,
            &digest,
            &dead_nodes,
            &[
                (&node1, "key_a", "1", 1, None),
                (&node1, "key_b", "2", 2, None),
            ],
        );
    }

    #[test]
    fn test_cluster_state_compute_delta_with_old_node_state_that_needs_reset() {
        let mut cluster_state = ClusterState::default();

        let node1 = ChitchatId::for_local_test(10_001);
        let node1_state = cluster_state.node_state_mut(&node1);
        node1_state.heartbeat = Heartbeat(10000);
        node1_state.set_with_version("key_a".to_string(), "1".to_string(), 1); // 1
        node1_state.set_with_version("key_b".to_string(), "2".to_string(), 2); // 2

        let node2 = ChitchatId::for_local_test(10_002);
        let node2_state = cluster_state.node_state_mut(&node2);
        node2_state.set_with_version("key_c".to_string(), "3".to_string(), 2); // 2

        let mut digest = Digest::default();
        let node1 = ChitchatId::for_local_test(10_001);
        digest.add_node(node1.clone(), Heartbeat(0), 1);
        {
            let delta = cluster_state.compute_delta(
                &digest,
                MAX_UDP_DATAGRAM_PAYLOAD_SIZE,
                &HashSet::new(),
                10_000,
            );
            assert!(delta.nodes_to_reset.is_empty());
            let mut expected_delta = Delta::default();
            expected_delta.add_node(node1.clone(), Heartbeat(10_000));
            expected_delta.add_kv(&node1, "key_b", "2", 2, None);
            expected_delta.add_node(node2.clone(), Heartbeat(0));
            expected_delta.add_kv(&node2.clone(), "key_c", "3", 2, None);
            assert_eq!(delta, expected_delta);
        }
        {
            // Node 1 heartbeat in digest + grace period (9_999) is inferior to the
            // node1's hearbeat in the cluster state. Thus we expect the cluster to compute a
            // delta that will reset node 1.
            let delta = cluster_state.compute_delta(
                &digest,
                MAX_UDP_DATAGRAM_PAYLOAD_SIZE,
                &HashSet::new(),
                9_999,
            );
            let mut expected_delta = Delta::default();
            expected_delta.add_node_to_reset(node1.clone());
            expected_delta.add_node(node1.clone(), Heartbeat(10_000));
            expected_delta.add_kv(&node1, "key_a", "1", 1, None);
            expected_delta.add_kv(&node1, "key_b", "2", 2, None);
            expected_delta.add_node(node2.clone(), Heartbeat(0));
            expected_delta.add_kv(&node2.clone(), "key_c", "3", 2, None);
            assert_eq!(delta, expected_delta);
        }
    }

    #[test]
    fn test_iter_prefix() {
        let mut node_state = NodeState::default();
        node_state.set("Europe", "");
        node_state.set("Europe:", "");
        node_state.set("Europe:UK", "");
        node_state.set("Asia:Japan", "");
        node_state.set("Europe:Italy", "");
        node_state.set("Africa:Uganda", "");
        node_state.set("Oceania", "");
        node_state.mark_for_deletion("Europe:UK");
        let node_states: Vec<&str> = node_state
            .iter_prefix("Europe:")
            .map(|(key, _v)| key)
            .collect();
        assert_eq!(node_states, &["Europe:", "Europe:Italy"]);
    }
}

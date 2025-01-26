use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::net::{Ipv4Addr, SocketAddr};
use std::ops::Bound;
use std::time::Duration;

use itertools::Itertools;
use rand::prelude::SliceRandom;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::delta::{Delta, DeltaSerializer, NodeDelta};
use crate::digest::{Digest, NodeDigest};
use crate::listener::Listeners;
use crate::types::{DeletionStatus, DeletionStatusMutation};
use crate::{ChitchatId, Heartbeat, KeyChangeEvent, Version, VersionedValue};

#[derive(Clone, Serialize, Deserialize)]
pub struct NodeState {
    chitchat_id: ChitchatId,
    heartbeat: Heartbeat,
    key_values: BTreeMap<String, VersionedValue>,
    #[serde(skip)]
    listeners: Listeners,
    max_version: Version,
    // This is the maximum version of the last tombstone GC.
    //
    // Due to the garbage collection of tombstones, we cannot
    // safely do replication with nodes that are asking for a
    // diff from a version lower than this.
    //
    // `last_gc_version` expresses the idea: what is the oldest version from which I can
    // confidently emit delta from. The reason why we update it here, is
    // because a node that was just reset or just joined the cluster will get updates
    // from another node that are actually only make sense in the context of the
    // emission of delta from a `last_gc_version`.
    last_gc_version: Version,
    // A proper interpretation of `max_version` and `last_gc_version` is the following:
    // The state contains exactly:
    // - all of the (non-deleted) key values present at snapshot `max_version`.
    // - all of the tombstones of the entry that were marked for deletion between
    //   (`last_gc_version`, `max_version]`.
    //
    // It does not contain any trace of the tombstones of the entries that were marked for deletion
    // before `<= last_gc_version`.
    //
    // Disclaimer: We do not necessarily have max_version >= last_gc_version.
    // After a reset, a node will have its `last_gc_version` set to the version of the node
    // it is getting its KV from, and it will receive a possible partial set of KVs from that node.
    // As a result it is possible for node to have `last_gc_version` > `max_version`.
}

impl Debug for NodeState {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("NodeState")
            .field("heartbeat", &self.heartbeat)
            .field("key_values", &self.key_values)
            .field("max_version", &self.max_version)
            .finish()
    }
}

impl NodeState {
    fn new(chitchat_id: ChitchatId, listeners: Listeners) -> NodeState {
        NodeState {
            chitchat_id,
            heartbeat: Heartbeat(0),
            key_values: Default::default(),
            max_version: 0u64,
            listeners,
            last_gc_version: 0u64,
        }
    }

    pub fn chitchat_id(&self) -> &ChitchatId {
        &self.chitchat_id
    }

    pub fn last_gc_version(&self) -> Version {
        self.last_gc_version
    }

    pub(crate) fn set_last_gc_version(&mut self, last_gc_version: Version) {
        self.last_gc_version = last_gc_version;
    }

    pub fn for_test() -> NodeState {
        NodeState {
            chitchat_id: ChitchatId {
                node_id: "test-node".to_string(),
                generation_id: 0,
                gossip_advertise_addr: SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 7280),
            },
            heartbeat: Heartbeat(0),
            key_values: Default::default(),
            max_version: Default::default(),
            listeners: Listeners::default(),
            last_gc_version: 0u64,
        }
    }

    /// Returns the node's last heartbeat value.
    pub fn heartbeat(&self) -> Heartbeat {
        self.heartbeat
    }

    /// Returns the node's max version.
    pub fn max_version(&self) -> Version {
        self.max_version
    }

    /// Returns an iterator over keys matching the given predicate.
    /// Disclaimer: This also returns keys marked for deletion.
    pub fn key_values_including_deleted(&self) -> impl Iterator<Item = (&str, &VersionedValue)> {
        self.key_values
            .iter()
            .map(|(key, versioned_value)| (key.as_str(), versioned_value))
    }

    /// Returns an iterator over all of the (non-deleted) key-values.
    pub fn key_values(&self) -> impl Iterator<Item = (&str, &str)> {
        self.key_values_including_deleted()
            .filter(|(_, versioned_value)| !versioned_value.is_deleted())
            .map(|(key, versioned_value)| (key, versioned_value.value.as_str()))
    }

    pub fn set_max_version(&mut self, max_version: Version) {
        self.max_version = max_version;
    }

    // Prepare the node state to receive a delta.
    // Returns `true` if the delta can be applied. In that case, the node state may be mutated (if a
    // reset is required) Returns `false` if the delta cannot be applied. In that case, the node
    // state is not modified.
    #[must_use]
    fn prepare_apply_delta(&mut self, node_delta: &NodeDelta) -> bool {
        if node_delta.from_version_excluded > self.max_version {
            // This delta is coming from the future.
            // We probably experienced a reset and this delta is not usable for us anymore.
            // This is not a bug, it can happen, but we just need to ignore it!
            info!(
                node=?node_delta.chitchat_id,
                from_version=node_delta.from_version_excluded,
                last_gc_version=node_delta.last_gc_version,
                current_last_gc_version=self.last_gc_version,
                "received delta from the future, ignoring it"
            );
            return false;
        }

        if self.max_version > node_delta.last_gc_version {
            // The GCed tombstone have all been already received.
            // We won't miss anything by applying the delta!
            return true;
        }

        // This delta might be missing tombstones with a version within
        // (`node_state.max_version`..`node_delta.last_gc_version`].
        //
        // It is ok if we don't have the associated values to begin
        // with.
        if self.last_gc_version >= node_delta.last_gc_version {
            return true;
        }

        if node_delta.from_version_excluded > 0 {
            warn!(
                node=?node_delta.chitchat_id,
                from_version=node_delta.from_version_excluded,
                last_gc_version=node_delta.last_gc_version,
                current_last_gc_version=self.last_gc_version,
                "received an inapplicable delta, ignoring it");
        }

        let Some(delta_max_version) = node_delta
            .key_values
            .iter()
            .map(|key_value_mutation| key_value_mutation.version)
            .max()
            .or(node_delta.max_version)
        else {
            // This can happen if we just hit the mtu at the moment
            // of writing the SetMaxVersion operation.
            return false;
        };

        if (node_delta.last_gc_version, delta_max_version)
            <= (self.last_gc_version, self.max_version())
        {
            // There is not point applying this delta as it is not bringing us to a newer state.
            warn!(
                node=?node_delta.chitchat_id,
                from_version=node_delta.from_version_excluded,
                delta_max_version=delta_max_version,
                last_gc_version=node_delta.last_gc_version,
                current_last_gc_version=self.last_gc_version,
                "received a delta that does not bring us to a fresher state, ignoring it");
            return false;
        }

        // We are out of sync. This delta is an invitation to `reset` our state.
        info!(
            node=?node_delta.chitchat_id,
            last_gc_version=node_delta.last_gc_version,
            current_last_gc_version=self.last_gc_version,
            "resetting node");
        *self = NodeState::new(node_delta.chitchat_id.clone(), self.listeners.clone());
        // The node_delta max_version  whe
        if let Some(max_version) = node_delta.max_version {
            if node_delta.key_values.is_empty() {
                self.max_version = max_version;
            } else {
                warn!(
                    "Received a delta with a max_version, and key_values as well. This is \
                     unexpected, please report."
                );
            }
        }
        // We need to reset our `last_gc_version`.
        self.last_gc_version = node_delta.last_gc_version;
        true
    }

    fn apply_delta(&mut self, node_delta: NodeDelta, now: Instant) {
        if !self.prepare_apply_delta(&node_delta) {
            return;
        }
        let current_max_version = self.max_version();
        for key_value_mutation in node_delta.key_values {
            if key_value_mutation.version <= current_max_version {
                // We already know about this KV.
                continue;
            }
            if key_value_mutation.status.scheduled_for_deletion() {
                // This KV has already been GCed.
                if key_value_mutation.version <= self.last_gc_version {
                    continue;
                }
            }
            let new_versioned_value = VersionedValue {
                value: key_value_mutation.value,
                version: key_value_mutation.version,
                status: key_value_mutation.status.into_status(now),
            };
            self.set_versioned_value(key_value_mutation.key, new_versioned_value);
        }
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
            .filter(|&(_, versioned_value)| !versioned_value.is_deleted())
            .map(|(key, versioned_value)| (key.as_str(), versioned_value))
    }

    /// Returns the number of key-value pairs, excluding keys marked for deletion.
    pub fn num_key_values(&self) -> usize {
        self.key_values().count()
    }

    /// Returns false if the key is inexistant or marked for deletion.
    pub fn contains_key(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        let versioned_value = self.get_versioned(key)?;
        if versioned_value.is_deleted() {
            return None;
        }
        Some(versioned_value.value.as_str())
    }

    /// If the key is tombstoned, this method will still return the versioned value.
    pub fn get_versioned(&self, key: &str) -> Option<&VersionedValue> {
        self.key_values.get(key)
    }

    /// Sets a new value for a given key.
    ///
    /// Setting a new value automatically increments the
    /// version of the entire NodeState unless the value stays
    /// the same.
    pub fn set(&mut self, key: impl ToString, value: impl ToString) {
        let key = key.to_string();
        let value = value.to_string();
        if let Some(previous_versioned_value) = self.get_versioned(&key) {
            if previous_versioned_value.value == value
                && matches!(previous_versioned_value.status, DeletionStatus::Set)
            {
                // No need to change anything, the value is already set!
                return;
            }
        }
        let new_version = self.max_version + 1;
        self.set_with_version(key, value, new_version);
    }

    /// Sets a new value with a TTL.
    pub fn set_with_ttl(&mut self, key: impl ToString, value: impl ToString) {
        let key = key.to_string();
        let value = value.to_string();
        if let Some(previous_versioned_value) = self.get_versioned(&key) {
            if previous_versioned_value.value == value
                && matches!(
                    previous_versioned_value.status,
                    DeletionStatus::DeleteAfterTtl(_)
                )
            {
                // No need to change anything, the value is already set!
                return;
            }
        }
        let new_version = self.max_version + 1;
        self.set_versioned_value(
            key.to_string(),
            VersionedValue {
                value: value.to_string(),
                version: new_version,
                status: DeletionStatus::DeleteAfterTtl(Instant::now()),
            },
        );
    }

    /// Deletes the entry associated to the given key.
    ///
    /// From the reader's perspective, the entry is deleted right away.
    ///
    /// In reality, the entry is not removed from memory right away, but rather
    /// marked with a tombstone.
    /// That tombstone is annotated with the time of removal, so that after a configurable
    /// grace period, it will be remove by the garbage collection.
    pub fn delete(&mut self, key: &str) {
        let Some(versioned_value) = self.key_values.get_mut(key) else {
            warn!("Key `{key}` does not exist in the node's state and could not be deleted.",);
            return;
        };
        self.max_version += 1;
        versioned_value.version = self.max_version;
        versioned_value.value = "".to_string();
        versioned_value.status = DeletionStatusMutation::Delete.into_status(Instant::now());
    }

    /// Contrary to `delete`, this does not delete an entry right away,
    /// but rather schedules its deletion for after the grace period.
    ///
    /// At the grace period, the entry will be really deleted just like a regular
    /// tombstoned entry.
    ///
    /// Implementation wise, the only difference with `delete` is that it is
    /// treated as if it was present during the grace period.``
    pub fn delete_after_ttl(&mut self, key: &str) {
        let Some(versioned_value) = self.key_values.get_mut(key) else {
            warn!(
                "Key `{key}` does not exist in the node's state and could not scheduled for an \
                 eventual deletion.",
            );
            return;
        };
        self.max_version += 1;
        versioned_value.version = self.max_version;
        versioned_value.status = DeletionStatusMutation::DeleteAfterTtl.into_status(Instant::now());
    }

    pub(crate) fn inc_heartbeat(&mut self) {
        self.heartbeat.inc();
    }

    /// Attempts to set the heartbeat of another node.
    /// If the value is actually not an update, just ignore the data and return false.
    /// As a corner case, the first value is not considered an update.
    ///
    /// Otherwise, returns true.
    pub fn try_set_heartbeat(&mut self, heartbeat_new_value: Heartbeat) -> bool {
        if self.heartbeat.0 == 0 {
            // This is the first heartbeat.
            // Let's set it, but we do not consider it as an update.
            self.heartbeat = heartbeat_new_value;
            return false;
        }
        if heartbeat_new_value > self.heartbeat {
            self.heartbeat = heartbeat_new_value;
            true
        } else {
            false
        }
    }

    fn digest(&self) -> NodeDigest {
        NodeDigest {
            heartbeat: self.heartbeat,
            last_gc_version: self.last_gc_version,
            max_version: self.max_version,
        }
    }

    /// Removes the keys marked for deletion such that `tombstone + grace_period > heartbeat`.
    fn gc_keys_marked_for_deletion(&mut self, grace_period: Duration) {
        let now = Instant::now();
        let mut max_deleted_version = self.last_gc_version;
        self.key_values
            .retain(|_, versioned_value: &mut VersionedValue| {
                let Some(deleted_start_instant) = versioned_value
                    .status
                    .time_of_start_scheduled_for_deletion()
                else {
                    // The KV is not deleted. We keep it!
                    return true;
                };
                if now < deleted_start_instant + grace_period {
                    // We haved not passed the grace period yet. We keep it!
                    return true;
                }
                // We have exceeded the tombstone grace period. Time to remove it.
                max_deleted_version = versioned_value.version.max(max_deleted_version);
                false
            });
        self.last_gc_version = max_deleted_version;
    }

    /// Removes a key-value pair without marking it for deletion.
    ///
    /// Most of the time, you do not want to call this method but,
    /// `mark_for_deletion` instead.
    pub(crate) fn remove_key_value_internal(&mut self, key: &str) {
        self.key_values.remove(key);
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
        self.key_values_including_deleted()
            .filter(move |(_key, versioned_value)| versioned_value.version > floor_version)
    }

    /// Sets a new versioned value to associate to a given key.
    /// This operation is ignored if the key value inserted has a version that is obsolete.
    ///
    /// This method also update the max_version if necessary.
    pub(crate) fn set_versioned_value(
        &mut self,
        key: String,
        versioned_value_update: VersionedValue,
    ) {
        let key_clone = key.clone();
        let key_change_event = KeyChangeEvent {
            key: key_clone.as_str(),
            value: &versioned_value_update.value,
            node: &self.chitchat_id,
        };
        self.max_version = versioned_value_update.version.max(self.max_version);

        match self.key_values.entry(key) {
            Entry::Occupied(mut occupied) => {
                let occupied_versioned_value = occupied.get_mut();
                // The current version is more recent than the newer version.
                if occupied_versioned_value.version >= versioned_value_update.version {
                    return;
                }
                *occupied_versioned_value = versioned_value_update.clone();
            }
            Entry::Vacant(vacant) => {
                vacant.insert(versioned_value_update.clone());
            }
        };
        if !versioned_value_update.is_deleted() {
            self.listeners.trigger_event(key_change_event);
        }
    }

    fn set_with_version(&mut self, key: impl ToString, value: impl ToString, version: Version) {
        assert!(version > self.max_version);
        self.set_versioned_value(
            key.to_string(),
            VersionedValue {
                value: value.to_string(),
                version,
                status: DeletionStatus::Set,
            },
        );
    }
}

pub(crate) struct ClusterState {
    pub(crate) node_states: BTreeMap<ChitchatId, NodeState>,
    seed_addrs: watch::Receiver<HashSet<SocketAddr>>,
    pub(crate) listeners: Listeners,
}

impl Debug for ClusterState {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("Cluster")
            .field("seed_addrs", &self.seed_addrs.borrow())
            .field("node_states", &self.node_states)
            .finish()
    }
}

#[cfg(test)]
impl Default for ClusterState {
    fn default() -> Self {
        let (_seed_addrs_tx, seed_addrs_rx) = watch::channel(Default::default());
        Self {
            node_states: Default::default(),
            seed_addrs: seed_addrs_rx,
            listeners: Default::default(),
        }
    }
}

impl ClusterState {
    pub fn with_seed_addrs(seed_addrs: watch::Receiver<HashSet<SocketAddr>>) -> ClusterState {
        ClusterState {
            seed_addrs,
            node_states: BTreeMap::new(),
            listeners: Default::default(),
        }
    }

    pub(crate) fn node_state_mut(&mut self, chitchat_id: &ChitchatId) -> &mut NodeState {
        // TODO use the `hash_raw_entry` feature once it gets stabilized.
        // Most of the time the entry is already present. We avoid cloning chitchat_id with
        // this if statement.
        self.node_states
            .entry(chitchat_id.clone())
            .or_insert_with(|| NodeState::new(chitchat_id.clone(), self.listeners.clone()))
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
        let now = Instant::now();
        // Apply delta.
        for node_delta in delta.node_deltas {
            let node_state = self.node_state_mut(&node_delta.chitchat_id);
            node_state.apply_delta(node_delta, now);
        }
    }

    pub fn compute_digest(&self, scheduled_for_deletion: &HashSet<&ChitchatId>) -> Digest {
        Digest {
            node_digests: self
                .node_states
                .iter()
                .filter(|(chitchat_id, _)| !scheduled_for_deletion.contains(chitchat_id))
                .map(|(chitchat_id, node_state)| (chitchat_id.clone(), node_state.digest()))
                .collect(),
        }
    }

    pub fn gc_keys_marked_for_deletion(&mut self, marked_for_deletion_grace_period: Duration) {
        for node_state in self.node_states.values_mut() {
            node_state.gc_keys_marked_for_deletion(marked_for_deletion_grace_period);
        }
    }

    /// Implements the Scuttlebutt reconciliation with the scuttle-depth ordering.
    ///
    /// Nodes that are scheduled for deletion (as passed by argument) are not shared.
    pub fn compute_partial_delta_respecting_mtu(
        &self,
        digest: &Digest,
        mtu: usize,
        scheduled_for_deletion: &HashSet<&ChitchatId>,
    ) -> Delta {
        let mut stale_nodes = SortedStaleNodes::default();

        for (chitchat_id, node_state) in &self.node_states {
            if scheduled_for_deletion.contains(chitchat_id) {
                continue;
            }

            let (digest_last_gc_version, digest_max_version) = digest
                .node_digests
                .get(chitchat_id)
                .map(|node_digest| (node_digest.last_gc_version, node_digest.max_version))
                .unwrap_or((0u64, 0u64));

            if node_state.max_version <= digest_max_version {
                // Our version is actually older than the version of the digest.
                // We have no update to offer.
                continue;
            }

            // We have garbage collected some tombstones that the other node does not know about
            // yet. A reset is needed.
            let should_reset = digest_last_gc_version < node_state.last_gc_version
                && digest_max_version < node_state.last_gc_version;

            let from_version_excluded = if should_reset {
                warn!(
                    "Node to reset {chitchat_id:?} last gc version: {} max version: {}",
                    node_state.last_gc_version, digest_max_version
                );
                0u64
            } else {
                digest_max_version
            };

            stale_nodes.offer(chitchat_id, node_state, from_version_excluded);
        }
        let mut delta_serializer = DeltaSerializer::with_mtu(mtu);

        for stale_node in stale_nodes.into_iter() {
            debug!(chitchat_id=?stale_node.chitchat_id, from_version=stale_node.from_version_excluded, "adding stale node");
            if !delta_serializer.try_add_node(
                stale_node.chitchat_id.clone(),
                stale_node.node_state.last_gc_version,
                stale_node.from_version_excluded,
            ) {
                debug!("reached mtu after adding node");
                break;
            };

            let mut added_something = false;
            for (key, versioned_value) in stale_node.stale_key_values() {
                if !delta_serializer.try_add_kv(key, versioned_value.clone()) {
                    return delta_serializer.finish();
                }
                added_something = true;
            }
            // There aren't any key-values in the state_node apparently.
            // Let's add a specific instruction to the delta to set the max version.
            if !added_something {
                // This call returns false if the mtu has been reached.
                //
                // In that case, this empty node update is useless but does not hurt correctness.
                debug!(
                    max_version = stale_node.node_state.max_version,
                    "set max version"
                );
                let _ = delta_serializer.try_set_max_version(stale_node.node_state.max_version);
            }
        }

        delta_serializer.finish()
    }
}

/// Score used to decide which member should be gossiped first.
///
/// Number of stale key-value pairs carried by the node. A key-value is considered stale if its
/// local version is higher than the max version of the digest, also called "floor version".
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
struct Staleness {
    is_unknown: bool,
    max_version: u64,
    num_stale_key_values: usize,
}

/// The ord should be considered a "priority". The higher, the faster a node's
/// information is gossiped.
impl Ord for Staleness {
    fn cmp(&self, other: &Self) -> Ordering {
        // Nodes get gossiped in priority.
        // Unknown nodes get gossiped first.
        // If several nodes are unknown, the one with the lowest max_version gets gossiped first.
        // This is a bit of a hack to make sure we know about the metastore
        // as soon as possible in quickwit, even when the indexer's chitchat state is bloated.
        //
        // Within known nodes, the one with the highest number of stale records gets gossiped first,
        // as described in the scuttlebutt paper.
        self.is_unknown.cmp(&other.is_unknown).then_with(|| {
            if self.is_unknown {
                self.max_version.cmp(&other.max_version).reverse()
            } else {
                // Then nodes with the highest number of stale records get higher priority.
                self.num_stale_key_values.cmp(&other.num_stale_key_values)
            }
        })
    }
}

impl PartialOrd for Staleness {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Sorts the stale nodes in decreasing order of staleness.
#[derive(Default)]
struct SortedStaleNodes<'a> {
    stale_nodes: BTreeMap<Staleness, Vec<StaleNode<'a>>>,
}

/// The `staleness_score` is used to decide which node should be gossiped first.
/// `floor_version` is the version (transmitted in the digest), below which
/// all the records have already been received.
///
/// There is no such thing as a KV for version 0. So if `floor_version == 0`,
/// it means the node is entirely new.
/// We artificially prioritize those nodes to make sure their membership (in quickwit the service
/// key for instance) and initial KVs spread rapidly.
///
/// If no KV is stale, there is nothing to gossip, and we simply return `None`:
/// the node is not a candidate for gossip.
fn staleness_score(node_state: &NodeState, floor_version: u64) -> Option<Staleness> {
    if node_state.max_version() <= floor_version {
        return None;
    }
    let is_unknown = floor_version == 0u64;
    let num_stale_key_values = if is_unknown {
        node_state.num_key_values()
    } else {
        node_state.stale_key_values(floor_version).count()
    };
    Some(Staleness {
        is_unknown,
        max_version: node_state.max_version,
        num_stale_key_values,
    })
}

impl<'a> SortedStaleNodes<'a> {
    /// Adds a to the list of stale nodes.
    /// If the node is not stale (meaning we have no fresher Key Values to share), then this
    /// function simply returns.
    fn offer(
        &mut self,
        chitchat_id: &'a ChitchatId,
        node_state: &'a NodeState,
        from_version_excluded: u64,
    ) {
        let Some(staleness) = staleness_score(node_state, from_version_excluded) else {
            // The node does not have any stale KV.
            return;
        };
        let stale_node = StaleNode {
            chitchat_id,
            node_state,
            from_version_excluded,
        };
        self.stale_nodes
            .entry(staleness)
            .or_default()
            .push(stale_node);
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
    node_state: &'a NodeState,
    from_version_excluded: u64,
}

impl StaleNode<'_> {
    /// Iterates over the stale key-value pairs in decreasing order of staleness.
    fn stale_key_values(&self) -> impl Iterator<Item = (&str, &VersionedValue)> {
        self.node_state
            .stale_key_values(self.from_version_excluded)
            .sorted_unstable_by_key(|(_, versioned_value)| versioned_value.version)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterStateSnapshot {
    pub node_states: Vec<NodeState>,
    pub seed_addrs: HashSet<SocketAddr>,
}

impl From<&ClusterState> for ClusterStateSnapshot {
    fn from(cluster_state: &ClusterState) -> Self {
        let node_states = cluster_state.node_states.values().cloned().collect();
        Self {
            node_states,
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
    use crate::types::{DeletionStatusMutation, KeyValueMutation};
    use crate::MAX_UDP_DATAGRAM_PAYLOAD_SIZE;

    #[test]
    fn test_stale_node_iter_stale_key_values() {
        {
            let node = ChitchatId::for_local_test(10_001);
            let node_state = NodeState::for_test();
            let stale_node = StaleNode {
                chitchat_id: &node,
                node_state: &node_state,
                from_version_excluded: 0u64,
            };
            assert!(stale_node.stale_key_values().next().is_none());
        }
        {
            let node = ChitchatId::for_local_test(10_001);
            let mut node_state = NodeState::for_test();
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
                node_state: &node_state,
                from_version_excluded: 1u64,
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
        let node2 = ChitchatId::for_local_test(10_002);
        let node3 = ChitchatId::for_local_test(10_003);

        // No stale KV. We still insert the node!
        // That way it will get a node state, and be a candidate for gossip later.
        let mut node_state1 = NodeState::for_test();
        node_state1.set_max_version(2);

        stale_nodes.offer(&node1, &node_state1, 0u64);
        assert_eq!(stale_nodes.stale_nodes.len(), 1);

        let mut node2_state = NodeState::for_test();
        node2_state.set_with_version("key_a", "value_a", 1);
        stale_nodes.offer(&node2, &node2_state, 0u64);
        let expected_staleness = Staleness {
            is_unknown: true,
            max_version: 1,
            num_stale_key_values: 0,
        };
        assert_eq!(stale_nodes.stale_nodes[&expected_staleness].len(), 1);

        let mut node3_state = NodeState::for_test();
        node3_state.set_with_version("key_b", "value_b", 2);
        node3_state.set_with_version("key_c", "value_c", 3);

        stale_nodes.offer(&node3, &node3_state, 0u64);
        let expected_staleness = Staleness {
            is_unknown: true,
            max_version: 3,
            num_stale_key_values: 3,
        };
        assert_eq!(stale_nodes.stale_nodes[&expected_staleness].len(), 1);
    }

    #[test]
    fn test_sorted_stale_nodes_offer() {
        let mut stale_nodes = SortedStaleNodes::default();

        let node1 = ChitchatId::for_local_test(10_001);
        let node1_state = NodeState::for_test();
        stale_nodes.offer(&node1, &node1_state, 1u64);
        // No stale records. This is not a candidate for gossip.
        assert!(stale_nodes.stale_nodes.is_empty());

        let node2 = ChitchatId::for_local_test(10_002);
        let mut node2_state = NodeState::for_test();
        node2_state.set_with_version("key_a", "value_a", 1);
        stale_nodes.offer(&node2, &node2_state, 1u64);
        // No stale records (due to the floor version). This is not a candidate for gossip.
        assert!(stale_nodes.stale_nodes.is_empty());

        let node3 = ChitchatId::for_local_test(10_003);
        let mut node3_state = NodeState::for_test();
        node3_state.set_with_version("key_a", "value_a", 1);
        node3_state.set_with_version("key_b", "value_b", 2);
        node3_state.set_with_version("key_c", "value_c", 3);
        stale_nodes.offer(&node3, &node3_state, 1u64);
        assert_eq!(stale_nodes.stale_nodes.len(), 1);

        let node4 = ChitchatId::for_local_test(10_004);
        let mut node4_state = NodeState::for_test();
        node4_state.set_with_version("key_a", "value_a", 1);
        node4_state.set_with_version("key_b", "value_b", 2);
        node4_state.set_with_version("key_c", "value_c", 3);
        node4_state.set_with_version("key_d", "value_d", 4);
        node4_state.set_with_version("key_e", "value_e", 5);
        node4_state.set_with_version("key_f", "value_f", 6);
        node4_state.set_with_version("key_g", "value_g", 7);
        node4_state.set_with_version("key_h", "value_h", 8);
        stale_nodes.offer(&node4, &node4_state, 4u64);
        assert_eq!(stale_nodes.stale_nodes.len(), 2);

        let expected_staleness1 = Staleness {
            is_unknown: false,
            max_version: 3,
            num_stale_key_values: 2,
        };
        let expected_staleness2 = Staleness {
            is_unknown: false,
            max_version: 8,
            num_stale_key_values: 4,
        };

        assert_eq!(stale_nodes.stale_nodes[&expected_staleness1].len(), 1);
        assert_eq!(stale_nodes.stale_nodes[&expected_staleness2].len(), 1);

        let stale_nodes_sorted: Vec<StaleNode<'_>> = stale_nodes.into_iter().collect();
        assert_eq!(stale_nodes_sorted.len(), 2);

        assert_eq!(stale_nodes_sorted[0].chitchat_id, &node4);
        assert_eq!(stale_nodes_sorted[1].chitchat_id, &node3);
    }

    #[test]
    fn test_sorted_stale_nodes_into_iter() {
        let mut stale_nodes = SortedStaleNodes::default();
        let node1 = ChitchatId::for_local_test(10_001);
        let mut node_state1 = NodeState::for_test();
        node_state1.set_with_version("key_a", "value_a", 1);
        node_state1.set_with_version("key_b", "value_b", 2);
        node_state1.set_with_version("key_c", "value_c", 3);
        stale_nodes.offer(&node1, &node_state1, 1u64);
        // 2 stale values.

        let node2 = ChitchatId::for_local_test(10_002);
        let mut node_state2 = NodeState::for_test();
        node_state2.set_with_version("key_a", "value", 1);
        node_state2.set_with_version("key_b", "value_b", 2);
        node_state2.set_with_version("key_c", "value_c", 5);
        stale_nodes.offer(&node2, &node_state2, 2u64);
        // 1 stale value.

        let node3 = ChitchatId::for_local_test(10_003);
        let mut node_state3 = NodeState::for_test();
        node_state3.set_with_version("key_a", "value_a", 1);
        node_state3.set_with_version("key_b", "value_b", 2);
        node_state3.set_with_version("key_c", "value_c", 3);
        stale_nodes.offer(&node3, &node_state3, 7u64);
        // 0 stale values.

        let node4 = ChitchatId::for_local_test(10_004);
        let mut node_state4 = NodeState::for_test();
        node_state4.set_with_version("key_a", "value_a", 1);
        node_state4.set_with_version("key_b", "value_b", 2);
        node_state4.set_with_version("key_c", "value_c", 5);
        node_state4.set_with_version("key_d", "value_d", 7);
        stale_nodes.offer(&node4, &node_state4, 1);

        // 3 stale values
        let node5 = ChitchatId::for_local_test(10_005);
        let node_state5 = NodeState::for_test();
        stale_nodes.offer(&node5, &node_state5, 0);

        // 0 stale values
        let node6 = ChitchatId::for_local_test(10_006);
        let mut node_state6 = NodeState::for_test();
        node_state6.set_with_version("key_a", "value_a", 1);
        stale_nodes.offer(&node6, &node_state6, 0u64);

        // 1 stale values
        assert_eq!(
            stale_nodes
                .into_iter()
                .map(|stale_node| stale_node.chitchat_id.gossip_advertise_addr.port())
                .collect::<Vec<_>>(),
            vec![10_006, 10_004, 10_001, 10_002]
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
                status: DeletionStatus::Set,
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
                status: DeletionStatus::Set,
            }
        );
        node_state.set("key_b", "2");
        assert_eq!(
            node_state.get_versioned("key_a").unwrap(),
            &VersionedValue {
                value: "1".to_string(),
                version: 1,
                status: DeletionStatus::Set,
            }
        );
        assert_eq!(
            node_state.get_versioned("key_b").unwrap(),
            &VersionedValue {
                value: "2".to_string(),
                version: 2,
                status: DeletionStatus::Set,
            }
        );
        node_state.set("key_a", "3");
        assert_eq!(
            node_state.get_versioned("key_a").unwrap(),
            &VersionedValue {
                value: "3".to_string(),
                version: 3,
                status: DeletionStatus::Set
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
                status: DeletionStatus::Set
            }
        );
        node_state.set("key", "1");
        assert_eq!(
            node_state.get_versioned("key").unwrap(),
            &VersionedValue {
                value: "1".to_string(),
                version: 1,
                status: DeletionStatus::Set,
            }
        );
    }

    #[test]
    fn test_cluster_state_set_and_mark_for_deletion() {
        let mut cluster_state = ClusterState::default();
        let node_state = cluster_state.node_state_mut(&ChitchatId::for_local_test(10_001));
        node_state.heartbeat = Heartbeat(10);
        node_state.set("key", "1");
        node_state.delete("key");
        assert!(node_state.get("key").is_none());
        {
            let versioned_value = node_state.get_versioned("key").unwrap();
            assert_eq!(&versioned_value.value, "");
            assert_eq!(versioned_value.version, 2u64);
            assert!(versioned_value.is_deleted());
            assert!(versioned_value
                .status
                .time_of_start_scheduled_for_deletion()
                .is_some());
        }

        // Overriding the same key
        node_state.set("key", "2");
        {
            let versioned_value = node_state.get_versioned("key").unwrap();
            assert_eq!(&versioned_value.value, "2");
            assert_eq!(versioned_value.version, 3u64);
            assert!(!versioned_value.is_deleted());
            assert!(versioned_value
                .status
                .time_of_start_scheduled_for_deletion()
                .is_none());
        }
    }

    #[test]
    fn test_cluster_state_delete_after_ttl() {
        let mut cluster_state = ClusterState::default();
        let node_state = cluster_state.node_state_mut(&ChitchatId::for_local_test(10_001));
        node_state.heartbeat = Heartbeat(10);
        node_state.set("key", "1");
        node_state.delete_after_ttl("key");
        {
            let value = node_state.get("key").unwrap();
            assert_eq!(value, "1");
            let versioned_value = node_state.get_versioned("key").unwrap();
            assert_eq!(&versioned_value.value, "1");
            assert_eq!(versioned_value.version, 2u64);
            assert!(versioned_value
                .status
                .time_of_start_scheduled_for_deletion()
                .is_some());
            assert!(!versioned_value.is_deleted());
            assert!(matches!(
                versioned_value.status,
                DeletionStatus::DeleteAfterTtl(_)
            ));
        }

        // Overriding the same key
        node_state.set("key", "2");
        {
            let versioned_value = node_state.get_versioned("key").unwrap();
            assert_eq!(&versioned_value.value, "2");
            assert_eq!(versioned_value.version, 3u64);
            assert!(!versioned_value.is_deleted());
            assert!(versioned_value
                .status
                .time_of_start_scheduled_for_deletion()
                .is_none());
            assert!(matches!(versioned_value.status, DeletionStatus::Set));
        }
    }

    #[test]
    fn test_cluster_state_compute_digest() {
        let mut cluster_state = ClusterState::default();
        let node1 = ChitchatId::for_local_test(10_001);
        let node1_state = cluster_state.node_state_mut(&node1);
        node1_state.set("key_a", "");

        let node2 = ChitchatId::for_local_test(10_002);
        let node2_state = cluster_state.node_state_mut(&node2);
        node2_state.set_last_gc_version(10u64);
        node2_state.set("key_a", "");
        node2_state.set("key_b", "");

        let digest = cluster_state.compute_digest(&HashSet::new());

        let mut expected_node_digests = Digest::default();
        expected_node_digests.add_node(node1.clone(), Heartbeat(0), 0, 1);
        expected_node_digests.add_node(node2.clone(), Heartbeat(0), 10u64, 2);

        assert_eq!(&digest, &expected_node_digests);
    }

    #[tokio::test]
    async fn test_cluster_state_gc_keys_marked_for_deletion() {
        tokio::time::pause();
        let mut cluster_state = ClusterState::default();
        let node1 = ChitchatId::for_local_test(10_001);
        let node1_state = cluster_state.node_state_mut(&node1);
        node1_state.set("key_a", "1");
        node1_state.delete("key_a"); // Version 2. Tombstone set to heartbeat 100.
        tokio::time::advance(Duration::from_secs(5)).await;
        node1_state.set_with_version("key_b".to_string(), "3".to_string(), 13); // 3
        node1_state.heartbeat = Heartbeat(110);
        // No GC as tombstone is less than 10 secs old.
        cluster_state.gc_keys_marked_for_deletion(Duration::from_secs(10));

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
        tokio::time::advance(Duration::from_secs(5)).await;
        cluster_state.gc_keys_marked_for_deletion(Duration::from_secs(10));
        assert!(!cluster_state
            .node_state(&node1)
            .unwrap()
            .key_values
            .contains_key("key_a"));
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
        delta.add_node(node1.clone(), 0u64, 0u64);
        delta.add_kv(&node1, "key_a", "4", 4, false);
        delta.add_kv(&node1, "key_b", "2", 2, false);

        // We reset node 2
        delta.add_node(node2.clone(), 3, 0);
        delta.add_kv(&node2, "key_d", "4", 4, false);
        cluster_state.apply_delta(delta);

        let node1_state = cluster_state.node_state(&node1).unwrap();
        assert_eq!(
            node1_state.get_versioned("key_a").unwrap(),
            &VersionedValue {
                value: "4".to_string(),
                version: 4,
                status: DeletionStatus::Set,
            }
        );
        // We ignore stale values.
        assert_eq!(
            node1_state.get_versioned("key_b").unwrap(),
            &VersionedValue {
                value: "3".to_string(),
                version: 3,
                status: DeletionStatus::Set,
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
                status: DeletionStatus::Set
            }
        );
    }

    // This helper test function will test all possible mtu version, and check that the resulting
    // delta matches the expectation.
    fn test_with_varying_max_transmitted_kv_helper(
        cluster_state: &ClusterState,
        digest: &Digest,
        dead_nodes: &HashSet<&ChitchatId>,
        expected_delta_atoms: &[(&ChitchatId, &str, &str, Version, bool)],
    ) {
        let max_delta =
            cluster_state.compute_partial_delta_respecting_mtu(digest, usize::MAX, dead_nodes);
        let mut buf = Vec::new();
        max_delta.serialize(&mut buf);
        let mut mtu_per_num_entries = Vec::new();
        for mtu in 100..buf.len() {
            let delta = cluster_state.compute_partial_delta_respecting_mtu(digest, mtu, dead_nodes);
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
                expected_delta.add_node(node.clone(), 0u64, 0u64);
                expected_delta.add_kv(node, key, val, version, tombstone);
            }
            {
                let delta =
                    cluster_state.compute_partial_delta_respecting_mtu(digest, mtu, dead_nodes);
                assert_eq!(&delta, &expected_delta);
            }
            {
                let delta =
                    cluster_state.compute_partial_delta_respecting_mtu(digest, mtu + 1, dead_nodes);
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
        node2_state.delete("key_d"); // 5

        cluster_state
    }

    #[test]
    fn test_cluster_state_compute_delta_depth_first_single_node() {
        let cluster_state = test_cluster_state();

        let mut digest = Digest::default();
        let node1 = ChitchatId::for_local_test(10_001);
        let node2 = ChitchatId::for_local_test(10_002);
        digest.add_node(node1.clone(), Heartbeat(0), 0, 1);
        digest.add_node(node2.clone(), Heartbeat(0), 0, 2);

        test_with_varying_max_transmitted_kv_helper(
            &cluster_state,
            &digest,
            &HashSet::new(),
            &[
                (&node2, "key_c", "3", 3, false),
                (&node2, "key_d", "", 5, true),
                (&node1, "key_b", "2", 2, false),
            ],
        );
    }

    #[test]
    fn test_cluster_state_compute_delta_depth_first_chitchat() {
        let cluster_state = test_cluster_state();

        let mut digest = Digest::default();
        let node1 = ChitchatId::for_local_test(10_001);
        let node2 = ChitchatId::for_local_test(10_002);
        digest.add_node(node1.clone(), Heartbeat(0), 0, 1);
        digest.add_node(node2.clone(), Heartbeat(0), 0, 2);

        test_with_varying_max_transmitted_kv_helper(
            &cluster_state,
            &digest,
            &HashSet::new(),
            &[
                (&node2, "key_c", "3", 3, false),
                (&node2, "key_d", "", 5, true),
                (&node1, "key_b", "2", 2, false),
            ],
        );
    }

    #[test]
    fn test_cluster_state_compute_delta_missing_node() {
        let cluster_state = test_cluster_state();

        let mut digest = Digest::default();
        let node1 = ChitchatId::for_local_test(10_001);
        let node2 = ChitchatId::for_local_test(10_002);
        digest.add_node(node2.clone(), Heartbeat(0), 0, 3);

        test_with_varying_max_transmitted_kv_helper(
            &cluster_state,
            &digest,
            &HashSet::new(),
            &[
                (&node1, "key_a", "1", 1, false),
                (&node1, "key_b", "2", 2, false),
                (&node2, "key_d", "4", 4, false),
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
                (&node1, "key_a", "1", 1, false),
                (&node1, "key_b", "2", 2, false),
            ],
        );
    }

    #[tokio::test]
    async fn test_cluster_state_compute_delta_with_old_node_state_that_needs_reset() {
        tokio::time::pause();
        let mut cluster_state = ClusterState::default();

        let node1 = ChitchatId::for_local_test(10_001);
        let node2 = ChitchatId::for_local_test(10_002);
        {
            let node1_state = cluster_state.node_state_mut(&node1);
            node1_state.heartbeat = Heartbeat(10000);
            node1_state.set_with_version("key_a".to_string(), "1".to_string(), 1); // 1
            node1_state.set_with_version("key_b".to_string(), "2".to_string(), 2); // 2

            let node2_state = cluster_state.node_state_mut(&node2);
            node2_state.set_with_version("key_c".to_string(), "3".to_string(), 2);
            // 2
        }

        {
            let mut digest = Digest::default();
            digest.add_node(node1.clone(), Heartbeat(0), 0, 1);
            let delta = cluster_state.compute_partial_delta_respecting_mtu(
                &digest,
                MAX_UDP_DATAGRAM_PAYLOAD_SIZE,
                &HashSet::new(),
            );
            let mut expected_delta = Delta::default();
            expected_delta.add_node(node2.clone(), 0u64, 0u64);
            expected_delta.add_kv(&node2.clone(), "key_c", "3", 2, false);
            expected_delta.add_node(node1.clone(), 0u64, 1u64);
            expected_delta.add_kv(&node1, "key_b", "2", 2, false);
            expected_delta.set_serialized_len(76);
            assert_eq!(delta, expected_delta);
        }

        cluster_state.node_state_mut(&node1).delete("key_a");
        tokio::time::advance(Duration::from_secs(5)).await;
        cluster_state.gc_keys_marked_for_deletion(Duration::from_secs(10));

        {
            let mut digest = Digest::default();
            let node1 = ChitchatId::for_local_test(10_001);
            digest.add_node(node1.clone(), Heartbeat(0), 0, 1);
            let delta = cluster_state.compute_partial_delta_respecting_mtu(
                &digest,
                MAX_UDP_DATAGRAM_PAYLOAD_SIZE,
                &HashSet::new(),
            );
            let mut expected_delta = Delta::default();
            expected_delta.add_node(node2.clone(), 0u64, 0u64);
            expected_delta.add_kv(&node2.clone(), "key_c", "3", 2, false);
            expected_delta.add_node(node1.clone(), 0u64, 1u64);
            expected_delta.add_kv(&node1, "key_b", "2", 2, false);
            expected_delta.add_kv(&node1, "key_a", "", 3, true);
            expected_delta.set_serialized_len(90);
            assert_eq!(delta, expected_delta);
        }

        const DELETE_GRACE_PERIOD: Duration = Duration::from_secs(10);
        // node1 / key a will be deleted here.
        tokio::time::advance(DELETE_GRACE_PERIOD).await;
        cluster_state
            .node_state_mut(&node1)
            .gc_keys_marked_for_deletion(DELETE_GRACE_PERIOD);

        {
            let mut digest = Digest::default();
            digest.add_node(node1.clone(), Heartbeat(0), 0, 1);
            let delta = cluster_state.compute_partial_delta_respecting_mtu(
                &digest,
                MAX_UDP_DATAGRAM_PAYLOAD_SIZE,
                &HashSet::new(),
            );
            let mut expected_delta = Delta::default();
            expected_delta.add_node(node2.clone(), 0u64, 0u64);
            expected_delta.add_kv(&node2.clone(), "key_c", "3", 2, false);
            // Last gc set to 3 and from version to 0. That's a reset right there.
            expected_delta.add_node(node1.clone(), 3u64, 0u64);
            expected_delta.add_kv(&node1, "key_b", "2", 2, false);
            expected_delta.set_serialized_len(75);
            assert_eq!(&delta, &expected_delta);
        }
    }

    #[test]
    fn test_iter_prefix() {
        let mut node_state = NodeState::for_test();
        node_state.set("Europe", "");
        node_state.set("Europe:", "");
        node_state.set("Europe:UK", "");
        node_state.set("Asia:Japan", "");
        node_state.set("Europe:Italy", "");
        node_state.set("Africa:Uganda", "");
        node_state.set("Oceania", "");
        node_state.delete("Europe:UK");
        let node_states: Vec<&str> = node_state
            .iter_prefix("Europe:")
            .map(|(key, _v)| key)
            .collect();
        assert_eq!(node_states, &["Europe:", "Europe:Italy"]);
    }

    #[test]
    fn test_node_apply_delta_simple() {
        let mut node_state = NodeState::for_test();
        node_state.set_with_version("key_a", "val_a", 1);
        node_state.set_with_version("key_b", "val_a", 2);
        let node_delta = NodeDelta {
            chitchat_id: node_state.chitchat_id.clone(),
            from_version_excluded: 2,
            last_gc_version: 0u64,
            max_version: None,
            key_values: vec![
                KeyValueMutation {
                    key: "key_c".to_string(),
                    value: "val_c".to_string(),
                    version: 4,
                    status: DeletionStatusMutation::Set,
                },
                KeyValueMutation {
                    key: "key_b".to_string(),
                    value: "val_b2".to_string(),
                    version: 3,
                    status: DeletionStatusMutation::Set,
                },
            ],
        };
        node_state.apply_delta(node_delta, Instant::now());
        assert_eq!(node_state.num_key_values(), 3);
        assert_eq!(node_state.max_version(), 4);
        assert_eq!(node_state.last_gc_version, 0);
        assert_eq!(node_state.get("key_a").unwrap(), "val_a");
        assert_eq!(node_state.get("key_b").unwrap(), "val_b2");
        assert_eq!(node_state.get("key_c").unwrap(), "val_c");
    }

    // Here we check that the accessor that dismiss resetting a Kv to the same value is not
    // used in apply delta. Resetting to the same value is very possible in reality several updates
    // happened in a row but were shadowed by the scuttlebutt logic. We DO need to update the
    // version.
    #[test]
    fn test_node_apply_same_value_different_version() {
        let mut node_state = NodeState::for_test();
        node_state.set_with_version("key_a", "val_a", 1);
        let node_delta = NodeDelta {
            chitchat_id: node_state.chitchat_id.clone(),
            from_version_excluded: 1,
            last_gc_version: 0,
            max_version: None,
            key_values: vec![KeyValueMutation {
                key: "key_a".to_string(),
                value: "val_a".to_string(),
                version: 3,
                status: DeletionStatusMutation::Set,
            }],
        };
        node_state.apply_delta(node_delta, Instant::now());
        let versioned_a = node_state.get_versioned("key_a").unwrap();
        assert_eq!(versioned_a.version, 3);
        assert_eq!(versioned_a.status, DeletionStatus::Set);
        assert_eq!(&versioned_a.value, "val_a");
    }

    #[test]
    fn test_node_skip_delta_from_the_future() {
        let mut node_state = NodeState::for_test();
        node_state.set_with_version("key_a", "val_a", 5);
        assert_eq!(node_state.max_version(), 5);
        let node_delta = NodeDelta {
            chitchat_id: node_state.chitchat_id.clone(),
            from_version_excluded: 6, // we skipped version 6 here.
            last_gc_version: 0,
            max_version: None,
            key_values: vec![KeyValueMutation {
                key: "key_a".to_string(),
                value: "new_val".to_string(),
                version: 7,
                status: DeletionStatusMutation::Set,
            }],
        };
        node_state.apply_delta(node_delta, Instant::now());
        let versioned_a = node_state.get_versioned("key_a").unwrap();
        assert_eq!(versioned_a.version, 5);
        assert_eq!(versioned_a.status, DeletionStatus::Set);
        assert_eq!(&versioned_a.value, "val_a");
    }

    #[tokio::test]
    async fn test_node_apply_delta_different_last_gc_is_ok_if_below_max_version() {
        tokio::time::pause();
        const GC_PERIOD: Duration = Duration::from_secs(10);
        let mut node_state = NodeState::for_test();
        node_state.set_with_version("key_a", "val_a", 17);
        node_state.delete("key_a");
        tokio::time::advance(GC_PERIOD).await;
        node_state.gc_keys_marked_for_deletion(GC_PERIOD);
        assert_eq!(node_state.last_gc_version, 18);
        assert_eq!(node_state.max_version(), 18);
        node_state.set_with_version("key_a", "val_a", 31);
        let node_delta = NodeDelta {
            chitchat_id: node_state.chitchat_id.clone(),
            from_version_excluded: 31, // we skipped version 6 here.
            last_gc_version: 30,
            max_version: None,
            key_values: vec![KeyValueMutation {
                key: "key_a".to_string(),
                value: "new_val".to_string(),
                version: 32,
                status: DeletionStatusMutation::Set,
            }],
        };
        node_state.apply_delta(node_delta, Instant::now());
        let versioned_a = node_state.get_versioned("key_a").unwrap();
        assert_eq!(versioned_a.version, 32);
        assert_eq!(node_state.max_version(), 32);
        assert_eq!(versioned_a.status, DeletionStatus::Set);
        assert_eq!(&versioned_a.value, "new_val");
    }

    #[tokio::test]
    async fn test_node_apply_delta_on_reset_fresher_version() {
        tokio::time::pause();
        let mut node_state = NodeState::for_test();
        node_state.set_with_version("key_a", "val_a", 17);
        assert_eq!(node_state.max_version(), 17);
        let node_delta = NodeDelta {
            chitchat_id: node_state.chitchat_id.clone(),
            from_version_excluded: 0, // we skipped version 6 here.
            last_gc_version: 30,
            max_version: None,
            key_values: vec![KeyValueMutation {
                key: "key_b".to_string(),
                value: "val_b".to_string(),
                version: 32,
                status: DeletionStatusMutation::Set,
            }],
        };
        node_state.apply_delta(node_delta, Instant::now());
        assert!(node_state.get_versioned("key_a").is_none());
        let versioned_b = node_state.get_versioned("key_b").unwrap();
        assert_eq!(versioned_b.version, 32);
    }

    #[tokio::test]
    async fn test_node_apply_delta_no_reset_if_older_version() {
        tokio::time::pause();
        let mut node_state = NodeState::for_test();
        node_state.set_with_version("key_a", "val_a", 31);
        node_state.set_with_version("key_b", "val_b2", 32);
        assert_eq!(node_state.max_version(), 32);
        // This does look like a valid reset, but we are already at version 32.
        // Let's ignore this.
        let node_delta = NodeDelta {
            chitchat_id: node_state.chitchat_id.clone(),
            from_version_excluded: 0, // we skipped version 6 here.
            last_gc_version: 17,
            max_version: None,
            key_values: vec![KeyValueMutation {
                key: "key_b".to_string(),
                value: "val_b".to_string(),
                version: 30,
                status: DeletionStatusMutation::Set,
            }],
        };
        node_state.apply_delta(node_delta, Instant::now());
        assert_eq!(node_state.max_version, 32);
        let versioned_b = node_state.get_versioned("key_b").unwrap();
        assert_eq!(versioned_b.version, 32);
        assert_eq!(versioned_b.value, "val_b2");
    }

    #[test]
    fn test_node_set_delete() {
        let mut node_state = NodeState::for_test();
        node_state.set("key_a", "val_b");
        node_state.delete("key_a");
        assert!(node_state.get("key_a").is_none());
    }

    #[test]
    fn test_node_set_delete_after_ttl_set() {
        let mut node_state = NodeState::for_test();
        node_state.set("key_a", "val_b");
        node_state.delete_after_ttl("key_a");
        node_state.set("key_a", "val_b2");
        assert!(matches!(
            node_state.get_versioned("key_a").unwrap().status,
            DeletionStatus::Set
        ));
    }

    #[test]
    fn test_node_set_with_ttl() {
        let mut node_state = NodeState::for_test();
        node_state.set_with_ttl("key_a", "val_b");
        let versioned_value = node_state.get_versioned("key_a").unwrap();
        assert!(matches!(
            versioned_value.status,
            DeletionStatus::DeleteAfterTtl(_)
        ));
        assert_eq!(versioned_value.value, "val_b");
    }
}

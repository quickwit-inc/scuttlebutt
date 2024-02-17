use std::collections::{HashMap, HashSet};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tracing::debug;

use crate::ChitchatId;

/// A phi accrual failure detector implementation.
pub struct FailureDetector {
    /// Heartbeat samples for each node.
    node_samples: HashMap<ChitchatId, SamplingWindow>,
    /// Failure detector configuration.
    config: FailureDetectorConfig,
    /// Denotes live nodes.
    live_nodes: HashSet<ChitchatId>,
    /// Denotes dead nodes.
    dead_nodes: HashMap<ChitchatId, Instant>,
}

impl FailureDetector {
    pub fn new(config: FailureDetectorConfig) -> Self {
        Self {
            node_samples: HashMap::new(),
            config,
            live_nodes: HashSet::new(),
            dead_nodes: HashMap::new(),
        }
    }

    /// Reports node heartbeat.
    pub fn report_heartbeat(&mut self, chitchat_id: &ChitchatId) {
        debug!(node_id=%chitchat_id.node_id, "reporting node heartbeat.");
        let heartbeat_window = self
            .node_samples
            .entry(chitchat_id.clone())
            .or_insert_with(|| {
                SamplingWindow::new(
                    self.config.sampling_window_size,
                    self.config.max_interval,
                    self.config.initial_interval,
                )
            });
        heartbeat_window.report_heartbeat();
    }

    pub fn report_unknown(&mut self, chitchat_id: &ChitchatId) {
        debug!(node_id=%chitchat_id.node_id, "reporting unknown node heartbeat.");
        self.node_samples
            .entry(chitchat_id.clone())
            .or_insert_with(|| {
                SamplingWindow::new(
                    self.config.sampling_window_size,
                    self.config.max_interval,
                    self.config.initial_interval,
                )
            });
    }

    /// Marks the node as dead or alive based on the current phi value.
    pub fn update_node_liveness(&mut self, chitchat_id: &ChitchatId) {
        if let Some(phi) = self.phi(chitchat_id) {
            debug!(node_id=%chitchat_id.node_id, phi=phi, "updating node liveness");
            if phi > self.config.phi_threshold {
                self.live_nodes.remove(chitchat_id);
                self.dead_nodes.insert(chitchat_id.clone(), Instant::now());
                // Remove current sampling window so that when the node
                // comes back online, we start with a fresh sampling window.
                self.node_samples.remove(chitchat_id);
            } else {
                self.live_nodes.insert(chitchat_id.clone());
                self.dead_nodes.remove(chitchat_id);
            }
        }
    }

    /// Removes and returns the list of garbage collectible nodes.
    pub fn garbage_collect(&mut self) -> Vec<ChitchatId> {
        let mut garbage_collected_nodes = Vec::new();
        let now = Instant::now();
        for (chitchat_id, &time_of_death) in &self.dead_nodes {
            if now >= time_of_death + self.config.dead_node_grace_period {
                garbage_collected_nodes.push(chitchat_id.clone())
            }
        }
        for chitchat_id in garbage_collected_nodes.iter() {
            self.dead_nodes.remove(chitchat_id);
        }
        garbage_collected_nodes
    }

    /// Returns the list of nodes considered live by the failure detector.
    pub fn live_nodes(&self) -> impl Iterator<Item = &ChitchatId> {
        self.live_nodes.iter()
    }

    /// Returns the list of nodes considered dead by the failure detector.
    pub fn dead_nodes(&self) -> impl Iterator<Item = &ChitchatId> {
        self.dead_nodes.keys()
    }

    /// Returns the list of nodes considered dead by the failure detector.
    pub fn scheduled_for_deletion_nodes(&self) -> impl Iterator<Item = &ChitchatId> {
        let now = Instant::now();
        let half_dead_node_grace_period = self.config.dead_node_grace_period.div_f32(2.0f32);
        // Note: we can't just compute the threshold now - half_dead_node_grace_period, because it
        // would underflow on some platform (MacOS).
        self.dead_nodes
            .iter()
            .filter_map(move |(chitchat_id, time_of_death)| {
                if *time_of_death + half_dead_node_grace_period < now {
                    Some(chitchat_id)
                } else {
                    None
                }
            })
    }

    /// Returns the current phi value of a node.
    fn phi(&mut self, chitchat_id: &ChitchatId) -> Option<f64> {
        self.node_samples
            .get(chitchat_id)
            .map(|sampling_window| sampling_window.phi())
    }
}

/// The failure detector config struct.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureDetectorConfig {
    /// Phi threshold value above which a node is flagged as faulty.
    pub phi_threshold: f64,
    /// Sampling window size
    pub sampling_window_size: usize,
    /// Heartbeat longer than this will be dropped.
    pub max_interval: Duration,
    /// Initial interval used on startup when no previous heartbeat exists.
    pub initial_interval: Duration,
    /// Threshold period after which dead node can be removed from the cluster.
    pub dead_node_grace_period: Duration,
}

impl FailureDetectorConfig {
    pub fn new(
        phi_threshold: f64,
        sampling_window_size: usize,
        max_interval: Duration,
        initial_interval: Duration,
        dead_node_grace_period: Duration,
    ) -> Self {
        Self {
            phi_threshold,
            sampling_window_size,
            max_interval,
            initial_interval,
            dead_node_grace_period,
        }
    }
}

impl Default for FailureDetectorConfig {
    fn default() -> Self {
        Self {
            phi_threshold: 8.0,
            sampling_window_size: 1_000,
            max_interval: Duration::from_secs(10),
            initial_interval: Duration::from_secs(5),
            dead_node_grace_period: Duration::from_secs(24 * 60 * 60), // 24 hours
        }
    }
}

/// A fixed-sized window that keeps track of the most recent heartbeat arrival intervals.
#[derive(Debug)]
struct SamplingWindow {
    /// The set of collected intervals.
    intervals: BoundedArrayStats,
    /// Last heartbeat reported time.
    last_heartbeat: Option<Instant>,
    /// Heartbeat intervals greater than this value are ignored.
    max_interval: Duration,
    /// The initial interval on startup.
    initial_interval: Duration,
}

impl SamplingWindow {
    // Construct a new instance.
    pub fn new(window_size: usize, max_interval: Duration, initial_interval: Duration) -> Self {
        Self {
            intervals: BoundedArrayStats::new(window_size),
            last_heartbeat: None,
            max_interval,
            initial_interval,
        }
    }

    /// Reports a heartbeat.
    pub fn report_heartbeat(&mut self) {
        if let Some(last_value) = &self.last_heartbeat {
            let interval = last_value.elapsed();
            if interval <= self.max_interval {
                self.intervals.append(interval.as_secs_f64());
            }
        } else {
            self.intervals.append(self.initial_interval.as_secs_f64());
        };
        self.last_heartbeat = Some(Instant::now());
    }

    /// Computes the sampling window's phi value.
    pub fn phi(&self) -> f64 {
        if let Some(last_heartbeat) = self.last_heartbeat {
            assert!(self.intervals.mean() > 0.0);
            let elapsed_time = last_heartbeat.elapsed().as_secs_f64();
            elapsed_time / self.intervals.mean()
        } else {
            // if we phi is called before we have a sample, we assume the node isn't really alive.
            f64::INFINITY
        }
    }
}

/// An array that retains a fixed number of streaming values.
#[derive(Debug)]
struct BoundedArrayStats {
    /// The values.
    data: Vec<f64>,
    /// Number of accumulated values.
    size: usize,
    /// Is the values array filled?
    is_filled: bool,
    /// Position of the index within the values array.
    index: usize,
    /// The accumulated sum of values.
    sum: f64,
    /// The accumulated mean of values.
    mean: f64,
}

impl BoundedArrayStats {
    pub fn new(size: usize) -> Self {
        Self {
            data: vec![0.0; size],
            size,
            is_filled: false,
            index: 0,
            sum: 0.0,
            mean: 0.0,
        }
    }

    /// Returns the mean.
    pub fn mean(&self) -> f64 {
        self.mean
    }

    /// Appends a new value and updates the statistics.
    pub fn append(&mut self, interval: f64) {
        if self.index == self.size {
            self.is_filled = true;
            self.index = 0;
        }

        if self.is_filled {
            self.sum -= self.data[self.index];
        }
        self.sum += interval;

        self.data[self.index] = interval;
        self.index += 1;

        self.mean = self.sum / self.len() as f64;
    }

    fn len(&self) -> usize {
        if self.is_filled {
            return self.size;
        }
        self.index
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rand::prelude::*;

    use super::{BoundedArrayStats, SamplingWindow};
    use crate::failure_detector::{FailureDetector, FailureDetectorConfig};
    use crate::ChitchatId;

    #[tokio::test]
    async fn test_failure_detector() {
        tokio::time::pause();
        let mut rng = rand::thread_rng();
        let mut failure_detector = FailureDetector::new(FailureDetectorConfig::default());

        let intervals_choices = [1u64, 2];
        let chitchat_ids_choices = vec![
            ChitchatId::for_local_test(10_001),
            ChitchatId::for_local_test(10_002),
            ChitchatId::for_local_test(10_003),
        ];
        for _ in 0..=2000 {
            let time_offset = intervals_choices.choose(&mut rng).unwrap();
            let chitchat_id = chitchat_ids_choices.choose(&mut rng).unwrap();
            tokio::time::advance(Duration::from_secs(*time_offset)).await;
            failure_detector.report_heartbeat(chitchat_id);
        }

        for chitchat_id in &chitchat_ids_choices {
            failure_detector.update_node_liveness(chitchat_id);
        }

        let mut live_nodes = failure_detector
            .live_nodes()
            .map(|chitchat_id| chitchat_id.node_id.as_str())
            .collect::<Vec<_>>();
        live_nodes.sort_unstable();
        assert_eq!(live_nodes, vec!["node-10001", "node-10002", "node-10003"]);
        assert_eq!(failure_detector.garbage_collect(), Vec::new());

        // stop reporting heartbeat for few seconds
        tokio::time::advance(Duration::from_secs(50)).await;
        for chitchat_id in &chitchat_ids_choices {
            failure_detector.update_node_liveness(chitchat_id);
        }
        let mut dead_nodes = failure_detector
            .dead_nodes()
            .map(|chitchat_id| chitchat_id.node_id.as_str())
            .collect::<Vec<_>>();
        dead_nodes.sort_unstable();
        assert_eq!(dead_nodes, vec!["node-10001", "node-10002", "node-10003"]);
        assert_eq!(failure_detector.garbage_collect(), Vec::new());

        // Wait for dead_node_grace_period & garbage collect.
        tokio::time::advance(Duration::from_secs(25 * 60 * 60)).await;
        let garbage_collected_nodes = failure_detector.garbage_collect();
        assert_eq!(
            failure_detector
                .live_nodes()
                .map(|chitchat_id| chitchat_id.node_id.as_str())
                .collect::<Vec<_>>(),
            Vec::<&str>::new()
        );
        assert_eq!(
            failure_detector
                .dead_nodes()
                .map(|chitchat_id| chitchat_id.node_id.as_str())
                .collect::<Vec<_>>(),
            Vec::<&str>::new()
        );
        let mut removed_nodes = garbage_collected_nodes
            .iter()
            .map(|chitchat_id| chitchat_id.node_id.as_str())
            .collect::<Vec<_>>();
        removed_nodes.sort_unstable();
        assert_eq!(
            removed_nodes,
            vec!["node-10001", "node-10002", "node-10003"]
        );
    }

    #[tokio::test]
    async fn test_failure_detector_node_state_from_live_to_down_to_live() {
        tokio::time::pause();
        let mut rng = rand::thread_rng();
        let mut failure_detector = FailureDetector::new(FailureDetectorConfig::default());
        let intervals_choices = [1u64, 2];
        let node_1 = ChitchatId::for_local_test(10_001);

        for _ in 0..=2000 {
            let time_offset = intervals_choices.choose(&mut rng).unwrap();
            tokio::time::advance(Duration::from_secs(*time_offset)).await;
            failure_detector.report_heartbeat(&node_1);
        }

        failure_detector.update_node_liveness(&node_1);
        assert_eq!(
            failure_detector
                .live_nodes()
                .map(|chitchat_id| chitchat_id.node_id.as_str())
                .collect::<Vec<_>>(),
            vec!["node-10001"]
        );

        // Check node-1 is down (stop reporting heartbeat).
        tokio::time::advance(Duration::from_secs(20)).await;
        failure_detector.update_node_liveness(&node_1);
        assert_eq!(
            failure_detector
                .live_nodes()
                .map(|chitchat_id| chitchat_id.node_id.as_str())
                .collect::<Vec<_>>(),
            Vec::<&str>::new()
        );

        // Check node-1 is back up (resume reporting heartbeat).
        for _ in 0..=500 {
            let time_offset = intervals_choices.choose(&mut rng).unwrap();
            tokio::time::advance(Duration::from_secs(*time_offset)).await;
            failure_detector.report_heartbeat(&node_1);
        }
        failure_detector.update_node_liveness(&node_1);
        assert_eq!(
            failure_detector
                .live_nodes()
                .map(|chitchat_id| chitchat_id.node_id.as_str())
                .collect::<Vec<_>>(),
            vec!["node-10001"]
        );
    }

    #[tokio::test]
    async fn test_failure_detector_node_state_after_initial_interval() {
        tokio::time::pause();
        let mut failure_detector = FailureDetector::new(FailureDetectorConfig::default());

        let chitchat_id = ChitchatId::for_local_test(10_001);
        failure_detector.report_heartbeat(&chitchat_id);

        tokio::time::advance(Duration::from_secs(1)).await;
        failure_detector.update_node_liveness(&chitchat_id);

        let live_nodes = failure_detector
            .live_nodes()
            .map(|chitchat_id| chitchat_id.node_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(live_nodes, vec!["node-10001"]);
        tokio::time::advance(Duration::from_secs(40)).await;
        failure_detector.update_node_liveness(&chitchat_id);

        let live_nodes = failure_detector
            .live_nodes()
            .map(|chitchat_id| chitchat_id.node_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(live_nodes, Vec::<&str>::new());
    }

    #[tokio::test]
    async fn test_sampling_window() {
        tokio::time::pause();
        let mut sampling_window =
            SamplingWindow::new(10, Duration::from_secs(5), Duration::from_secs(2));
        sampling_window.report_heartbeat();

        tokio::time::advance(Duration::from_secs(3)).await;
        sampling_window.report_heartbeat();

        // Now intervals window is: [2.0, 3.0].
        let mean = (2.0 + 3.0) / 2.0;

        // 0s elapsed since last reported heartbeat.
        assert!((sampling_window.phi() - (0.0 / mean)).abs() < f64::EPSILON);

        // 1s elapsed since last reported heartbeat.
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!((sampling_window.phi() - (1.0 / mean)).abs() < f64::EPSILON);

        // Check reported heartbeat later than max_interval is ignore.
        tokio::time::advance(Duration::from_secs(5)).await;
        sampling_window.report_heartbeat();
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(
            (sampling_window.phi() - (2.0 / mean)).abs() < f64::EPSILON,
            "Mean value should not change."
        );
    }

    #[test]
    fn test_bounded_array_stats() {
        let mut bounded_array = BoundedArrayStats::new(10);
        for i in 1..10 {
            bounded_array.append(i as f64);
        }
        assert_eq!(bounded_array.index, 9);
        assert_eq!(bounded_array.len(), 9);
        assert!(!bounded_array.is_filled);
        assert!((bounded_array.mean() - 5.0f64).abs() < f64::EPSILON);

        for i in 10..14 {
            bounded_array.append(i as f64);
        }
        assert_eq!(bounded_array.index, 3);
        assert_eq!(bounded_array.len(), 10);
        assert!(bounded_array.is_filled);
        assert!((bounded_array.mean() - 8.5f64).abs() < f64::EPSILON);
    }
}

#![allow(clippy::bool_assert_comparison)]

mod helpers;

use std::process::Child;
use std::thread;
use std::time::Duration;

use chitchat_test::ApiResponse;
use helpers::spawn_command;

struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}

fn setup_nodes(
    port_offset: usize,
    num_nodes: usize,
    wait_stabilization_secs: u64,
    dns_required_for_seed: bool,
) -> Vec<KillOnDrop> {
    let seed_port = port_offset;
    let seed_node =
        spawn_command(format!("--listen_addr 127.0.0.1:{seed_port} --interval_ms 120").as_str())
            .unwrap();
    let mut child_process_handles = vec![KillOnDrop(seed_node)];
    for i in 1..num_nodes {
        let node_port = seed_port + i;
        let seed_host_name = if dns_required_for_seed {
            "localhost"
        } else {
            "127.0.0.1"
        };
        let command_args = format!(
            "--listen_addr 127.0.0.1:{node_port} --seed {seed_host_name}:{seed_port} --node_id \
             node_{i} --interval_ms 50"
        );
        let node = spawn_command(&command_args).unwrap();
        child_process_handles.push(KillOnDrop(node));
    }
    thread::sleep(Duration::from_secs(wait_stabilization_secs));
    child_process_handles
}

fn get_node_info(node_api_endpoint: &str) -> anyhow::Result<ApiResponse> {
    let response = reqwest::blocking::get(node_api_endpoint)?.json::<ApiResponse>()?;
    Ok(response)
}

#[test]
fn test_multiple_nodes() {
    let child_handles = setup_nodes(13_000, 5, 5, false);
    assert_eq!(child_handles.len(), 5);
    // Check node states through api.
    let info = get_node_info("http://127.0.0.1:13001").unwrap();
    assert!(info.cluster_state.node_states.get("node_3").is_some());
    assert_eq!(info.cluster_id, "testing");
    assert_eq!(info.live_nodes.len(), 4);
    assert_eq!(info.dead_nodes.len(), 0);
}

#[test]
fn test_multiple_nodes_with_dns_resolution_for_seed() {
    let _child_handles = setup_nodes(12_000, 5, 5, true);
    // Check node states through api.
    let info = get_node_info("http://127.0.0.1:12001").unwrap();
    assert!(info.cluster_state.node_states.get("node_3").is_some());
    assert_eq!(info.cluster_id, "testing");
    assert_eq!(info.live_nodes.len(), 4);
    assert_eq!(info.dead_nodes.len(), 0);
}

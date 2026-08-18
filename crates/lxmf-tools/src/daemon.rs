//! LXMF daemon configuration and runner.
//!
//! Python reference: LXMF/Utilities/lxmd.py.

use crate::config::Config;
use lxmf_core::constants::*;
use lxmf_core::router::{LxmRouter, RouterConfig, RouterConfigExt};
use lxmf_core::storage::{
    SqliteStorageOptions, StorageError, StorageHandle, spawn_sqlite_storage_actor_with_options,
};
use std::path::PathBuf;

/// Daemon configuration parsed from an INI config file.
#[derive(Debug, Clone, PartialEq)]
pub struct DaemonConfig {
    pub display_name: Option<String>,
    pub node_name: Option<String>,
    pub announce_at_start: bool,
    pub announce_interval: Option<u64>,
    pub stamp_cost: Option<u8>,
    pub propagation_enabled: bool,
    pub outbound_propagation_node: Option<String>,
    pub propagation_stamp_cost: u8,
    pub propagation_stamp_flex: u8,
    pub peering_cost: u8,
    pub max_peering_cost: u8,
    pub max_peers: usize,
    pub autopeer: bool,
    pub autopeer_maxdepth: usize,
    pub propagation_limit_kb: usize,
    pub sync_limit_kb: usize,
    pub on_inbound_command: Option<String>,
    pub node_announce_at_start: bool,
    pub node_announce_interval: Option<u64>,
    pub auth_required: bool,
    pub control_allowed: Vec<String>,
    pub static_peers: Vec<String>,
    pub prioritise_destinations: Vec<String>,
    pub enforce_stamps: bool,
    pub message_storage_limit: Option<usize>,
    /// Optional SQLite path; relative values are resolved from the LXMF config directory.
    pub database_path: Option<PathBuf>,
    /// SQLite page cache budget in KiB (`PRAGMA cache_size=-N`).
    pub page_cache_size: u32,
    /// Period between passive WAL checkpoint / incremental vacuum passes.
    pub vacuum_interval: u64,
    /// Maximum freelist pages reclaimed in one maintenance pass.
    pub vacuum_pages: u32,
    pub from_static_only: bool,
    /// Max accepted inbound delivery transfer size in KB. Python reference:
    /// `delivery_transfer_max_accepted_size` in `lxmd.py`.
    pub delivery_transfer_max_accepted_size: usize,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            display_name: Some("Anonymous Peer".to_string()),
            node_name: None,
            announce_at_start: false,
            announce_interval: None,
            stamp_cost: None,
            propagation_enabled: false,
            outbound_propagation_node: None,
            propagation_stamp_cost: PROPAGATION_COST,
            propagation_stamp_flex: PROPAGATION_COST_FLEX,
            peering_cost: PEERING_COST,
            max_peering_cost: MAX_PEERING_COST,
            max_peers: MAX_PEERS,
            autopeer: true,
            autopeer_maxdepth: AUTOPEER_MAXDEPTH,
            propagation_limit_kb: PROPAGATION_LIMIT,
            sync_limit_kb: SYNC_LIMIT,
            on_inbound_command: None,
            node_announce_at_start: false,
            node_announce_interval: None,
            auth_required: false,
            control_allowed: Vec::new(),
            static_peers: Vec::new(),
            prioritise_destinations: Vec::new(),
            enforce_stamps: false,
            message_storage_limit: Some(500_000_000),
            database_path: None,
            page_cache_size: 1024,
            vacuum_interval: 3600,
            vacuum_pages: 128,
            from_static_only: false,
            delivery_transfer_max_accepted_size: DELIVERY_LIMIT,
        }
    }
}

impl DaemonConfig {
    pub fn to_router_config(&self) -> RouterConfig {
        RouterConfig {
            propagation_enabled: self.propagation_enabled,
            autopeer: self.autopeer,
            max_peers: self.max_peers,
            propagation_limit_kb: self.propagation_limit_kb,
            delivery_limit_kb: self.delivery_transfer_max_accepted_size,
            sync_limit_kb: self.sync_limit_kb,
            propagation_stamp_cost: self.propagation_stamp_cost,
            propagation_stamp_flex: self.propagation_stamp_flex,
            stamp_cost: self.stamp_cost,
            ext: RouterConfigExt {
                autopeer_maxdepth: self.autopeer_maxdepth,
                peering_cost: self.peering_cost,
                max_peering_cost: self.max_peering_cost,
                auth_required: self.auth_required,
                message_storage_limit: self.message_storage_limit,
                name: self.node_name.clone(),
                from_static_only: self.from_static_only,
                ..Default::default()
            },
        }
    }

    /// Convert the validated YAML model into runtime units and structures.
    pub fn from_config(config: &Config) -> Self {
        Self {
            display_name: Some(config.lxmf.display_name.clone()),
            node_name: config.propagation.node_name.clone(),
            announce_at_start: config.lxmf.announce_at_start,
            announce_interval: minutes_to_seconds(config.lxmf.announce_interval),
            stamp_cost: config.lxmf.stamp_cost,
            propagation_enabled: config.propagation.enable_node,
            outbound_propagation_node: config.propagation.outbound_node.clone(),
            propagation_stamp_cost: config.propagation.propagation_stamp_cost_target,
            propagation_stamp_flex: config.propagation.propagation_stamp_cost_flexibility,
            peering_cost: config.propagation.peering_cost,
            max_peering_cost: config.propagation.remote_peering_cost_max,
            max_peers: config.propagation.max_peers,
            autopeer: config.propagation.autopeer,
            autopeer_maxdepth: config.propagation.autopeer_maxdepth,
            propagation_limit_kb: kb_to_usize_ceil(
                config.propagation.propagation_message_max_accepted_size,
            ),
            sync_limit_kb: kb_to_usize_ceil(config.propagation.propagation_sync_max_accepted_size),
            on_inbound_command: config.lxmf.on_inbound.clone(),
            node_announce_at_start: config.propagation.announce_at_start,
            node_announce_interval: minutes_to_seconds(config.propagation.announce_interval),
            auth_required: config.propagation.auth_required,
            control_allowed: config.propagation.control_allowed.clone(),
            static_peers: config.propagation.static_peers.clone(),
            prioritise_destinations: config.propagation.prioritise_destinations.clone(),
            enforce_stamps: config.propagation.enforce_stamps,
            message_storage_limit: megabytes_to_bytes(config.propagation.message_storage_limit),
            database_path: config.storage.database_path.clone(),
            page_cache_size: config.storage.page_cache_size,
            vacuum_interval: config.storage.vacuum_interval,
            vacuum_pages: config.storage.vacuum_pages,
            from_static_only: config.propagation.from_static_only,
            delivery_transfer_max_accepted_size: kb_to_usize_ceil(
                config.lxmf.delivery_transfer_max_accepted_size,
            ),
        }
    }
}

fn minutes_to_seconds(value: Option<u64>) -> Option<u64> {
    value.map(|minutes| minutes.saturating_mul(60))
}

fn kb_to_usize_ceil(value: f64) -> usize {
    value.max(0.0).ceil().max(1.0) as usize
}

fn megabytes_to_bytes(value: f64) -> Option<usize> {
    let bytes = (value.max(0.0) * 1_000_000.0) as usize;
    (bytes > 0).then_some(bytes)
}

pub fn create_router(config: &DaemonConfig) -> LxmRouter {
    LxmRouter::new(config.to_router_config())
}

pub fn create_router_with_transport(
    config: &DaemonConfig,
    transport_tx: tokio::sync::mpsc::Sender<rns_transport::messages::TransportMessage>,
) -> LxmRouter {
    let mut router = LxmRouter::new(config.to_router_config());
    router.set_transport(transport_tx);
    router
}

pub fn create_router_with_sqlite(
    config: &DaemonConfig,
    transport_tx: tokio::sync::mpsc::Sender<rns_transport::messages::TransportMessage>,
    database_path: &std::path::Path,
) -> Result<(LxmRouter, StorageHandle), StorageError> {
    let storage = spawn_sqlite_storage_actor_with_options(
        database_path.to_path_buf(),
        SqliteStorageOptions {
            page_cache_kib: config.page_cache_size,
        },
    )?;
    let mut router =
        LxmRouter::with_shared_storage_backend(config.to_router_config(), storage.clone());
    router.set_transport(transport_tx);
    Ok((router, storage))
}

/// Execute an `on_inbound` hook with a versioned JSON envelope on stdin.
pub fn execute_on_inbound(command: &str, json: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let parts: Vec<&str> = command.split_whitespace().collect();
    if parts.is_empty() {
        return Ok(());
    }

    let mut cmd = Command::new(parts[0]);
    for arg in &parts[1..] {
        cmd.arg(arg);
    }
    cmd.stdin(Stdio::piped());
    let mut child = cmd.spawn()?;
    child.stdin.take().expect("piped stdin").write_all(json)?;
    let status = child.wait()?;
    if !status.success() {
        tracing::warn!("on_inbound command exited with status: {}", status);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_defaults_match_daemon_defaults() {
        assert_eq!(
            DaemonConfig::from_config(&Config::default()),
            DaemonConfig::default()
        );
    }

    #[test]
    fn typed_config_normalizes_runtime_units() {
        let config = Config::parse(
            "lxmf:\n  announce_interval: 10\n  stamp_cost: 7\npropagation:\n  enable_node: true\n  announce_interval: 20\n  message_storage_limit: 2.5\nstorage:\n  database_path: data/lxmf.sqlite\n",
            "config.yaml",
        )
        .unwrap();
        let runtime = DaemonConfig::from_config(&config);
        assert_eq!(runtime.announce_interval, Some(600));
        assert_eq!(runtime.node_announce_interval, Some(1200));
        assert_eq!(runtime.stamp_cost, Some(7));
        assert!(runtime.propagation_enabled);
        assert_eq!(runtime.message_storage_limit, Some(2_500_000));
        assert_eq!(
            runtime.database_path,
            Some(PathBuf::from("data/lxmf.sqlite"))
        );
    }

    #[test]
    fn router_config_receives_typed_values() {
        let mut config = Config::default();
        config.propagation.enable_node = true;
        config.propagation.max_peers = 9;
        config.propagation.enforce_stamps = true;
        let daemon = DaemonConfig::from_config(&config);
        let router = daemon.to_router_config();
        assert!(router.propagation_enabled);
        assert_eq!(router.max_peers, 9);
        assert!(daemon.enforce_stamps);
    }
}

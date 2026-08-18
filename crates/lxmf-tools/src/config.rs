//! Typed YAML configuration for `lxmd-rs`.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use thiserror::Error;

use lxmf_core::constants::{
    AUTOPEER_MAXDEPTH, MAX_PEERING_COST, MAX_PEERS, PEERING_COST, PROPAGATION_COST,
    PROPAGATION_COST_FLEX, PROPAGATION_LIMIT, SYNC_LIMIT,
};

pub const CONFIG_FILE_NAME: &str = "config.yaml";
pub const EXAMPLE_CONFIG: &str = r#"# rsLXMF YAML configuration
lxmf:
  display_name: Anonymous Peer
  announce_at_start: false
  # announce_interval: 360
  delivery_transfer_max_accepted_size: 1000
  # on_inbound: /path/to/handler

propagation:
  enable_node: false
  # node_name: Anonymous Propagation Node
  announce_at_start: true
  announce_interval: 360
  autopeer: true
  autopeer_maxdepth: 6
  auth_required: false
  # control_allowed:
  #   - 7d7e542829b40f32364499b27438dba8
  # static_peers:
  #   - e17f833c4ddf8890dd3a79a6fea8161d
  # prioritise_destinations:
  #   - 4a594a8cced4a8f6adf23a8ac67b4011

storage:
  # database_path: storage/lxmf/lxmf.sqlite
  page_cache_size: 1024
  vacuum_interval: 3600
  vacuum_pages: 128

logging:
  level: 4
"#;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse {path}: {message}")]
    Parse { path: PathBuf, message: String },
    #[error("invalid configuration: {0}")]
    Validation(String),
    #[error("failed to serialize configuration: {0}")]
    Serialize(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub lxmf: LxmfConfig,
    pub propagation: PropagationConfig,
    pub storage: StorageConfig,
    pub logging: LoggingConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            lxmf: LxmfConfig::default(),
            propagation: PropagationConfig::default(),
            storage: StorageConfig::default(),
            logging: LoggingConfig::default(),
        }
    }
}

impl Config {
    pub fn parse(input: &str, path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let mut config: Self =
            serde_saphyr::from_str(input).map_err(|error| ConfigError::Parse {
                path: path.to_path_buf(),
                message: error.to_string(),
            })?;
        config.normalize();
        config.validate()?;
        Ok(config)
    }

    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let input = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        Self::parse(&input, path)
    }

    pub fn to_yaml(&self) -> Result<String, ConfigError> {
        let mut value = serde_json::to_value(self)
            .map_err(|error| ConfigError::Serialize(error.to_string()))?;
        let defaults = serde_json::to_value(Self::default())
            .map_err(|error| ConfigError::Serialize(error.to_string()))?;
        prune_defaults(&mut value, &defaults);
        serde_saphyr::to_string(&value).map_err(|error| ConfigError::Serialize(error.to_string()))
    }

    fn normalize(&mut self) {
        self.lxmf.delivery_transfer_max_accepted_size =
            self.lxmf.delivery_transfer_max_accepted_size.max(0.38);
        self.propagation.message_storage_limit = self.propagation.message_storage_limit.max(0.005);
        self.propagation.propagation_message_max_accepted_size = self
            .propagation
            .propagation_message_max_accepted_size
            .max(0.38);
        self.propagation.propagation_sync_max_accepted_size = self
            .propagation
            .propagation_sync_max_accepted_size
            .max(0.38);
        self.storage.page_cache_size = self.storage.page_cache_size.clamp(64, 65_536);
        self.storage.vacuum_interval = self.storage.vacuum_interval.max(60);
    }

    fn validate(&self) -> Result<(), ConfigError> {
        for (field, value) in [
            (
                "lxmf.delivery_transfer_max_accepted_size",
                self.lxmf.delivery_transfer_max_accepted_size,
            ),
            (
                "propagation.message_storage_limit",
                self.propagation.message_storage_limit,
            ),
            (
                "propagation.propagation_message_max_accepted_size",
                self.propagation.propagation_message_max_accepted_size,
            ),
            (
                "propagation.propagation_sync_max_accepted_size",
                self.propagation.propagation_sync_max_accepted_size,
            ),
        ] {
            if !value.is_finite() {
                return Err(ConfigError::Validation(format!(
                    "{field} must be a finite number"
                )));
            }
        }
        if !(0..=7).contains(&self.logging.level) {
            return Err(ConfigError::Validation(
                "logging.level must be in 0..=7".into(),
            ));
        }
        validate_hash(
            "propagation.outbound_node",
            self.propagation.outbound_node.as_deref(),
        )?;
        for (field, values) in [
            (
                "propagation.control_allowed",
                &self.propagation.control_allowed,
            ),
            ("propagation.static_peers", &self.propagation.static_peers),
            (
                "propagation.prioritise_destinations",
                &self.propagation.prioritise_destinations,
            ),
        ] {
            let mut unique = HashSet::new();
            for value in values {
                validate_hash(field, Some(value))?;
                if !unique.insert(value) {
                    return Err(ConfigError::Validation(format!(
                        "{field} contains duplicate hash {value}"
                    )));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LxmfConfig {
    pub display_name: String,
    pub announce_at_start: bool,
    /// Minutes; converted to seconds for runtime.
    pub announce_interval: Option<u64>,
    pub stamp_cost: Option<u8>,
    /// KiB.
    pub delivery_transfer_max_accepted_size: f64,
    pub on_inbound: Option<String>,
}

impl Default for LxmfConfig {
    fn default() -> Self {
        Self {
            display_name: "Anonymous Peer".into(),
            announce_at_start: false,
            announce_interval: None,
            stamp_cost: None,
            delivery_transfer_max_accepted_size: 1000.0,
            on_inbound: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PropagationConfig {
    pub enable_node: bool,
    pub node_name: Option<String>,
    pub outbound_node: Option<String>,
    pub auth_required: bool,
    pub announce_at_start: bool,
    /// Minutes; converted to seconds for runtime.
    pub announce_interval: Option<u64>,
    pub autopeer: bool,
    pub autopeer_maxdepth: usize,
    pub max_peers: usize,
    pub from_static_only: bool,
    /// MB.
    pub message_storage_limit: f64,
    /// KiB.
    pub propagation_message_max_accepted_size: f64,
    /// KiB.
    pub propagation_sync_max_accepted_size: f64,
    pub propagation_stamp_cost_target: u8,
    pub propagation_stamp_cost_flexibility: u8,
    pub peering_cost: u8,
    pub remote_peering_cost_max: u8,
    pub control_allowed: Vec<String>,
    pub static_peers: Vec<String>,
    pub prioritise_destinations: Vec<String>,
    pub enforce_stamps: bool,
}

impl Default for PropagationConfig {
    fn default() -> Self {
        Self {
            enable_node: false,
            node_name: None,
            outbound_node: None,
            auth_required: false,
            announce_at_start: false,
            announce_interval: None,
            autopeer: true,
            autopeer_maxdepth: AUTOPEER_MAXDEPTH,
            max_peers: MAX_PEERS,
            from_static_only: false,
            message_storage_limit: 500.0,
            propagation_message_max_accepted_size: PROPAGATION_LIMIT as f64,
            propagation_sync_max_accepted_size: SYNC_LIMIT as f64,
            propagation_stamp_cost_target: PROPAGATION_COST,
            propagation_stamp_cost_flexibility: PROPAGATION_COST_FLEX,
            peering_cost: PEERING_COST,
            remote_peering_cost_max: MAX_PEERING_COST,
            control_allowed: Vec::new(),
            static_peers: Vec::new(),
            prioritise_destinations: Vec::new(),
            enforce_stamps: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    pub database_path: Option<PathBuf>,
    pub page_cache_size: u32,
    pub vacuum_interval: u64,
    pub vacuum_pages: u32,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            database_path: None,
            page_cache_size: 1024,
            vacuum_interval: 3600,
            vacuum_pages: 128,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LoggingConfig {
    pub level: i32,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self { level: 4 }
    }
}

fn validate_hash(field: &str, value: Option<&str>) -> Result<(), ConfigError> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.len() != 32 || hex::decode(value).is_err() {
        return Err(ConfigError::Validation(format!(
            "{field} values must be 32 hexadecimal characters"
        )));
    }
    Ok(())
}

fn prune_defaults(value: &mut serde_json::Value, defaults: &serde_json::Value) {
    let (serde_json::Value::Object(value), serde_json::Value::Object(defaults)) = (value, defaults)
    else {
        return;
    };
    let keys: Vec<String> = value.keys().cloned().collect();
    for key in keys {
        let Some(default) = defaults.get(&key) else {
            continue;
        };
        let remove = match value.get_mut(&key) {
            Some(current @ serde_json::Value::Object(_))
                if matches!(default, serde_json::Value::Object(_)) =>
            {
                prune_defaults(current, default);
                current.as_object().is_some_and(|object| object.is_empty())
            }
            Some(current) => current == default,
            None => false,
        };
        if remove {
            value.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_yaml_applies_defaults() {
        assert_eq!(
            Config::parse("{}\n", "config.yaml").unwrap(),
            Config::default()
        );
    }

    #[test]
    fn compact_yaml_round_trips() {
        let mut config = Config::default();
        config.propagation.enable_node = true;
        let yaml = config.to_yaml().unwrap();
        assert_eq!(yaml, "propagation:\n  enable_node: true\n");
        assert_eq!(Config::parse(&yaml, "config.yaml").unwrap(), config);
    }

    #[test]
    fn unknown_and_invalid_values_are_rejected() {
        assert!(Config::parse("lxmf:\n  display_nmae: x\n", "config.yaml").is_err());
        assert!(Config::parse("logging:\n  level: 8\n", "config.yaml").is_err());
        assert!(Config::parse("propagation:\n  outbound_node: deadbeef\n", "config.yaml").is_err());
    }
}

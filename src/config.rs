use std::{env, path::PathBuf, time::Duration};

use crate::error::ConfigError;

pub const DEFAULT_MAX_FRAME_SIZE: usize = 1024 * 1024;
pub const DEFAULT_MAX_KEY_SIZE: usize = 4 * 1024;
pub const DEFAULT_MAX_VALUE_SIZE: usize = 1024 * 1024;
pub const DEFAULT_REPLICATION_MAX_BATCH_SIZE: usize = 4 * 1024 * 1024;
pub const DEFAULT_REPLICATION_MAX_SNAPSHOT_SIZE: usize = 256 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationRole {
    Standalone,
    Leader,
    Follower,
}

impl ReplicationRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Standalone => "standalone",
            Self::Leader => "leader",
            Self::Follower => "follower",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FsyncMode {
    Always,
    EverySecond,
    None,
}

impl FsyncMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Always => "always",
            Self::EverySecond => "everysec",
            Self::None => "none",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub data_dir: PathBuf,
    pub shards: usize,
    pub max_frame_size: usize,
    pub max_key_size: usize,
    pub max_value_size: usize,
    pub expiration_interval: Duration,
    pub fsync: FsyncMode,
    pub max_connections: usize,
    pub wal_compaction_threshold_bytes: u64,
    pub metrics_enabled: bool,
    pub metrics_host: String,
    pub metrics_port: u16,
    pub replication_role: ReplicationRole,
    pub replication_host: String,
    pub replication_port: u16,
    pub leader_address: String,
    pub replication_interval: Duration,
    pub replication_max_batch_size: usize,
    pub replication_max_snapshot_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_owned(),
            port: 6380,
            data_dir: PathBuf::from("data"),
            shards: 64,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            max_key_size: DEFAULT_MAX_KEY_SIZE,
            max_value_size: DEFAULT_MAX_VALUE_SIZE,
            expiration_interval: Duration::from_millis(1_000),
            fsync: FsyncMode::Always,
            max_connections: 1_024,
            wal_compaction_threshold_bytes: 64 * 1024 * 1024,
            metrics_enabled: true,
            metrics_host: "127.0.0.1".to_owned(),
            metrics_port: 9090,
            replication_role: ReplicationRole::Standalone,
            replication_host: "127.0.0.1".to_owned(),
            replication_port: 6381,
            leader_address: "127.0.0.1:6381".to_owned(),
            replication_interval: Duration::from_millis(250),
            replication_max_batch_size: DEFAULT_REPLICATION_MAX_BATCH_SIZE,
            replication_max_snapshot_size: DEFAULT_REPLICATION_MAX_SNAPSHOT_SIZE,
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    pub fn from_lookup<F>(lookup: F) -> Result<Self, ConfigError>
    where
        F: Fn(&str) -> Option<String>,
    {
        let defaults = Self::default();
        let host = lookup("FORGEKV_HOST").unwrap_or(defaults.host);
        if host.trim().is_empty() {
            return Err(invalid("FORGEKV_HOST", host, "non-empty host"));
        }

        let port = parse_or("FORGEKV_PORT", defaults.port, &lookup)?;
        if port == 0 {
            return Err(invalid("FORGEKV_PORT", "0", "port in 1..=65535"));
        }

        let data_dir = PathBuf::from(
            lookup("FORGEKV_DATA_DIR")
                .unwrap_or_else(|| defaults.data_dir.to_string_lossy().into_owned()),
        );
        if data_dir.as_os_str().is_empty() {
            return Err(invalid("FORGEKV_DATA_DIR", "", "non-empty directory path"));
        }

        let shards: usize = parse_or("FORGEKV_SHARDS", defaults.shards, &lookup)?;
        if !(1..=4096).contains(&shards) {
            return Err(invalid(
                "FORGEKV_SHARDS",
                shards.to_string(),
                "integer in 1..=4096",
            ));
        }

        let max_frame_size = parse_or("FORGEKV_MAX_FRAME_SIZE", defaults.max_frame_size, &lookup)?;
        let max_key_size = parse_or("FORGEKV_MAX_KEY_SIZE", defaults.max_key_size, &lookup)?;
        let max_value_size = parse_or("FORGEKV_MAX_VALUE_SIZE", defaults.max_value_size, &lookup)?;
        if max_frame_size < 2 {
            return Err(invalid(
                "FORGEKV_MAX_FRAME_SIZE",
                max_frame_size.to_string(),
                "integer of at least 2 bytes",
            ));
        }
        if max_frame_size > u32::MAX as usize {
            return Err(invalid(
                "FORGEKV_MAX_FRAME_SIZE",
                max_frame_size.to_string(),
                "integer no greater than u32::MAX",
            ));
        }
        if max_key_size == 0 || max_value_size == 0 {
            return Err(ConfigError::InvalidCombination(
                "FORGEKV_MAX_KEY_SIZE and FORGEKV_MAX_VALUE_SIZE must be greater than zero"
                    .to_owned(),
            ));
        }

        let expiration_ms: u64 = parse_or("FORGEKV_EXPIRATION_INTERVAL_MS", 1_000u64, &lookup)?;
        if expiration_ms == 0 {
            return Err(invalid(
                "FORGEKV_EXPIRATION_INTERVAL_MS",
                "0",
                "positive integer milliseconds",
            ));
        }

        let fsync_value = lookup("FORGEKV_FSYNC").unwrap_or_else(|| "always".to_owned());
        let fsync = match fsync_value.to_ascii_lowercase().as_str() {
            "always" => FsyncMode::Always,
            "everysec" => FsyncMode::EverySecond,
            "none" => FsyncMode::None,
            _ => {
                return Err(invalid(
                    "FORGEKV_FSYNC",
                    fsync_value,
                    "one of: always, everysec, none",
                ))
            }
        };

        let max_connections =
            parse_or("FORGEKV_MAX_CONNECTIONS", defaults.max_connections, &lookup)?;
        if !(1..=1_000_000).contains(&max_connections) {
            return Err(invalid(
                "FORGEKV_MAX_CONNECTIONS",
                max_connections.to_string(),
                "integer in 1..=1000000",
            ));
        }
        let wal_compaction_threshold_bytes = parse_or(
            "FORGEKV_WAL_COMPACTION_THRESHOLD_BYTES",
            defaults.wal_compaction_threshold_bytes,
            &lookup,
        )?;
        let metrics_enabled = match lookup("FORGEKV_METRICS_ENABLED") {
            Some(value) => value
                .parse::<bool>()
                .map_err(|_| invalid("FORGEKV_METRICS_ENABLED", value, "true or false"))?,
            None => defaults.metrics_enabled,
        };
        let metrics_host = lookup("FORGEKV_METRICS_HOST").unwrap_or(defaults.metrics_host);
        if metrics_enabled && metrics_host.trim().is_empty() {
            return Err(invalid(
                "FORGEKV_METRICS_HOST",
                metrics_host,
                "non-empty host",
            ));
        }
        let metrics_port = parse_or("FORGEKV_METRICS_PORT", defaults.metrics_port, &lookup)?;
        if metrics_enabled && metrics_port == 0 {
            return Err(invalid("FORGEKV_METRICS_PORT", "0", "port in 1..=65535"));
        }

        let replication_role_value =
            lookup("FORGEKV_ROLE").unwrap_or_else(|| "standalone".to_owned());
        let replication_role = match replication_role_value.to_ascii_lowercase().as_str() {
            "standalone" => ReplicationRole::Standalone,
            "leader" => ReplicationRole::Leader,
            "follower" => ReplicationRole::Follower,
            _ => {
                return Err(invalid(
                    "FORGEKV_ROLE",
                    replication_role_value,
                    "one of: standalone, leader, follower",
                ))
            }
        };
        let replication_host =
            lookup("FORGEKV_REPLICATION_HOST").unwrap_or(defaults.replication_host);
        if replication_role == ReplicationRole::Leader && replication_host.trim().is_empty() {
            return Err(invalid(
                "FORGEKV_REPLICATION_HOST",
                replication_host,
                "non-empty host",
            ));
        }
        let replication_port = parse_or(
            "FORGEKV_REPLICATION_PORT",
            defaults.replication_port,
            &lookup,
        )?;
        if replication_role == ReplicationRole::Leader && replication_port == 0 {
            return Err(invalid(
                "FORGEKV_REPLICATION_PORT",
                "0",
                "port in 1..=65535",
            ));
        }
        let leader_address = lookup("FORGEKV_LEADER_ADDRESS").unwrap_or(defaults.leader_address);
        if replication_role == ReplicationRole::Follower && leader_address.trim().is_empty() {
            return Err(invalid(
                "FORGEKV_LEADER_ADDRESS",
                leader_address,
                "non-empty leader address",
            ));
        }
        let replication_interval_ms = parse_or("FORGEKV_REPLICATION_INTERVAL_MS", 250u64, &lookup)?;
        if replication_interval_ms == 0 {
            return Err(invalid(
                "FORGEKV_REPLICATION_INTERVAL_MS",
                "0",
                "positive integer milliseconds",
            ));
        }
        let replication_max_batch_size = parse_or(
            "FORGEKV_REPLICATION_MAX_BATCH_SIZE",
            defaults.replication_max_batch_size,
            &lookup,
        )?;
        if !(36..=64 * 1024 * 1024).contains(&replication_max_batch_size) {
            return Err(invalid(
                "FORGEKV_REPLICATION_MAX_BATCH_SIZE",
                replication_max_batch_size.to_string(),
                "integer in 36..=67108864",
            ));
        }
        if replication_role != ReplicationRole::Standalone {
            let minimum_batch_size = max_key_size
                .checked_add(max_value_size)
                .and_then(|size| size.checked_add(36))
                .ok_or_else(|| {
                    ConfigError::InvalidCombination(
                        "key and value limits exceed the supported replication record size"
                            .to_owned(),
                    )
                })?;
            if replication_max_batch_size < minimum_batch_size {
                return Err(invalid(
                    "FORGEKV_REPLICATION_MAX_BATCH_SIZE",
                    replication_max_batch_size.to_string(),
                    "size large enough for one configured WAL record",
                ));
            }
        }
        let replication_max_snapshot_size = parse_or(
            "FORGEKV_REPLICATION_MAX_SNAPSHOT_SIZE",
            defaults.replication_max_snapshot_size,
            &lookup,
        )?;
        if !(16..=1024 * 1024 * 1024).contains(&replication_max_snapshot_size) {
            return Err(invalid(
                "FORGEKV_REPLICATION_MAX_SNAPSHOT_SIZE",
                replication_max_snapshot_size.to_string(),
                "integer in 16..=1073741824",
            ));
        }

        Ok(Self {
            host,
            port,
            data_dir,
            shards,
            max_frame_size,
            max_key_size,
            max_value_size,
            expiration_interval: Duration::from_millis(expiration_ms),
            fsync,
            max_connections,
            wal_compaction_threshold_bytes,
            metrics_enabled,
            metrics_host,
            metrics_port,
            replication_role,
            replication_host,
            replication_port,
            leader_address,
            replication_interval: Duration::from_millis(replication_interval_ms),
            replication_max_batch_size,
            replication_max_snapshot_size,
        })
    }

    pub fn listen_address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn wal_path(&self) -> PathBuf {
        self.data_dir.join("forgekv.wal")
    }

    pub fn snapshot_path(&self) -> PathBuf {
        self.data_dir.join("forgekv.snapshot")
    }

    pub fn metrics_address(&self) -> String {
        format!("{}:{}", self.metrics_host, self.metrics_port)
    }

    pub fn replication_address(&self) -> String {
        format!("{}:{}", self.replication_host, self.replication_port)
    }

    pub fn node_id_path(&self) -> PathBuf {
        self.data_dir.join("forgekv.node")
    }

    pub fn wal_generation_path(&self) -> PathBuf {
        self.data_dir.join("forgekv.generation")
    }

    pub fn replication_state_path(&self) -> PathBuf {
        self.data_dir.join("forgekv.replica")
    }
}

fn parse_or<T, F>(name: &'static str, default: T, lookup: &F) -> Result<T, ConfigError>
where
    T: std::str::FromStr,
    F: Fn(&str) -> Option<String>,
{
    match lookup(name) {
        Some(value) => value
            .parse::<T>()
            .map_err(|_| invalid(name, value, "valid positive integer")),
        None => Ok(default),
    }
}

fn invalid(name: &'static str, value: impl Into<String>, expected: &'static str) -> ConfigError {
    ConfigError::InvalidValue {
        name,
        value: value.into(),
        expected,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{Config, FsyncMode, ReplicationRole};

    #[test]
    fn defaults_are_safe() {
        let config = Config::from_lookup(|_| None).expect("defaults should be valid");
        assert_eq!(config.port, 6380);
        assert_eq!(config.shards, 64);
        assert_eq!(config.fsync, FsyncMode::Always);
    }

    #[test]
    fn rejects_invalid_shard_count() {
        let values = HashMap::from([("FORGEKV_SHARDS", "0".to_owned())]);
        let result = Config::from_lookup(|name| values.get(name).cloned());
        assert!(result.is_err());
    }

    #[test]
    fn rejects_unknown_fsync_mode() {
        let values = HashMap::from([("FORGEKV_FSYNC", "sometimes".to_owned())]);
        let result = Config::from_lookup(|name| values.get(name).cloned());
        assert!(result.is_err());
    }

    #[test]
    fn accepts_everysec_and_connection_limit() {
        let values = HashMap::from([
            ("FORGEKV_FSYNC", "everysec".to_owned()),
            ("FORGEKV_MAX_CONNECTIONS", "128".to_owned()),
        ]);
        let config = Config::from_lookup(|name| values.get(name).cloned())
            .expect("v0.3 configuration should be valid");
        assert_eq!(config.fsync, FsyncMode::EverySecond);
        assert_eq!(config.max_connections, 128);
    }

    #[test]
    fn rejects_excessive_connection_limit() {
        let values = HashMap::from([("FORGEKV_MAX_CONNECTIONS", usize::MAX.to_string())]);
        let result = Config::from_lookup(|name| values.get(name).cloned());
        assert!(result.is_err());
    }

    #[test]
    fn accepts_follower_replication_configuration() {
        let values = HashMap::from([
            ("FORGEKV_ROLE", "follower".to_owned()),
            ("FORGEKV_LEADER_ADDRESS", "leader:6381".to_owned()),
        ]);
        let config = Config::from_lookup(|name| values.get(name).cloned())
            .expect("follower configuration should be valid");
        assert_eq!(config.replication_role, ReplicationRole::Follower);
        assert_eq!(config.leader_address, "leader:6381");
    }
}

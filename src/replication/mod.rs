use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    sync::Arc,
    time::Duration,
};

use crc32fast::Hasher;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{watch, Semaphore},
    task::JoinSet,
    time::{self, timeout, MissedTickBehavior},
};
use tracing::{debug, error, info, warn};

use crate::{
    config::{Config, ReplicationRole},
    error::ReplicationError,
    metrics::Metrics,
    persistence::{Database, ReplicationBatch, ReplicationSnapshot},
};

const PROTOCOL_MAGIC: [u8; 4] = *b"FKRP";
const STATE_MAGIC: [u8; 4] = *b"FKRS";
const REPLICATION_VERSION: u8 = 1;
const HELLO_KIND: u8 = 1;
const BATCH_KIND: u8 = 2;
const SNAPSHOT_KIND: u8 = 3;
const HELLO_FIXED_SIZE: usize = 24;
const RESPONSE_FIXED_SIZE: usize = 48;
const STATE_FIXED_SIZE: usize = 26;
const MAX_NODE_ID_SIZE: usize = 64;
const MAX_REPLICATION_CONNECTIONS: usize = 16;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_INITIAL_ROUNDS: usize = 10_000;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ReplicaState {
    leader_id: String,
    generation: u64,
    offset: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Hello {
    leader_id: String,
    generation: u64,
    offset: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResponseKind {
    Batch,
    Snapshot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SyncResponse {
    kind: ResponseKind,
    leader_id: String,
    generation: u64,
    start_offset: u64,
    end_offset: u64,
    leader_end: u64,
    payload: Vec<u8>,
}

pub async fn initial_sync(
    config: &Config,
    database: Arc<Database>,
    metrics: Arc<Metrics>,
) -> Result<(), ReplicationError> {
    if config.replication_role != ReplicationRole::Follower {
        return Ok(());
    }
    let mut state = load_state(&config.replication_state_path())?;
    for _ in 0..MAX_INITIAL_ROUNDS {
        if synchronize_once(config, database.as_ref(), metrics.as_ref(), &mut state).await? {
            info!(
                leader = %state.leader_id,
                generation = state.generation,
                offset = state.offset,
                "initial follower synchronization completed"
            );
            return Ok(());
        }
    }
    Err(ReplicationError::InvalidProtocol(
        "initial synchronization did not converge",
    ))
}

pub async fn run_follower(
    config: Config,
    database: Arc<Database>,
    metrics: Arc<Metrics>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ReplicationError> {
    let mut state = load_state(&config.replication_state_path())?;
    let mut interval = time::interval(config.replication_interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval.tick().await;
    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            _ = interval.tick() => {
                if let Err(error) = synchronize_once(
                    &config,
                    database.as_ref(),
                    metrics.as_ref(),
                    &mut state,
                ).await {
                    metrics.replication_error();
                    error!(%error, "follower synchronization failed");
                }
            }
        }
    }
    Ok(())
}

pub async fn run_leader(
    listener: TcpListener,
    config: Config,
    database: Arc<Database>,
    metrics: Arc<Metrics>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ReplicationError> {
    info!(
        address = %listener.local_addr()?,
        node_id = %database.node_id(),
        "leader replication endpoint started"
    );
    let permits = Arc::new(Semaphore::new(MAX_REPLICATION_CONNECTIONS));
    let mut connections = JoinSet::new();
    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(value) => value,
                    Err(error) => {
                        warn!(%error, "replication accept failed");
                        continue;
                    }
                };
                let permit = match Arc::clone(&permits).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        warn!(%peer, "replication connection rejected: limit reached");
                        drop(stream);
                        continue;
                    }
                };
                metrics.replication_connection();
                let database = Arc::clone(&database);
                let metrics = Arc::clone(&metrics);
                let config = config.clone();
                connections.spawn(async move {
                    let _permit = permit;
                    if let Err(error) = handle_leader_connection(
                        stream,
                        database,
                        metrics.as_ref(),
                        &config,
                    ).await {
                        metrics.replication_error();
                        warn!(%peer, %error, "replication connection failed");
                    }
                });
            }
        }
    }
    while let Some(result) = connections.join_next().await {
        if let Err(error) = result {
            warn!(%error, "replication task did not exit cleanly");
        }
    }
    info!("leader replication endpoint stopped");
    Ok(())
}

async fn handle_leader_connection(
    mut stream: TcpStream,
    database: Arc<Database>,
    metrics: &Metrics,
    config: &Config,
) -> Result<(), ReplicationError> {
    let hello = timeout(IO_TIMEOUT, read_hello(&mut stream))
        .await
        .map_err(|_| timed_out("replication hello timed out"))??;
    let response = if hello.leader_id == database.node_id() {
        match database
            .read_replication_batch(
                hello.generation,
                hello.offset,
                config.replication_max_batch_size,
            )
            .await?
        {
            Some(batch) => response_from_batch(database.node_id(), batch),
            None => response_from_snapshot(
                database.node_id(),
                database
                    .replication_snapshot(config.replication_max_snapshot_size)
                    .await?,
            ),
        }
    } else {
        response_from_snapshot(
            database.node_id(),
            database
                .replication_snapshot(config.replication_max_snapshot_size)
                .await?,
        )
    };
    let payload_size = response.payload.len() as u64;
    timeout(IO_TIMEOUT, write_response(&mut stream, &response))
        .await
        .map_err(|_| timed_out("replication response timed out"))??;
    metrics.replication_sent(payload_size, response.kind == ResponseKind::Snapshot);
    Ok(())
}

async fn synchronize_once(
    config: &Config,
    database: &Database,
    metrics: &Metrics,
    state: &mut ReplicaState,
) -> Result<bool, ReplicationError> {
    let mut stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(&config.leader_address))
        .await
        .map_err(|_| timed_out("leader connection timed out"))??;
    let hello = Hello {
        leader_id: state.leader_id.clone(),
        generation: state.generation,
        offset: state.offset,
    };
    timeout(IO_TIMEOUT, write_hello(&mut stream, &hello))
        .await
        .map_err(|_| timed_out("replication hello write timed out"))??;
    let response = timeout(IO_TIMEOUT, read_response(&mut stream, config))
        .await
        .map_err(|_| timed_out("replication response read timed out"))??;
    let previous_state = state.clone();
    apply_response(database, metrics, state, &response).await?;
    if *state != previous_state {
        save_state(&config.replication_state_path(), state)?;
    }
    Ok(response.end_offset >= response.leader_end)
}

async fn apply_response(
    database: &Database,
    metrics: &Metrics,
    state: &mut ReplicaState,
    response: &SyncResponse,
) -> Result<(), ReplicationError> {
    if response.leader_id.is_empty()
        || response.generation == 0
        || response.end_offset > response.leader_end
    {
        return Err(ReplicationError::InvalidProtocol(
            "invalid leader identity or offset range",
        ));
    }
    match response.kind {
        ResponseKind::Batch => {
            if state.leader_id != response.leader_id
                || state.generation != response.generation
                || state.offset != response.start_offset
            {
                return Err(ReplicationError::InvalidProtocol(
                    "incremental response does not continue follower state",
                ));
            }
            let expected_length = response
                .end_offset
                .checked_sub(response.start_offset)
                .ok_or(ReplicationError::InvalidProtocol(
                    "incremental offset underflow",
                ))?;
            if expected_length != response.payload.len() as u64 {
                return Err(ReplicationError::InvalidProtocol(
                    "incremental payload length does not match offsets",
                ));
            }
            if !response.payload.is_empty() {
                database
                    .apply_replication_batch(&response.payload, response.start_offset)
                    .await?;
            }
        }
        ResponseKind::Snapshot => {
            if response.start_offset != 0 || response.end_offset != response.leader_end {
                return Err(ReplicationError::InvalidProtocol(
                    "snapshot response does not describe one captured boundary",
                ));
            }
            database
                .install_replication_snapshot(&response.payload)
                .await?;
        }
    }
    state.leader_id.clone_from(&response.leader_id);
    state.generation = response.generation;
    state.offset = response.end_offset;
    metrics.replication_received(response.payload.len() as u64);
    metrics.set_replication_lag(response.leader_end.saturating_sub(response.end_offset));
    debug!(
        generation = state.generation,
        offset = state.offset,
        leader_end = response.leader_end,
        "follower state advanced"
    );
    Ok(())
}

fn response_from_batch(node_id: &str, batch: ReplicationBatch) -> SyncResponse {
    SyncResponse {
        kind: ResponseKind::Batch,
        leader_id: node_id.to_owned(),
        generation: batch.generation,
        start_offset: batch.start_offset,
        end_offset: batch.end_offset,
        leader_end: batch.leader_end,
        payload: batch.bytes,
    }
}

fn response_from_snapshot(node_id: &str, snapshot: ReplicationSnapshot) -> SyncResponse {
    SyncResponse {
        kind: ResponseKind::Snapshot,
        leader_id: node_id.to_owned(),
        generation: snapshot.generation,
        start_offset: 0,
        end_offset: snapshot.offset,
        leader_end: snapshot.offset,
        payload: snapshot.bytes,
    }
}

async fn write_hello(stream: &mut TcpStream, hello: &Hello) -> Result<(), ReplicationError> {
    validate_node_id(&hello.leader_id, true)?;
    let id_length = u16::try_from(hello.leader_id.len())
        .map_err(|_| ReplicationError::InvalidProtocol("node identity is too long"))?;
    let mut encoded = Vec::with_capacity(HELLO_FIXED_SIZE + hello.leader_id.len());
    encoded.extend_from_slice(&PROTOCOL_MAGIC);
    encoded.push(REPLICATION_VERSION);
    encoded.push(HELLO_KIND);
    encoded.extend_from_slice(&id_length.to_be_bytes());
    encoded.extend_from_slice(&hello.generation.to_be_bytes());
    encoded.extend_from_slice(&hello.offset.to_be_bytes());
    encoded.extend_from_slice(hello.leader_id.as_bytes());
    stream.write_all(&encoded).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_hello(stream: &mut TcpStream) -> Result<Hello, ReplicationError> {
    let mut fixed = [0u8; HELLO_FIXED_SIZE];
    stream.read_exact(&mut fixed).await?;
    validate_prefix(&fixed, HELLO_KIND)?;
    let id_length = read_u16(&fixed, 6)? as usize;
    if id_length > MAX_NODE_ID_SIZE {
        return Err(ReplicationError::InvalidProtocol(
            "node identity is too long",
        ));
    }
    let mut id = vec![0u8; id_length];
    stream.read_exact(&mut id).await?;
    let leader_id = String::from_utf8(id)
        .map_err(|_| ReplicationError::InvalidProtocol("node identity is not UTF-8"))?;
    validate_node_id(&leader_id, true)?;
    Ok(Hello {
        leader_id,
        generation: read_u64(&fixed, 8)?,
        offset: read_u64(&fixed, 16)?,
    })
}

async fn write_response(
    stream: &mut TcpStream,
    response: &SyncResponse,
) -> Result<(), ReplicationError> {
    validate_node_id(&response.leader_id, false)?;
    let id_length = u16::try_from(response.leader_id.len())
        .map_err(|_| ReplicationError::InvalidProtocol("node identity is too long"))?;
    let payload_length = u64::try_from(response.payload.len())
        .map_err(|_| ReplicationError::InvalidProtocol("payload length overflow"))?;
    let mut fixed = Vec::with_capacity(RESPONSE_FIXED_SIZE + response.leader_id.len());
    fixed.extend_from_slice(&PROTOCOL_MAGIC);
    fixed.push(REPLICATION_VERSION);
    fixed.push(match response.kind {
        ResponseKind::Batch => BATCH_KIND,
        ResponseKind::Snapshot => SNAPSHOT_KIND,
    });
    fixed.extend_from_slice(&id_length.to_be_bytes());
    fixed.extend_from_slice(&response.generation.to_be_bytes());
    fixed.extend_from_slice(&response.start_offset.to_be_bytes());
    fixed.extend_from_slice(&response.end_offset.to_be_bytes());
    fixed.extend_from_slice(&response.leader_end.to_be_bytes());
    fixed.extend_from_slice(&payload_length.to_be_bytes());
    fixed.extend_from_slice(response.leader_id.as_bytes());
    stream.write_all(&fixed).await?;
    stream.write_all(&response.payload).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_response(
    stream: &mut TcpStream,
    config: &Config,
) -> Result<SyncResponse, ReplicationError> {
    let mut fixed = [0u8; RESPONSE_FIXED_SIZE];
    stream.read_exact(&mut fixed).await?;
    if fixed[..4] != PROTOCOL_MAGIC || fixed[4] != REPLICATION_VERSION {
        return Err(ReplicationError::InvalidProtocol(
            "invalid replication response prefix",
        ));
    }
    let kind = match fixed[5] {
        BATCH_KIND => ResponseKind::Batch,
        SNAPSHOT_KIND => ResponseKind::Snapshot,
        _ => return Err(ReplicationError::InvalidProtocol("invalid response kind")),
    };
    let id_length = read_u16(&fixed, 6)? as usize;
    if id_length == 0 || id_length > MAX_NODE_ID_SIZE {
        return Err(ReplicationError::InvalidProtocol(
            "invalid leader identity length",
        ));
    }
    let payload_length = read_u64(&fixed, 40)?;
    let maximum = match kind {
        ResponseKind::Batch => config.replication_max_batch_size,
        ResponseKind::Snapshot => config.replication_max_snapshot_size,
    };
    if payload_length > maximum as u64 {
        return Err(ReplicationError::PayloadTooLarge {
            actual: payload_length,
            maximum,
        });
    }
    let mut id = vec![0u8; id_length];
    stream.read_exact(&mut id).await?;
    let leader_id = String::from_utf8(id)
        .map_err(|_| ReplicationError::InvalidProtocol("leader identity is not UTF-8"))?;
    validate_node_id(&leader_id, false)?;
    let payload_size = usize::try_from(payload_length)
        .map_err(|_| ReplicationError::InvalidProtocol("payload length overflow"))?;
    let mut payload = vec![0u8; payload_size];
    stream.read_exact(&mut payload).await?;
    Ok(SyncResponse {
        kind,
        leader_id,
        generation: read_u64(&fixed, 8)?,
        start_offset: read_u64(&fixed, 16)?,
        end_offset: read_u64(&fixed, 24)?,
        leader_end: read_u64(&fixed, 32)?,
        payload,
    })
}

fn load_state(path: &Path) -> Result<ReplicaState, ReplicationError> {
    restore_state_backup_if_needed(path)?;
    if !path.exists() {
        return Ok(ReplicaState::default());
    }
    let encoded = fs::read(path)?;
    if encoded.len() < STATE_FIXED_SIZE + 4
        || encoded[..4] != STATE_MAGIC
        || encoded[4] != REPLICATION_VERSION
        || encoded[5..8] != [0, 0, 0]
    {
        return Err(ReplicationError::InvalidProtocol(
            "invalid replica state header",
        ));
    }
    let id_length = read_u16(&encoded, 24)? as usize;
    let checksum_offset =
        STATE_FIXED_SIZE
            .checked_add(id_length)
            .ok_or(ReplicationError::InvalidProtocol(
                "replica state length overflow",
            ))?;
    let expected_length =
        checksum_offset
            .checked_add(4)
            .ok_or(ReplicationError::InvalidProtocol(
                "replica state length overflow",
            ))?;
    if id_length > MAX_NODE_ID_SIZE || encoded.len() != expected_length {
        return Err(ReplicationError::InvalidProtocol(
            "invalid replica state length",
        ));
    }
    let expected_checksum = read_u32(&encoded, checksum_offset)?;
    if checksum(&encoded[..checksum_offset]) != expected_checksum {
        return Err(ReplicationError::InvalidProtocol(
            "replica state checksum mismatch",
        ));
    }
    let leader_id = String::from_utf8(encoded[STATE_FIXED_SIZE..checksum_offset].to_vec())
        .map_err(|_| ReplicationError::InvalidProtocol("replica state identity is not UTF-8"))?;
    validate_node_id(&leader_id, false)?;
    Ok(ReplicaState {
        leader_id,
        generation: read_u64(&encoded, 8)?,
        offset: read_u64(&encoded, 16)?,
    })
}

fn save_state(path: &Path, state: &ReplicaState) -> Result<(), ReplicationError> {
    validate_node_id(&state.leader_id, false)?;
    let id_length = u16::try_from(state.leader_id.len())
        .map_err(|_| ReplicationError::InvalidProtocol("leader identity is too long"))?;
    let mut encoded = Vec::with_capacity(STATE_FIXED_SIZE + state.leader_id.len() + 4);
    encoded.extend_from_slice(&STATE_MAGIC);
    encoded.push(REPLICATION_VERSION);
    encoded.extend_from_slice(&[0, 0, 0]);
    encoded.extend_from_slice(&state.generation.to_be_bytes());
    encoded.extend_from_slice(&state.offset.to_be_bytes());
    encoded.extend_from_slice(&id_length.to_be_bytes());
    encoded.extend_from_slice(state.leader_id.as_bytes());
    let state_checksum = checksum(&encoded);
    encoded.extend_from_slice(&state_checksum.to_be_bytes());
    write_state_atomic(path, &encoded)
}

fn write_state_atomic(path: &Path, encoded: &[u8]) -> Result<(), ReplicationError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("replica.tmp");
    let backup = path.with_extension("replica.bak");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(encoded)?;
    file.flush()?;
    file.sync_data()?;
    drop(file);
    if backup.exists() {
        fs::remove_file(&backup)?;
    }
    if path.exists() {
        fs::rename(path, &backup)?;
    }
    if let Err(error) = fs::rename(&temporary, path) {
        if backup.exists() && !path.exists() {
            let _ = fs::rename(&backup, path);
        }
        return Err(error.into());
    }
    if backup.exists() {
        fs::remove_file(backup)?;
    }
    Ok(())
}

fn restore_state_backup_if_needed(path: &Path) -> Result<(), ReplicationError> {
    let backup = path.with_extension("replica.bak");
    if !path.exists() && backup.exists() {
        fs::rename(backup, path)?;
    }
    Ok(())
}

fn validate_prefix(encoded: &[u8], expected_kind: u8) -> Result<(), ReplicationError> {
    if encoded.len() < 6
        || encoded[..4] != PROTOCOL_MAGIC
        || encoded[4] != REPLICATION_VERSION
        || encoded[5] != expected_kind
    {
        return Err(ReplicationError::InvalidProtocol(
            "invalid replication message prefix",
        ));
    }
    Ok(())
}

fn validate_node_id(node_id: &str, allow_empty: bool) -> Result<(), ReplicationError> {
    if allow_empty && node_id.is_empty() {
        return Ok(());
    }
    if (16..=MAX_NODE_ID_SIZE).contains(&node_id.len())
        && node_id.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        Ok(())
    } else {
        Err(ReplicationError::InvalidProtocol(
            "invalid hexadecimal node identity",
        ))
    }
}

fn read_u16(encoded: &[u8], start: usize) -> Result<u16, ReplicationError> {
    let end = start
        .checked_add(2)
        .ok_or(ReplicationError::InvalidProtocol("integer overflow"))?;
    let bytes: [u8; 2] = encoded
        .get(start..end)
        .ok_or(ReplicationError::InvalidProtocol("truncated integer"))?
        .try_into()
        .map_err(|_| ReplicationError::InvalidProtocol("truncated integer"))?;
    Ok(u16::from_be_bytes(bytes))
}

fn read_u32(encoded: &[u8], start: usize) -> Result<u32, ReplicationError> {
    let end = start
        .checked_add(4)
        .ok_or(ReplicationError::InvalidProtocol("integer overflow"))?;
    let bytes: [u8; 4] = encoded
        .get(start..end)
        .ok_or(ReplicationError::InvalidProtocol("truncated integer"))?
        .try_into()
        .map_err(|_| ReplicationError::InvalidProtocol("truncated integer"))?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_u64(encoded: &[u8], start: usize) -> Result<u64, ReplicationError> {
    let end = start
        .checked_add(8)
        .ok_or(ReplicationError::InvalidProtocol("integer overflow"))?;
    let bytes: [u8; 8] = encoded
        .get(start..end)
        .ok_or(ReplicationError::InvalidProtocol("truncated integer"))?
        .try_into()
        .map_err(|_| ReplicationError::InvalidProtocol("truncated integer"))?;
    Ok(u64::from_be_bytes(bytes))
}

fn checksum(encoded: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(encoded);
    hasher.finalize()
}

fn timed_out(message: &'static str) -> ReplicationError {
    ReplicationError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, message))
}

#[cfg(test)]
mod tests {
    use super::{load_state, save_state, ReplicaState};
    use tempfile::tempdir;

    #[test]
    fn replica_state_round_trip_and_checksum() {
        let directory = tempdir().expect("temporary directory should exist");
        let path = directory.path().join("forgekv.replica");
        let state = ReplicaState {
            leader_id: "0123456789abcdef0123456789abcdef".to_owned(),
            generation: 7,
            offset: 128,
        };
        save_state(&path, &state).expect("state should persist");
        assert_eq!(load_state(&path).expect("state should load"), state);
        let mut encoded = std::fs::read(&path).expect("state should read");
        let last = encoded.last_mut().expect("state should contain checksum");
        *last ^= 1;
        std::fs::write(&path, encoded).expect("corruption should write");
        assert!(load_state(&path).is_err());
    }
}

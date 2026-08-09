use std::{net::SocketAddr, sync::Arc, time::Instant};

use tokio::{net::TcpStream, sync::watch};
use tracing::{debug, error, warn};

use crate::{
    cluster::ClusterTopology,
    command::{parse_command, Command},
    config::{FsyncMode, ReplicationRole},
    error::{ForgeError, ProtocolError},
    metrics::Metrics,
    persistence::Database,
    protocol::{read_frame, write_frame, ProtocolLimits, Response},
    store::TtlState,
    VERSION,
};

pub(super) struct ConnectionContext {
    pub(super) database: Arc<Database>,
    pub(super) metrics: Arc<Metrics>,
    pub(super) limits: ProtocolLimits,
    pub(super) started_at: Instant,
    pub(super) listening_address: String,
    pub(super) fsync: FsyncMode,
    pub(super) replication_role: ReplicationRole,
    pub(super) cluster: Option<Arc<ClusterTopology>>,
}

pub(super) async fn handle_connection(
    mut stream: TcpStream,
    peer: SocketAddr,
    context: Arc<ConnectionContext>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ForgeError> {
    let _active = ActiveConnection::new(Arc::clone(&context.metrics));
    debug!(%peer, "connection opened");
    loop {
        if *shutdown.borrow() {
            break;
        }
        let frame = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
            result = read_frame(&mut stream, context.limits) => {
                match result {
                    Ok(Some(frame)) => frame,
                    Ok(None) => break,
                    Err(error) => {
                        context.metrics.protocol_error();
                        warn!(%peer, %error, "protocol error");
                        send_error(&mut stream, context.limits, &error).await?;
                        break;
                    }
                }
            }
        };

        let command = match parse_command(&frame, context.limits) {
            Ok(command) => command,
            Err(error) => {
                context.metrics.protocol_error();
                warn!(%peer, %error, "invalid command");
                send_error(&mut stream, context.limits, &error).await?;
                continue;
            }
        };
        context.metrics.command();
        if let (Some(cluster), Some(key)) = (&context.cluster, command.key()) {
            if !cluster.is_local(key) {
                let owner = cluster.owner(key);
                context.metrics.cluster_redirect();
                let response = Response::Redirect(owner.address().to_owned()).into_frame()?;
                write_frame(&mut stream, &response, context.limits).await?;
                continue;
            }
            context.metrics.cluster_local_command();
        }
        let response = match execute(
            command,
            &context.database,
            &context.metrics,
            context.started_at,
            &context.listening_address,
            context.fsync,
            context.replication_role,
            context.cluster.as_deref(),
        )
        .await
        {
            Ok(response) => response,
            Err(error) => {
                error!(%peer, %error, "command failed");
                Response::ServerError("internal server error".to_owned())
            }
        };
        let response_frame = response.into_frame()?;
        write_frame(&mut stream, &response_frame, context.limits).await?;
    }
    debug!(%peer, "connection closed");
    Ok(())
}

async fn execute(
    command: Command,
    database: &Database,
    metrics: &Metrics,
    started_at: Instant,
    listening_address: &str,
    fsync: FsyncMode,
    replication_role: ReplicationRole,
    cluster: Option<&ClusterTopology>,
) -> Result<Response, ForgeError> {
    if replication_role == ReplicationRole::Follower
        && matches!(
            &command,
            Command::Set { .. }
                | Command::Del { .. }
                | Command::SetEx { .. }
                | Command::Persist { .. }
        )
    {
        return Ok(Response::ServerError(
            "follower is read-only; send mutations to the leader".to_owned(),
        ));
    }
    match command {
        Command::Ping => Ok(Response::Pong),
        Command::Set { key, value } => {
            database.set(key, value).await?;
            Ok(Response::Ok)
        }
        Command::Get { key } => Ok(match database.store().get(&key)? {
            Some(value) => Response::Value(value),
            None => Response::NotFound,
        }),
        Command::Del { key } => Ok(Response::Integer(bool_to_integer(
            database.delete(key).await?,
        ))),
        Command::Exists { key } => Ok(Response::Integer(bool_to_integer(
            database.store().exists(&key)?,
        ))),
        Command::SetEx { key, ttl, value } => {
            database.set_ex(key, value, ttl).await?;
            Ok(Response::Ok)
        }
        Command::Ttl { key } => {
            let value = match database.store().ttl(&key)? {
                TtlState::Missing => -2,
                TtlState::Persistent => -1,
                TtlState::ExpiresIn(duration) => {
                    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
                }
            };
            Ok(Response::Integer(value))
        }
        Command::Persist { key } => Ok(Response::Integer(bool_to_integer(
            database.persist(key).await?,
        ))),
        Command::Info => {
            let mut fields = vec![
                ("version".to_owned(), VERSION.to_owned()),
            (
                "uptime_seconds".to_owned(),
                started_at.elapsed().as_secs().to_string(),
            ),
            ("keys".to_owned(), database.store().len()?.to_string()),
            (
                "shards".to_owned(),
                database.store().shard_count().to_string(),
            ),
            ("listening_address".to_owned(), listening_address.to_owned()),
            ("fsync".to_owned(), fsync.as_str().to_owned()),
            (
                "replication_role".to_owned(),
                replication_role.as_str().to_owned(),
            ),
                ("cluster_enabled".to_owned(), cluster.is_some().to_string()),
            ];
            if let Some(cluster) = cluster {
                fields.extend([
                    (
                        "cluster_node_id".to_owned(),
                        cluster.local_node().id().to_owned(),
                    ),
                    ("cluster_nodes".to_owned(), cluster.node_count().to_string()),
                    (
                        "cluster_virtual_nodes".to_owned(),
                        cluster.virtual_nodes().to_string(),
                    ),
                ]);
            }
            Ok(Response::Info(fields))
        }
        Command::Stats => {
            let snapshot = metrics.snapshot();
            Ok(Response::Stats(vec![
                ("connections_total".to_owned(), snapshot.connections_total),
                ("connections_active".to_owned(), snapshot.connections_active),
                ("commands_total".to_owned(), snapshot.commands_total),
                ("gets_total".to_owned(), snapshot.gets_total),
                ("sets_total".to_owned(), snapshot.sets_total),
                ("deletes_total".to_owned(), snapshot.deletes_total),
                ("hits_total".to_owned(), snapshot.hits_total),
                ("misses_total".to_owned(), snapshot.misses_total),
                ("expired_keys_total".to_owned(), snapshot.expired_keys_total),
                (
                    "protocol_errors_total".to_owned(),
                    snapshot.protocol_errors_total,
                ),
                (
                    "wal_records_written".to_owned(),
                    snapshot.wal_records_written,
                ),
                ("wal_bytes_written".to_owned(), snapshot.wal_bytes_written),
                (
                    "connections_rejected_total".to_owned(),
                    snapshot.connections_rejected_total,
                ),
                (
                    "snapshots_created_total".to_owned(),
                    snapshot.snapshots_created_total,
                ),
                (
                    "wal_compactions_total".to_owned(),
                    snapshot.wal_compactions_total,
                ),
                (
                    "snapshot_entries_written".to_owned(),
                    snapshot.snapshot_entries_written,
                ),
                (
                    "replication_connections_total".to_owned(),
                    snapshot.replication_connections_total,
                ),
                (
                    "replication_syncs_total".to_owned(),
                    snapshot.replication_syncs_total,
                ),
                (
                    "replication_full_syncs_total".to_owned(),
                    snapshot.replication_full_syncs_total,
                ),
                (
                    "replication_bytes_sent_total".to_owned(),
                    snapshot.replication_bytes_sent_total,
                ),
                (
                    "replication_bytes_received_total".to_owned(),
                    snapshot.replication_bytes_received_total,
                ),
                (
                    "replication_errors_total".to_owned(),
                    snapshot.replication_errors_total,
                ),
                (
                    "replication_lag_bytes".to_owned(),
                    snapshot.replication_lag_bytes,
                ),
                (
                    "cluster_redirects_total".to_owned(),
                    snapshot.cluster_redirects_total,
                ),
                (
                    "cluster_local_commands_total".to_owned(),
                    snapshot.cluster_local_commands_total,
                ),
            ]))
        }
    }
}

fn bool_to_integer(value: bool) -> i64 {
    i64::from(u8::from(value))
}

async fn send_error(
    stream: &mut TcpStream,
    limits: ProtocolLimits,
    error: &ProtocolError,
) -> Result<(), ProtocolError> {
    let response = Response::InvalidRequest(error.to_string()).into_frame()?;
    write_frame(stream, &response, limits).await
}

struct ActiveConnection {
    metrics: Arc<Metrics>,
}

impl ActiveConnection {
    fn new(metrics: Arc<Metrics>) -> Self {
        metrics.connection_opened();
        Self { metrics }
    }
}

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.metrics.connection_closed();
    }
}

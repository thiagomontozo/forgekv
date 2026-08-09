use std::sync::Arc;

use forgekv::{
    config::{Config, ReplicationRole},
    error::ForgeError,
    metrics::Metrics,
    persistence::Database,
    replication::initial_sync,
    server::Server,
    store::ShardedStore,
};
use tokio::{net::TcpListener, sync::watch};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), ForgeError> {
    initialize_logging();
    let config = Config::from_env()?;
    let metrics = Arc::new(Metrics::default());
    let store = Arc::new(ShardedStore::new(config.shards, Arc::clone(&metrics))?);

    info!(path = %config.wal_path().display(), "initializing WAL");
    let (database, recovery) =
        Database::open(&config, Arc::clone(&store), Arc::clone(&metrics)).await?;
    info!(
        records = recovery.records_replayed,
        keys = recovery.recovered_keys,
        truncated_tail = recovery.truncated_tail_removed,
        snapshot_entries = recovery.snapshot_entries_loaded,
        "WAL replay completed"
    );
    let database = Arc::new(database);

    if config.replication_role == ReplicationRole::Follower {
        info!(leader = %config.leader_address, "performing initial follower synchronization");
        initial_sync(&config, Arc::clone(&database), Arc::clone(&metrics)).await?;
    }

    let listener = TcpListener::bind(config.listen_address()).await?;
    let listening_address = listener.local_addr()?.to_string();
    let server = Arc::new(Server::new(
        config,
        Arc::clone(&database),
        metrics,
        listening_address,
    ));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let signal_task = tokio::spawn(async move {
        if let Err(error) = tokio::signal::ctrl_c().await {
            error!(%error, "failed to listen for Ctrl+C");
        }
        let _ = shutdown_tx.send(true);
    });

    let run_result = server.run(listener, shutdown_rx).await;
    if !signal_task.is_finished() {
        signal_task.abort();
    }
    info!("flushing WAL");
    if let Err(error) = database.flush().await {
        error!(%error, "failed to flush WAL during shutdown");
        return Err(error.into());
    }
    if let Err(error) = run_result {
        warn!(%error, "server exited with an error");
        return Err(error);
    }
    info!("graceful shutdown complete");
    Ok(())
}

fn initialize_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

use std::{error::Error, sync::Arc};

use bytes::Bytes;
use forgekv::{
    config::{Config, FsyncMode, ReplicationRole},
    metrics::Metrics,
    persistence::Database,
    replication::{initial_sync, run_leader},
    store::ShardedStore,
};
use tempfile::TempDir;
use tokio::{net::TcpListener, sync::watch, time::timeout};

async fn database(
    data: &TempDir,
    role: ReplicationRole,
) -> Result<(Config, Arc<Database>, Arc<Metrics>), Box<dyn Error>> {
    let config = Config {
        data_dir: data.path().to_path_buf(),
        shards: 8,
        fsync: FsyncMode::None,
        metrics_enabled: false,
        replication_role: role,
        wal_compaction_threshold_bytes: 0,
        ..Config::default()
    };
    let metrics = Arc::new(Metrics::default());
    let store = Arc::new(ShardedStore::new(config.shards, Arc::clone(&metrics))?);
    let (database, _) = Database::open(&config, store, Arc::clone(&metrics)).await?;
    Ok((config, Arc::new(database), metrics))
}

#[tokio::test]
async fn follower_receives_snapshot_then_incremental_wal() -> Result<(), Box<dyn Error>> {
    let leader_data = tempfile::tempdir()?;
    let follower_data = tempfile::tempdir()?;
    let (leader_config, leader, leader_metrics) =
        database(&leader_data, ReplicationRole::Leader).await?;
    leader
        .set(
            Bytes::from_static(b"snapshot:key"),
            Bytes::from_static(b"snapshot-value"),
        )
        .await?;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let leader_address = listener.local_addr()?.to_string();
    let (shutdown, receiver) = watch::channel(false);
    let leader_task = tokio::spawn(run_leader(
        listener,
        leader_config,
        Arc::clone(&leader),
        leader_metrics,
        receiver,
    ));

    let (mut follower_config, follower, follower_metrics) =
        database(&follower_data, ReplicationRole::Follower).await?;
    follower_config.leader_address = leader_address;
    initial_sync(
        &follower_config,
        Arc::clone(&follower),
        Arc::clone(&follower_metrics),
    )
    .await?;
    assert_eq!(
        follower.store().get(b"snapshot:key")?,
        Some(Bytes::from_static(b"snapshot-value"))
    );

    leader
        .set(
            Bytes::from_static(b"incremental:key"),
            Bytes::from_static(b"incremental-value"),
        )
        .await?;
    initial_sync(
        &follower_config,
        Arc::clone(&follower),
        follower_metrics,
    )
    .await?;
    assert_eq!(
        follower.store().get(b"incremental:key")?,
        Some(Bytes::from_static(b"incremental-value"))
    );

    shutdown.send(true)?;
    timeout(std::time::Duration::from_secs(3), leader_task).await???;

    drop(follower);
    let restart_metrics = Arc::new(Metrics::default());
    let restart_store = Arc::new(ShardedStore::new(
        follower_config.shards,
        Arc::clone(&restart_metrics),
    )?);
    let (_restart, _) = Database::open(&follower_config, Arc::clone(&restart_store), restart_metrics)
        .await?;
    assert_eq!(
        restart_store.get(b"snapshot:key")?,
        Some(Bytes::from_static(b"snapshot-value"))
    );
    assert_eq!(
        restart_store.get(b"incremental:key")?,
        Some(Bytes::from_static(b"incremental-value"))
    );
    Ok(())
}

#[tokio::test]
async fn compaction_invalidates_old_replication_generation() -> Result<(), Box<dyn Error>> {
    let data = tempfile::tempdir()?;
    let (_config, database, _metrics) = database(&data, ReplicationRole::Leader).await?;
    let generation = database.wal_generation();
    database
        .set(Bytes::from_static(b"key"), Bytes::from_static(b"value"))
        .await?;
    database.compact().await?;
    assert!(database.wal_generation() > generation);
    assert!(database
        .read_replication_batch(generation, 8, 4 * 1024 * 1024)
        .await?
        .is_none());
    Ok(())
}

mod connection;
mod metrics_export;

use std::{sync::Arc, time::Instant};

use tokio::{
    net::TcpListener,
    sync::{watch, Semaphore},
    task::JoinSet,
    time::{self, MissedTickBehavior},
};
use tracing::{error, info, warn};

use crate::{
    config::Config, error::ForgeError, metrics::Metrics, persistence::Database,
    protocol::ProtocolLimits,
};

use connection::{handle_connection, ConnectionContext};
use metrics_export::run_metrics_export;

#[derive(Debug)]
pub struct Server {
    config: Config,
    database: Arc<Database>,
    metrics: Arc<Metrics>,
    started_at: Instant,
    listening_address: String,
}

impl Server {
    pub fn new(
        config: Config,
        database: Arc<Database>,
        metrics: Arc<Metrics>,
        listening_address: String,
    ) -> Self {
        Self {
            config,
            database,
            metrics,
            started_at: Instant::now(),
            listening_address,
        }
    }

    pub async fn run(
        self: Arc<Self>,
        listener: TcpListener,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), ForgeError> {
        let mut connections = JoinSet::new();
        let connection_limit = Arc::new(Semaphore::new(self.config.max_connections));
        let metrics_listener = if self.config.metrics_enabled {
            Some(TcpListener::bind(self.config.metrics_address()).await?)
        } else {
            None
        };
        let metrics_task = metrics_listener.map(|listener| {
            let metrics = Arc::clone(&self.metrics);
            let metrics_shutdown = shutdown.clone();
            tokio::spawn(run_metrics_export(listener, metrics, metrics_shutdown))
        });
        let expiration_store = Arc::clone(self.database.store());
        let maintenance_database = Arc::clone(&self.database);
        let expiration_interval = self.config.expiration_interval;
        let mut expiration_shutdown = shutdown.clone();
        let expiration_task = tokio::spawn(async move {
            let mut interval = time::interval(expiration_interval);
            let mut durability_interval = time::interval(std::time::Duration::from_secs(1));
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            durability_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            interval.tick().await;
            durability_interval.tick().await;
            loop {
                tokio::select! {
                    changed = expiration_shutdown.changed() => {
                        if changed.is_err() || *expiration_shutdown.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        match expiration_store.purge_expired() {
                            Ok(removed) if removed > 0 => {
                                tracing::debug!(removed, "expired keys removed");
                            }
                            Ok(_) => {}
                            Err(error) => error!(%error, "background expiration failed"),
                        }
                    }
                    _ = durability_interval.tick() => {
                        if let Err(error) = maintenance_database.sync_if_needed().await {
                            error!(%error, "periodic WAL synchronization failed");
                        }
                        match maintenance_database.compact_if_needed().await {
                            Ok(true) => info!("WAL compaction completed"),
                            Ok(false) => {}
                            Err(error) => error!(%error, "automatic WAL compaction failed"),
                        }
                    }
                }
            }
        });

        info!(address = %self.listening_address, "ForgeKV server started");
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
                        Ok(connection) => connection,
                        Err(error) => {
                            warn!(%error, "TCP accept failed");
                            time::sleep(std::time::Duration::from_millis(100)).await;
                            continue;
                        }
                    };
                    let permit = match Arc::clone(&connection_limit).try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            self.metrics.connection_rejected();
                            warn!(%peer, "connection rejected: configured limit reached");
                            drop(stream);
                            continue;
                        }
                    };
                    let context = Arc::new(ConnectionContext {
                        database: Arc::clone(&self.database),
                        metrics: Arc::clone(&self.metrics),
                        limits: ProtocolLimits::from(&self.config),
                        started_at: self.started_at,
                        listening_address: self.listening_address.clone(),
                        fsync: self.config.fsync,
                    });
                    let connection_shutdown = shutdown.clone();
                    connections.spawn(async move {
                        let _permit = permit;
                        if let Err(error) = handle_connection(
                            stream,
                            peer,
                            context,
                            connection_shutdown,
                        ).await {
                            warn!(%peer, %error, "connection terminated with an error");
                        }
                    });
                }
            }
        }

        info!("shutdown requested; waiting for connections");
        while let Some(result) = connections.join_next().await {
            if let Err(error) = result {
                warn!(%error, "connection task did not exit cleanly");
            }
        }
        if let Err(error) = expiration_task.await {
            warn!(%error, "expiration task did not exit cleanly");
        }
        if let Some(task) = metrics_task {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => warn!(%error, "metrics exporter stopped with an error"),
                Err(error) => warn!(%error, "metrics exporter task did not exit cleanly"),
            }
        }
        info!("ForgeKV server stopped accepting connections");
        Ok(())
    }
}

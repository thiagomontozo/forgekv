mod connection;

use std::{sync::Arc, time::Instant};

use tokio::{
    net::TcpListener,
    sync::watch,
    task::JoinSet,
    time::{self, MissedTickBehavior},
};
use tracing::{error, info, warn};

use crate::{
    config::Config, error::ForgeError, metrics::Metrics, persistence::Database,
    protocol::ProtocolLimits,
};

use connection::{handle_connection, ConnectionContext};

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
        let expiration_store = Arc::clone(self.database.store());
        let expiration_interval = self.config.expiration_interval;
        let mut expiration_shutdown = shutdown.clone();
        let expiration_task = tokio::spawn(async move {
            let mut interval = time::interval(expiration_interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            interval.tick().await;
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
        info!("ForgeKV server stopped accepting connections");
        Ok(())
    }
}

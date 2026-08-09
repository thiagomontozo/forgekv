use std::sync::Arc;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{watch, Semaphore},
    task::JoinSet,
    time::{timeout, Duration},
};
use tracing::{debug, warn};

use crate::{error::ForgeError, metrics::Metrics};

const MAX_HTTP_HEADER_SIZE: usize = 8 * 1024;
const MAX_METRICS_CONNECTIONS: usize = 64;

pub(super) async fn run_metrics_export(
    listener: TcpListener,
    metrics: Arc<Metrics>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ForgeError> {
    debug!(address = %listener.local_addr()?, "metrics exporter started");
    let mut connections = JoinSet::new();
    let connection_limit = Arc::new(Semaphore::new(MAX_METRICS_CONNECTIONS));
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
                let (stream, peer) = accepted?;
                let permit = match Arc::clone(&connection_limit).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        warn!(%peer, "metrics connection rejected: limit reached");
                        drop(stream);
                        continue;
                    }
                };
                let metrics = Arc::clone(&metrics);
                connections.spawn(async move {
                    let _permit = permit;
                    if let Err(error) = serve_metrics(stream, metrics).await {
                        warn!(%peer, %error, "metrics request failed");
                    }
                });
            }
        }
    }
    while let Some(result) = connections.join_next().await {
        if let Err(error) = result {
            warn!(%error, "metrics connection task did not exit cleanly");
        }
    }
    Ok(())
}

async fn serve_metrics(mut stream: TcpStream, metrics: Arc<Metrics>) -> Result<(), ForgeError> {
    let mut request = vec![0u8; MAX_HTTP_HEADER_SIZE];
    let bytes_read = timeout(Duration::from_secs(2), stream.read(&mut request))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "metrics read timed out"))??;
    let is_metrics = request[..bytes_read].starts_with(b"GET /metrics HTTP/1.");
    let (status, body) = if is_metrics {
        ("200 OK", render_prometheus(&metrics))
    } else {
        ("404 Not Found", "not found\n".to_owned())
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    timeout(Duration::from_secs(2), stream.write_all(response.as_bytes()))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "metrics write timed out"))??;
    Ok(())
}

pub(crate) fn render_prometheus(metrics: &Metrics) -> String {
    let snapshot = metrics.snapshot();
    let values = [
        ("forgekv_connections_total", "counter", snapshot.connections_total),
        ("forgekv_connections_active", "gauge", snapshot.connections_active),
        ("forgekv_connections_rejected_total", "counter", snapshot.connections_rejected_total),
        ("forgekv_commands_total", "counter", snapshot.commands_total),
        ("forgekv_gets_total", "counter", snapshot.gets_total),
        ("forgekv_sets_total", "counter", snapshot.sets_total),
        ("forgekv_deletes_total", "counter", snapshot.deletes_total),
        ("forgekv_hits_total", "counter", snapshot.hits_total),
        ("forgekv_misses_total", "counter", snapshot.misses_total),
        ("forgekv_expired_keys_total", "counter", snapshot.expired_keys_total),
        ("forgekv_protocol_errors_total", "counter", snapshot.protocol_errors_total),
        ("forgekv_wal_records_written", "counter", snapshot.wal_records_written),
        ("forgekv_wal_bytes_written", "counter", snapshot.wal_bytes_written),
        ("forgekv_snapshots_created_total", "counter", snapshot.snapshots_created_total),
        ("forgekv_wal_compactions_total", "counter", snapshot.wal_compactions_total),
        ("forgekv_snapshot_entries_written", "counter", snapshot.snapshot_entries_written),
    ];
    let mut output = String::new();
    for (name, metric_type, value) in values {
        output.push_str("# TYPE ");
        output.push_str(name);
        output.push(' ');
        output.push_str(metric_type);
        output.push('\n');
        output.push_str(name);
        output.push(' ');
        output.push_str(&value.to_string());
        output.push('\n');
    }
    output
}

#[cfg(test)]
mod tests {
    use super::render_prometheus;
    use crate::metrics::Metrics;

    #[test]
    fn renders_prometheus_text() {
        let metrics = Metrics::default();
        metrics.command();
        let output = render_prometheus(&metrics);
        assert!(output.contains("forgekv_commands_total 1\n"));
        assert!(!output.contains('{'));
    }
}

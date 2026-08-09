use std::{error::Error, io, sync::Arc, time::Duration};

use bytes::Bytes;
use forgekv::{
    client::Client,
    config::{Config, FsyncMode, ReplicationRole},
    metrics::Metrics,
    persistence::Database,
    protocol::{read_frame, ProtocolLimits, Response},
    server::Server,
    store::ShardedStore,
};
use tempfile::TempDir;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinHandle,
    time::timeout,
};

struct RunningServer {
    address: String,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<(), forgekv::error::ForgeError>>,
    _data: TempDir,
}

impl RunningServer {
    async fn start() -> Result<Self, Box<dyn Error>> {
        Self::start_with_options(1_024, ReplicationRole::Standalone).await
    }

    async fn start_with_limit(max_connections: usize) -> Result<Self, Box<dyn Error>> {
        Self::start_with_options(max_connections, ReplicationRole::Standalone).await
    }

    async fn start_with_options(
        max_connections: usize,
        replication_role: ReplicationRole,
    ) -> Result<Self, Box<dyn Error>> {
        let data = tempfile::tempdir()?;
        let config = Config {
            host: "127.0.0.1".to_owned(),
            port: 6380,
            data_dir: data.path().to_path_buf(),
            shards: 8,
            max_frame_size: 64 * 1024,
            max_key_size: 1024,
            max_value_size: 32 * 1024,
            expiration_interval: Duration::from_millis(25),
            fsync: FsyncMode::None,
            max_connections,
            metrics_enabled: false,
            replication_role,
            ..Config::default()
        };
        let metrics = Arc::new(Metrics::default());
        let store = Arc::new(ShardedStore::new(config.shards, Arc::clone(&metrics))?);
        let (database, _) = Database::open(&config, store, Arc::clone(&metrics)).await?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?.to_string();
        let server = Arc::new(Server::new(
            config,
            Arc::new(database),
            metrics,
            address.clone(),
        ));
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(server.run(listener, receiver));
        Ok(Self {
            address,
            shutdown,
            task,
            _data: data,
        })
    }

    fn limits(&self) -> ProtocolLimits {
        ProtocolLimits {
            max_frame_size: 64 * 1024,
            max_key_size: 1024,
            max_value_size: 32 * 1024,
        }
    }

    async fn stop(self) -> Result<(), Box<dyn Error>> {
        self.shutdown.send(true)?;
        timeout(Duration::from_secs(3), self.task).await???;
        Ok(())
    }
}

#[tokio::test]
async fn follower_rejects_mutating_client_commands() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start_with_options(1_024, ReplicationRole::Follower).await?;
    let mut client = Client::connect(&server.address, server.limits()).await?;
    assert_eq!(
        client
            .set(Bytes::from_static(b"key"), Bytes::from_static(b"value"))
            .await?,
        Response::ServerError("follower is read-only; send mutations to the leader".to_owned())
    );
    server.stop().await
}

#[tokio::test]
async fn ordered_pipeline_uses_one_tcp_connection() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start().await?;
    let mut client = Client::connect(&server.address, server.limits()).await?;
    let responses = client
        .execute_pipeline(vec![
            forgekv::command::Command::Set {
                key: Bytes::from_static(b"pipeline:key"),
                value: Bytes::from_static(b"value"),
            },
            forgekv::command::Command::Get {
                key: Bytes::from_static(b"pipeline:key"),
            },
            forgekv::command::Command::Del {
                key: Bytes::from_static(b"pipeline:key"),
            },
        ])
        .await?;
    assert_eq!(
        responses,
        vec![
            Response::Ok,
            Response::Value(Bytes::from_static(b"value")),
            Response::Integer(1)
        ]
    );
    server.stop().await
}

#[tokio::test]
async fn connection_limit_rejects_excess_clients() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start_with_limit(1).await?;
    let mut first = Client::connect(&server.address, server.limits()).await?;
    assert_eq!(first.ping().await?, Response::Pong);
    if let Ok(mut second) = Client::connect(&server.address, server.limits()).await {
        let rejected = timeout(Duration::from_secs(2), second.ping()).await?;
        assert!(rejected.is_err());
    }
    drop(first);
    server.stop().await
}

#[tokio::test]
async fn ping_set_get_delete_over_tcp() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start().await?;
    let mut client = Client::connect(&server.address, server.limits()).await?;
    assert_eq!(client.ping().await?, Response::Pong);
    assert_eq!(
        client
            .set(Bytes::from_static(b"key"), Bytes::from_static(b"value"))
            .await?,
        Response::Ok
    );
    assert_eq!(
        client.get(Bytes::from_static(b"key")).await?,
        Response::Value(Bytes::from_static(b"value"))
    );
    assert_eq!(
        client.delete(Bytes::from_static(b"key")).await?,
        Response::Integer(1)
    );
    server.stop().await
}

#[tokio::test]
async fn multiple_clients_are_processed_concurrently() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start().await?;
    let mut tasks = Vec::new();
    for index in 0..16 {
        let address = server.address.clone();
        let limits = server.limits();
        tasks.push(tokio::spawn(async move {
            let mut client = Client::connect(&address, limits).await?;
            let key = Bytes::from(format!("key:{index}"));
            client
                .set(key.clone(), Bytes::from_static(b"value"))
                .await?;
            client.get(key).await
        }));
    }
    for task in tasks {
        assert_eq!(task.await??, Response::Value(Bytes::from_static(b"value")));
    }
    server.stop().await
}

#[tokio::test]
async fn malformed_protocol_returns_structured_error() -> Result<(), Box<dyn Error>> {
    let server = RunningServer::start().await?;
    let mut stream = TcpStream::connect(&server.address).await?;
    stream.write_all(&[0, 0, 0, 2, 1, 0xff]).await?;
    let response = read_frame(&mut stream, server.limits())
        .await?
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "missing response"))?;
    assert!(matches!(
        Response::from_frame(response)?,
        Response::InvalidRequest(_)
    ));
    server.stop().await
}

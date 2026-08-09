use std::{error::Error, io, sync::Arc, time::Duration};

use bytes::Bytes;
use forgekv::{
    client::Client,
    config::{Config, FsyncMode},
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
        };
        let metrics = Arc::new(Metrics::default());
        let store = Arc::new(ShardedStore::new(config.shards, Arc::clone(&metrics))?);
        let (database, _) =
            Database::open(&config, store, Arc::clone(&metrics)).await?;
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
            client.set(key.clone(), Bytes::from_static(b"value")).await?;
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

use std::{error::Error, io, sync::Arc, time::Duration};

use bytes::Bytes;
use forgekv::{
    client::Client,
    cluster::ClusterTopology,
    config::{Config, FsyncMode},
    metrics::Metrics,
    persistence::Database,
    protocol::{read_frame, write_frame, ProtocolLimits, Response},
    server::Server,
    store::ShardedStore,
};
use tempfile::TempDir;
use tokio::{net::TcpListener, sync::watch, task::JoinHandle, time::timeout};

struct RunningNode {
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<(), forgekv::error::ForgeError>>,
    _data: TempDir,
}

impl RunningNode {
    async fn start(
        listener: TcpListener,
        node_id: &str,
        membership: &str,
    ) -> Result<Self, Box<dyn Error>> {
        let data = tempfile::tempdir()?;
        let address = listener.local_addr()?.to_string();
        let config = Config {
            data_dir: data.path().to_path_buf(),
            shards: 8,
            max_frame_size: 64 * 1024,
            max_key_size: 1024,
            max_value_size: 32 * 1024,
            expiration_interval: Duration::from_millis(25),
            fsync: FsyncMode::None,
            metrics_enabled: false,
            cluster_enabled: true,
            cluster_node_id: node_id.to_owned(),
            cluster_nodes: membership.to_owned(),
            cluster_virtual_nodes: 64,
            ..Config::default()
        };
        let metrics = Arc::new(Metrics::default());
        let store = Arc::new(ShardedStore::new(config.shards, Arc::clone(&metrics))?);
        let (database, _) = Database::open(&config, store, Arc::clone(&metrics)).await?;
        let server = Arc::new(Server::new(config, Arc::new(database), metrics, address));
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(server.run(listener, receiver));
        Ok(Self {
            shutdown,
            task,
            _data: data,
        })
    }

    async fn stop(self) -> Result<(), Box<dyn Error>> {
        self.shutdown.send(true)?;
        timeout(Duration::from_secs(3), self.task).await???;
        Ok(())
    }
}

fn limits() -> ProtocolLimits {
    ProtocolLimits {
        max_frame_size: 64 * 1024,
        max_key_size: 1024,
        max_value_size: 32 * 1024,
    }
}

#[tokio::test]
async fn client_follows_redirect_to_key_owner() -> Result<(), Box<dyn Error>> {
    let listener_a = TcpListener::bind("127.0.0.1:0").await?;
    let listener_b = TcpListener::bind("127.0.0.1:0").await?;
    let address_a = listener_a.local_addr()?.to_string();
    let address_b = listener_b.local_addr()?.to_string();
    let membership = format!("node-a@{address_a},node-b@{address_b}");
    let topology = ClusterTopology::new("node-a", &membership, 64)?;
    let key = (0..10_000)
        .map(|index| Bytes::from(format!("cluster-key-{index}")))
        .find(|key| topology.owner(key).id() == "node-b")
        .ok_or_else(|| io::Error::other("test topology did not assign a key to node-b"))?;

    let node_a = RunningNode::start(listener_a, "node-a", &membership).await?;
    let node_b = RunningNode::start(listener_b, "node-b", &membership).await?;
    let mut client = Client::connect(&address_a, limits()).await?;
    assert_eq!(
        client
            .set(key.clone(), Bytes::from_static(b"partitioned-value"))
            .await?,
        Response::Ok
    );

    let mut seed_client = Client::connect(&address_a, limits()).await?;
    assert_eq!(
        seed_client.get(key).await?,
        Response::Value(Bytes::from_static(b"partitioned-value"))
    );
    let responses = seed_client
        .execute_pipeline(vec![
            forgekv::command::Command::Del { key: key.clone() },
            forgekv::command::Command::Exists { key },
        ])
        .await?;
    assert_eq!(responses, vec![Response::Integer(1), Response::Integer(0)]);

    drop(client);
    drop(seed_client);
    node_a.stop().await?;
    node_b.stop().await
}

#[tokio::test]
async fn client_rejects_redirect_loop() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let redirect_address = address.clone();
    let responder = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        read_frame(&mut stream, limits()).await?.ok_or_else(|| {
            forgekv::error::ProtocolError::InvalidPayload("client closed before request")
        })?;
        let response = Response::Redirect(redirect_address).into_frame()?;
        write_frame(&mut stream, &response, limits()).await
    });

    let mut client = Client::connect(&address, limits()).await?;
    let error = client.ping().await.expect_err("redirect loop must fail");
    assert!(matches!(
        error,
        forgekv::error::ProtocolError::RedirectLoop(_)
    ));
    responder.await??;
    Ok(())
}

mod common;

use common::NatsServer;
use hostname;
use rants::{Address, Client};

#[tokio::test(flavor = "multi_thread")]
async fn ping_pong() {
    common::init();
    let _nats_server = NatsServer::new(&["--port=5678"]).await;

    let address = Address::new(&hostname::get().unwrap().to_string_lossy(), 5678, None);
    let client = Client::new(vec![address]);

    assert!(client.state().await.is_disconnected());

    client.connect().await;

    assert!(client.state().await.is_connected());

    // Send a ping and wait for a pong
    let ping_pong = client.ping_pong().await;
    assert!(ping_pong.is_ok());

    client.disconnect().await;
}

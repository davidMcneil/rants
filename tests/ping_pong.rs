mod common;

use common::NatsServer;
use rants::Client;

#[tokio::test(threaded_scheduler)]
async fn ping_pong() {
    common::init();
    let _nats_server = NatsServer::new(&[]).await;

    let address = "127.0.0.1".parse().unwrap();
    let client = Client::new(vec![address]);

    assert!(client.state().await.is_disconnected());

    client.connect().await;

    assert!(client.state().await.is_connected());

    // Send a ping and wait for a pong
    let ping_pong = client.ping_pong().await;
    assert!(ping_pong.is_ok());

    client.disconnect().await;
}

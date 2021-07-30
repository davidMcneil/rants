mod common;

use common::NatsServer;
use rants::Client;

#[tokio::test(flavor = "multi_thread")]
async fn authorization_override() {
    common::init();
    let _nats_server = NatsServer::new(&["--user=test", "--pass=not_secure="]).await;

    let address = "test:not_secure=@127.0.0.1".parse().unwrap();
    let client = Client::new(vec![address]);
    client
        .connect_mut()
        .await
        // The authorization in the Address should override this authorization
        .token(String::from("this token does not work"));
    client.connect().await;

    // Send a ping and wait for a pong
    let ping_pong = client.ping_pong().await;
    assert!(ping_pong.is_ok());

    client.disconnect().await;
}

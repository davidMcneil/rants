mod common;

use rants::Client;

async fn main() {
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

#[test]
fn ping_pong() {
    common::run_integration_test(main());
}

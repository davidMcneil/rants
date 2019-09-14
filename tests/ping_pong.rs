mod common;

use rants::Client;
use std::sync::Arc;

async fn main() {
    let address = "127.0.0.1".parse().unwrap();
    let wrapped_client = Client::new(vec![address]);

    assert!(wrapped_client.lock().await.state().is_disconnected());

    Client::connect(Arc::clone(&wrapped_client)).await;

    assert!(wrapped_client.lock().await.state().is_connected());

    // Send a ping and wait for a pong
    let ping_pong = Client::ping_pong(Arc::clone(&wrapped_client)).await;
    assert!(ping_pong.is_ok());

    Client::disconnect(wrapped_client).await;
}

#[test]
fn ping_pong() {
    common::run_integration_test(main());
}

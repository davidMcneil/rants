mod common;

use futures::stream::StreamExt;
use rants::Client;
use std::sync::Arc;

async fn main() {
    let address = "127.0.0.1".parse().unwrap();
    let wrapped_client = Client::new(vec![address]);
    wrapped_client.lock().await.connect_mut().echo(true);

    let subject = "test.subscription".parse().unwrap();

    Client::connect(Arc::clone(&wrapped_client)).await;

    // Subscribe to the subject
    let (_, mut subscription) = {
        let mut client = wrapped_client.lock().await;
        client.subscribe(&subject, 4).await.unwrap()
    };

    // Publish messages to the subject
    {
        let mut client = wrapped_client.lock().await;
        client.publish(&subject, &[1]).await.unwrap();
        client.publish(&subject, &[2]).await.unwrap();
    }

    assert_eq!(subscription.next().await.unwrap().into_payload(), &[1]);
    assert_eq!(subscription.next().await.unwrap().into_payload(), &[2]);

    // Disconnect
    Client::disconnect(Arc::clone(&wrapped_client)).await;

    // Can not publish while disconnected
    {
        let mut client = wrapped_client.lock().await;
        assert!(client.publish(&subject, &[3]).await.is_err());
    }

    // Reconnect
    Client::connect(Arc::clone(&wrapped_client)).await;

    // Publish more messages to the subject
    {
        let mut client = wrapped_client.lock().await;
        client.publish(&subject, &[4]).await.unwrap();
        client.publish(&subject, &[5]).await.unwrap();
    }

    assert_eq!(subscription.next().await.unwrap().payload(), &[4]);
    assert_eq!(subscription.next().await.unwrap().payload(), &[5]);

    Client::disconnect(wrapped_client).await;

    assert!(subscription.next().await.is_none());
}

#[test]
fn reconnect() {
    common::run_integration_test(main());
}

mod common;

use common::NatsServer;
use rants::Client;

#[tokio::test(flavor = "multi_thread")]
async fn reconnect() {
    common::init();
    let _nats_server = NatsServer::new(&[]).await;

    let address = "127.0.0.1".parse().unwrap();
    let client = Client::new(vec![address]);
    client.connect_mut().await.echo(true);

    let subject = "test.subscription".parse().unwrap();

    client.connect().await;

    // Subscribe to the subject
    let (_, mut subscription) = client.subscribe(&subject, 4).await.unwrap();
    // Publish messages to the subject
    client.publish(&subject, &[1]).await.unwrap();
    client.publish(&subject, &[2]).await.unwrap();

    assert_eq!(subscription.recv().await.unwrap().into_payload(), &[1]);
    assert_eq!(subscription.recv().await.unwrap().into_payload(), &[2]);

    // Disconnect
    client.disconnect().await;

    // Can not publish while disconnected
    assert!(client.publish(&subject, &[3]).await.is_err());

    // Reconnect
    client.connect().await;

    // Publish more messages to the subject
    client.publish(&subject, &[4]).await.unwrap();
    client.publish(&subject, &[5]).await.unwrap();

    assert_eq!(subscription.recv().await.unwrap().payload(), &[4]);
    assert_eq!(subscription.recv().await.unwrap().payload(), &[5]);

    client.disconnect().await;
    std::mem::drop(client);

    assert!(subscription.recv().await.is_none());
}

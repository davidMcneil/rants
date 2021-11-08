mod common;

use common::NatsServer;
use futures::{future, stream::StreamExt};
use rants::Client;
use tokio_stream::wrappers::ReceiverStream;

#[tokio::test(flavor = "multi_thread")]
async fn echo() {
    common::init();
    let _nats_server = NatsServer::new(&[]).await;

    let number_of_messages = 1024;

    // Create a new client
    let address = "127.0.0.1".parse().unwrap();
    let client = Client::new(vec![address]);

    // Trying to publish before connecting should result in a not connected error
    let subject = "test".parse().unwrap();
    assert!(client
        .publish(&subject, b"test")
        .await
        .unwrap_err()
        .not_connected());

    // Configure it to echo messages so we can receive messages we sent
    client
        .connect_mut()
        .await
        .verbose(true)
        .pedantic(true)
        .echo(true);

    // Connect the client
    client.connect().await;

    // Subscribe to a subscription called "test"
    let subject = "test".parse().unwrap();
    let (_, subscription) = client
        .subscribe(&subject, number_of_messages)
        .await
        .unwrap();
    let subscription = ReceiverStream::new(subscription);

    // Publish messages to "test" subscription
    let mut publishers = Vec::new();
    for i in 0..number_of_messages {
        let client = Client::clone(&client);
        publishers.push(async move {
            let subject = "test".parse().unwrap();
            let message = format!("{}", i);
            client.publish(&subject, message.as_bytes()).await.unwrap();
        });
    }
    future::join_all(publishers).await;

    // Check all messages received from the subscription
    let mut messages = subscription
        .take(number_of_messages)
        .map(|msg| {
            String::from_utf8(msg.into_payload())
                .unwrap()
                .parse::<usize>()
                .unwrap()
        })
        .collect::<Vec<usize>>()
        .await;
    messages.sort();
    assert_eq!(messages, (0..number_of_messages).collect::<Vec<usize>>());

    // Disconnect
    client.disconnect().await;
}

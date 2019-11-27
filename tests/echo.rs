mod common;

use futures::{future, stream::StreamExt};
use rants::Client;

async fn main() {
    let number_of_messages = 1024;

    // Create a new client
    let address = "127.0.0.1".parse().unwrap();
    let client = Client::new(vec![address]);

    // Configure it to echo messages so we can receive messages we sent
    client.connect_mut().await.echo(true);

    // Connect the client
    client.connect().await;

    // Subscribe to a subscription called "test"
    let subject = "test".parse().unwrap();
    let (_, subscription) = client
        .subscribe(&subject, number_of_messages)
        .await
        .unwrap();

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
        .take(number_of_messages as u64)
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

#[test]
fn echo() {
    common::run_integration_test(main(), &[]);
}

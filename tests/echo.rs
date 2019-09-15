mod common;

use futures::{future, stream::StreamExt};
use rants::Client;
use std::sync::Arc;

async fn main() {
    let number_of_messages = 1024;

    // Create a new client
    let address = "127.0.0.1".parse().unwrap();
    let wrapped_client = Client::new(vec![address]);

    // Configure it to echo messages so we can receive messages we sent
    wrapped_client.lock().await.connect_mut().echo(true);

    // Connect the client
    Client::connect(Arc::clone(&wrapped_client)).await;

    // Subscribe to a subscription called "test"
    let (_, subscription) = {
        let subject = "test".parse().unwrap();
        let mut client = wrapped_client.lock().await;
        client
            .subscribe(&subject, number_of_messages)
            .await
            .unwrap()
    };

    // Publish messages to "test" subscription
    let mut publishers = Vec::new();
    for i in 0..number_of_messages {
        let wrapped_client = Arc::clone(&wrapped_client);
        publishers.push(async move {
            let subject = "test".parse().unwrap();
            let message = format!("{}", i);
            let mut client = wrapped_client.lock().await;
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
    Client::disconnect(wrapped_client).await;
}

#[test]
fn echo() {
    common::run_integration_test(main());
}

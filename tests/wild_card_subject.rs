mod common;

use futures::stream::StreamExt;
use rants::Client;
use std::sync::Arc;

async fn main() {
    let address = "127.0.0.1".parse().unwrap();
    let wrapped_client = Client::new(vec![address]);
    wrapped_client.lock().await.connect_mut().echo(true);

    Client::connect(Arc::clone(&wrapped_client)).await;

    let wild_card_subject = "test.>".parse().unwrap();
    let a_subject = "test.a".parse().unwrap();

    // Subscribe to the subject
    let (mut wild_card_subscription, mut a_subscription) = {
        let mut client = wrapped_client.lock().await;
        (
            client.subscribe(&wild_card_subject, 3).await.unwrap().1,
            client.subscribe(&a_subject, 3).await.unwrap().1,
        )
    };

    // Publish messages to the subject
    {
        let mut client = wrapped_client.lock().await;
        client.publish(&a_subject, &[1]).await.unwrap();
        client.publish(&a_subject, &[2]).await.unwrap();
        let ab_subject = "test.a.b".parse().unwrap();
        client.publish(&ab_subject, &[3]).await.unwrap();
    }

    assert_eq!(wild_card_subscription.next().await.unwrap().payload(), &[1]);
    assert_eq!(wild_card_subscription.next().await.unwrap().payload(), &[2]);
    assert_eq!(wild_card_subscription.next().await.unwrap().payload(), &[3]);

    assert_eq!(a_subscription.next().await.unwrap().payload(), &[1]);
    assert_eq!(a_subscription.next().await.unwrap().payload(), &[2]);

    Client::disconnect(wrapped_client).await;

    assert!(wild_card_subscription.next().await.is_none());
    assert!(a_subscription.next().await.is_none());
}

#[test]
fn wild_card_subject() {
    common::run_integration_test(main());
}

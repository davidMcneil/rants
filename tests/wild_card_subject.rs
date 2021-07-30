mod common;

use common::NatsServer;
use rants::Client;

#[tokio::test(flavor = "multi_thread")]
async fn wild_card_subject() {
    common::init();
    let _nats_server = NatsServer::new(&[]).await;

    let address = "127.0.0.1".parse().unwrap();
    let client = Client::new(vec![address]);
    client.connect_mut().await.echo(true);

    client.connect().await;

    let wild_card_subject = "test.>".parse().unwrap();
    let a_subject = "test.a".parse().unwrap();

    // Subscribe to the subjects
    let mut wild_card_subscription = client.subscribe(&wild_card_subject, 3).await.unwrap().1;
    let mut a_subscription = client.subscribe(&a_subject, 3).await.unwrap().1;

    // Publish messages to the subjects
    client.publish(&a_subject, &[1]).await.unwrap();
    client.publish(&a_subject, &[2]).await.unwrap();
    let ab_subject = "test.a.b".parse().unwrap();
    client.publish(&ab_subject, &[3]).await.unwrap();

    assert_eq!(wild_card_subscription.recv().await.unwrap().payload(), &[1]);
    assert_eq!(wild_card_subscription.recv().await.unwrap().payload(), &[2]);
    assert_eq!(wild_card_subscription.recv().await.unwrap().payload(), &[3]);

    assert_eq!(a_subscription.recv().await.unwrap().payload(), &[1]);
    assert_eq!(a_subscription.recv().await.unwrap().payload(), &[2]);

    client.disconnect().await;
    std::mem::drop(client);

    assert!(wild_card_subscription.recv().await.is_none());
    assert!(a_subscription.recv().await.is_none());
}

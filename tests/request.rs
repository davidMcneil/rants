mod common;

use futures::stream::StreamExt;
use rants::{Client, Subject};

async fn make_subscription(client: Client, subject: &Subject) {
    let mut subscription = client.subscribe(subject, 1).await.unwrap().1;
    let client_copy = Client::clone(&client);
    tokio::spawn(async move {
        // Wait for the request
        let request = subscription.next().await.unwrap();
        let reply_to = request.reply_to().unwrap().clone();
        let request = String::from_utf8(request.into_payload()).unwrap();
        assert_eq!(&request, "the request");
        // Send the reply
        let client = client_copy;
        client.publish(&reply_to, b"the reply").await.unwrap();
    });
}

async fn main() {
    let address = "127.0.0.1".parse().unwrap();
    let client = Client::new(vec![address]);
    client.connect_mut().await.echo(true);
    client.connect().await;

    // Spawn a few subscription tasks to send the reply
    let subject1 = "test_subject1".parse::<Subject>().unwrap();
    make_subscription(Client::clone(&client), &subject1).await;
    let subject2 = "test_subject2".parse::<Subject>().unwrap();
    make_subscription(Client::clone(&client), &subject2).await;

    // Make a first request
    let reply = client.request(&subject1, b"the request").await.unwrap();
    let reply = String::from_utf8(reply.into_payload()).unwrap();
    assert_eq!(&reply, "the reply");

    // Make a second request
    let reply = client.request(&subject2, b"the request").await.unwrap();
    let reply = String::from_utf8(reply.into_payload()).unwrap();
    assert_eq!(&reply, "the reply");

    client.disconnect().await;
}

#[test]
fn request() {
    common::run_integration_test(main());
}

mod common;

use futures::stream::StreamExt;
use rants::{Client, Subject};

async fn main() {
    let address = "127.0.0.1".parse().unwrap();
    let client = Client::new(vec![address]);
    client.connect_mut().await.echo(true);

    let subject = "test_subject".parse::<Subject>().unwrap();

    client.connect().await;

    // Spawn a task to send the reply
    let client_copy = Client::clone(&client);
    let subject_copy = subject.clone();
    tokio::spawn(async move {
        let client = client_copy;
        let mut subscription = client.subscribe(&subject_copy, 1).await.unwrap().1;
        // Wait for the request
        let request = subscription.next().await.unwrap();
        let reply_to = request.reply_to().unwrap().clone();
        let request = String::from_utf8(request.into_payload()).unwrap();
        assert_eq!(&request, "the request");
        // Send the reply
        client.publish(&reply_to, b"the reply").await.unwrap();
    });

    // Make a request
    let reply = client.request(&subject, b"the request").await.unwrap();
    let reply = String::from_utf8(reply.into_payload()).unwrap();
    assert_eq!(&reply, "the reply");

    client.disconnect().await;
}

#[test]
fn request() {
    common::run_integration_test(main());
}

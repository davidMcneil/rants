mod common;

use futures::stream::StreamExt;
use rants::{Client, Subject};
use std::sync::Arc;

async fn main() {
    let address = "127.0.0.1".parse().unwrap();
    let wrapped_client = Client::new(vec![address]);
    wrapped_client.lock().await.connect_mut().echo(true);

    let subject = "test_subject".parse::<Subject>().unwrap();

    Client::connect(Arc::clone(&wrapped_client)).await;

    // Spawn a task to send the reply
    let wrapped_client_copy = Arc::clone(&wrapped_client);
    let subject_copy = subject.clone();
    tokio::spawn(async move {
        let mut subscription = {
            let mut client = wrapped_client_copy.lock().await;
            client.subscribe(&subject_copy, 1).await.unwrap().1
        };
        let request = subscription.next().await.unwrap();
        let reply_to = request.reply_to().unwrap().clone();
        let request = String::from_utf8(request.payload()).unwrap();
        assert_eq!(&request, "the request");
        let mut client = wrapped_client_copy.lock().await;
        client.publish(&reply_to, b"the reply").await.unwrap();
    });

    // Make a request
    let reply = Client::request(Arc::clone(&wrapped_client), &subject, b"the request")
        .await
        .unwrap();
    let reply = String::from_utf8(reply.payload()).unwrap();
    assert_eq!(&reply, "the reply");

    Client::disconnect(wrapped_client).await;
}

#[test]
fn request() {
    common::run_integration_test(main());
}

mod common;

use common::NatsServer;
use rants::{error::Error, Client, Subject};

async fn make_subscription(client: Client, subject: &Subject) {
    let mut subscription = client.subscribe(subject, 1).await.unwrap().1;
    let client_copy = Client::clone(&client);
    tokio::spawn(async move {
        // Wait for the request
        let request = subscription.recv().await.unwrap();
        let reply_to = request.reply_to().unwrap().clone();
        let request = String::from_utf8(request.into_payload()).unwrap();
        assert_eq!(&request, "the request");
        // Send the reply
        let client = client_copy;
        client.publish(&reply_to, b"the reply").await.unwrap();
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn request() {
    common::init();
    let _nats_server = NatsServer::new(&["--auth=abc123"]).await;

    let address = "127.0.0.1".parse().unwrap();
    let client = Client::new(vec![address]);
    client
        .connect_mut()
        .await
        .echo(true)
        .token(String::from("abc123"));
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

    // Make a third request (via timeout interface to show it's wired correctly)
    let subject3 = "test_subject3".parse::<Subject>().unwrap();
    make_subscription(Client::clone(&client), &subject3).await;
    let reply = client
        .request_with_timeout(
            &subject3,
            b"the request",
            std::time::Duration::from_millis(1000),
        )
        .await
        .unwrap();
    let reply = String::from_utf8(reply.into_payload()).unwrap();
    assert_eq!(&reply, "the reply");

    // Make a fourth request (that will timeout)
    let subject4 = "test_subject4".parse::<Subject>().unwrap();
    let err = client
        .request_with_timeout(
            &subject4,
            b"the request",
            std::time::Duration::from_millis(10),
        )
        .await
        .unwrap_err();

    // Ensure we get a timeout error. Fail otherwise.
    match err {
        Error::Timeout(_) => {}
        _ => assert!(false),
    };

    client.disconnect().await;
}

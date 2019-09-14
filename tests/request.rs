// mod common;

// use futures::stream::StreamExt;
// use rants::{Client, Subject};
// use std::sync::Arc;

// async fn main() {
//     let address = "127.0.0.1".parse().unwrap();
//     let wrapped_client = Client::new(vec![address]);

//     let subject = "test_subject".parse::<Subject>().unwrap();

//     Client::connect(Arc::clone(&wrapped_client)).await;

//     let wrapped_client_copy = Arc::clone(&wrapped_client);
//     let subject_copy = subject.clone();
//     tokio::spawn(async move {
//         let mut client = wrapped_client_copy.lock().await;
//         let (_, mut subscription) = client.subscribe(&subject_copy, 1).await.unwrap();
//         let request = subscription.next().await.unwrap();
//         let request = String::from_utf8(request.payload()).unwrap();
//         assert_eq!(&request, "the request");
//     });

//     // Make a request
//     let reply = Client::request(Arc::clone(&wrapped_client), &subject, b"the request")
//         .await
//         .unwrap();
//     let reply = String::from_utf8(reply.payload()).unwrap();
//     assert_eq!(&reply, "the reply");

//     Client::disconnect(wrapped_client).await;
// }

// #[test]
// fn request() {
//     common::run_integration_test(main());
// }

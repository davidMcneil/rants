use super::*;
use env_logger::Builder as LogBuilder;
use log::LevelFilter;
use rand::Rng;
use std::time;
use tokio::{runtime::current_thread::Runtime, timer};

// #[test]
// #[ignore]
// fn integration_simple() {
//     LogBuilder::new()
//         .default_format_timestamp(false)
//         .default_format_module_path(false)
//         .filter_level(LevelFilter::Trace)
//         .filter(Some("hyper"), LevelFilter::Warn)
//         .filter(Some("reqwest"), LevelFilter::Warn)
//         .filter(Some("tokio_reactor"), LevelFilter::Warn)
//         .init();

//     let f = async {
//         let mut rng = rand::thread_rng();
//         let mut connect = Connect::new();
//         connect.verbose(false).pedantic(false);
//         let client = Client::with_connect("0.0.0.0:4222", connect);
//         Client::connect(Arc::clone(&client)).await;
//         for i in 0..10 {
//             let client = client.clone();
//             let delay = rng.gen_range(0, 100);
//             tokio::spawn(async move {
//                 let s = "test".parse().unwrap();
//                 let message = format!("this is iteration {} delay {}", i, delay);
//                 let till = time::Instant::now() + time::Duration::from_millis(delay);
//                 timer::delay(till).await;
//                 let mut lock = client.lock().await;
//                 let connect = lock.connect_mut();
//                 connect.verbose(!connect.is_verbose());
//                 lock.send_connect().await.unwrap();
//                 lock.publish_with_reply(&s, &s, message.as_bytes())
//                     .await
//                     .unwrap();
//             });
//         }
//         let client = client.clone();
//         tokio::spawn(async move {
//             let till = time::Instant::now() + time::Duration::from_secs(3);
//             timer::delay(till).await;
//             println!("now!");
//             let mut lock = client.lock().await;
//             let s = "test".parse().unwrap();
//             if let Err(e) = lock.publish(&s, b"testing123").await {
//                 println!("Failed to publish, {}", e);
//             }
//         });
//     };
//     Runtime::new().unwrap().spawn(f).run().unwrap();
// }

#[test]
fn integration_echo() {
    // LogBuilder::new()
    //     .filter_level(LevelFilter::Trace)
    //     .filter(Some("mio"), LevelFilter::Warn)
    //     .filter(Some("tokio"), LevelFilter::Warn)
    //     .init();

    let f = async {
        let mut rng = rand::thread_rng();
        let total = 100;
        let wrapped_client = Client::new("0.0.0.0:4222");
        {
            let mut client = wrapped_client.lock().await;
            client.connect_mut().echo(true);
        }
        Client::connect(Arc::clone(&wrapped_client)).await;
        let mut subscription = {
            let subject = "test".parse().unwrap();
            let mut client = wrapped_client.lock().await;
            client.subscribe(&subject, 1024).await.unwrap()
        };
        let results = Arc::new(Mutex::new(Vec::new()));
        let results2 = Arc::clone(&results);
        let subscriber = async move {
            let mut count = 0;
            while let Some(message) = subscription.next().await {
                let message = String::from_utf8(message).unwrap();
                results.lock().await.push(message);
                count += 1;
                if count >= total {
                    break;
                }
            }
        };
        let mut publishers = Vec::new();
        for i in 0..total {
            let wrapped_client = Arc::clone(&wrapped_client);
            let delay = rng.gen_range(0, 100);
            publishers.push(async move {
                let subject = "test".parse().unwrap();
                let till = time::Instant::now() + time::Duration::from_millis(delay);
                timer::delay(till).await;
                let message = format!("iteration {}", i);
                let mut client = wrapped_client.lock().await;
                client.publish(&subject, message.as_bytes()).await.unwrap();
            });
        }
        future::join(subscriber, future::join_all(publishers)).await;
        Client::disconnect(wrapped_client).await;
        assert!(results2.lock().await.len() == total);
    };
    Runtime::new().unwrap().spawn(f).run().unwrap();
}

// #[test]
// fn integration_watch() {
//     use futures::sink::SinkExt;
//     use tokio::sync::watch;
//     let f = async {
//         let (mut tx, mut rx) = watch::channel(-1);
//         tokio::spawn(async move {
//             for i in 0..10i32 {
//                 let till = time::Instant::now() + time::Duration::from_secs(1);
//                 timer::delay(till).await;
//                 tx.send(i).await.unwrap();
//             }
//         });
//         let till = time::Instant::now() + time::Duration::from_secs(12);
//         timer::delay(till).await;
//         while let Some(n) = rx.recv().await {
//             println!("got {}", n);
//         }
//     };
//     Runtime::new().unwrap().spawn(f).run().unwrap();
// }

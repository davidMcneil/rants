use super::*;
use rand::Rng;
use std::{str, time};
use tokio::{runtime::current_thread::Runtime, timer};
use env_logger::Builder as LogBuilder;
use log::LevelFilter;

#[test]
#[ignore]
fn integration_simple() {
        LogBuilder::new().default_format_timestamp(false)
                     .default_format_module_path(false)
                     .filter_level(LevelFilter::Trace)
                     .filter(Some("hyper"), LevelFilter::Warn)
                     .filter(Some("reqwest"), LevelFilter::Warn)
                     .filter(Some("tokio_reactor"), LevelFilter::Warn)
                     .init();

    let f = async {
        let mut rng = rand::thread_rng();
        let mut connect = Connect::new();
        connect.verbose(false).pedantic(false);
        let client = Client::with_connect("0.0.0.0:4222", connect);
        Client::connect(Arc::clone(&client)).await;
        for i in 0..10 {
            let client = client.clone();
            let delay = rng.gen_range(0, 100);
            tokio::spawn(async move {
                let s = "test".parse().unwrap();
                let message = format!("this is iteration {} delay {}", i, delay);
                let till = time::Instant::now() + time::Duration::from_millis(delay);
                timer::delay(till).await;
                let mut lock = client.lock().await;
                let connect = lock.connect_mut();
                connect.verbose(!connect.is_verbose());
                lock.send_connect().await.unwrap();
                lock.publish_with_reply(&s, &s, message.as_bytes())
                    .await
                    .unwrap();
                println!("{}", message);
            });
        }
        let client = client.clone();
        tokio::spawn(async move {
            let till = time::Instant::now() + time::Duration::from_secs(3);
            timer::delay(till).await;
            println!("now!");
            let mut lock = client.lock().await;
            let s = "test".parse().unwrap();
            if let Err(e) = lock.publish(&s, b"testing123").await {
                println!("Failed to publish, {}", e);
            }
        });
    };
    Runtime::new().unwrap().spawn(f).run().unwrap();
}

#[test]
fn integration_echo() {
            LogBuilder::new()
                     .filter_level(LevelFilter::Trace)
                     .filter(Some("mio"), LevelFilter::Warn)
                     .filter(Some("tokio"), LevelFilter::Warn)
                     .init();

    let f = async {
        let mut rng = rand::thread_rng();
        let client_wrapper = Client::new("0.0.0.0:4222");
        {
            let mut client = client_wrapper.lock().await;
            client.connect_mut().echo(true);
        }
        Client::connect(Arc::clone(&client_wrapper)).await;
        let subject = "test".parse().unwrap();
        let mut client = client_wrapper.lock().await;
        let mut subscription = client.subscribe(&subject, 1024).await.unwrap();
        tokio::spawn(async move {
            while let Some(message) = subscription.next().await {
                let message = str::from_utf8(&message).unwrap();
                println!("Received message '{}'", message);
            }
        });
        for i in 0..10 {
            let client_wrapper = client_wrapper.clone();
            let delay = rng.gen_range(0, 100);
            tokio::spawn(async move {
                let s = "test".parse().unwrap();
                let till = time::Instant::now() + time::Duration::from_millis(delay);
                timer::delay(till).await;
                let mut client = client_wrapper.lock().await;
                let message = format!("this is iteration {} delay {}", i, delay);
                client.publish(&s, message.as_bytes()).await.unwrap();
            });
        }
    };
    Runtime::new().unwrap().spawn(f).run().unwrap();
}

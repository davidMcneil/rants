use super::*;

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use std::time;
    use tokio::{runtime::current_thread::Runtime, timer};
    #[test]
    fn integration_simple() {
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
                    let connect = lock.get_connect();
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
}

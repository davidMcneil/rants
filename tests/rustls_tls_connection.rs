#[cfg(feature = "rustls-tls")]
mod common;

#[cfg(feature = "rustls-tls")]
mod test {
    use super::common::{self, NatsServer};
    use rants::{rustls::ClientConfig, Client};
    use std::{fs::File, io::BufReader};

    #[tokio::test(flavor = "multi_thread")]
    async fn rustls_tls_connection() {
        common::init();
        let _nats_server = NatsServer::new(&[
            "--tlscert=tests/certs/server-crt.pem",
            "--tlskey=tests/certs/server-key.pem",
        ])
        .await;

        let address = "127.0.0.1".parse().unwrap();
        let mut client = Client::new(vec![address]);

        // Load the server root certificate
        let file = File::open("tests/certs/ca.pem").unwrap();
        let mut file = BufReader::new(file);

        // Set the TLS config
        let mut tls_config = ClientConfig::new();
        tls_config.root_store.add_pem_file(&mut file).unwrap();
        client.set_tls_config(tls_config).await;
        client.set_tls_domain(String::from("example.com")).await;

        client.connect().await;
        client.disconnect().await;
    }
}

#[cfg(feature = "rustls-tls")]
mod common;

#[cfg(feature = "rustls-tls")]
mod test {
    use super::common::{self, NatsServer};
    use rants::{
        rustls::{Certificate, ClientConfig, RootCertStore},
        Client,
    };
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
        let certs = rustls_pemfile::certs(&mut file).unwrap();
        let mut root_certs = RootCertStore::empty();
        for cert in certs {
            root_certs.add(&Certificate(cert)).unwrap();
        }
        let tls_config = ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(root_certs)
            .with_no_client_auth();
        client.set_tls_config(tls_config).await;
        client.set_tls_domain(String::from("example.com")).await;

        client.connect().await;
        client.disconnect().await;
    }
}

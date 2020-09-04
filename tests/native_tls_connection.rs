#[cfg(feature = "native-tls")]
mod common;

#[cfg(feature = "native-tls")]
mod test {
    use super::common::{self, NatsServer};
    use rants::{
        native_tls::{Certificate, TlsConnector},
        Client,
    };
    use std::{fs::File, io::Read};

    #[tokio::test(threaded_scheduler)]
    async fn tls_connection() {
        common::init();
        let _nats_server = NatsServer::new(&[
            "--tlscert=tests/certs/server.pem",
            "--tlskey=tests/certs/key.pem",
        ])
        .await;

        let address = "127.0.0.1".parse().unwrap();
        let mut client = Client::new(vec![address]);

        // Load the server root certificate
        let mut file = File::open("tests/certs/ca.pem").unwrap();
        let mut certificate = vec![];
        file.read_to_end(&mut certificate).unwrap();
        let certificate = Certificate::from_pem(&certificate).unwrap();

        // Set the TLS connector
        let tls_connector = TlsConnector::builder()
            .add_root_certificate(certificate)
            .build()
            .unwrap();
        client.set_tls_connector(tls_connector).await;

        client.connect().await;
        client.disconnect().await;
    }
}

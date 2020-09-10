#[cfg(feature = "rustls-tls")]
use crate::error::Error;
use crate::error::Result;
#[cfg(feature = "native-tls")]
pub use native_tls_crate::{Error as TlsError, TlsConnector as TlsConfig};
#[cfg(feature = "rustls-tls")]
pub use rustls::{ClientConfig as TlsConfig, TLSError as TlsError};
#[cfg(feature = "rustls-tls")]
use std::sync::Arc;
use tokio::net::TcpStream;
#[cfg(feature = "native-tls")]
pub use tokio_native_tls::{TlsConnector, TlsStream};
#[cfg(feature = "rustls-tls")]
pub use tokio_rustls::{client::TlsStream, webpki::DNSNameRef, TlsConnector};

pub async fn tls_stream(
    tls_config: TlsConfig,
    domain: &str,
    stream: TcpStream,
) -> Result<TlsStream<TcpStream>> {
    #[cfg(feature = "native-tls")]
    let tls_connector = TlsConnector::from(tls_config);
    #[cfg(feature = "rustls-tls")]
    let (tls_connector, domain) = (
        TlsConnector::from(Arc::new(tls_config)),
        DNSNameRef::try_from_ascii_str(domain)
            .map_err(|_| Error::InvalidDnsName(String::from(domain)))?,
    );
    Ok(tls_connector.connect(domain, stream).await?)
}

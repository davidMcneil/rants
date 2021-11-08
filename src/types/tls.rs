#[cfg(feature = "rustls-tls")]
use crate::error::Error;
use crate::error::Result;
#[cfg(feature = "native-tls")]
pub use native_tls_crate::{Error as TlsError, TlsConnector as TlsConfig};
#[cfg(feature = "rustls-tls")]
pub use rustls::{ClientConfig as TlsConfig, Error as TlsError, ServerName};
#[cfg(feature = "rustls-tls")]
use std::{convert::TryFrom, sync::Arc};
use tokio::net::TcpStream;
#[cfg(feature = "native-tls")]
pub use tokio_native_tls::{TlsConnector, TlsStream};
#[cfg(feature = "rustls-tls")]
pub use tokio_rustls::{client::TlsStream, TlsConnector};

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
        ServerName::try_from(domain).map_err(|_| Error::InvalidDnsName(String::from(domain)))?,
    );
    Ok(tls_connector.connect(domain, stream).await?)
}

#[cfg(feature = "tls")]
use native_tls::{self, TlsConnector};
use pin_project::{pin_project, project};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{self, AsyncRead, AsyncWrite},
    net::TcpStream,
};
#[cfg(feature = "tls")]
use tokio_tls::{TlsConnector as TokioTlsConnector, TlsStream};

/// A simple wrapper type that can either be a raw TCP stream or a TCP stream with TLS enabled.
#[pin_project]
#[derive(Debug)]
pub enum TlsOrTcpStream {
    TcpStream(#[pin] TcpStream),
    #[cfg(feature = "tls")]
    TlsStream(#[pin] TlsStream<TcpStream>),
}

impl TlsOrTcpStream {
    pub fn new(stream: TcpStream) -> Self {
        Self::TcpStream(stream)
    }

    #[cfg(feature = "tls")]
    pub async fn upgrade(
        self,
        tls_connector: TlsConnector,
        domain: &str,
    ) -> Result<Self, native_tls::Error> {
        Ok(match self {
            Self::TcpStream(stream) => {
                let tokio_tls_connector = TokioTlsConnector::from(tls_connector);
                let tls_stream = tokio_tls_connector.connect(domain, stream).await?;
                Self::TlsStream(tls_stream)
            }
            Self::TlsStream(stream) => Self::TlsStream(stream),
        })
    }
}

impl AsyncRead for TlsOrTcpStream {
    #[project]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        #[project]
        match self.project() {
            TlsOrTcpStream::TcpStream(stream) => stream.poll_read(cx, buf),
            #[cfg(feature = "tls")]
            TlsOrTcpStream::TlsStream(stream) => stream.poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for TlsOrTcpStream {
    #[project]
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        #[project]
        match self.project() {
            TlsOrTcpStream::TcpStream(stream) => stream.poll_write(cx, buf),
            #[cfg(feature = "tls")]
            TlsOrTcpStream::TlsStream(stream) => stream.poll_write(cx, buf),
        }
    }

    #[project]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        #[project]
        match self.project() {
            TlsOrTcpStream::TcpStream(stream) => stream.poll_flush(cx),
            #[cfg(feature = "tls")]
            TlsOrTcpStream::TlsStream(stream) => stream.poll_flush(cx),
        }
    }

    #[project]
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        #[project]
        match self.project() {
            TlsOrTcpStream::TcpStream(stream) => stream.poll_shutdown(cx),
            #[cfg(feature = "tls")]
            TlsOrTcpStream::TlsStream(stream) => stream.poll_shutdown(cx),
        }
    }
}

#[cfg(feature = "tls")]
use crate::types::{
    error::Result,
    tls::{self, TlsConfig, TlsStream},
};
use pin_project::pin_project;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{self, AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};

/// A simple wrapper type that can either be a raw TCP stream or a TCP stream with TLS enabled.
#[pin_project(project = TlsOrTcpStreamProj)]
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
    pub async fn upgrade(self, tls_config: TlsConfig, domain: &str) -> Result<Self> {
        Ok(match self {
            Self::TcpStream(stream) => {
                let tls_stream = tls::tls_stream(tls_config, domain, stream).await?;
                Self::TlsStream(tls_stream)
            }
            Self::TlsStream(stream) => Self::TlsStream(stream),
        })
    }
}

impl AsyncRead for TlsOrTcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        match self.project() {
            TlsOrTcpStreamProj::TcpStream(stream) => stream.poll_read(cx, buf),
            #[cfg(feature = "tls")]
            TlsOrTcpStreamProj::TlsStream(stream) => stream.poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for TlsOrTcpStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.project() {
            TlsOrTcpStreamProj::TcpStream(stream) => stream.poll_write(cx, buf),
            #[cfg(feature = "tls")]
            TlsOrTcpStreamProj::TlsStream(stream) => stream.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.project() {
            TlsOrTcpStreamProj::TcpStream(stream) => stream.poll_flush(cx),
            #[cfg(feature = "tls")]
            TlsOrTcpStreamProj::TlsStream(stream) => stream.poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.project() {
            TlsOrTcpStreamProj::TcpStream(stream) => stream.poll_shutdown(cx),
            #[cfg(feature = "tls")]
            TlsOrTcpStreamProj::TlsStream(stream) => stream.poll_shutdown(cx),
        }
    }
}

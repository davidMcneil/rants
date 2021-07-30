//! `nats` `Error` and `Result`

use std::{fmt, io};

#[cfg(feature = "tls")]
use crate::types::tls::TlsError;
use crate::{types::Sid, util};

use tokio::time;

/// All potential `rants` errors
#[derive(Debug)]
pub enum Error {
    /// Occurs when trying to a publish greater than the `max_payload` of the server
    ExceedsMaxPayload {
        /// The number of bytes tried
        tried: usize,
        /// The `max_payload` set by the server
        limit: usize,
    },
    /// Occurs when trying to parse an invalid [`Address`](../struct.Address.html)
    InvalidAddress(String),
    /// Occurs when input is not a valid DNS name
    #[cfg(feature = "rustls-tls")]
    InvalidDnsName(String),
    /// Occurs when trying to parse an [`Address`](../struct.Address.html) with an invalid network
    /// scheme
    InvalidNetworkScheme(String),
    /// Occurs when trying to parse an invalid control line from the server
    InvalidServerControl(String),
    /// Occurs when trying to parse an invalid [`Subject`](../struct.Subject.html)
    InvalidSubject(String),
    /// Occurs when the payload of a server message has an invalid terminator
    InvalidTerminator(Vec<u8>),
    /// Wrapper for all IO errors
    Io(io::Error),
    /// Wrapper for all tls errors
    #[cfg(feature = "tls")]
    Tls(TlsError),
    /// Occurs when a [`request`](../struct.Client.html#method.request) does not receive a
    /// response
    NoResponse,
    /// Occurs when trying to use the [`Client`](../struct.Client.html) to communicate with the
    /// server while not in the [`Connected`](../enum.ClientState.html#variant.Connected) state
    NotConnected,
    /// Occurs when the server did not send enough data
    NotEnoughData,
    /// A timeout that has elapsed. For example: when a request does not a receive a response
    /// before the provided timeout duration has expired.
    Timeout(time::error::Elapsed),
    /// Occurs when no TLS connector was specified, but the server requires a TLS connection.
    TlsDisabled,
    /// Occurs when trying to [`unsubscribe`](../struct.Client.html#method.unsubscribe) with
    /// an unknown [`Sid`](../type.Sid.html)
    UnknownSid(Sid),
}

impl Error {
    /// Returns true if the error is a `NotConnected` error
    pub fn not_connected(&self) -> bool {
        matches!(self, Self::NotConnected)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::ExceedsMaxPayload { tried, limit } => {
                write!(f, "'{}' exceeds max payload '{}'", tried, limit)
            }
            Error::InvalidAddress(address) => write!(f, "invalid address {:?}", address),
            #[cfg(feature = "rustls-tls")]
            Error::InvalidDnsName(input) => write!(f, "invalid DNS name '{}'", input),
            Error::InvalidServerControl(line) => write!(f, "invalid control line {:?}", line),
            Error::InvalidSubject(subject) => write!(f, "invalid subject {:?}", subject),
            Error::InvalidTerminator(terminator) => {
                write!(f, "invalid message terminator {:?}", terminator)
            }
            Error::Io(e) => write!(f, "{}", e),
            #[cfg(feature = "tls")]
            Error::Tls(e) => write!(f, "{}", e),
            Error::NoResponse => write!(f, "no response"),
            Error::NotConnected => write!(f, "not connected"),
            Error::NotEnoughData => write!(f, "not enough data"),
            Error::InvalidNetworkScheme(protocol) => write!(
                f,
                "invalid scheme '{}' only '{}' is supported",
                protocol,
                util::NATS_NETWORK_SCHEME
            ),
            Error::UnknownSid(sid) => write!(f, "unknown sid '{}'", sid),
            Error::Timeout(e) => write!(f, "{}", e),
            Error::TlsDisabled => write!(f, "no TLS connector specified"),
        }
    }
}

impl std::error::Error for Error {}

impl From<tokio::time::error::Elapsed> for Error {
    fn from(e: tokio::time::error::Elapsed) -> Self {
        Error::Timeout(e)
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

#[cfg(feature = "tls")]
impl From<TlsError> for Error {
    fn from(e: TlsError) -> Self {
        Error::Tls(e)
    }
}

/// A `Result` that uses the `rants` [`Error`](enum.Error.html) type
pub type Result<T> = std::result::Result<T, Error>;

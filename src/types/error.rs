use std::{fmt, io, net};

use crate::{types::Sid, util};

#[derive(Debug)]
pub enum Error {
    ExceedsMaxPayload { tried: usize, limit: usize },
    InvalidAddress,
    InvalidNetworkScheme(String),
    InvalidServerControl(String),
    InvalidSubject(String),
    InvalidTerminator(Vec<u8>),
    Io(io::Error),
    NoResponse,
    NotConnected,
    NotEnoughData,
    UnknownSid(Sid),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::ExceedsMaxPayload { tried, limit } => {
                write!(f, "{} exceeds max payload {}", tried, limit)
            }
            Error::InvalidAddress => write!(f, "invalid address"),
            Error::InvalidServerControl(line) => write!(f, "invalid control line {:?}", line),
            Error::InvalidSubject(subject) => write!(f, "invalid subject {:?}", subject),
            Error::InvalidTerminator(terminator) => {
                write!(f, "invalid message terminator {:?}", terminator)
            }
            Error::Io(e) => write!(f, "{}", e),
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
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<net::AddrParseError> for Error {
    fn from(_: net::AddrParseError) -> Self {
        Error::InvalidAddress
    }
}

pub type Result<T> = std::result::Result<T, Error>;

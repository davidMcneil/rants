use std::{fmt, io, net};

use crate::types::Sid;

#[derive(Debug)]
pub enum RantsError {
    ExceedsMaxPayload { tried: usize, limit: usize },
    InvalidAddress,
    InvalidProtocol(String),
    InvalidServerControl(String),
    InvalidSubject(String),
    InvalidTerminator(Vec<u8>),
    Io(io::Error),
    NotConnected,
    NotEnoughData,
    UnknownSid(Sid),
}

impl fmt::Display for RantsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RantsError::ExceedsMaxPayload { tried, limit } => {
                write!(f, "{} exceeds max payload {}", tried, limit)
            }
            RantsError::InvalidAddress => write!(f, "invalid address"),
            RantsError::InvalidServerControl(line) => write!(f, "invalid control line {:?}", line),
            RantsError::InvalidSubject(subject) => write!(f, "invalid subject {:?}", subject),
            RantsError::InvalidTerminator(terminator) => {
                write!(f, "invalid message terminator {:?}", terminator)
            }
            RantsError::Io(e) => write!(f, "{}", e),
            RantsError::NotConnected => write!(f, "not connected"),
            RantsError::NotEnoughData => write!(f, "not enough data"),
            RantsError::InvalidProtocol(protocol) => write!(f, "invalid protocol '{}'", protocol),
            RantsError::UnknownSid(sid) => write!(f, "unknown sid '{}'", sid),
        }
    }
}

impl std::error::Error for RantsError {}

impl From<io::Error> for RantsError {
    fn from(e: io::Error) -> Self {
        RantsError::Io(e)
    }
}

impl From<net::AddrParseError> for RantsError {
    fn from(_: net::AddrParseError) -> Self {
        RantsError::InvalidAddress
    }
}

pub type RantsResult<T> = Result<T, RantsError>;

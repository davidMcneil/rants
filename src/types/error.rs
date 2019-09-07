use std::{fmt, io};

#[derive(Debug)]
pub enum RantsError {
    FailedToParse(String),
    InvalidSubject(String),
    InvalidTerminator(Vec<u8>),
    Io(io::Error),
    NotConnected,
    NotEnoughData,
}

impl fmt::Display for RantsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RantsError::FailedToParse(line) => write!(f, "failed to parse line {:?}", line),
            RantsError::InvalidSubject(subject) => write!(f, "invalid subject {:?}", subject),
            RantsError::InvalidTerminator(terminator) => {
                write!(f, "invalid message terminator {:?}", terminator)
            }
            RantsError::Io(e) => write!(f, "{}", e),
            RantsError::NotConnected => write!(f, "not connected"),
            RantsError::NotEnoughData => write!(f, "not enough data"),
        }
    }
}

impl std::error::Error for RantsError {}

impl From<io::Error> for RantsError {
    fn from(e: io::Error) -> Self {
        RantsError::Io(e)
    }
}

pub type RantsResult<T> = Result<T, RantsError>;

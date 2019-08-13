mod parser;

use serde::Serialize;
use std::{convert::TryFrom,
          fmt};

pub use crate::types::parser::ServerMessage;
use crate::{constants::{CLIENT_VERSION,
                        MESSAGE_TERMINATOR},
            types::parser::parse_subject};

#[derive(Debug, PartialEq)]
pub enum GnatError {
    InvalidSubject(String),
}

pub type GnatResult<T> = Result<T, GnatError>;

pub enum ConnectionState {
    Connected,
}

#[derive(Debug, PartialEq, Serialize)]
pub struct Connect {
    /// Turns on +OK protocol acknowledgements.
    verbose: bool,
    /// Turns on additional strict format checking, e.g. for properly formed subjects
    pedantic: bool,
    /// Indicates whether the client requires an SSL connection.
    tls_required: bool,
    /// Client authorization token (if auth_required is set)
    #[serde(skip_serializing_if = "Option::is_none")]
    auth_token: Option<String>,
    /// Connection username (if auth_required is set)
    #[serde(skip_serializing_if = "Option::is_none")]
    user: Option<String>,
    /// Connection password (if auth_required is set)
    #[serde(skip_serializing_if = "Option::is_none")]
    pass: Option<String>,
    /// Optional client name
    name: Option<String>,
    /// The implementation language of the client.
    lang: String,
    /// The version of the client.
    version: String,
    /// Optional int. Sending 0 (or absent) indicates client supports original protocol. Sending 1
    /// indicates that the client supports dynamic reconfiguration of cluster topology changes by
    /// asynchronously receiving INFO messages with known servers it can reconnect to.
    protocol: i32,
    /// Optional boolean. If set to true, the server (version 1.2.0+) will not send originating
    /// messages from this connection to its own subscriptions. Clients should set this to true
    /// only for server supporting this feature, which is when proto in the INFO protocol is set to
    /// at least 1
    echo: bool,
}

impl Connect {
    pub fn as_message(&self) -> String {
        format!("CONNECT {}{}",
                serde_json::to_string(self).expect("to serialize Connect"),
                MESSAGE_TERMINATOR)
    }
}

impl Default for Connect {
    fn default() -> Self {
        Self { verbose:      false,
               pedantic:     false,
               tls_required: false,
               auth_token:   None,
               user:         None,
               pass:         None,
               name:         None,
               lang:         String::from("rust"),
               version:      String::from(CLIENT_VERSION),
               protocol:     0,
               echo:         false, }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Subject {
    tokens:        Vec<String>,
    full_wildcard: bool,
}

impl Subject {
    /// Create a new subject without verifying that all tokens are valid
    fn new_unchecked(tokens: &Vec<&str>, full_wildcard: bool) -> Self {
        let tokens = tokens.iter().map(|s| String::from(*s)).collect();
        Self { tokens,
               full_wildcard }
    }
}

impl TryFrom<&str> for Subject {
    type Error = GnatError;

    fn try_from(input: &str) -> Result<Self, Self::Error> { parse_subject(input) }
}

impl fmt::Display for Subject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.tokens.join("."))?;
        if self.full_wildcard {
            write!(f, ".>")?;
        }
        Ok(())
    }
}

pub fn header_for_publish_message(subject: &Subject,
                                  reply_to: &Option<&Subject>,
                                  len: usize)
                                  -> String {
    if let Some(reply_to) = reply_to {
        format!("PUB {} {} {}{}", subject, reply_to, len, MESSAGE_TERMINATOR)
    } else {
        format!("PUB {} {}{}", subject, len, MESSAGE_TERMINATOR)
    }
}

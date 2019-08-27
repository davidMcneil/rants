mod parser;

use serde::{Deserialize,
            Serialize};
use std::{fmt,
          io};

use crate::constants;

#[derive(Debug)]
pub enum GnatError {
    FailedToParse(String),
    InvalidSubject(String),
    InvalidTerminator(Vec<u8>),
    Io(io::Error),
    NotEnoughData,
}

impl fmt::Display for GnatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GnatError::FailedToParse(line) => write!(f, "failed to parse line '{}'", line),
            GnatError::InvalidSubject(subject) => write!(f, "invalid subject '{}'", subject),
            GnatError::InvalidTerminator(terminator) => {
                write!(f, "invalid message terminator {:?}", terminator)
            }
            GnatError::Io(e) => write!(f, "{}", e),
            GnatError::NotEnoughData => write!(f, "not enough data"),
        }
    }
}

impl std::error::Error for GnatError {}

impl From<io::Error> for GnatError {
    fn from(e: io::Error) -> GnatError { GnatError::Io(e) }
}

pub type GnatResult<T> = Result<T, GnatError>;

pub enum ConnectionState {
    Connected,
}

/// https://nats-io.github.io/docs/nats_protocol/nats-protocol.html#info  
#[derive(Debug, Deserialize, PartialEq)]
pub struct Info {
    /// The unique identifier of the NATS server
    pub server_id: String,
    /// The version of the NATS server
    pub version: String,
    /// The version of golang the NATS server was built with
    pub go: String,
    /// The IP address used to start the NATS server, by default this will be 0.0.0.0 and can be
    /// configured with -client_advertise host:port
    pub host: String,
    /// The port number the NATS server is configured to listen on
    pub port: u16,
    /// Maximum payload size, in bytes, that the server will accept from the client.
    pub max_payload: u64,
    /// An integer indicating the protocol version of the server. The server version 1.2.0 sets
    /// this to 1 to indicate that it supports the "Echo" feature.
    pub proto: i32,
    /// An optional unsigned integer (64 bits) representing the internal client identifier in the
    /// server. This can be used to filter client connections in monitoring, correlate with error
    /// logs, etc...
    pub client_id: Option<u64>,
    #[serde(default)]
    /// If this is set, then the client should try to authenticate upon connect.
    pub auth_required: bool,
    #[serde(default)]
    /// If this is set, then the client must perform the TLS/1.2 handshake. Note, this used to be
    /// ssl_required and has been updated along with the protocol from SSL to TLS.
    pub tls_required: bool,
    #[serde(default)]
    /// If this is set, the client must provide a valid certificate during the TLS handshake.
    pub tls_verify: bool,
    #[serde(default)]
    /// An optional list of server urls that a client can connect to.
    pub connect_urls: Vec<String>,
}

/// https://nats-io.github.io/docs/nats_protocol/nats-protocol.html#connect
#[derive(Clone, Debug, PartialEq, Serialize)]
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
               version:      String::from(constants::CLIENT_VERSION),
               protocol:     0,
               echo:         false, }
    }
}

/// https://nats-io.github.io/docs/nats_protocol/nats-protocol.html#okerr
#[derive(Debug, PartialEq)]
pub enum ProtocolErr {
    UnknownProtocolOperation,
    AttemptedToConnectToRoutePort,
    AuthorizationViolation,
    AuthorizationTimeout,
    InvalidClientProtocol,
    MaximumControlLineExceeded,
    ParserError,
    SecureConnectionTlsRequired,
    StaleConnection,
    MaximumConnectionsExceeded,
    SlowConsumer,
    MaximumPayloadViolation,
    InvalidSubject,
    PermissionsViolationForSubscription(Subject),
    PermissionsViolationForPublish(Subject),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Subject {
    tokens:        Vec<String>,
    full_wildcard: bool,
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

#[derive(Debug, PartialEq)]
pub struct Msg {
    pub subject:  Subject,
    pub sid:      String,
    pub reply_to: Option<Subject>,
    pub payload:  Vec<u8>,
}

impl Msg {
    pub fn new(subject: Subject, sid: String, reply_to: Option<Subject>, payload: Vec<u8>) -> Self {
        Self { subject,
               sid,
               reply_to,
               payload }
    }
}

/// Representation of all possible server control lines. A control line is the first line of a
/// message.
#[derive(Debug, PartialEq)]
pub enum ServerControl {
    Info(Info),
    Msg {
        subject:  Subject,
        sid:      String,
        reply_to: Option<Subject>,
        len:      u64,
    },
    Ping,
    Pong,
    Ok,
    Err(ProtocolErr),
}

/// Representation of all possible server messages. This is similar to `ServerControl` however it
/// contains a full message type.
#[derive(Debug, PartialEq)]
pub enum ServerMessage {
    Info(Info),
    Msg(Msg),
    Ping,
    Pong,
    Ok,
    Err(ProtocolErr),
}

impl From<ServerControl> for ServerMessage {
    fn from(control: ServerControl) -> Self {
        match control {
            ServerControl::Info(info) => ServerMessage::Info(info),
            // We should never try to directly convert a `ServerControl::Msg` to
            // `ServerMessage::Msg`. The reason is the `Msg` message has a payload and therefore
            // requires further parsing.
            ServerControl::Msg { .. } => unreachable!(),
            ServerControl::Ping => ServerMessage::Ping,
            ServerControl::Pong => ServerMessage::Pong,
            ServerControl::Ok => ServerMessage::Ok,
            ServerControl::Err(e) => ServerMessage::Err(e),
        }
    }
}

/// Representation of all possible client messages.
pub enum ClientMessage {
    Connect(Connect),
    Pub(Msg),
    Sub,
    Unsub,
    Ping,
    Pong,
}

mod address;
pub mod error;
mod parser;
mod state;
#[cfg(test)]
mod tests;

use log::trace;
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::sync::mpsc::Sender as MpscSender;

use crate::util;

pub use self::{
    address::Address,
    state::{ClientState, ConnectionState, StateTransition, StateTransitionResult},
};

///////////////////////////////////////////////////////////////////////////////////////////////////

/// As soon as the server accepts a connection from the client, it will send information about
/// itself and the configuration and security requirements that are necessary for the client to
/// successfully authenticate with the server and exchange messages.
///
/// When using the updated client protocol (see CONNECT below), INFO messages can be sent anytime
/// by the server. This means clients with that protocol level need to be able to asynchronously
/// handle INFO messages.
///
/// https://nats-io.github.io/docs/nats_protocol/nats-protocol.html#info  
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct Info {
    pub(crate) server_id: String,
    pub(crate) version: String,
    pub(crate) go: String,
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) max_payload: usize,
    pub(crate) proto: i32,
    pub(crate) client_id: Option<u64>,
    #[serde(default)]
    pub(crate) auth_required: bool,
    #[serde(default)]
    pub(crate) tls_required: bool,
    #[serde(default)]
    pub(crate) tls_verify: bool,
    #[serde(default)]
    pub(crate) connect_urls: Vec<Address>,
}

impl Info {
    /// Construct a new default `Info`
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The unique identifier of the NATS server
    pub fn server_id(&self) -> &str {
        &self.server_id
    }

    /// The version of the NATS server
    pub fn version(&self) -> &str {
        &self.version
    }

    /// The version of golang the NATS server was built with
    pub fn go(&self) -> &str {
        &self.go
    }

    /// The IP address used to start the NATS server, by default this will be 0.0.0.0 and can be
    /// configured with -client_advertise host:port
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The port number the NATS server is configured to listen on
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Maximum payload size, in bytes, that the server will accept from the client.
    pub fn max_payload(&self) -> usize {
        self.max_payload
    }

    /// An integer indicating the protocol version of the server. The server version 1.2.0 sets
    /// this to 1 to indicate that it supports the "Echo" feature.
    pub fn proto(&self) -> i32 {
        self.proto
    }

    /// An optional unsigned integer (64 bits) representing the internal client identifier in the
    /// server. This can be used to filter client connections in monitoring, correlate with error
    /// logs, etc...
    pub fn client_id(&self) -> Option<u64> {
        self.client_id
    }

    /// If this is set, then the client should try to authenticate upon connect.
    pub fn auth_required(&self) -> bool {
        self.auth_required
    }

    /// If this is set, then the client must perform the TLS/1.2 handshake. Note, this used to be
    /// ssl_required and has been updated along with the protocol from SSL to TLS.
    ///
    /// TODO: The client does not currently support TLS
    pub fn tls_required(&self) -> bool {
        self.tls_required
    }

    /// If this is set, the client must provide a valid certificate during the TLS handshake.
    pub fn tls_verify(&self) -> bool {
        self.tls_verify
    }

    /// An optional list of server urls that a client can connect to.
    pub fn connect_urls(&self) -> &[Address] {
        &self.connect_urls
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(untagged)]
pub enum Authorization {
    Token {
        #[serde(rename = "auth_token")]
        token: String,
    },
    UsernamePassword {
        #[serde(rename = "user")]
        username: String,
        #[serde(rename = "pass")]
        password: String,
    },
}

impl Authorization {
    pub fn token(token: String) -> Self {
        Authorization::Token { token }
    }

    pub fn username_password(username: String, password: String) -> Self {
        Authorization::UsernamePassword { username, password }
    }
}

impl fmt::Display for Authorization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            Authorization::Token { token } => write!(f, "{}", token)?,
            Authorization::UsernamePassword { username, password } => {
                write!(f, "{}:{}", username, password)?
            }
        }
        Ok(())
    }
}

/// The CONNECT message is the client version of the INFO message. Once the client has established
/// a TCP/IP socket connection with the NATS server, and an INFO message has been received from the
/// server, the client may send a CONNECT message to the NATS server to provide more information
/// about the current connection as well as security information.
///
/// https://nats-io.github.io/docs/nats_protocol/nats-protocol.html#connect
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Connect {
    verbose: bool,
    pedantic: bool,
    tls_required: bool,
    #[serde(flatten)]
    authorization: Option<Authorization>,
    name: Option<String>,
    #[serde(rename = "lang")]
    language: String,
    version: String,
    protocol: i32,
    echo: bool,
}

impl Connect {
    /// Construct a new default `Connect`
    pub fn new() -> Self {
        Self::default()
    }

    /// Return `true` if the connection is verbose.
    pub fn is_verbose(&self) -> bool {
        self.verbose
    }

    /// Turns on +OK protocol acknowledgements. [default = `false`]
    pub fn verbose(&mut self, verbose: bool) -> &mut Self {
        self.verbose = verbose;
        self
    }

    /// Return `true` if the connection is pedantic.
    pub fn is_pedantic(&self) -> bool {
        self.pedantic
    }

    /// Turns on additional strict format checking, e.g. for properly formed subjects [default =
    /// `false`]
    pub fn pedantic(&mut self, pedantic: bool) -> &mut Self {
        self.pedantic = pedantic;
        self
    }

    /// Return `true` if the connection requires TLS.
    pub fn is_tls_required(&self) -> bool {
        self.tls_required
    }

    /// Indicates whether the client requires an SSL connection. [default = `false`]
    pub fn tls_required(&mut self, tls_required: bool) -> &mut Self {
        self.tls_required = tls_required;
        self
    }

    /// Set the authorization to use a token
    pub fn token(&mut self, token: String) -> &mut Self {
        self.authorization = Some(Authorization::token(token));
        self
    }

    /// Set the authorization to use a username and password
    pub fn username_password(&mut self, username: String, password: String) -> &mut Self {
        self.authorization = Some(Authorization::username_password(username, password));
        self
    }

    /// Set the authorization
    pub fn set_authorization(&mut self, authorization: Option<Authorization>) -> &mut Self {
        self.authorization = authorization;
        self
    }

    /// Remove all authorization.
    pub fn clear_authorization(&mut self) -> &mut Self {
        self.authorization = None;
        self
    }

    /// Get the optional name of the client.
    pub fn get_name(&self) -> Option<&str> {
        self.name.as_ref().map(String::as_str)
    }

    /// Set the optional client name. [default = `None`]
    pub fn name(&mut self, name: String) -> &mut Self {
        self.name = Some(name);
        self
    }

    /// Remove the optional client name [default = `None`]
    pub fn clear_name(&mut self) -> &mut Self {
        self.name = None;
        self
    }

    /// The implementation language of the client. [always = `"rust"`]
    pub fn get_lang(&self) -> &str {
        &self.language
    }

    /// The version of the client. [always = `"<the crate version>"`]
    pub fn get_version(&self) -> &str {
        &self.version
    }

    /// Optional int. Sending 0 (or absent) indicates client supports original protocol. Sending 1
    /// indicates that the client supports dynamic reconfiguration of cluster topology changes by
    /// asynchronously receiving INFO messages with known servers it can reconnect to. [always =
    /// `1`]
    pub fn get_protocol(&self) -> i32 {
        self.protocol
    }

    /// Return `true` if echo is enabled on the connection.
    pub fn is_echo(&self) -> bool {
        self.echo
    }

    /// Optional boolean. If set to true, the server (version 1.2.0+) will not send originating
    /// messages from this connection to its own subscriptions. Clients should set this to true
    /// only for server supporting this feature, which is when proto in the INFO protocol is set to
    /// at least 1 [default = `false`]
    pub fn echo(&mut self, echo: bool) -> &mut Self {
        self.echo = echo;
        self
    }
}

impl Default for Connect {
    fn default() -> Self {
        Self {
            verbose: false,
            pedantic: false,
            tls_required: false,
            authorization: None,
            name: None,
            language: String::from("rust"),
            version: String::from(util::CLIENT_VERSION),
            protocol: 1,
            echo: false,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

/// https://nats-io.github.io/docs/nats_protocol/nats-protocol.html#okerr
#[derive(Debug, PartialEq)]
pub enum ProtocolError {
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

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::UnknownProtocolOperation => {
                write!(f, "{}", util::UNKNOWN_PROTOCOL_OPERATION)?
            }
            ProtocolError::AttemptedToConnectToRoutePort => {
                write!(f, "{}", util::ATTEMPTED_TO_CONNECT_TO_ROUTE_PORT)?
            }
            ProtocolError::AuthorizationViolation => {
                write!(f, "{}", util::AUTHORIZATION_VIOLATION)?
            }
            ProtocolError::AuthorizationTimeout => write!(f, "{}", util::AUTHORIZATION_TIMEOUT)?,
            ProtocolError::InvalidClientProtocol => write!(f, "{}", util::INVALID_CLIENT_PROTOCOL)?,
            ProtocolError::MaximumControlLineExceeded => {
                write!(f, "{}", util::MAXIMUM_CONTROL_LINE_EXCEEDED)?
            }
            ProtocolError::ParserError => write!(f, "{}", util::PARSER_ERROR)?,
            ProtocolError::SecureConnectionTlsRequired => {
                write!(f, "{}", util::SECURE_CONNECTION_TLS_REQUIRED)?
            }
            ProtocolError::StaleConnection => write!(f, "{}", util::STALE_CONNECTION)?,
            ProtocolError::MaximumConnectionsExceeded => {
                write!(f, "{}", util::MAXIMUM_CONNECTIONS_EXCEEDED)?
            }
            ProtocolError::SlowConsumer => write!(f, "{}", util::SLOW_CONSUMER)?,
            ProtocolError::MaximumPayloadViolation => {
                write!(f, "{}", util::MAXIMUM_PAYLOAD_VIOLATION)?
            }
            ProtocolError::InvalidSubject => write!(f, "{}", util::INVALID_SUBJECT)?,
            ProtocolError::PermissionsViolationForSubscription(subject) => write!(
                f,
                "{} {}",
                util::PERMISSIONS_VIOLATION_FOR_SUBSCRIPTION,
                subject
            )?,
            ProtocolError::PermissionsViolationForPublish(subject) => {
                write!(f, "{} {}", util::PERMISSIONS_VIOLATION_FOR_PUBLISH, subject)?
            }
        }
        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq)]
pub struct Subject {
    tokens: Vec<String>,
    full_wildcard: bool,
}

impl fmt::Display for Subject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Is the whole subject a full wildcard
        if self.tokens.is_empty() {
            write!(f, ">")?;
            return Ok(());
        }
        write!(f, "{}", self.tokens.join("."))?;
        if self.full_wildcard {
            write!(f, ".>")?;
        }
        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq)]
pub struct Msg {
    pub(crate) subject: Subject,
    pub(crate) sid: String,
    pub(crate) reply_to: Option<Subject>,
    pub(crate) payload: Vec<u8>,
}

impl Msg {
    pub fn new(subject: Subject, sid: String, reply_to: Option<Subject>, payload: Vec<u8>) -> Self {
        Self {
            subject,
            sid,
            reply_to,
            payload,
        }
    }

    pub fn payload(self) -> Vec<u8> {
        self.payload
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

// A subscription id can really be any alphanumeric string but within this library we only use u64s
pub type Sid = u64;
static SID: AtomicU64 = AtomicU64::new(0);

pub struct Subscription {
    pub(crate) subject: Subject,
    pub(crate) sid: Sid,
    pub(crate) queue_group: Option<String>,
    pub(crate) unsubscribe_after: Option<u64>,
    pub(crate) tx: MpscSender<Msg>,
}

impl Subscription {
    pub fn new(subject: Subject, queue_group: Option<String>, tx: MpscSender<Msg>) -> Self {
        Self {
            subject,
            sid: SID.fetch_add(1, Ordering::Relaxed),
            queue_group,
            unsubscribe_after: None,
            tx,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

/// Representation of all possible server control lines. A control line is the first line of a
/// message.
#[derive(Debug, PartialEq)]
pub enum ServerControl {
    Info(Info),
    Msg {
        subject: Subject,
        sid: String,
        reply_to: Option<Subject>,
        len: u64,
    },
    Ping,
    Pong,
    Ok,
    Err(ProtocolError),
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
    Err(ProtocolError),
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

///////////////////////////////////////////////////////////////////////////////////////////////////

pub enum ClientControl<'a> {
    Connect(&'a Connect),
    Pub(&'a Subject, Option<&'a Subject>, usize),
    Sub(&'a Subscription),
    Unsub(Sid, Option<u64>),
    Ping,
    Pong,
}

impl fmt::Display for ClientControl<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(connect) => write!(
                f,
                "{} {}{}",
                util::CONNECT_OP_NAME,
                serde_json::to_string(connect).expect("to serialize Connect"),
                util::MESSAGE_TERMINATOR
            ),
            Self::Pub(subject, reply_to, len) => {
                if let Some(reply_to) = reply_to {
                    write!(
                        f,
                        "{} {} {} {}{}",
                        util::PUB_OP_NAME,
                        subject,
                        reply_to,
                        len,
                        util::MESSAGE_TERMINATOR
                    )
                } else {
                    write!(
                        f,
                        "{} {} {}{}",
                        util::PUB_OP_NAME,
                        subject,
                        len,
                        util::MESSAGE_TERMINATOR
                    )
                }
            }
            Self::Sub(subscription) => {
                if let Some(queue_group) = &subscription.queue_group {
                    write!(
                        f,
                        "{} {} {} {}{}",
                        util::SUB_OP_NAME,
                        subscription.subject,
                        queue_group,
                        subscription.sid,
                        util::MESSAGE_TERMINATOR
                    )
                } else {
                    write!(
                        f,
                        "{} {} {}{}",
                        util::SUB_OP_NAME,
                        subscription.subject,
                        subscription.sid,
                        util::MESSAGE_TERMINATOR
                    )
                }
            }
            Self::Unsub(sid, max_msgs) => {
                if let Some(max_msgs) = max_msgs {
                    write!(
                        f,
                        "{} {} {}{}",
                        util::UNSUB_OP_NAME,
                        sid,
                        max_msgs,
                        util::MESSAGE_TERMINATOR
                    )
                } else {
                    write!(
                        f,
                        "{} {}{}",
                        util::UNSUB_OP_NAME,
                        sid,
                        util::MESSAGE_TERMINATOR
                    )
                }
            }
            Self::Ping => write!(f, "{}{}", util::PING_OP_NAME, util::MESSAGE_TERMINATOR),
            Self::Pong => write!(f, "{}{}", util::PONG_OP_NAME, util::MESSAGE_TERMINATOR),
        }
    }
}

impl ClientControl<'_> {
    pub fn to_line(&self) -> String {
        let s = self.to_string();
        trace!("->> {:?}", s);
        s
    }
}

//! Parse server messages according to the NATS protocol described [here](https://nats-io.github.io/docs/nats_protocol/nats-protocol.html).

mod test;

use nom::{branch::alt,
          bytes::complete::{tag,
                            tag_no_case,
                            take,
                            take_until},
          character::complete::{alphanumeric1,
                                digit1,
                                space1},
          combinator::{map_res,
                       opt},
          multi::separated_nonempty_list,
          sequence::delimited,
          Err,
          IResult};
use serde::Deserialize;
use serde_json;

use crate::{constants::MESSAGE_TERMINATOR,
            types::{GnatError,
                    GnatResult,
                    Subject}};

const UNKNOWN_PROTOCOL_OPERATION: &str = "Unknown Protocol Operation";
const ATTEMPTED_TO_CONNECT_TO_ROUTE_PORT: &str = "Attempted To Connect To Route Port";
const AUTHORIZATION_VIOLATION: &str = "Authorization Violation";
const AUTHORIZATION_TIMEOUT: &str = "Authorization Timeout";
const INVALID_CLIENT_PROTOCOL: &str = "Invalid Client Protocol";
const MAXIMUM_CONTROL_LINE_EXCEEDED: &str = "Maximum Control Line Exceeded";
const PARSER_ERROR: &str = "Parser Error";
const SECURE_CONNECTION_TLS_REQUIRED: &str = "Secure Connection - TLS Required";
const STALE_CONNECTION: &str = "Stale Connection";
const MAXIMUM_CONNECTIONS_EXCEEDED: &str = "Maximum Connections Exceeded";
const SLOW_CONSUMER: &str = "Slow Consumer";
const MAXIMUM_PAYLOAD_VIOLATION: &str = "Maximum Payload Violation";
const INVALID_SUBJECT: &str = "Invalid Subject";
const PERMISSIONS_VIOLATION_FOR_SUBSCRIPTION: &str = "Permissions Violation for Subscription to ";
const PERMISSIONS_VIOLATION_FOR_PUBLISH: &str = "Permissions Violation for Publish to ";

/// https://nats-io.github.io/docs/nats_protocol/nats-protocol.html#info  
#[derive(Debug, Deserialize, PartialEq)]
pub struct Info {
    /// The unique identifier of the NATS server
    server_id: String,
    /// The version of the NATS server
    version: String,
    /// The version of golang the NATS server was built with
    go: String,
    /// The IP address used to start the NATS server, by default this will be 0.0.0.0 and can be
    /// configured with -client_advertise host:port
    host: String,
    /// The port number the NATS server is configured to listen on
    port: u16,
    /// Maximum payload size, in bytes, that the server will accept from the client.
    max_payload: u64,
    /// An integer indicating the protocol version of the server. The server version 1.2.0 sets
    /// this to 1 to indicate that it supports the "Echo" feature.
    proto: i32,
    /// An optional unsigned integer (64 bits) representing the internal client identifier in the
    /// server. This can be used to filter client connections in monitoring, correlate with error
    /// logs, etc...
    client_id: Option<u64>,
    #[serde(default)]
    /// If this is set, then the client should try to authenticate upon connect.
    auth_required: bool,
    #[serde(default)]
    /// If this is set, then the client must perform the TLS/1.2 handshake. Note, this used to be
    /// ssl_required and has been updated along with the protocol from SSL to TLS.
    tls_required: bool,
    #[serde(default)]
    /// If this is set, the client must provide a valid certificate during the TLS handshake.
    tls_verify: bool,
    #[serde(default)]
    /// An optional list of server urls that a client can connect to.
    connect_urls: Vec<String>,
}

struct MsgHeader<'a> {
    subject:  Subject,
    sid:      &'a str,
    reply_to: Option<Subject>,
}

/// https://nats-io.github.io/docs/nats_protocol/nats-protocol.html#msg
#[derive(Debug, PartialEq)]
pub struct Msg<'a> {
    subject:  Subject,
    sid:      &'a str,
    reply_to: Option<Subject>,
    payload:  &'a str,
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

/// Representation of all possible server messages
#[derive(Debug, PartialEq)]
pub enum ServerMessage<'a> {
    Info(Info),
    Msg(Msg<'a>),
    Ping,
    Pong,
    Ok,
    Err(ProtocolErr),
}

impl<'a> ServerMessage<'a> {
    /// Create a new server message by parsing an input string
    /// TODO: We dont support non unicode payloads
    pub fn new(input: &'a str) -> Result<Self, Err<(&'a str, nom::error::ErrorKind)>> {
        let (input, m) = alt((info, msg, ping, pong, plus_ok, minus_err))(input)?;
        message_terminator(input)?;
        Ok(m)
    }
}

pub fn parse_subject(input: &str) -> GnatResult<Subject> {
    let (_, s) = subject(input).map_err(|_| GnatError::InvalidSubject(String::from(input)))?;
    Ok(s)
}

// Nom parser combinators

fn message_terminator(input: &str) -> IResult<&str, &str> { tag(MESSAGE_TERMINATOR)(input) }

fn info(input: &str) -> IResult<&str, ServerMessage> {
    let (input, _) = tag_no_case("INFO")(input)?;
    let (input, _) = space1(input)?;
    let (input, info) = map_res(take_until(MESSAGE_TERMINATOR), serde_json::from_str)(input)?;
    Ok((input, ServerMessage::Info(info)))
}

fn token(input: &str) -> IResult<&str, &str> { alt((alphanumeric1, tag("*")))(input) }

fn subject(input: &str) -> IResult<&str, Subject> {
    let (input, tokens) = separated_nonempty_list(tag("."), token)(input)?;
    let (input, full_wildcard) = opt(tag(".>"))(input)?;
    Ok((input, Subject::new_unchecked(&tokens, full_wildcard.is_some())))
}

fn reply_to(input: &str) -> IResult<&str, Subject> {
    let (input, subject) = subject(input)?;
    let (input, _) = space1(input)?;
    Ok((input, subject))
}

fn msg_header(input: &str) -> IResult<&str, MsgHeader> {
    let (input, _) = tag_no_case("MSG")(input)?;
    let (input, _) = space1(input)?;
    let (input, subject) = subject(input)?;
    let (input, _) = space1(input)?;
    let (input, sid) = alphanumeric1(input)?;
    let (input, _) = space1(input)?;
    let (input, reply_to) = opt(reply_to)(input)?;
    Ok((input,
        MsgHeader { subject,
                    sid,
                    reply_to }))
}

fn payload(input: &str) -> IResult<&str, &str> {
    let (input, len) = map_res(digit1, |s: &str| s.parse::<u64>())(input)?;
    let (input, _) = message_terminator(input)?;
    let (input, payload) = take(len)(input)?;
    Ok((input, payload))
}

fn msg(input: &str) -> IResult<&str, ServerMessage> {
    let (input, header) = msg_header(input)?;
    let (input, payload) = payload(input)?;
    Ok((input,
        ServerMessage::Msg(Msg { subject: header.subject,
                                 sid: header.sid,
                                 reply_to: header.reply_to,
                                 payload })))
}

fn ping(input: &str) -> IResult<&str, ServerMessage> {
    let (input, _) = tag_no_case("PING")(input)?;
    Ok((input, ServerMessage::Ping))
}

fn pong(input: &str) -> IResult<&str, ServerMessage> {
    let (input, _) = tag_no_case("PONG")(input)?;
    Ok((input, ServerMessage::Pong))
}

fn plus_ok(input: &str) -> IResult<&str, ServerMessage> {
    let (input, _) = tag_no_case("+OK")(input)?;
    Ok((input, ServerMessage::Ok))
}

fn unknown_protocol_operation(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(UNKNOWN_PROTOCOL_OPERATION)(input)?;
    Ok((input, ProtocolErr::UnknownProtocolOperation))
}

fn attempted_to_connect_to_route_port(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(ATTEMPTED_TO_CONNECT_TO_ROUTE_PORT)(input)?;
    Ok((input, ProtocolErr::AttemptedToConnectToRoutePort))
}

fn authorization_violation(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(AUTHORIZATION_VIOLATION)(input)?;
    Ok((input, ProtocolErr::AuthorizationViolation))
}

fn authorization_timeout(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(AUTHORIZATION_TIMEOUT)(input)?;
    Ok((input, ProtocolErr::AuthorizationTimeout))
}

fn invalid_client_protocol(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(INVALID_CLIENT_PROTOCOL)(input)?;
    Ok((input, ProtocolErr::InvalidClientProtocol))
}

fn maximum_control_line_exceeded(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(MAXIMUM_CONTROL_LINE_EXCEEDED)(input)?;
    Ok((input, ProtocolErr::MaximumControlLineExceeded))
}

fn parser_error(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(PARSER_ERROR)(input)?;
    Ok((input, ProtocolErr::ParserError))
}

fn secure_connection_tls_required(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(SECURE_CONNECTION_TLS_REQUIRED)(input)?;
    Ok((input, ProtocolErr::SecureConnectionTlsRequired))
}

fn stale_connection(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(STALE_CONNECTION)(input)?;
    Ok((input, ProtocolErr::StaleConnection))
}

fn maximum_connections_exceeded(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(MAXIMUM_CONNECTIONS_EXCEEDED)(input)?;
    Ok((input, ProtocolErr::MaximumConnectionsExceeded))
}

fn slow_consumer(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(SLOW_CONSUMER)(input)?;
    Ok((input, ProtocolErr::SlowConsumer))
}

fn maximum_payload_violation(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(MAXIMUM_PAYLOAD_VIOLATION)(input)?;
    Ok((input, ProtocolErr::MaximumPayloadViolation))
}

fn invalid_subject(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(INVALID_SUBJECT)(input)?;
    Ok((input, ProtocolErr::InvalidSubject))
}

fn permissions_violation_for_subscription(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(PERMISSIONS_VIOLATION_FOR_SUBSCRIPTION)(input)?;
    let (input, subject) = subject(input)?;
    Ok((input, ProtocolErr::PermissionsViolationForSubscription(subject)))
}

fn permissions_violation_for_publish(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(PERMISSIONS_VIOLATION_FOR_PUBLISH)(input)?;
    let (input, subject) = subject(input)?;
    Ok((input, ProtocolErr::PermissionsViolationForPublish(subject)))
}

fn parse_protocol_err(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, protocol_err) = delimited(tag("'"),
                                          alt((unknown_protocol_operation,
                                               attempted_to_connect_to_route_port,
                                               authorization_violation,
                                               authorization_timeout,
                                               invalid_client_protocol,
                                               maximum_control_line_exceeded,
                                               parser_error,
                                               secure_connection_tls_required,
                                               stale_connection,
                                               maximum_connections_exceeded,
                                               slow_consumer,
                                               maximum_payload_violation,
                                               invalid_subject,
                                               permissions_violation_for_subscription,
                                               permissions_violation_for_publish)),
                                          tag("'"))(input)?;
    Ok((input, protocol_err))
}

fn minus_err(input: &str) -> IResult<&str, ServerMessage> {
    let (input, _) = tag_no_case("-Err")(input)?;
    let (input, _) = space1(input)?;
    let (input, e) = parse_protocol_err(input)?;
    Ok((input, ServerMessage::Err(e)))
}

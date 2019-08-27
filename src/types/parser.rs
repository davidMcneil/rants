//! Parse server messages according to the NATS protocol described [here](https://nats-io.github.io/docs/nats_protocol/nats-protocol.html).

#[cfg(test)]
mod tests;

use nom::{branch::alt,
          bytes::complete::{tag,
                            tag_no_case,
                            take_until},
          character::complete::{alphanumeric1,
                                digit1,
                                space1},
          combinator::{map_res,
                       opt},
          multi::separated_nonempty_list,
          sequence::delimited,
          IResult};
use serde_json;
use std::str::FromStr;

use crate::{constants,
            types::{GnatError,
                    ProtocolErr,
                    ServerControl,
                    Subject}};

impl FromStr for ServerControl {
    type Err = GnatError;

    fn from_str(s: &str) -> Result<Self, GnatError> {
        let (_, control) = control(s).map_err(|_| GnatError::FailedToParse(String::from(s)))?;
        Ok(control)
    }
}

impl FromStr for Subject {
    type Err = GnatError;

    fn from_str(s: &str) -> Result<Self, GnatError> {
        let (_, subject) = subject(s).map_err(|_| GnatError::InvalidSubject(String::from(s)))?;
        Ok(subject)
    }
}

// Nom parser combinators

fn control(input: &str) -> IResult<&str, ServerControl> {
    let (input, control) = alt((info, msg, ping, pong, plus_ok, minus_err))(input)?;
    tag(constants::MESSAGE_TERMINATOR)(input)?;
    Ok((input, control))
}

fn info(input: &str) -> IResult<&str, ServerControl> {
    let (input, _) = tag_no_case(constants::INFO_OP_NAME)(input)?;
    let (input, _) = space1(input)?;
    let (input, info) = map_res(take_until(constants::MESSAGE_TERMINATOR),
                                serde_json::from_str)(input)?;
    Ok((input, ServerControl::Info(info)))
}

fn token(input: &str) -> IResult<&str, &str> {
    alt((alphanumeric1, tag(constants::SUBJECT_WILDCARD)))(input)
}

fn full_wildcard(input: &str) -> IResult<&str, &str> {
    let (input, _) = tag(constants::SUBJECT_TOKEN_DELIMITER)(input)?;
    tag(constants::SUBJECT_FULL_WILDCARD)(input)
}

fn subject(input: &str) -> IResult<&str, Subject> {
    let (input, tokens) =
        separated_nonempty_list(tag(constants::SUBJECT_TOKEN_DELIMITER), token)(input)?;
    let (input, full_wildcard) = opt(full_wildcard)(input)?;
    let tokens = tokens.iter().map(|s| String::from(*s)).collect();
    let subject = Subject { tokens,
                            full_wildcard: full_wildcard.is_some() };
    Ok((input, subject))
}

fn reply_to(input: &str) -> IResult<&str, Subject> {
    let (input, subject) = subject(input)?;
    let (input, _) = space1(input)?;
    Ok((input, subject))
}

fn msg(input: &str) -> IResult<&str, ServerControl> {
    let (input, _) = tag_no_case(constants::MSG_OP_NAME)(input)?;
    let (input, _) = space1(input)?;
    let (input, subject) = subject(input)?;
    let (input, _) = space1(input)?;
    let (input, sid) = alphanumeric1(input)?;
    let (input, _) = space1(input)?;
    let (input, reply_to) = opt(reply_to)(input)?;
    let (input, len) = map_res(digit1, |s: &str| s.parse::<u64>())(input)?;
    Ok((input,
        ServerControl::Msg { subject,
                             sid: String::from(sid),
                             reply_to,
                             len }))
}

fn ping(input: &str) -> IResult<&str, ServerControl> {
    let (input, _) = tag_no_case(constants::PING_OP_NAME)(input)?;
    Ok((input, ServerControl::Ping))
}

fn pong(input: &str) -> IResult<&str, ServerControl> {
    let (input, _) = tag_no_case(constants::PONG_OP_NAME)(input)?;
    Ok((input, ServerControl::Pong))
}

fn plus_ok(input: &str) -> IResult<&str, ServerControl> {
    let (input, _) = tag_no_case(constants::OK_OP_NAME)(input)?;
    Ok((input, ServerControl::Ok))
}

fn unknown_protocol_operation(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(constants::UNKNOWN_PROTOCOL_OPERATION)(input)?;
    Ok((input, ProtocolErr::UnknownProtocolOperation))
}

fn attempted_to_connect_to_route_port(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(constants::ATTEMPTED_TO_CONNECT_TO_ROUTE_PORT)(input)?;
    Ok((input, ProtocolErr::AttemptedToConnectToRoutePort))
}

fn authorization_violation(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(constants::AUTHORIZATION_VIOLATION)(input)?;
    Ok((input, ProtocolErr::AuthorizationViolation))
}

fn authorization_timeout(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(constants::AUTHORIZATION_TIMEOUT)(input)?;
    Ok((input, ProtocolErr::AuthorizationTimeout))
}

fn invalid_client_protocol(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(constants::INVALID_CLIENT_PROTOCOL)(input)?;
    Ok((input, ProtocolErr::InvalidClientProtocol))
}

fn maximum_control_line_exceeded(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(constants::MAXIMUM_CONTROL_LINE_EXCEEDED)(input)?;
    Ok((input, ProtocolErr::MaximumControlLineExceeded))
}

fn parser_error(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(constants::PARSER_ERROR)(input)?;
    Ok((input, ProtocolErr::ParserError))
}

fn secure_connection_tls_required(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(constants::SECURE_CONNECTION_TLS_REQUIRED)(input)?;
    Ok((input, ProtocolErr::SecureConnectionTlsRequired))
}

fn stale_connection(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(constants::STALE_CONNECTION)(input)?;
    Ok((input, ProtocolErr::StaleConnection))
}

fn maximum_connections_exceeded(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(constants::MAXIMUM_CONNECTIONS_EXCEEDED)(input)?;
    Ok((input, ProtocolErr::MaximumConnectionsExceeded))
}

fn slow_consumer(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(constants::SLOW_CONSUMER)(input)?;
    Ok((input, ProtocolErr::SlowConsumer))
}

fn maximum_payload_violation(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(constants::MAXIMUM_PAYLOAD_VIOLATION)(input)?;
    Ok((input, ProtocolErr::MaximumPayloadViolation))
}

fn invalid_subject(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(constants::INVALID_SUBJECT)(input)?;
    Ok((input, ProtocolErr::InvalidSubject))
}

fn permissions_violation_for_subscription(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(constants::PERMISSIONS_VIOLATION_FOR_SUBSCRIPTION)(input)?;
    let (input, subject) = subject(input)?;
    Ok((input, ProtocolErr::PermissionsViolationForSubscription(subject)))
}

fn permissions_violation_for_publish(input: &str) -> IResult<&str, ProtocolErr> {
    let (input, _) = tag(constants::PERMISSIONS_VIOLATION_FOR_PUBLISH)(input)?;
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

fn minus_err(input: &str) -> IResult<&str, ServerControl> {
    let (input, _) = tag_no_case(constants::ERR_OP_NAME)(input)?;
    let (input, _) = space1(input)?;
    let (input, e) = parse_protocol_err(input)?;
    Ok((input, ServerControl::Err(e)))
}

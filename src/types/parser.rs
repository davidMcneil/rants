//! Parse server messages according to the NATS protocol described [here](https://nats-io.github.io/docs/nats_protocol/nats-protocol.html).

#[cfg(test)]
mod tests;

use nom::{
    branch::alt,
    bytes::complete::{is_not, tag, tag_no_case, take_until},
    character::complete::{digit1, space1},
    combinator::{all_consuming, cut, map_res, opt},
    multi::separated_list1,
    sequence::delimited,
    IResult,
};
use std::str::FromStr;

use crate::{
    types::{
        error::{Error, Result},
        Info, ProtocolError, ServerControl, Sid, Subject,
    },
    util,
};

impl FromStr for ServerControl {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let (_, control) = all_consuming(control_line)(s)
            .map_err(|_| Error::InvalidServerControl(String::from(s)))?;
        Ok(control)
    }
}

impl FromStr for Subject {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let (_, subject) =
            all_consuming(subject)(s).map_err(|_| Error::InvalidSubject(String::from(s)))?;
        Ok(subject)
    }
}

// Nom parser combinators

fn control_line(input: &str) -> IResult<&str, ServerControl> {
    let (input, control) = alt((info, msg, ping, pong, plus_ok, minus_err))(input)?;
    let (input, _) = tag(util::MESSAGE_TERMINATOR)(input)?;
    Ok((input, control))
}

///////////////////////////////////////////////////////////////////////////////////////////////////

fn info_after_op_name(input: &str) -> IResult<&str, Info> {
    let (input, _) = space1(input)?;
    map_res(take_until(util::MESSAGE_TERMINATOR), serde_json::from_str)(input)
}

fn info(input: &str) -> IResult<&str, ServerControl> {
    let (input, _) = tag_no_case(util::INFO_OP_NAME)(input)?;
    let (input, info) = cut(info_after_op_name)(input)?;
    Ok((input, ServerControl::Info(info)))
}

///////////////////////////////////////////////////////////////////////////////////////////////////

fn token(input: &str) -> IResult<&str, &str> {
    alt((
        is_not(util::SUBJECT_TOKEN_INVALID_CHARACTERS),
        tag(util::SUBJECT_WILDCARD),
    ))(input)
}

fn full_wildcard(input: &str) -> IResult<&str, &str> {
    tag(util::SUBJECT_FULL_WILDCARD)(input)
}

fn trailing_full_wildcard(input: &str) -> IResult<&str, &str> {
    let (input, _) = tag(util::SUBJECT_TOKEN_DELIMITER)(input)?;
    full_wildcard(input)
}

fn full_wildcard_subject(input: &str) -> IResult<&str, Subject> {
    let (input, _) = full_wildcard(input)?;
    let subject = Subject {
        tokens: Vec::new(),
        full_wildcard: true,
    };
    Ok((input, subject))
}

fn not_full_wildcard_subject(input: &str) -> IResult<&str, Subject> {
    let (input, tokens) = separated_list1(tag(util::SUBJECT_TOKEN_DELIMITER), token)(input)?;
    let (input, full_wildcard) = opt(trailing_full_wildcard)(input)?;
    let tokens = tokens.iter().map(|s| String::from(*s)).collect();
    let subject = Subject {
        tokens,
        full_wildcard: full_wildcard.is_some(),
    };
    Ok((input, subject))
}

fn subject(input: &str) -> IResult<&str, Subject> {
    alt((full_wildcard_subject, not_full_wildcard_subject))(input)
}

///////////////////////////////////////////////////////////////////////////////////////////////////

fn reply_to(input: &str) -> IResult<&str, Subject> {
    let (input, subject) = subject(input)?;
    let (input, _) = space1(input)?;
    Ok((input, subject))
}

fn msg_after_op_name(input: &str) -> IResult<&str, ServerControl> {
    let (input, _) = space1(input)?;
    let (input, subject) = subject(input)?;
    let (input, _) = space1(input)?;
    // Technically, the subscription ID can be any ASCII string, but this client only uses
    // subscription IDs of type `Sid`.
    let (input, sid) = map_res(digit1, |s: &str| s.parse::<Sid>())(input)?;
    let (input, _) = space1(input)?;
    let (input, reply_to) = opt(reply_to)(input)?;
    let (input, len) = map_res(digit1, |s: &str| s.parse::<u64>())(input)?;
    Ok((
        input,
        ServerControl::Msg {
            subject,
            sid,
            reply_to,
            len,
        },
    ))
}

fn msg(input: &str) -> IResult<&str, ServerControl> {
    let (input, _) = tag_no_case(util::MSG_OP_NAME)(input)?;
    cut(msg_after_op_name)(input)
}

///////////////////////////////////////////////////////////////////////////////////////////////////

fn ping(input: &str) -> IResult<&str, ServerControl> {
    let (input, _) = tag_no_case(util::PING_OP_NAME)(input)?;
    Ok((input, ServerControl::Ping))
}

///////////////////////////////////////////////////////////////////////////////////////////////////

fn pong(input: &str) -> IResult<&str, ServerControl> {
    let (input, _) = tag_no_case(util::PONG_OP_NAME)(input)?;
    Ok((input, ServerControl::Pong))
}

///////////////////////////////////////////////////////////////////////////////////////////////////

fn plus_ok(input: &str) -> IResult<&str, ServerControl> {
    let (input, _) = tag_no_case(util::OK_OP_NAME)(input)?;
    Ok((input, ServerControl::Ok))
}

///////////////////////////////////////////////////////////////////////////////////////////////////

fn unknown_protocol_operation(input: &str) -> IResult<&str, ProtocolError> {
    let (input, _) = tag(util::UNKNOWN_PROTOCOL_OPERATION)(input)?;
    Ok((input, ProtocolError::UnknownProtocolOperation))
}

fn attempted_to_connect_to_route_port(input: &str) -> IResult<&str, ProtocolError> {
    let (input, _) = tag(util::ATTEMPTED_TO_CONNECT_TO_ROUTE_PORT)(input)?;
    Ok((input, ProtocolError::AttemptedToConnectToRoutePort))
}

fn authorization_violation(input: &str) -> IResult<&str, ProtocolError> {
    let (input, _) = tag(util::AUTHORIZATION_VIOLATION)(input)?;
    Ok((input, ProtocolError::AuthorizationViolation))
}

fn authorization_timeout(input: &str) -> IResult<&str, ProtocolError> {
    let (input, _) = tag(util::AUTHORIZATION_TIMEOUT)(input)?;
    Ok((input, ProtocolError::AuthorizationTimeout))
}

fn invalid_client_protocol(input: &str) -> IResult<&str, ProtocolError> {
    let (input, _) = tag(util::INVALID_CLIENT_PROTOCOL)(input)?;
    Ok((input, ProtocolError::InvalidClientProtocol))
}

fn maximum_control_line_exceeded(input: &str) -> IResult<&str, ProtocolError> {
    let (input, _) = tag(util::MAXIMUM_CONTROL_LINE_EXCEEDED)(input)?;
    Ok((input, ProtocolError::MaximumControlLineExceeded))
}

fn parser_error(input: &str) -> IResult<&str, ProtocolError> {
    let (input, _) = tag(util::PARSER_ERROR)(input)?;
    Ok((input, ProtocolError::ParserError))
}

fn secure_connection_tls_required(input: &str) -> IResult<&str, ProtocolError> {
    let (input, _) = tag(util::SECURE_CONNECTION_TLS_REQUIRED)(input)?;
    Ok((input, ProtocolError::SecureConnectionTlsRequired))
}

fn stale_connection(input: &str) -> IResult<&str, ProtocolError> {
    let (input, _) = tag(util::STALE_CONNECTION)(input)?;
    Ok((input, ProtocolError::StaleConnection))
}

fn maximum_connections_exceeded(input: &str) -> IResult<&str, ProtocolError> {
    let (input, _) = tag(util::MAXIMUM_CONNECTIONS_EXCEEDED)(input)?;
    Ok((input, ProtocolError::MaximumConnectionsExceeded))
}

fn slow_consumer(input: &str) -> IResult<&str, ProtocolError> {
    let (input, _) = tag(util::SLOW_CONSUMER)(input)?;
    Ok((input, ProtocolError::SlowConsumer))
}

fn maximum_payload_violation(input: &str) -> IResult<&str, ProtocolError> {
    let (input, _) = tag(util::MAXIMUM_PAYLOAD_VIOLATION)(input)?;
    Ok((input, ProtocolError::MaximumPayloadViolation))
}

fn invalid_subject(input: &str) -> IResult<&str, ProtocolError> {
    let (input, _) = tag(util::INVALID_SUBJECT)(input)?;
    Ok((input, ProtocolError::InvalidSubject))
}

fn permissions_violation_for_subscription(input: &str) -> IResult<&str, ProtocolError> {
    let (input, _) = tag(util::PERMISSIONS_VIOLATION_FOR_SUBSCRIPTION)(input)?;
    let (input, _) = space1(input)?;
    let (input, subject) = subject(input)?;
    Ok((
        input,
        ProtocolError::PermissionsViolationForSubscription(subject),
    ))
}

fn permissions_violation_for_publish(input: &str) -> IResult<&str, ProtocolError> {
    let (input, _) = tag(util::PERMISSIONS_VIOLATION_FOR_PUBLISH)(input)?;
    let (input, _) = space1(input)?;
    let (input, subject) = subject(input)?;
    Ok((
        input,
        ProtocolError::PermissionsViolationForPublish(subject),
    ))
}

fn parse_protocol_err(input: &str) -> IResult<&str, ProtocolError> {
    let (input, protocol_err) = delimited(
        tag("'"),
        alt((
            unknown_protocol_operation,
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
            permissions_violation_for_publish,
        )),
        tag("'"),
    )(input)?;
    Ok((input, protocol_err))
}

fn minus_err_after_op_name(input: &str) -> IResult<&str, ServerControl> {
    let (input, _) = space1(input)?;
    let (input, e) = parse_protocol_err(input)?;
    Ok((input, ServerControl::Err(e)))
}

fn minus_err(input: &str) -> IResult<&str, ServerControl> {
    let (input, _) = tag_no_case(util::ERR_OP_NAME)(input)?;
    cut(minus_err_after_op_name)(input)
}

///////////////////////////////////////////////////////////////////////////////////////////////////

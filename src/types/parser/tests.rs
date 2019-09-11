use super::*;
use crate::{constants::*, types::*};

#[test]
fn unit_info() {
    assert_eq!(
        ServerControl::from_str(
            "INFO {\"server_id\":\"Zk0GQ3JBSrg3oyxCRRlE09\",\"\
             version\":\"1.2.0\",\"proto\":1,\"go\":\"go1.10.3\",\"\
             host\":\"0.0.0.0\",\"port\":4222,\"max_payload\":\
             1048576,\"client_id\":2392}\r\n"
        )
        .unwrap(),
        ServerControl::Info(Info {
            server_id: String::from("Zk0GQ3JBSrg3oyxCRRlE09"),
            version: String::from("1.2.0"),
            go: String::from("go1.10.3"),
            host: String::from("0.0.0.0"),
            port: 4222,
            max_payload: 1048576,
            proto: 1,
            client_id: Some(2392),
            auth_required: false,
            tls_required: false,
            tls_verify: false,
            connect_urls: Vec::new(),
        })
    );
}

#[test]
fn unit_msg() {
    let s = Subject::from_str("FOO.BAR").unwrap();
    assert_eq!(
        control("MSG FOO.BAR 9 1032\r\n").unwrap().1,
        ServerControl::Msg {
            subject: s,
            sid: String::from("9"),
            reply_to: None,
            len: 1032,
        }
    );

    let s = Subject::from_str("FOO.BAR").unwrap();
    let s2 = Subject::from_str("INBOX.34").unwrap();
    assert_eq!(
        ServerControl::from_str("MSG FOO.BAR 9 INBOX.34 11\r\n").unwrap(),
        ServerControl::Msg {
            subject: s,
            sid: String::from("9"),
            reply_to: Some(s2),
            len: 11,
        }
    );
    assert!(ServerControl::from_str("MSG FOO.BAR 9 INBOX.34 abc\r\n").is_err());
}

#[test]
fn unit_ping() {
    assert_eq!(
        ServerControl::from_str("PiNG\r\n").unwrap(),
        ServerControl::Ping
    );
}

#[test]
fn unit_pong() {
    assert_eq!(
        ServerControl::from_str("poNG\r\n").unwrap(),
        ServerControl::Pong
    );
}

#[test]
fn unit_ok() {
    assert_eq!(
        ServerControl::from_str("+ok\r\n").unwrap(),
        ServerControl::Ok
    );
    assert_eq!(
        ServerControl::from_str("+OK\r\n").unwrap(),
        ServerControl::Ok
    );
}

#[test]
fn unit_err() {
    let m = format!("-err '{}'\r\n", UNKNOWN_PROTOCOL_OPERATION);
    assert_eq!(
        ServerControl::from_str(&m).unwrap(),
        ServerControl::Err(ProtocolError::UnknownProtocolOperation)
    );

    let m = format!("-err '{}'\r\n", UNKNOWN_PROTOCOL_OPERATION);
    assert_eq!(
        ServerControl::from_str(&m).unwrap(),
        ServerControl::Err(ProtocolError::UnknownProtocolOperation)
    );
    let m = format!("-err '{}'\r\n", ATTEMPTED_TO_CONNECT_TO_ROUTE_PORT);
    assert_eq!(
        ServerControl::from_str(&m).unwrap(),
        ServerControl::Err(ProtocolError::AttemptedToConnectToRoutePort)
    );
    let m = format!("-err '{}'\r\n", AUTHORIZATION_VIOLATION);
    assert_eq!(
        ServerControl::from_str(&m).unwrap(),
        ServerControl::Err(ProtocolError::AuthorizationViolation)
    );
    let m = format!("-err '{}'\r\n", AUTHORIZATION_TIMEOUT);
    assert_eq!(
        ServerControl::from_str(&m).unwrap(),
        ServerControl::Err(ProtocolError::AuthorizationTimeout)
    );
    let m = format!("-err '{}'\r\n", INVALID_CLIENT_PROTOCOL);
    assert_eq!(
        ServerControl::from_str(&m).unwrap(),
        ServerControl::Err(ProtocolError::InvalidClientProtocol)
    );
    let m = format!("-err '{}'\r\n", MAXIMUM_CONTROL_LINE_EXCEEDED);
    assert_eq!(
        ServerControl::from_str(&m).unwrap(),
        ServerControl::Err(ProtocolError::MaximumControlLineExceeded)
    );
    let m = format!("-err '{}'\r\n", PARSER_ERROR);
    assert_eq!(
        ServerControl::from_str(&m).unwrap(),
        ServerControl::Err(ProtocolError::ParserError)
    );
    let m = format!("-err '{}'\r\n", SECURE_CONNECTION_TLS_REQUIRED);
    assert_eq!(
        ServerControl::from_str(&m).unwrap(),
        ServerControl::Err(ProtocolError::SecureConnectionTlsRequired)
    );
    let m = format!("-err '{}'\r\n", STALE_CONNECTION);
    assert_eq!(
        ServerControl::from_str(&m).unwrap(),
        ServerControl::Err(ProtocolError::StaleConnection)
    );
    let m = format!("-err '{}'\r\n", MAXIMUM_CONNECTIONS_EXCEEDED);
    assert_eq!(
        ServerControl::from_str(&m).unwrap(),
        ServerControl::Err(ProtocolError::MaximumConnectionsExceeded)
    );
    let m = format!("-err '{}'\r\n", SLOW_CONSUMER);
    assert_eq!(
        ServerControl::from_str(&m).unwrap(),
        ServerControl::Err(ProtocolError::SlowConsumer)
    );
    let m = format!("-err '{}'\r\n", MAXIMUM_PAYLOAD_VIOLATION);
    assert_eq!(
        ServerControl::from_str(&m).unwrap(),
        ServerControl::Err(ProtocolError::MaximumPayloadViolation)
    );
    let m = format!("-err '{}'\r\n", INVALID_SUBJECT);
    assert_eq!(
        ServerControl::from_str(&m).unwrap(),
        ServerControl::Err(ProtocolError::InvalidSubject)
    );
    let s = Subject::from_str("test.x.*.y.>").unwrap();

    let m = format!(
        "-err '{} test.x.*.y.>'\r\n",
        PERMISSIONS_VIOLATION_FOR_SUBSCRIPTION
    );
    assert_eq!(
        ServerControl::from_str(&m).unwrap(),
        ServerControl::Err(ProtocolError::PermissionsViolationForSubscription(
            s.clone()
        ))
    );
    let m = format!(
        "-err '{} test.x.*.y.>'\r\n",
        PERMISSIONS_VIOLATION_FOR_PUBLISH
    );
    assert_eq!(
        ServerControl::from_str(&m).unwrap(),
        ServerControl::Err(ProtocolError::PermissionsViolationForPublish(s))
    );
}

#[test]
fn unit_fails() {
    assert!(ServerControl::from_str("+ok").is_err());
    assert!(ServerControl::from_str("+err 'test'\r\n").is_err());
    assert!(ServerControl::from_str("some_random_text\r\n").is_err());
}

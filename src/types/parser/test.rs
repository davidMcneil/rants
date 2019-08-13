#![cfg(test)]

use super::*;
use serde_json;

#[test]
fn test_info() {
    assert_eq!(ServerMessage::new("INFO {\"server_id\":\"Zk0GQ3JBSrg3oyxCRRlE09\",\"\
                                        version\":\"1.2.0\",\"proto\":1,\"go\":\"go1.10.3\",\"\
                                        host\":\"0.0.0.0\",\"port\":4222,\"max_payload\":\
                                        1048576,\"client_id\":2392}\r\n").unwrap(),
               ServerMessage::Info(Info { server_id:     String::from("Zk0GQ3JBSrg3oyxCRRlE09"),
                                          version:       String::from("1.2.0"),
                                          go:            String::from("go1.10.3"),
                                          host:          String::from("0.0.0.0"),
                                          port:          4222,
                                          max_payload:   1048576,
                                          proto:         1,
                                          client_id:     Some(2392),
                                          auth_required: false,
                                          tls_required:  false,
                                          tls_verify:    false,
                                          connect_urls:  Vec::new(), }));
}

#[test]
fn test_msg() {
    let s = Subject::new_unchecked(&vec!["FOO", "BAR"], false);
    assert_eq!(ServerMessage::new("MSG FOO.BAR 9 11\r\nHello World\r\n").unwrap(),
               ServerMessage::Msg(Msg { subject:  s,
                                        sid:      "9",
                                        reply_to: None,
                                        payload:  "Hello World", }));

    let s = Subject::new_unchecked(&vec!["FOO", "BAR"], false);
    let s2 = Subject::new_unchecked(&vec!["INBOX", "34"], false);
    assert_eq!(ServerMessage::new("MSG FOO.BAR 9 INBOX.34 11\r\nHello World\r\n").unwrap(),
               ServerMessage::Msg(Msg { subject:  s,
                                        sid:      "9",
                                        reply_to: Some(s2),
                                        payload:  "Hello World", }));
}

#[test]
fn test_ping() {
    assert_eq!(ServerMessage::new("PiNG\r\n").unwrap(), ServerMessage::Ping);
}

#[test]
fn test_pong() {
    assert_eq!(ServerMessage::new("poNG\r\n").unwrap(), ServerMessage::Pong);
}

#[test]
fn test_ok() {
    assert_eq!(ServerMessage::new("+ok\r\n").unwrap(), ServerMessage::Ok);
    assert_eq!(ServerMessage::new("+OK\r\n").unwrap(), ServerMessage::Ok);
}

#[test]
fn test_err() {
    let m = format!("-err '{}'\r\n", UNKNOWN_PROTOCOL_OPERATION);
    assert_eq!(ServerMessage::new(&m).unwrap(),
               ServerMessage::Err(ProtocolErr::UnknownProtocolOperation));

    let s = Subject::new_unchecked(&vec!["test", "x", "*", "y"], true);
    let m = format!("-err '{}test.x.*.y.>'\r\n",
                    PERMISSIONS_VIOLATION_FOR_SUBSCRIPTION);
    assert_eq!(ServerMessage::new(&m).unwrap(),
               ServerMessage::Err(ProtocolErr::PermissionsViolationForSubscription(s)));
}

#[test]
fn test_fails() {
    assert!(ServerMessage::new("+ok").is_err());
    assert!(ServerMessage::new("+err 'test'\r\n").is_err());
    assert!(ServerMessage::new("jibberish\r\n").is_err());
}

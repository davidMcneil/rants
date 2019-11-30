use std::time::Duration;

pub const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const NATS_DEFAULT_PORT: u16 = 4222;
pub const DEFAULT_TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_CONNECT_SERIES_ATTEMPTS_BEFORE_COOL_DOWN: u64 = 3;
pub const DEFAULT_CONNECT_DELAY: Duration = Duration::from_secs(0);
pub const DEFAULT_CONNECT_SERIES_DELAY: Duration = Duration::from_secs(5);
pub const DEFAULT_COOL_DOWN: Duration = Duration::from_secs(60);
pub const INBOX_PREFIX: &str = "_INBOX";

// Address special characters
pub const NATS_NETWORK_SCHEME: &str = "nats";
pub const NETWORK_SCHEME_SEPARATOR: &str = "://";
pub const AUTHORIZATION_SEPARATOR: &str = "@";
pub const USERNAME_PASSWORD_SEPARATOR: &str = ":";
pub const DOMAIN_PORT_SEPARATOR: &str = ":";

// Subject special characters
pub const SUBJECT_TOKEN_DELIMITER: &str = ".";
pub const SUBJECT_WILDCARD: &str = "*";
pub const SUBJECT_FULL_WILDCARD: &str = ">";
pub const SUBJECT_TOKEN_INVALID_CHARACTERS: &str = " \t.*>";

// Protocol op names
pub const INFO_OP_NAME: &str = "INFO";
pub const CONNECT_OP_NAME: &str = "CONNECT";
pub const PUB_OP_NAME: &str = "PUB";
pub const SUB_OP_NAME: &str = "SUB";
pub const UNSUB_OP_NAME: &str = "UNSUB";
pub const MSG_OP_NAME: &str = "MSG";
pub const PING_OP_NAME: &str = "PING";
pub const PONG_OP_NAME: &str = "PONG";
pub const OK_OP_NAME: &str = "+OK";
pub const ERR_OP_NAME: &str = "-ERR";

pub const MESSAGE_TERMINATOR: &str = "\r\n";

// Error strings
pub const UNKNOWN_PROTOCOL_OPERATION: &str = "Unknown Protocol Operation";
pub const ATTEMPTED_TO_CONNECT_TO_ROUTE_PORT: &str = "Attempted To Connect To Route Port";
pub const AUTHORIZATION_VIOLATION: &str = "Authorization Violation";
pub const AUTHORIZATION_TIMEOUT: &str = "Authorization Timeout";
pub const INVALID_CLIENT_PROTOCOL: &str = "Invalid Client Protocol";
pub const MAXIMUM_CONTROL_LINE_EXCEEDED: &str = "Maximum Control Line Exceeded";
pub const PARSER_ERROR: &str = "Parser Error";
pub const SECURE_CONNECTION_TLS_REQUIRED: &str = "Secure Connection - TLS Required";
pub const STALE_CONNECTION: &str = "Stale Connection";
pub const MAXIMUM_CONNECTIONS_EXCEEDED: &str = "Maximum Connections Exceeded";
pub const SLOW_CONSUMER: &str = "Slow Consumer";
pub const MAXIMUM_PAYLOAD_VIOLATION: &str = "Maximum Payload Violation";
pub const INVALID_SUBJECT: &str = "Invalid Subject";
pub const PERMISSIONS_VIOLATION_FOR_SUBSCRIPTION: &str =
    "Permissions Violation for Subscription to";
pub const PERMISSIONS_VIOLATION_FOR_PUBLISH: &str = "Permissions Violation for Publish to";

pub fn split_after<'a>(s: &'a str, pat: &str) -> (&'a str, Option<&'a str>) {
    let mut splitter = s.splitn(2, pat);
    let first = splitter.next().expect("always at least one split");
    let rest = splitter.next();
    (first, rest)
}

pub fn split_before<'a>(s: &'a str, pat: &str) -> (Option<&'a str>, &'a str) {
    match split_after(s, pat) {
        (first, None) => (None, first),
        (first, Some(rest)) => (Some(first), rest),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_after() {
        assert_eq!(
            split_after("first:second:third", ":"),
            ("first", Some("second:third"))
        );
        assert_eq!(split_after("first:", ":"), ("first", Some("")));
        assert_eq!(split_after(":second", ":"), ("", Some("second")));
        assert_eq!(split_after("none", ":"), ("none", None));
        assert_eq!(split_after("", ":"), ("", None));
        assert_eq!(split_after("test", "test"), ("", Some("")));
    }

    #[test]
    fn test_split_before() {
        assert_eq!(
            split_before("first:second:third", ":"),
            (Some("first"), "second:third")
        );
        assert_eq!(split_before("first:", ":"), (Some("first"), ""));
        assert_eq!(split_before(":second", ":"), (Some(""), "second"));
        assert_eq!(split_before("none", ":"), (None, "none"));
        assert_eq!(split_before("", ":"), (None, ""));
        assert_eq!(split_before("test", "test"), (Some(""), ""));
    }
}

pub const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");

// Subject special characters
pub const SUBJECT_TOKEN_DELIMITER: &str = ".";
pub const SUBJECT_WILDCARD: &str = "*";
pub const SUBJECT_FULL_WILDCARD: &str = ">";

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

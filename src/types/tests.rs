use super::*;
use serde_json;

#[test]
fn unit_connect_serialization() {
    let connect = Connect {
        verbose: true,
        pedantic: false,
        tls_required: true,
        authorization: Some(Authorization::token(String::from("auth_token"))),
        name: Some(String::from("client_name")),
        language: String::from("rust"),
        version: String::from("0.1.0"),
        protocol: 1,
        echo: true,
    };
    let serialized = serde_json::to_string(&connect).expect("to serialize Connect");
    assert_eq!(
        &serialized,
        "{\"verbose\":true,\"pedantic\":false,\"tls_required\":true,\"auth_token\":\"\
         auth_token\",\"name\":\"client_name\",\"lang\":\"rust\",\"version\":\"0.1.0\",\"\
         protocol\":1,\"echo\":true}"
    );
    let connect = Connect {
        verbose: true,
        pedantic: false,
        tls_required: true,
        authorization: Some(Authorization::username_password(
            String::from("username"),
            String::from("password"),
        )),
        name: Some(String::from("client_name")),
        language: String::from("rust"),
        version: String::from("0.1.0"),
        protocol: 1,
        echo: true,
    };
    let serialized = serde_json::to_string(&connect).expect("to serialize Connect");
    assert_eq!(
        &serialized,
        "{\"verbose\":true,\"pedantic\":false,\"tls_required\":true,\"user\":\"username\",\
         \"pass\":\"password\",\"name\":\"client_name\",\"lang\":\"rust\",\"version\":\"0.\
         1.0\",\"protocol\":1,\"echo\":true}"
    );
}

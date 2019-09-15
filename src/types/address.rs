use serde::{de, Deserialize, Deserializer};
use std::{
    convert::Infallible,
    fmt,
    net::{AddrParseError, SocketAddr},
    str::FromStr,
};

use super::Authorization;
use crate::{types::error::Error, util};

/// An address used to connect to a server
///
/// An `Address` consists of an [`Authorization`](enum.Authorization.html) and a `SocketAddr`.
///
/// The string representation of an `Address` can take the following forms:
/// * `nats://<username>:<password>@<ip_address>:<port>`
/// * `nats://<token>@<ip_address>:<port>`
///
/// The only required part of the address string is the `<ip_address>`. This makes the simplest
/// address solely an IP address (eg `127.0.0.1`). If no port is specified the default, port `4222`,
///  is used. Some example addresses include:
///
/// **Note**: When a client attempts to connect to the server at an address, the authorization
/// specified by the address will always override the client's `Connect` default
/// [`authorization`](struct.Connect.html#method.authorization).
///
/// # Example
///  ```
/// use rants::Address;
///
/// let address = "nats://username:password@127.0.0.1:8080".parse::<Address>();
/// assert!(address.is_ok());
/// let address = "auth_token@1.2.3.4".parse::<Address>();
/// assert!(address.is_ok());
/// let address = "nats://auth_token@1.2.3.4:5780".parse::<Address>();
/// assert!(address.is_ok());
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct Address {
    address: SocketAddr,
    authorization: Option<Authorization>,
}

impl Address {
    /// Create a new `Address`
    pub fn new(address: SocketAddr, authorization: Option<Authorization>) -> Self {
        Self {
            address,
            authorization,
        }
    }

    /// Get the `Address`'s `SocketAddr`
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Get the `Address`'s [`Authorization`](enum.Authorization.html)
    pub fn authorization(&self) -> Option<&Authorization> {
        self.authorization.as_ref()
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(authorization) = &self.authorization {
            write!(f, "{}{}", authorization, util::AUTHORIZATION_SEPARATOR)?;
        }
        write!(f, "{}", self.address)?;
        Ok(())
    }
}

fn split_once<'a>(s: &'a str, pat: &str) -> (Option<&'a str>, &'a str) {
    let mut splitter = s.splitn(2, pat);
    let first = splitter.next().expect("always at least one split");
    let rest = splitter.next();
    match (first, rest) {
        (first, None) => (None, first),
        (first, Some(rest)) => (Some(first), rest),
    }
}

impl FromStr for Authorization {
    type Err = Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match split_once(s, util::USERNAME_PASSWORD_SEPARATOR) {
            (None, token) => Ok(Authorization::token(String::from(token))),
            (Some(username), password) => Ok(Authorization::username_password(
                String::from(username),
                String::from(password),
            )),
        }
    }
}

fn parse_ip_and_maybe_port(s: &str) -> std::result::Result<SocketAddr, AddrParseError> {
    // If the string to parse does not contain a ':', it does not have a port
    Ok(if s.contains(util::IP_ADDR_PORT_SEPARATOR) {
        s.parse()?
    } else {
        let ip = s.parse()?;
        SocketAddr::new(ip, util::NATS_DEFAULT_PORT)
    })
}

impl FromStr for Address {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Parse the protocol
        let (maybe_protocol, s) = split_once(s, util::NETWORK_SCHEME_SEPARATOR);
        if let Some(protocol) = maybe_protocol {
            if protocol != util::NATS_NETWORK_SCHEME {
                return Err(Error::InvalidNetworkScheme(String::from(protocol)));
            }
        }
        // Split apart the authorization and the address
        let (maybe_authorization, ip_and_maybe_port) = split_once(s, util::AUTHORIZATION_SEPARATOR);
        let address = parse_ip_and_maybe_port(ip_and_maybe_port)
            .map_err(|_| Error::InvalidAddress(String::from(s)))?;
        let authorization =
            maybe_authorization.map(|s| s.parse().expect("parsing authorization is infallible"));
        Ok(Address {
            address,
            authorization,
        })
    }
}

impl<'de> Deserialize<'de> for Address {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        FromStr::from_str(&s).map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_split_once() {
        assert_eq!(
            split_once("first:second:third", ":"),
            (Some("first"), "second:third")
        );
        assert_eq!(split_once("first:", ":"), (Some("first"), ""));
        assert_eq!(split_once(":second", ":"), (Some(""), "second"));
        assert_eq!(split_once("none", ":"), (None, "none"));
        assert_eq!(split_once("", ":"), (None, ""));
        assert_eq!(split_once("test", "test"), (Some(""), ""));
    }

    #[test]
    fn parse_address() {
        let a = "nats://127.0.0.1:90".parse::<Address>().unwrap();
        assert_eq!(&a.to_string(), "127.0.0.1:90");
        assert!(a.authorization.is_none());
        assert_eq!(a.address.port(), 90);
        let a = "127.0.0.1".parse::<Address>().unwrap();
        assert!(a.authorization.is_none());
        assert_eq!(a.address.port(), 4222);
        let a = "nats://127.0.0.1".parse::<Address>().unwrap();
        assert!(a.authorization.is_none());
        assert_eq!(a.address.port(), 4222);
        let a = "username:password@127.0.0.1:1023"
            .parse::<Address>()
            .unwrap();
        assert_eq!(&a.to_string(), "username:password@127.0.0.1:1023");
        assert_eq!(
            a.authorization.unwrap(),
            Authorization::username_password(String::from("username"), String::from("password"))
        );
        assert_eq!(a.address.port(), 1023);
        let a = "nats://token@127.0.0.1".parse::<Address>().unwrap();
        assert_eq!(&a.to_string(), "token@127.0.0.1:4222");
        assert_eq!(
            a.authorization.unwrap(),
            Authorization::token(String::from("token"))
        );
        assert_eq!(a.address.port(), 4222);
        let a = "username:@127.0.0.1:80".parse::<Address>().unwrap();
        assert_eq!(
            a.authorization.unwrap(),
            Authorization::username_password(String::from("username"), String::from(""))
        );
        assert_eq!(a.address.port(), 80);
        let a = "@127.0.0.1:80".parse::<Address>().unwrap();
        assert_eq!(
            a.authorization.unwrap(),
            Authorization::token(String::from(""))
        );
        let a = "0.0.0.0:56".parse::<Address>().unwrap();
        assert_eq!(a.address.port(), 56);

        let a = "http://127.0.0.1:90".parse::<Address>();
        assert!(a.is_err());
        let a = "token@".parse::<Address>();
        assert!(a.is_err());
        let a = "nats:://this_is_bad".parse::<Address>();
        assert!(a.is_err());
    }
}

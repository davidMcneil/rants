use serde::{de, Deserialize, Deserializer};
use std::{convert::Infallible, fmt, net::SocketAddr, str::FromStr};

use super::Authorization;
use crate::{types::error::Error, util};

#[derive(Clone, Debug, PartialEq)]
pub struct Address {
    address: SocketAddr,
    pub(crate) authorization: Option<Authorization>,
}

impl Address {
    pub fn new(address: SocketAddr, authorization: Option<Authorization>) -> Self {
        Self {
            address,
            authorization,
        }
    }

    pub fn address(&self) -> SocketAddr {
        self.address
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
        // Parse the address. If it does not contain a ':', it does not have a port
        let address = if ip_and_maybe_port.contains(util::IP_ADDR_PORT_SEPARATOR) {
            ip_and_maybe_port.parse()?
        } else {
            let ip = ip_and_maybe_port.parse()?;
            SocketAddr::new(ip, util::NATS_DEFAULT_PORT)
        };
        let authorization = maybe_authorization.map(|s| s.parse().expect("parse authorization"));
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

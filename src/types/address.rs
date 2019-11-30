use serde::{de, Deserialize, Deserializer};
use std::{fmt, str::FromStr};

use super::Authorization;
use crate::{types::error::Error, util};

/// An address used to connect to a NATS server
///
/// An `Address` consists of a domain, a port number, and an optional
/// [`Authorization`](enum.Authorization.html). The domain can simply be the string representation
/// of an IP address or a true domain name that will be resolved through DNS.
///
/// The string representation of an `Address` can take the following forms:
/// * `nats://<username>:<password>@<domain>:<port>`
/// * `nats://<token>@<domain>:<port>`
///
/// The only required part of the address string is the `<domain>`. This makes the simplest
/// address solely an IP address (eg `127.0.0.1`). If no port is specified the default, port `4222`,
///  is used.
///
/// **Note:** When a client attempts to connect to the server at an address, the authorization
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
    domain: String,
    port: u16,
    authorization: Option<Authorization>,
}

impl Address {
    /// Create a new `Address`
    pub fn new(domain: &str, port: u16, authorization: Option<Authorization>) -> Self {
        Self {
            domain: String::from(domain),
            port,
            authorization,
        }
    }

    /// Get the `Address`'s domain
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// Get the `Address`'s port
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Get the `Address`'s domain and port
    pub fn address(&self) -> (&str, u16) {
        (&self.domain, self.port())
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
        write!(f, "{}:{}", self.domain, self.port)?;
        Ok(())
    }
}

impl FromStr for Address {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Parse the protocol
        let (maybe_protocol, rest) = util::split_before(s, util::NETWORK_SCHEME_SEPARATOR);
        if let Some(protocol) = maybe_protocol {
            if protocol != util::NATS_NETWORK_SCHEME {
                return Err(Error::InvalidNetworkScheme(String::from(protocol)));
            }
        }

        if rest.is_empty() {
            return Err(Error::InvalidAddress(String::from(s)));
        }

        // Parse the authorization
        let (maybe_authorization, rest) = util::split_before(rest, util::AUTHORIZATION_SEPARATOR);
        let authorization =
            maybe_authorization.map(|s| s.parse().expect("parsing authorization is infallible"));

        if rest.is_empty() {
            return Err(Error::InvalidAddress(String::from(s)));
        }

        // Parse the domain and port
        let (domain, maybe_port) = util::split_after(rest, util::DOMAIN_PORT_SEPARATOR);
        if domain.is_empty() {
            return Err(Error::InvalidAddress(String::from(s)));
        }
        let port = if let Some(maybe_port) = maybe_port {
            maybe_port
                .parse()
                .map_err(|_| Error::InvalidAddress(String::from(s)))?
        } else {
            util::NATS_DEFAULT_PORT
        };

        Ok(Address::new(domain, port, authorization))
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
    fn parse_address() {
        let a = "nats://127.0.0.1:90".parse::<Address>().unwrap();
        assert_eq!(a.domain(), "127.0.0.1");
        assert_eq!(a.port(), 90);
        assert!(a.authorization().is_none());
        assert_eq!(&a.to_string(), "127.0.0.1:90");

        let a = "127.0.0.1".parse::<Address>().unwrap();
        assert_eq!(a.domain(), "127.0.0.1");
        assert_eq!(a.port(), 4222);
        assert!(a.authorization().is_none());
        assert_eq!(&a.to_string(), "127.0.0.1:4222");

        let a = "nats://127.0.0.1".parse::<Address>().unwrap();
        assert_eq!(a.domain(), "127.0.0.1");
        assert_eq!(a.port(), 4222);
        assert!(a.authorization().is_none());
        assert_eq!(&a.to_string(), "127.0.0.1:4222");

        let a = "username:password@127.0.0.1:1023"
            .parse::<Address>()
            .unwrap();
        assert_eq!(a.domain(), "127.0.0.1");
        assert_eq!(a.port(), 1023);
        assert_eq!(
            *a.authorization().unwrap(),
            Authorization::username_password(String::from("username"), String::from("password"))
        );
        assert_eq!(&a.to_string(), "username:password@127.0.0.1:1023");

        let a = "nats://token@my-machine".parse::<Address>().unwrap();
        assert_eq!(a.domain(), "my-machine");
        assert_eq!(a.port(), 4222);
        assert_eq!(
            *a.authorization().unwrap(),
            Authorization::token(String::from("token"))
        );
        assert_eq!(&a.to_string(), "token@my-machine:4222");

        let a = "username:@some.domain.com:80".parse::<Address>().unwrap();
        assert_eq!(a.domain(), "some.domain.com");
        assert_eq!(a.port(), 80);
        assert_eq!(
            *a.authorization().unwrap(),
            Authorization::username_password(String::from("username"), String::from(""))
        );
        assert_eq!(&a.to_string(), "username:@some.domain.com:80");

        let a = "@another.domain:80".parse::<Address>().unwrap();
        assert_eq!(a.domain(), "another.domain");
        assert_eq!(a.port(), 80);
        assert_eq!(
            *a.authorization().unwrap(),
            Authorization::token(String::from(""))
        );
        assert_eq!(&a.to_string(), "@another.domain:80");

        let a = "0.0.0.0:56".parse::<Address>().unwrap();
        assert_eq!(a.domain(), "0.0.0.0");
        assert_eq!(a.port(), 56);
        assert!(a.authorization().is_none());
        assert_eq!(&a.to_string(), "0.0.0.0:56");

        let a = "nats://this_is_a_domain".parse::<Address>().unwrap();
        assert_eq!(a.domain(), "this_is_a_domain");
        assert_eq!(a.port(), 4222);
        assert!(a.authorization().is_none());
        assert_eq!(&a.to_string(), "this_is_a_domain:4222");

        let a = "http://127.0.0.1:90".parse::<Address>();
        assert!(a.is_err());

        let a = "token@".parse::<Address>();
        assert!(a.is_err());

        let a = "".parse::<Address>();
        assert!(a.is_err());

        let a = ":1234".parse::<Address>();
        assert!(a.is_err());

        let a = "domain:".parse::<Address>();
        assert!(a.is_err());

        let a = "domain:100000".parse::<Address>();
        assert!(a.is_err());

        let a = "domain:bad".parse::<Address>();
        assert!(a.is_err());
    }
}

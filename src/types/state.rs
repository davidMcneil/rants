use std::fmt;
use tokio::io::WriteHalf;

use crate::{tls_or_tcp_stream::TlsOrTcpStream, types::Address};

// Internal state representation. Identical to `ClientState` but tracks internal implementation
// details such as `WriteHalf`.
//
// I would rather use an `Encoder` (instead of the raw `WriteHalf`) that operates
// on a `ClientControl` enum. Unfortunately, when I attempted this, it was painful to make the
// payload passed to publish be of type `&[u8]` instead of `Vec<u8>` without a clone. So for
// now, the writer operates at the tcp layer writing raw bytes while the reader uses a custom
// codec.
pub enum ConnectionState {
    Connected(Address, WriteHalf<TlsOrTcpStream>),
    Connecting(Address),
    Disconnected,
    // If we are coming from a connected state, we have a `WriteHalf` to close
    Disconnecting(Option<WriteHalf<TlsOrTcpStream>>),
}

#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
pub enum StateTransition {
    ToConnecting(Address),
    ToConnected(WriteHalf<TlsOrTcpStream>),
    ToDisconnecting,
    ToDisconnected,
}

// Used to return data from a state transition
pub enum StateTransitionResult {
    None,
    Writer(WriteHalf<TlsOrTcpStream>),
}

/// Client states
///
/// ```text
///                                      State Diagram
///                            +--------+-----------------------------------------+
///                            |        |                                         |
///                            |        v                                         v
/// +----------------+     +---+--------+---+     +----------------+     +--------+-------+
/// |                |     |                |     |                |     |                |
/// |  Disconnected  +---->+   Connecting   +---->+   Connected    +---->+ Disconnecting  |
/// |                |     |                |     |                |     |                |
/// +-------+--------+     +----------------+     +--------+-------+     +--------+-------+
///         ^                                              |                      |
///         |                                              |                      |
///         +----------------------------------------------+----------------------+
/// ```
#[derive(Clone, Debug)]
pub enum ClientState {
    /// The client is connected to an address.
    Connected(Address),
    /// The client is connecting to an address.
    Connecting(Address),
    /// The client is disconnected.
    Disconnected,
    /// The client has been instructed to disconnect by calling
    /// [`disconnect`](struct.Client.html#method.disconnect).
    Disconnecting,
}

impl ClientState {
    /// Is the state `Connected`?
    pub fn is_connected(&self) -> bool {
        if let Self::Connected(_) = self {
            return true;
        }
        false
    }

    /// Is the state `Connecting`?
    pub fn is_connecting(&self) -> bool {
        if let Self::Connecting(_) = self {
            return true;
        }
        false
    }

    /// Is the state `Disconnected`?
    pub fn is_disconnected(&self) -> bool {
        if let Self::Disconnected = self {
            return true;
        }
        false
    }

    /// Is the state `Disconnecting`?
    pub fn is_disconnecting(&self) -> bool {
        if let Self::Disconnecting = self {
            return true;
        }
        false
    }
}

impl From<&ConnectionState> for ClientState {
    fn from(s: &ConnectionState) -> Self {
        match s {
            ConnectionState::Connected(address, _) => ClientState::Connected(address.clone()),
            ConnectionState::Connecting(address) => ClientState::Connecting(address.clone()),
            ConnectionState::Disconnected => ClientState::Disconnected,
            ConnectionState::Disconnecting(_) => ClientState::Disconnecting,
        }
    }
}

impl fmt::Display for ClientState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientState::Connected(address) => write!(f, "Connected({})", address)?,
            ClientState::Connecting(address) => write!(f, "Connecting({})", address)?,
            ClientState::Disconnected => write!(f, "Disconnected")?,
            ClientState::Disconnecting => write!(f, "Disconnecting")?,
        }
        Ok(())
    }
}

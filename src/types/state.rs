use std::net::SocketAddr;
use tokio::net::tcp::split::TcpStreamWriteHalf;

// Internal state representation. Identical to `ClientState` but tracks internal implementation
// details such as `TcpStreamWriteHalf`.
//
// I would rather use an `Encoder` (instead of the raw `TcpStreamWriteHalf`) that operates
// on a `ClientControl` enum. Unfortunately, when I attempted this, it was painful to make the
// payload passed to publish be of type `&[u8]` instead of `Vec<u8>` without a clone. So for
// now, the writer operates at the tcp layer writing raw bytes while the reader uses a custom
// codec.
pub enum ConnectionState {
    Connected(SocketAddr, TcpStreamWriteHalf),
    Connecting(SocketAddr),
    Disconnected,
    // If we are coming from a connected state, we have a `TcpStreamWriteHalf` to close
    Disconnecting(Option<TcpStreamWriteHalf>),
}

#[derive(Debug)]
pub enum StateTransition {
    ToConnecting(SocketAddr),
    ToConnected(TcpStreamWriteHalf),
    ToDisconnecting,
    ToDisconnected,
}

// Used to return data from a state transition
pub enum StateTransitionResult {
    None,
    Writer(TcpStreamWriteHalf),
}

/// Possible client states.
///
/// ```text
///                                              Client State Diagram
///         +-------------------------------+--------------------------------------------------------------+
///         |                               |                                                              |
///         V                               |                                                              v
/// +-------+--------+             +--------+-------+            +----------------+               +--------+-------+
/// |                |             |                |            |                |               |                |
/// |  Disconnected  +------------>+   Connecting   +----------->+   Connected    +-------------->+ Disconnecting  |
/// |                |             |                |            |                |               |                |
/// +-------+--------+             +----------------+            +--------+-------+               +--------+-------+
///         ^                                                             |                                |
///         |                                                             |                                |
///         +-------------------------------------------------------------+--------------------------------+
/// ```
#[derive(Clone, Debug)]
pub enum ClientState {
    Connected(SocketAddr),
    Connecting(SocketAddr),
    Disconnected,
    Disconnecting,
}

impl ClientState {
    pub fn is_connected(&self) -> bool {
        if let Self::Connected(_) = self {
            return true;
        }
        false
    }

    pub fn is_connecting(&self) -> bool {
        if let Self::Connecting(_) = self {
            return true;
        }
        false
    }

    pub fn is_disconnected(&self) -> bool {
        if let Self::Disconnected = self {
            return true;
        }
        false
    }

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

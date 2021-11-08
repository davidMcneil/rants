//! An async [NATS](https://nats.io/) client library for the Rust programming language.
//!
//! The client aims to be an ergonomic, yet thin, wrapper over the NATS client protocol. The
//! easiest way to learn to use the client is by reading the
//! [NATS client protocol documentation](https://docs.nats.io/nats-protocol/nats-protocol).
//! The main entry point into the library's API is the [`Client`](struct.Client.html) struct.
//!
//! TLS support is powered by the [`native-tls` crate](https://github.com/sfackler/rust-native-tls).
//!
//! # Example
//!  ```rust no_run
//! use futures::stream::StreamExt;
//! use rants::Client;
//!
//!# #[tokio::main]
//!# async fn main
//!# // keep this to quell `needless_doctest_main` warning
//!# () {
//! // A NATS server must be running on `127.0.0.1:4222`
//! let address = "127.0.0.1:4222".parse().unwrap();
//! let client = Client::new(vec![address]);
//!
//! // Configure the client to receive messages even if it sent the message
//! client.connect_mut().await.echo(true);
//!
//! // Connect to the server
//! client.connect().await;
//!
//! // Create a new subject called "test"
//! let subject = "test".parse().unwrap();
//!
//! // Subscribe to the "test" subject
//! let (_, mut subscription) = client.subscribe(&subject, 1024).await.unwrap();
//!
//! // Publish a message to the "test" subject
//! client
//!     .publish(&subject, b"This is a message!")
//!     .await
//!     .unwrap();
//!
//! // Read a message from the subscription
//! let message = subscription.recv().await.unwrap();
//! let message = String::from_utf8(message.into_payload()).unwrap();
//! println!("Received '{}'", message);
//!
//! // Disconnect from the server
//! client.disconnect().await;
//!# }
//! ```

mod codec;
#[cfg(test)]
mod tests;
mod tls_or_tcp_stream;
mod types;
mod util;

#[cfg(feature = "tls")]
use crate::types::tls::TlsConfig;
use futures::{
    future::{self, Either, FutureExt},
    lock::{Mutex, MutexGuard},
    pin_mut,
    stream::StreamExt,
};
use log::{debug, error, info, trace, warn};
use owning_ref::{OwningRef, OwningRefMut};
use rand::seq::SliceRandom;
use std::{
    collections::HashMap, io::ErrorKind, mem, result::Result as StdResult, sync::Arc,
    time::Duration,
};
use tokio::{
    io::{self, AsyncWriteExt, ReadHalf, WriteHalf},
    net::TcpStream,
    sync::{
        mpsc, oneshot,
        watch::{self, Sender as WatchSender},
    },
    time::{self, error::Elapsed},
};
use tokio_stream::wrappers::WatchStream;
use tokio_util::codec::FramedRead;
use uuid::Uuid;

use crate::{
    codec::Codec,
    error::{Error, Result},
    tls_or_tcp_stream::TlsOrTcpStream,
    types::{
        ClientControl, ConnectionState, ServerMessage, StableMutexGuard, StateTransition,
        StateTransitionResult,
    },
};

#[cfg(feature = "native-tls")]
pub use native_tls_crate as native_tls;
#[cfg(feature = "rustls-tls")]
pub use rustls;
pub use tokio::sync::{
    mpsc::Receiver as MpscReceiver, mpsc::Sender as MpscSender, watch::Receiver as WatchReceiver,
};

pub use crate::types::{
    error, Address, Authorization, ClientRef, ClientRefMut, ClientState, Connect, Info, Msg,
    ProtocolError, Sid, Subject, SubjectBuilder, Subscription,
};

const TCP_SOCKET_DISCONNECTED_MESSAGE: &str = "TCP socket was disconnected";

/// The type of a [`Client`](struct.Client.html)'s [`delay_generator_mut`](struct.Client.html#method.delay_generator_mut)
///
/// # Arguments
/// * `client` - A reference to the `Client` that is trying to connect
/// * `attempts` - The number of previous connect attempts
/// * `addresses` - The number of addresses the `Client` is aware of
pub type DelayGenerator = Box<dyn Fn(&Client, u64, u64) -> Duration + Send>;

/// Generate a [`Client`](struct.Client.html)'s [`delay_generator_mut`](struct.Client.html#method.delay_generator_mut)
///
/// A `Client`s `delay_generator_mut` provides complete flexibility in determining delays between
/// connect attempts. A reasonable `delay_generator_mut` can be produced by defining the few values
/// required by this function.
///
/// # Arguments
///
/// * `connect_series_attempts_before_cool_down` - A `connect_series` is an attempt to try all
/// addresses: those explicitly [specified](struct.Client.html#method.addresses_mut) and
/// those received in an `INFO` message's [`connect_urls`](struct.Info.html#method.connect_urls).
/// This variable defines how many `connect_series` to try before delaying for the `cool_down`
/// duration
/// * `connect_delay` - The delay between each connect attempt.
/// * `connect_series_delay` - The delay after a complete `connect_series`.
/// * `cool_down_delay` - The delay after completing `connect_series_attempts_before_cool_down`
/// attempts.
pub fn generate_delay_generator(
    connect_series_attempts_before_cool_down: u64,
    connect_delay: Duration,
    connect_series_delay: Duration,
    cool_down: Duration,
) -> DelayGenerator {
    Box::new(move |_: &Client, connect_attempts: u64, addresses: u64| {
        if connect_attempts % (addresses * connect_series_attempts_before_cool_down) == 0 {
            trace!("Using cool down delay {}s", cool_down.as_secs_f32());
            cool_down
        } else if connect_attempts % addresses == 0 {
            trace!(
                "Using connect series delay {}s",
                connect_series_delay.as_secs_f32()
            );
            connect_series_delay
        } else {
            trace!("Using connect delay {}s", connect_delay.as_secs_f32());
            connect_delay
        }
    })
}

/// The entry point to the [NATS client protocol](https://nats-io.github.io/docs/nats_protocol/nats-protocol.html)
#[derive(Clone)]
pub struct Client {
    // For now, wrapper the entire underlying client in a single Mutex. There are probably more
    // performant locking schemes to explore.
    sync: Arc<Mutex<SyncClient>>,
}

impl Client {
    /// Create a new `Client` with a default [`Connect`](struct.Connect.html)
    ///
    /// # Arguments
    ///
    /// * `addresses` - the list of addresses to try and establish a connection to a server
    pub fn new(addresses: Vec<Address>) -> Self {
        Self::with_connect(addresses, Connect::new())
    }

    /// Create a new `Client` with the provided [`Connect`](struct.Connect.html)
    ///
    /// # Arguments
    ///
    /// * `addresses` - the list of addresses to try and establish a connection to a server
    pub fn with_connect(addresses: Vec<Address>, connect: Connect) -> Self {
        Self {
            sync: Arc::new(Mutex::new(SyncClient::with_connect(addresses, connect))),
        }
    }

    /// Get the current state of the `Client`
    pub async fn state(&self) -> ClientState {
        self.lock().await.state()
    }

    /// Get a watch stream of `Client` state transitions
    pub async fn state_stream(&self) -> WatchReceiver<ClientState> {
        self.lock().await.state_stream()
    }

    /// Get a the most recent [`Info`](struct.Info.html) sent from the server
    pub async fn info(&self) -> Info {
        self.lock().await.info()
    }

    /// Get a mutable reference to this `Client`'s [`Connect`](struct.Connect.html)
    pub async fn connect_mut(&self) -> ClientRefMut<'_, Connect> {
        ClientRefMut(
            OwningRefMut::new(StableMutexGuard(self.lock().await)).map_mut(|c| c.connect_mut()),
        )
    }

    /// Get a mutable reference to the list of addresses used to try and establish a connection to a
    /// server.
    pub async fn addresses_mut(&self) -> ClientRefMut<'_, [Address]> {
        ClientRefMut(
            OwningRefMut::new(StableMutexGuard(self.lock().await)).map_mut(|c| c.addresses_mut()),
        )
    }

    /// Get the configured TCP connect timeout. [default = `10s`]
    ///
    /// This is the timeout of a single connect attempt. It is not the timeout of the
    /// [`connect`](struct.Client.html#method.connect) future which has no internal timeout.
    pub async fn tcp_connect_timeout(&self) -> Duration {
        self.lock().await.tcp_connect_timeout()
    }

    /// Set the TCP connect timeout.
    pub async fn set_tcp_connect_timeout(&self, tcp_connect_timeout: Duration) -> &Self {
        self.lock()
            .await
            .set_tcp_connect_timeout(tcp_connect_timeout);
        self
    }

    /// Get the [`DelayGenerator`](type.DelayGenerator.html)
    ///
    /// The default generator is generated with [`generate_delay_generator`](fn.generate_delay_generator.html)
    /// with the following parameters:
    ///
    /// * `connect_series_attempts_before_cool_down` = `3`
    /// * `connect_delay` = `0s`
    /// * `connect_series_delay` = `5s`
    /// * `cool_down` = `60s`
    pub async fn delay_generator_mut(&self) -> ClientRefMut<'_, DelayGenerator> {
        ClientRefMut(
            OwningRefMut::new(StableMutexGuard(self.lock().await))
                .map_mut(|c| c.delay_generator_mut()),
        )
    }

    /// Set the `TlsConfig` to use when TLS is [required](struct.Info.html#method.tls_required).
    ///
    /// This method is only available when the `native-tls` or `rustls-tls` feature is enabled.
    /// When `native-tls` is enabled `tls_config` is of type
    // [`native_tls::TlsConnector`](https://docs.rs/native-tls/*/native_tls/struct.TlsConnector.html).
    /// When `rustls-tls` is enabled `tls_config` is of type
    /// [`rustls::ClientConfig`](https://docs.rs/rustls/*/rustls/struct.ClientConfig.html).
    #[cfg(feature = "tls")]
    pub async fn set_tls_config(&mut self, tls_config: TlsConfig) -> &mut Self {
        self.lock().await.set_tls_config(tls_config);
        self
    }

    /// Specify the domain to be used for both Server Name Indication (SNI) and certificate
    /// hostname validation. If this option is not explicitly set the client will use the hostname
    /// of the address it is currently connecting to.
    ///
    /// When `rustls-tls` is enabled `tls_domain` will try to be parsed as a
    /// [`webpki::DNSNameRef`](https://docs.rs/webpki/*/webpki/struct.DNSNameRef.html).
    #[cfg(feature = "tls")]
    pub async fn set_tls_domain(&mut self, tls_domain: String) -> &mut Self {
        self.lock().await.set_tls_domain(tls_domain);
        self
    }

    /// Get a list of all currently subscribed subscription IDs
    pub async fn sids(&self) -> Vec<Sid> {
        self.lock()
            .await
            .subscriptions()
            .map(|(sid, _)| *sid)
            .collect()
    }

    /// Return a reference to a [`Subscription`](struct.Subscription.html) if the client is aware
    /// of the specified subscription ID
    pub async fn subscription(&self, sid: Sid) -> Option<ClientRef<'_, Subscription>> {
        let client = self.lock().await;
        if client.subscriptions.contains_key(&sid) {
            Some(ClientRef(
                OwningRef::new(StableMutexGuard(client))
                    .map(|c| c.subscriptions.get(&sid).unwrap()),
            ))
        } else {
            None
        }
    }

    /// Send a `CONNECT` message to the server using the configured [`Connect`](struct.Connect.html).
    ///
    /// **Note:** [`connect`](struct.Client.html#method.connect) automatically sends a `CONNECT`
    /// message. This is only needed in the case that you want to change the connection parameters
    /// after already establishing a connection.
    pub async fn send_connect(&self) -> Result<()> {
        let mut client = self.lock().await;
        client.send_connect().await
    }

    /// Connect to a NATS server
    ///
    /// This will randomly shuffle a list consisting of all explicitly specified
    /// [addresses](struct.Client.html#method.addresses_mut) and
    /// addresses received in an `INFO` message's [`connect_urls`](struct.Info.html#method.connect_urls).
    ///  A randomized list of addresses is used to avoid a
    /// [thundering herd](https://nats-io.github.io/docs/developer/reconnect/random.html).
    /// The client will continuously try to connect to each address in this list. The timeout of
    /// each connect attempt is specified by the
    /// [`tcp_connect_timeout`](struct.Client.html#method.tcp_connect_timeout). The delay between
    /// each connect attempt is specified by the
    /// [`delay_generator_mut`](struct.Client.html#method.delay_generator_mut).
    ///
    /// When this future resolves, we are guaranteed to have entered the
    /// [`Connected`](enum.ClientState.html#variant.Connected) state.
    /// **Unless**, [`disconnect`](struct.Client.html#method.disconnect) was called.
    ///
    /// Should the client become disconnected for any reason, other than calling
    /// [`disconnect`](struct.Client.html#method.disconnect), the client will continuously try
    /// to reconnect. Upon a successful reconnect, the client will automatically subscribe to all
    /// subscriptions.
    pub async fn connect(&self) {
        SyncClient::connect(Self::clone(self)).await
    }

    /// Disconnect from the NATS server
    ///
    /// When this future resolves, we are guaranteed to have entered the
    /// [`Disconnected`](enum.ClientState.html#variant.Disconnected) state.
    ///
    /// **Note:** `Client` does not `disconnect` when it is `Drop`ped. In order to
    /// avoid leaking futures, you must explicitly call `disconnect`.
    pub async fn disconnect(&self) {
        SyncClient::disconnect(Arc::clone(&self.sync)).await
    }

    /// Convenience wrapper around [`publish_with_optional_reply`](struct.Client.html#method.publish_with_optional_reply)
    pub async fn publish(&self, subject: &Subject, payload: &[u8]) -> Result<()> {
        self.publish_with_optional_reply(subject, None, payload)
            .await
    }

    /// Convenience wrapper around [`publish_with_optional_reply`](struct.Client.html#method.publish_with_optional_reply)
    pub async fn publish_with_reply(
        &self,
        subject: &Subject,
        reply_to: &Subject,
        payload: &[u8],
    ) -> Result<()> {
        self.publish_with_optional_reply(subject, Some(reply_to), payload)
            .await
    }

    /// [`PUB`](https://nats-io.github.io/docs/nats_protocol/nats-protocol.html#pub)lish a message
    ///
    /// # Arguments
    /// * `subject` - The subject to publish to
    /// * `reply_to` - The optional reply to subject
    /// * `payload` - The actual contents of the message
    pub async fn publish_with_optional_reply(
        &self,
        subject: &Subject,
        reply_to: Option<&Subject>,
        payload: &[u8],
    ) -> Result<()> {
        let mut client = self.lock().await;
        client
            .publish_with_optional_reply(subject, reply_to, payload)
            .await
    }

    /// Implements the [request-reply pattern](https://nats-io.github.io/docs/developer/concepts/reqreply.html)
    pub async fn request(&self, subject: &Subject, payload: &[u8]) -> Result<Msg> {
        SyncClient::request_with_timeout(Arc::clone(&self.sync), subject, payload, None).await
    }

    /// Implements the [request-reply pattern](https://nats-io.github.io/docs/developer/concepts/reqreply.html) with a timeout
    pub async fn request_with_timeout(
        &self,
        subject: &Subject,
        payload: &[u8],
        duration: Duration,
    ) -> Result<Msg> {
        SyncClient::request_with_timeout(Arc::clone(&self.sync), subject, payload, Some(duration))
            .await
    }

    /// Convenience wrapper around [`subscribe_with_optional_queue_group`](struct.Client.html#method.subscribe_with_optional_queue_group)
    pub async fn subscribe(
        &self,
        subject: &Subject,
        buffer: usize,
    ) -> Result<(Sid, MpscReceiver<Msg>)> {
        self.subscribe_with_optional_queue_group(subject, None, buffer)
            .await
    }

    /// Convenience wrapper around [`subscribe_with_optional_queue_group`](struct.Client.html#method.subscribe_with_optional_queue_group)
    pub async fn subscribe_with_queue_group(
        &self,
        subject: &Subject,
        queue_group: &str,
        buffer: usize,
    ) -> Result<(Sid, MpscReceiver<Msg>)> {
        self.subscribe_with_optional_queue_group(subject, Some(queue_group), buffer)
            .await
    }

    /// [`SUB`](https://nats-io.github.io/docs/nats_protocol/nats-protocol.html#sub)scribe to a [`Subject`](struct.Subject.html)
    ///
    /// Returns the subscription ID of the newly created subscription and a channel to receive incoming messages on.
    ///
    /// # Arguments
    /// * `subject` - The subject to subscribe to
    /// * `reply_to` - The optional queue group to join
    /// * `buffer` - The size of the underlying mpsc channel
    pub async fn subscribe_with_optional_queue_group(
        &self,
        subject: &Subject,
        queue_group: Option<&str>,
        buffer: usize,
    ) -> Result<(Sid, MpscReceiver<Msg>)> {
        let mut client = self.lock().await;
        client
            .subscribe_with_optional_queue_group(subject, queue_group, buffer)
            .await
    }

    /// Convenience wrapper around [`unsubscribe_optional_max_msgs`](struct.Client.html#method.unsubscribe_optional_max_msgs)
    pub async fn unsubscribe(&self, sid: Sid) -> Result<()> {
        self.unsubscribe_optional_max_msgs(sid, None).await
    }

    /// Convenience wrapper around [`unsubscribe_optional_max_msgs`](struct.Client.html#method.unsubscribe_optional_max_msgs)
    pub async fn unsubscribe_with_max_msgs(&self, sid: Sid, max_msgs: u64) -> Result<()> {
        self.unsubscribe_optional_max_msgs(sid, Some(max_msgs))
            .await
    }

    /// [`UNSUB`](https://nats-io.github.io/docs/nats_protocol/nats-protocol.html#unsub)scribe from a subscription ID
    ///
    /// # Arguments
    /// * `sid` - The subscription id to unsubscribe from
    /// * `max_msgs` - Unsubscribe after receiving the specified number of messages. If this is
    /// `None`, the subscription is immediately unsubscribed.
    pub async fn unsubscribe_optional_max_msgs(
        &self,
        sid: Sid,
        max_msgs: Option<u64>,
    ) -> Result<()> {
        let mut client = self.lock().await;
        client.unsubscribe_optional_max_msgs(sid, max_msgs).await
    }

    /// Unsubscribe from all subscriptions
    pub async fn unsubscribe_all(&self) -> Result<()> {
        let unsubscribes = self
            .sids()
            .await
            .into_iter()
            .map(|sid| self.unsubscribe(sid));
        future::try_join_all(unsubscribes).await?;
        Ok(())
    }

    /// Get a watch stream of [`INFO`](https://nats-io.github.io/docs/nats_protocol/nats-protocol.html#info) messages received from the server
    pub async fn info_stream(&self) -> WatchReceiver<Info> {
        self.lock().await.info_stream()
    }

    /// Get a watch stream of [`PING`](https://nats-io.github.io/docs/nats_protocol/nats-protocol.html#pingpong) messages received from the server
    pub async fn ping_stream(&self) -> WatchReceiver<()> {
        self.lock().await.ping_stream()
    }

    /// Get a watch stream of [`PONG`](https://nats-io.github.io/docs/nats_protocol/nats-protocol.html#pingpong) messages received from the server
    pub async fn pong_stream(&self) -> WatchReceiver<()> {
        self.lock().await.pong_stream()
    }

    /// Get a watch stream of [`+OK`](https://nats-io.github.io/docs/nats_protocol/nats-protocol.html#okerr) messages received from the server
    pub async fn ok_stream(&self) -> WatchReceiver<()> {
        self.lock().await.ok_stream()
    }

    /// Get a watch stream of [`-ERR`](https://nats-io.github.io/docs/nats_protocol/nats-protocol.html#okerr) messages received from the server
    pub async fn err_stream(&self) -> WatchReceiver<ProtocolError> {
        self.lock().await.err_stream()
    }

    /// Send a [`PING`](https://nats-io.github.io/docs/nats_protocol/nats-protocol.html#pingpong) to the server.
    ///
    /// This method, coupled with a `pong_stream`, can be a useful way to check that the client is
    /// still connected to the server.
    pub async fn ping(&self) -> Result<()> {
        let mut client = self.lock().await;
        client.ping().await
    }

    /// Send a [`PONG`](https://nats-io.github.io/docs/nats_protocol/nats-protocol.html#pingpong) to the server.
    ///
    /// **Note:** you do not have to manually send a `PONG` as part of the servers ping/pong keep
    /// alive. The client library automatically handles replying to pings. You should not need
    /// to use this method.
    pub async fn pong(&self) -> Result<()> {
        let mut client = self.lock().await;
        client.pong().await
    }

    /// Send a `PONG` and wait for a `PONG` from the server
    pub async fn ping_pong(&self) -> Result<()> {
        SyncClient::ping_pong(Arc::clone(&self.sync)).await
    }

    async fn lock(&self) -> MutexGuard<'_, SyncClient> {
        self.sync.lock().await
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // TODO: Disconnect on drop
        trace!("Client was dropped");
    }
}

struct SyncClient {
    addresses: Vec<Address>,
    connect: Connect,
    state: ConnectionState,
    state_tx: WatchSender<ClientState>,
    state_rx: WatchReceiver<ClientState>,
    info_tx: WatchSender<Info>,
    info_rx: WatchReceiver<Info>,
    ping_tx: WatchSender<()>,
    ping_rx: WatchReceiver<()>,
    pong_tx: WatchSender<()>,
    pong_rx: WatchReceiver<()>,
    ok_tx: WatchSender<()>,
    ok_rx: WatchReceiver<()>,
    err_tx: WatchSender<ProtocolError>,
    err_rx: WatchReceiver<ProtocolError>,
    tcp_connect_timeout: Duration,
    delay_generator: DelayGenerator,
    #[cfg(feature = "tls")]
    tls_config: Option<TlsConfig>,
    #[cfg(feature = "tls")]
    tls_domain: Option<String>,
    subscriptions: HashMap<Sid, Subscription>,
    request_inbox_mapping: HashMap<Subject, MpscSender<Msg>>,
    request_wildcard_subscription: Option<Sid>,
    request_base_inbox: String,
}

impl SyncClient {
    fn with_connect(addresses: Vec<Address>, connect: Connect) -> Self {
        let state = ConnectionState::Disconnected;
        let (state_tx, state_rx) = watch::channel((&state).into());
        let (info_tx, info_rx) = watch::channel(Info::new());
        let (ping_tx, ping_rx) = watch::channel(());
        let (pong_tx, pong_rx) = watch::channel(());
        let (ok_tx, ok_rx) = watch::channel(());
        let (err_tx, err_rx) = watch::channel(ProtocolError::UnknownProtocolOperation);
        Self {
            addresses,
            connect,
            state,
            state_tx,
            state_rx,
            info_tx,
            info_rx,
            ping_tx,
            ping_rx,
            pong_tx,
            pong_rx,
            ok_tx,
            ok_rx,
            err_tx,
            err_rx,
            tcp_connect_timeout: util::DEFAULT_TCP_CONNECT_TIMEOUT,
            delay_generator: generate_delay_generator(
                util::DEFAULT_CONNECT_SERIES_ATTEMPTS_BEFORE_COOL_DOWN,
                util::DEFAULT_CONNECT_DELAY,
                util::DEFAULT_CONNECT_SERIES_DELAY,
                util::DEFAULT_COOL_DOWN,
            ),
            #[cfg(feature = "tls")]
            tls_config: None,
            #[cfg(feature = "tls")]
            tls_domain: None,
            subscriptions: HashMap::new(),
            request_inbox_mapping: HashMap::new(),
            request_wildcard_subscription: None,
            request_base_inbox: Uuid::new_v4().to_simple().to_string(),
        }
    }

    fn state(&self) -> ClientState {
        self.state_rx.borrow().clone()
    }

    fn state_stream(&self) -> WatchReceiver<ClientState> {
        self.state_rx.clone()
    }

    pub fn info(&self) -> Info {
        self.info_rx.borrow().clone()
    }

    fn connect_mut(&mut self) -> &mut Connect {
        &mut self.connect
    }

    fn addresses_mut(&mut self) -> &mut [Address] {
        &mut self.addresses
    }

    fn tcp_connect_timeout(&self) -> Duration {
        self.tcp_connect_timeout
    }

    fn set_tcp_connect_timeout(&mut self, tcp_connect_timeout: Duration) -> &mut Self {
        self.tcp_connect_timeout = tcp_connect_timeout;
        self
    }

    fn delay_generator_mut(&mut self) -> &mut DelayGenerator {
        &mut self.delay_generator
    }

    #[cfg(feature = "tls")]
    fn set_tls_config(&mut self, tls_config: TlsConfig) -> &mut Self {
        self.tls_config = Some(tls_config);
        self
    }

    #[cfg(feature = "tls")]
    fn set_tls_domain(&mut self, domain: String) -> &mut Self {
        self.tls_domain = Some(domain);
        self
    }

    fn subscriptions(&self) -> impl Iterator<Item = (&Sid, &Subscription)> {
        self.subscriptions.iter()
    }

    async fn send_connect(&mut self) -> Result<()> {
        if let ConnectionState::Connected(address, writer) = &mut self.state {
            Self::send_connect_with_writer(writer, &self.connect, address).await
        } else {
            Err(Error::NotConnected)
        }
    }

    #[cfg(feature = "tls")]
    async fn upgrade_to_tls(
        &mut self,
        stream: TlsOrTcpStream,
        domain: &str,
    ) -> Result<TlsOrTcpStream> {
        let domain = self.tls_domain.as_deref().unwrap_or(domain).to_string();
        info!(
            "Using '{}' as the domain to upgrade to a TLS connection",
            domain
        );
        let tls_config = self.tls_config.clone().ok_or(Error::TlsDisabled)?;
        Ok(stream.upgrade(tls_config, &domain).await?)
    }

    #[cfg(not(feature = "tls"))]
    async fn upgrade_to_tls(
        &mut self,
        _stream: TlsOrTcpStream,
        _domain: &str,
    ) -> Result<TlsOrTcpStream> {
        Err(Error::TlsDisabled)
    }

    #[allow(clippy::cognitive_complexity)]
    async fn connect(wrapped_client: Client) {
        // If we are already connected do not connect again
        if let ConnectionState::Connected(_, _) = wrapped_client.lock().await.state {
            return;
        }

        // Get a continuous iterator over a shuffled list of all possible addresses.
        // We randomize the addresses to avoid a thundering herd.
        // See https://nats-io.github.io/docs/developer/reconnect/random.html
        let (addresses_len, mut addresses_iter) = {
            let client = wrapped_client.lock().await;
            let mut addresses = client
                .addresses
                .iter()
                .chain(client.info_rx.borrow().connect_urls().iter())
                .cloned()
                .collect::<Vec<_>>();
            let addresses_len = addresses.len() as u64;
            addresses.shuffle(&mut rand::thread_rng());
            (addresses_len, addresses.into_iter().cycle())
        };

        let mut connect_attempts = 0;
        loop {
            // Effectively move the delay logic after we try a connect, but keep it at the start
            // of the loop so we do not have to do it before every continue statement.
            if connect_attempts != 0 {
                let delay = (wrapped_client.lock().await.delay_generator)(
                    &wrapped_client,
                    connect_attempts,
                    addresses_len,
                );
                debug!(
                    "Delaying for {}s after {} connect attempts with {} addresses",
                    delay.as_secs_f32(),
                    connect_attempts,
                    addresses_len
                );
                time::sleep(delay).await;
            }
            connect_attempts += 1;
            let mut client = wrapped_client.lock().await;

            match client.state {
                ConnectionState::Connected(_, _) => {
                    // It It is possible to have multiple connect tasks running. Make sure another
                    // connect task did not successfully connect while this task was trying to
                    // connect. We will now hold the lock until we transition to connected or fail.
                    return;
                }
                ConnectionState::Disconnecting(_) => {
                    // `Self::disconnect` was called and we should disconnect and stop trying to
                    // connect
                    client.state_transition(StateTransition::ToDisconnected);
                    return;
                }
                _ => (),
            }

            let address = if let Some(address) = addresses_iter.next() {
                address
            } else {
                error!("No addresses to connect to");
                continue;
            };
            client.state_transition(StateTransition::ToConnecting(address.clone()));

            // Try to establish a TCP connection
            let connect = time::timeout(
                client.tcp_connect_timeout,
                TcpStream::connect(address.address()),
            );
            let (reader, writer) = match connect.await {
                Ok(Ok(stream)) => {
                    // TODO: Currently, we use the generic io split in order to avoid lifetime
                    // complications. However, `TcpStream` does implement a specialized split. It
                    // may be worth switching to the specialized version to avoid overhead.
                    io::split(TlsOrTcpStream::new(stream))
                }
                Ok(Err(e)) => {
                    error!("Failed to connect to '{}', err: {}", address, e);
                    continue;
                }
                Err(_) => {
                    error!("Timed out while connecting to '{}'", address);
                    continue;
                }
            };

            let mut reader = FramedRead::new(reader, Codec::new());

            // Wait for the first server message. It should be an info message.
            let wait_for_info = time::timeout(client.tcp_connect_timeout, reader.next());
            let tls_required = if let Some(message) =
                Self::unwrap_server_message_with_timeout(wait_for_info.await, util::INFO_OP_NAME)
            {
                if let ServerMessage::Info(info) = message {
                    let tls_required = info.tls_required();
                    client.handle_info_message(info);
                    tls_required
                } else {
                    error!(
                        "First message should be {} instead received '{:?}'",
                        util::INFO_OP_NAME,
                        message
                    );
                    debug_assert!(false);
                    continue;
                }
            } else {
                // Logging errors is handled by `unwrap_server_message_with_timeout`
                continue;
            };

            // Upgrade to a TLS connection if necessary
            let (mut reader, mut writer) = if tls_required {
                // Combine the reader and writer back into a single stream
                let stream = reader.into_inner().unsplit(writer);
                let upgraded_stream = match client.upgrade_to_tls(stream, address.domain()).await {
                    Ok(stream) => stream,
                    Err(e) => {
                        error!("Failed to upgrade to TLS connection, err: {}", e);
                        continue;
                    }
                };
                // Split the stream back apart
                let (reader, writer) = io::split(upgraded_stream);
                (FramedRead::new(reader, Codec::new()), writer)
            } else {
                (reader, writer)
            };

            // Send a connect message
            if let Err(e) =
                Self::send_connect_with_writer(&mut writer, &client.connect, &address).await
            {
                error!("Failed to send connect message, err: {}", e);
                continue;
            }

            // If we are in verbose mode, wait for the ok server message.
            if client.connect_mut().is_verbose() {
                let wait_for_ok = time::timeout(client.tcp_connect_timeout, reader.next());
                if let Some(message) =
                    Self::unwrap_server_message_with_timeout(wait_for_ok.await, util::OK_OP_NAME)
                {
                    match message {
                        ServerMessage::Ok => (),
                        ServerMessage::Err(e) => {
                            error!(
                                "Protocol error waiting for {} message, err: {}",
                                util::OK_OP_NAME,
                                e
                            );
                            continue;
                        }
                        message => {
                            error!(
                                "Next message should be {} instead received '{:?}'",
                                util::OK_OP_NAME,
                                message
                            );
                            debug_assert!(false);
                            continue;
                        }
                    }
                } else {
                    // Logging errors is handled by `unwrap_server_message_with_timeout`
                    continue;
                };
            }

            // Perform a ping-pong to verify the connection was established
            if let Err(e) = Self::ping_with_writer(&mut writer).await {
                error!("Failed to send ping when verifying connection, err: {}", e);
                continue;
            }

            // Wait for the next server message. It should be a pong message.
            let wait_for_pong = time::timeout(client.tcp_connect_timeout, reader.next());
            if let Some(message) =
                Self::unwrap_server_message_with_timeout(wait_for_pong.await, util::PONG_OP_NAME)
            {
                match message {
                    ServerMessage::Pong => (),
                    ServerMessage::Err(e) => {
                        error!(
                            "Protocol error waiting for {} message, err: {}",
                            util::PONG_OP_NAME,
                            e
                        );
                        continue;
                    }
                    message => {
                        error!(
                            "Next message should be {} instead received '{:?}'",
                            util::PONG_OP_NAME,
                            message
                        );
                        debug_assert!(false);
                        continue;
                    }
                }
            } else {
                // Logging errors is handled by `unwrap_server_message_with_timeout`
                continue;
            };

            // Resubscribe to subscriptions
            let mut failed_to_resubscribe = Vec::new();
            for (sid, subscription) in &client.subscriptions {
                if let Err(e) =
                    Self::write_line(&mut writer, ClientControl::Sub(subscription)).await
                {
                    error!(
                        "Failed to resubscribe to sid '{}' with subject '{}', err: {}",
                        sid,
                        subscription.subject(),
                        e
                    );
                    failed_to_resubscribe.push(*sid);
                }
            }
            // Remove all subscriptions that failed to resubscribe
            client
                .subscriptions
                .retain(|sid, _| !failed_to_resubscribe.contains(sid));

            // Spawn the task to handle reading subsequent server messages
            tokio::spawn(Self::type_erased_server_messages_handler(
                Client::clone(&wrapped_client),
                reader,
            ));

            client.state_transition(StateTransition::ToConnected(writer));
            return;
        }
    }

    async fn disconnect(wrapped_client: Arc<Mutex<Self>>) {
        let (tx, rx) = oneshot::channel();
        {
            let mut client = wrapped_client.lock().await;

            // If we are already disconnected do not disconnect again
            if let ConnectionState::Disconnected = client.state {
                return;
            }

            let state_stream = client.state_stream();
            let mut state_stream = WatchStream::new(state_stream);
            // Spawn a future waiting for the disconnected state
            tokio::spawn(async move {
                while let Some(state) = state_stream.next().await {
                    if state.is_disconnected() {
                        tx.send(()).expect("to send disconnect signal");
                        break;
                    }
                }
            });
            // Transition to the disconnecting state. This will complete the disconnecting future
            // breaking out of any continuous loops.
            client.state_transition(StateTransition::ToDisconnecting);
        }
        // Wait till we are disconnected
        rx.await.expect("to receive disconnect signal");
    }

    async fn publish_with_reply(
        &mut self,
        subject: &Subject,
        reply_to: &Subject,
        payload: &[u8],
    ) -> Result<()> {
        self.publish_with_optional_reply(subject, Some(reply_to), payload)
            .await
    }

    async fn publish_with_optional_reply(
        &mut self,
        subject: &Subject,
        reply_to: Option<&Subject>,
        payload: &[u8],
    ) -> Result<()> {
        let max_payload = self.info().max_payload;
        if let ConnectionState::Connected(_, writer) = &mut self.state {
            // Check that payload's length does not exceed the servers max_payload
            if payload.len() > max_payload {
                return Err(Error::ExceedsMaxPayload {
                    tried: payload.len(),
                    limit: max_payload,
                });
            }

            Self::write_line(writer, ClientControl::Pub(subject, reply_to, payload.len())).await?;
            writer.write_all(payload).await?;
            writer
                .write_all(util::MESSAGE_TERMINATOR.as_bytes())
                .await?;
            Ok(())
        } else {
            Err(Error::NotConnected)
        }
    }

    async fn request_wildcard_handler(
        wrapped_client: Arc<Mutex<Self>>,
        mut subscription_rx: MpscReceiver<Msg>,
    ) {
        while let Some(msg) = subscription_rx.recv().await {
            let mut client = wrapped_client.lock().await;
            if let Some(requester_tx) = client.request_inbox_mapping.remove(msg.subject()) {
                requester_tx.send(msg).await.unwrap_or_else(|err| {
                    warn!("Could not write response to pending request via mapping channel. Skipping! Err: {}", err);
                    debug_assert!(false);
                });
            } else {
                warn!(
                    "Could not find response channel for request with subject: {}",
                    &msg.subject()
                );
            }
        }

        // At this point we are either disconnecting or we're in some
        // strange state where the subscription's rx has been closed.
        let mut client = wrapped_client.lock().await;
        client.request_inbox_mapping.clear();
        client.request_wildcard_subscription = None;
    }

    async fn request_with_timeout(
        wrapped_client: Arc<Mutex<Self>>,
        subject: &Subject,
        payload: &[u8],
        duration: Option<Duration>,
    ) -> Result<Msg> {
        let request = Request::new(wrapped_client).await?;
        request.call(subject, payload, duration).await
    }

    async fn subscribe(
        &mut self,
        subject: &Subject,
        buffer: usize,
    ) -> Result<(Sid, MpscReceiver<Msg>)> {
        self.subscribe_with_optional_queue_group(subject, None, buffer)
            .await
    }

    async fn subscribe_with_optional_queue_group(
        &mut self,
        subject: &Subject,
        queue_group: Option<&str>,
        buffer: usize,
    ) -> Result<(Sid, MpscReceiver<Msg>)> {
        if let ConnectionState::Connected(_, writer) = &mut self.state {
            let (tx, rx) = mpsc::channel(buffer);
            let subscription =
                Subscription::new(subject.clone(), queue_group.map(String::from), tx);
            Self::write_line(writer, ClientControl::Sub(&subscription)).await?;
            let sid = subscription.sid();
            self.subscriptions.insert(sid, subscription);
            Ok((sid, rx))
        } else {
            Err(Error::NotConnected)
        }
    }

    async fn unsubscribe(&mut self, sid: Sid) -> Result<()> {
        self.unsubscribe_optional_max_msgs(sid, None).await
    }

    async fn unsubscribe_optional_max_msgs(
        &mut self,
        sid: Sid,
        max_msgs: Option<u64>,
    ) -> Result<()> {
        if let ConnectionState::Connected(_, writer) = &mut self.state {
            let subscription = match self.subscriptions.get_mut(&sid) {
                Some(subscription) => subscription,
                None => return Err(Error::UnknownSid(sid)),
            };
            subscription.unsubscribe_after = max_msgs;
            Self::write_line(writer, ClientControl::Unsub(sid, max_msgs)).await?;
            // If we are not waiting for any messages, immediately remove the subscription
            if subscription.unsubscribe_after.is_none() {
                self.subscriptions.remove(&sid);
            }
            Ok(())
        } else {
            Err(Error::NotConnected)
        }
    }

    pub fn info_stream(&mut self) -> WatchReceiver<Info> {
        self.info_rx.clone()
    }

    pub fn ping_stream(&mut self) -> WatchReceiver<()> {
        self.ping_rx.clone()
    }

    pub fn pong_stream(&mut self) -> WatchReceiver<()> {
        self.pong_rx.clone()
    }

    pub fn ok_stream(&mut self) -> WatchReceiver<()> {
        self.ok_rx.clone()
    }

    pub fn err_stream(&mut self) -> WatchReceiver<ProtocolError> {
        self.err_rx.clone()
    }

    async fn ping(&mut self) -> Result<()> {
        if let ConnectionState::Connected(_, writer) = &mut self.state {
            Self::ping_with_writer(writer).await?;
            Ok(())
        } else {
            Err(Error::NotConnected)
        }
    }

    async fn pong(&mut self) -> Result<()> {
        if let ConnectionState::Connected(_, writer) = &mut self.state {
            Self::write_line(writer, ClientControl::Pong).await?;
            Ok(())
        } else {
            Err(Error::NotConnected)
        }
    }

    async fn ping_pong(wrapped_client: Arc<Mutex<Self>>) -> Result<()> {
        let mut pong_stream = {
            let mut client = wrapped_client.lock().await;
            let pong_stream = client.pong_stream();
            let mut pong_stream = WatchStream::new(pong_stream);
            // Clear the current value
            pong_stream.next().now_or_never();
            client.ping().await?;
            pong_stream
        };
        pong_stream.next().await;
        Ok(())
    }

    async fn server_messages_handler(
        wrapped_client: Client,
        mut reader: FramedRead<ReadHalf<TlsOrTcpStream>, Codec>,
    ) {
        let disconnecting =
            Self::disconnecting(WatchStream::new(wrapped_client.lock().await.state_stream()));
        pin_mut!(disconnecting);
        loop {
            // Select between the next message and disconnecting
            let wrapped_message = match future::select(reader.next(), disconnecting).await {
                Either::Left((Some(message), unresolved_disconnecting)) => {
                    disconnecting = unresolved_disconnecting;
                    message
                }
                Either::Left((None, _)) => {
                    error!("{}", TCP_SOCKET_DISCONNECTED_MESSAGE);
                    break;
                }
                Either::Right(((), _)) => break,
            };

            // Handle the message if it was valid
            match Disposition::from_output(wrapped_message) {
                Disposition::Message(m) => {
                    Self::handle_server_message(Arc::clone(&wrapped_client.sync), m).await;
                    continue;
                }
                Disposition::DecodingError(e) => {
                    error!("Received invalid server message, err: {}", e);
                    continue;
                }
                Disposition::UnrecoverableError(e) => {
                    // When the TCP stream drops (e.g., you pull out
                    // the network cable from the server), we get a
                    // ConnectionReset error on
                    // Windows. (Interestingly, we don't get any error
                    // on Linux.) Ideally, this would be translated
                    // somehow into a terminating stream.
                    //
                    // In this case, we must treat this as a
                    // disconnection and break out of our loop.
                    error!("TCP socket error, err: {}", e);
                    break;
                }
            }
        }
        // If we make it out of the above loop, we somehow disconnected
        let mut client = wrapped_client.lock().await;

        // When we're disconnecting (even if we're going to reconnect), we should
        // drop the request wildcard subscription here. Unsubscribe will not work
        // at this point because the current connection state prevents this.
        if let Some(request_wildcard_sid) = client.request_wildcard_subscription {
            client.subscriptions.remove(&request_wildcard_sid);
        }

        // If we are not in the disconnecting state, we did not intentionally disconnect
        // and should try to reconnect.
        let should_reconnect = !client.state().is_disconnecting();

        // Transition to the disconnected state and try and shutdown the TCP connection
        if let StateTransitionResult::Writer(writer) =
            client.state_transition(StateTransition::ToDisconnected)
        {
            let mut stream = reader.into_inner().unsplit(writer);
            if let Err(e) = stream.shutdown().await {
                if e.kind() != ErrorKind::NotConnected {
                    error!("Failed to shutdown TCP stream, err: {}", e);
                }
            }
        } else {
            // We should always have a writer to close
            error!("Disconnected with no TCP writer. Unable to shutdown TCP stream.");
            debug_assert!(false);
        }

        if should_reconnect {
            tokio::spawn(Self::connect(Client::clone(&wrapped_client)));
        }
    }

    #[allow(clippy::cognitive_complexity)]
    async fn handle_server_message(wrapped_client: Arc<Mutex<Self>>, message: ServerMessage) {
        match message {
            ServerMessage::Info(info) => {
                wrapped_client.lock().await.handle_info_message(info);
            }
            ServerMessage::Msg(msg) => {
                let sid = msg.sid();
                let mut client = wrapped_client.lock().await;
                // Try and lookup the subscription from the sid
                let subscription = match client.subscriptions.get_mut(&sid) {
                    Some(subscription) => subscription,
                    None => {
                        // If we do not know about this subscription, log an error and
                        // unsubscribe. This should never happen and means our subscription
                        // store got out of sync.
                        error!("Received unknown sid '{}'", sid);
                        debug_assert!(false);
                        let wrapped_client = Arc::clone(&wrapped_client);
                        tokio::spawn(async move {
                            info!("Unsubscribing from unknown sid '{}'", sid);
                            let mut client = wrapped_client.lock().await;
                            if let Err(e) = client.unsubscribe(sid).await {
                                error!("Failed to unsubscribe from '{}', err: {}", sid, e);
                            }
                        });
                        return;
                    }
                };
                // Try and send the message to the subscription receiver
                if subscription.tx.send(msg).await.is_err() {
                    // If we fail to send, it is because the receiver closed. We no longer
                    // care about this subscription and should unsubscribe
                    let wrapped_client = Arc::clone(&wrapped_client);
                    tokio::spawn(async move {
                        info!("Unsubscribing from closed sid '{}'", sid);
                        let mut client = wrapped_client.lock().await;
                        if let Err(e) = client.unsubscribe(sid).await {
                            error!("Failed to unsubscribe from sid '{}', err: {}", sid, e);
                        }
                    });
                }
                // If we have received all the messages we were waiting for, unsubscribe
                if let Some(unsubscribe_after) = &mut subscription.unsubscribe_after() {
                    *unsubscribe_after -= 1;
                    if *unsubscribe_after == 0 {
                        client.subscriptions.remove(&sid);
                    }
                }
            }
            ServerMessage::Ping => {
                if let Err(e) = wrapped_client.lock().await.ping_tx.send(()) {
                    error!("Failed to send {}, err: {}", util::PING_OP_NAME, e);
                }
                // Spawn a task to send a pong replying to the ping
                let wrapped_client = Arc::clone(&wrapped_client);
                tokio::spawn(async move {
                    let mut client = wrapped_client.lock().await;
                    if let Err(e) = client.pong().await {
                        error!("Failed to send {}, err: {}", util::PONG_OP_NAME, e);
                    }
                });
            }
            ServerMessage::Pong => {
                if let Err(e) = wrapped_client.lock().await.pong_tx.send(()) {
                    error!("Failed to send {}, err: {}", util::PONG_OP_NAME, e);
                }
            }
            ServerMessage::Ok => {
                if let Err(e) = wrapped_client.lock().await.ok_tx.send(()) {
                    error!("Failed to send {}, err: {}", util::OK_OP_NAME, e);
                }
            }
            ServerMessage::Err(e) => {
                error!("Protocol error, err: '{}'", e);
                if let Err(e) = wrapped_client.lock().await.err_tx.send(e) {
                    error!("Failed to send {}, err: {}", util::ERR_OP_NAME, e);
                }
            }
        }
    }

    fn handle_info_message(&mut self, info: Info) {
        if let Err(e) = self.info_tx.send(info) {
            error!("Failed to send {}, err: {}", util::INFO_OP_NAME, e);
        }
    }

    // We have to erase the type of `server_message_handler` in order to avoid a recursive future
    //
    // https://github.com/rust-lang/rust/issues/53690
    fn type_erased_server_messages_handler(
        wrapped_client: Client,
        reader: FramedRead<ReadHalf<TlsOrTcpStream>, Codec>,
    ) -> impl std::future::Future<Output = ()> + Send {
        Self::server_messages_handler(wrapped_client, reader)
    }

    // Create a future that waits for the disconnecting state.
    async fn disconnecting(mut state_stream: WatchStream<ClientState>) {
        while let Some(state) = state_stream.next().await {
            if state.is_disconnecting() {
                break;
            }
        }
    }

    async fn write_line(
        writer: &mut WriteHalf<TlsOrTcpStream>,
        control_line: ClientControl<'_>,
    ) -> Result<()> {
        let line = control_line.to_line();
        Ok(writer.write_all(line.as_bytes()).await?)
    }

    async fn send_connect_with_writer(
        writer: &mut WriteHalf<TlsOrTcpStream>,
        connect: &Connect,
        address: &Address,
    ) -> Result<()> {
        let mut connect = connect.clone();
        // If the address has authorization information, override the default authorization
        if let Some(authorization) = address.authorization() {
            connect.set_authorization(Some(authorization.clone()));
        }
        Self::write_line(writer, ClientControl::Connect(&connect)).await
    }

    async fn ping_with_writer(writer: &mut WriteHalf<TlsOrTcpStream>) -> Result<()> {
        Self::write_line(writer, ClientControl::Ping).await
    }

    fn unwrap_server_message_with_timeout(
        wrapped_server_message: StdResult<
            Option<StdResult<Result<ServerMessage>, io::Error>>,
            Elapsed,
        >,
        waiting_for: &str,
    ) -> Option<ServerMessage> {
        match wrapped_server_message {
            Ok(Some(wrapped_message)) => match Disposition::from_output(wrapped_message) {
                Disposition::Message(m) => Some(m),
                Disposition::DecodingError(e) => {
                    error!("Received invalid server message, err: {}", e);
                    None
                }
                Disposition::UnrecoverableError(e) => {
                    error!("TCP socket error, err: {}", e);
                    None
                }
            },
            Ok(None) => {
                error!("{}", TCP_SOCKET_DISCONNECTED_MESSAGE);
                None
            }
            Err(_) => {
                error!("Timed out waiting for {} message", waiting_for);
                None
            }
        }
    }

    fn state_transition(&mut self, transition: StateTransition) -> StateTransitionResult {
        let previous_client_state = ClientState::from(&self.state);
        // Temporarily set to the disconnected state so we can move values out of `previous_state`
        let previous_state = mem::replace(&mut self.state, ConnectionState::Disconnected);
        let (next_state, result) = match (previous_state, transition) {
            // From disconnected
            (ConnectionState::Disconnected, StateTransition::ToConnecting(address)) => (
                ConnectionState::Connecting(address),
                StateTransitionResult::None,
            ),
            // From connecting
            (ConnectionState::Connecting(_), StateTransition::ToConnecting(address)) => (
                ConnectionState::Connecting(address),
                StateTransitionResult::None,
            ),
            (ConnectionState::Connecting(address), StateTransition::ToConnected(writer)) => (
                ConnectionState::Connected(address, writer),
                StateTransitionResult::None,
            ),
            (ConnectionState::Connecting(_), StateTransition::ToDisconnecting) => (
                ConnectionState::Disconnecting(None),
                StateTransitionResult::None,
            ),
            // From connected
            (ConnectionState::Connected(_, writer), StateTransition::ToDisconnecting) => (
                ConnectionState::Disconnecting(Some(writer)),
                StateTransitionResult::None,
            ),
            (ConnectionState::Connected(_, writer), StateTransition::ToDisconnected) => (
                ConnectionState::Disconnected,
                StateTransitionResult::Writer(writer),
            ),
            // From disconnecting
            (ConnectionState::Disconnecting(Some(writer)), StateTransition::ToDisconnected) => (
                ConnectionState::Disconnected,
                StateTransitionResult::Writer(writer),
            ),
            (ConnectionState::Disconnecting(None), StateTransition::ToDisconnected) => {
                (ConnectionState::Disconnected, StateTransitionResult::None)
            }
            (_, transition) => {
                // Other state transitions are not allowed
                error!(
                    "Invalid transition '{:?}' from '{}'",
                    transition, previous_client_state,
                );
                unreachable!();
            }
        };
        self.state = next_state;
        let next_client_state = ClientState::from(&self.state);
        info!(
            "Transitioned to state '{}' from '{}'",
            next_client_state, previous_client_state
        );
        // If we can not send the state transition, we could end up in an inconsistent
        // state. This would be very bad so instead panic. This should never happen.
        self.state_tx
            .send(next_client_state)
            .expect("to send state transition");
        result
    }
}

/// Convenient type alias for what comes out of our Decoder stream.
type DecoderStreamOutput = StdResult<Result<ServerMessage>, io::Error>;

/// Translates the various cases of what `DecoderStreamOutput` into
/// easier-to-understand labels to aid in reasoning and to consolidate
/// this matching logic into a single place.
enum Disposition {
    /// The stream yielded a valid NATS message.
    Message(ServerMessage),
    /// The stream encountered a decoding error, but can still be read
    /// from in the future.
    DecodingError(Error),
    /// An error was thrown when trying to decode an item; don't try
    /// reading from the stream again.
    UnrecoverableError(io::Error),
}

impl Disposition {
    fn from_output(o: DecoderStreamOutput) -> Self {
        match o {
            Ok(Ok(m)) => Self::Message(m),
            Ok(Err(e)) => Self::DecodingError(e),
            Err(e) => Self::UnrecoverableError(e),
        }
    }
}

// Request helper type for NATS request
struct Request {
    reply_to: Subject,
    wrapped_client: Arc<Mutex<SyncClient>>,
    request_inbox_mapping_was_removed: bool,
}

impl Request {
    async fn new(wrapped_client: Arc<Mutex<SyncClient>>) -> Result<Self> {
        let inbox_uuid = Uuid::new_v4();
        let client = wrapped_client.lock().await;
        let request_inbox = inbox_uuid.to_simple();
        let reply_to: Subject = format!(
            "{}.{}.{}",
            util::INBOX_PREFIX,
            client.request_base_inbox,
            request_inbox
        )
        .parse()?;

        Ok(Self {
            reply_to,
            wrapped_client: wrapped_client.clone(),
            request_inbox_mapping_was_removed: false,
        })
    }

    // Call takes the ownership of the request to prevent double-requesting with
    // the same request inbox.
    async fn call(
        mut self,
        subject: &Subject,
        payload: &[u8],
        duration: Option<Duration>,
    ) -> Result<Msg> {
        let mut rx = {
            let mut client = self.wrapped_client.lock().await;

            // Only subscribe to the wildcard subscription when requested once!
            if client.request_wildcard_subscription.is_none() {
                let global_reply_to =
                    format!("{}.{}.*", util::INBOX_PREFIX, client.request_base_inbox).parse()?;
                let (sid, rx) = client.subscribe(&global_reply_to, 1024).await?;
                client.request_wildcard_subscription = Some(sid);

                // Spawn the task that watches the request wildcard receiver.
                tokio::spawn(SyncClient::request_wildcard_handler(
                    self.wrapped_client.clone(),
                    rx,
                ));
            }

            let (tx, rx) = mpsc::channel(1);
            client
                .request_inbox_mapping
                .insert(self.reply_to.clone(), tx);
            client
                .publish_with_reply(subject, &self.reply_to, payload)
                .await?;

            rx
        };

        // Use a timeout duration if it was provided by the caller.
        let next_message = match duration {
            Some(duration) => tokio::time::timeout(duration, rx.recv()).await?,
            None => rx.recv().await,
        };

        // Make sure we clean up on error (don't leave a dangling request
        // inbox mapping reference).
        match next_message {
            Some(response) => {
                // This happens automatically when a response is received.
                self.request_inbox_mapping_was_removed = true;
                Ok(response)
            }
            None => Err(Error::NoResponse),
        }
    }
}

impl Drop for Request {
    // When the request is dropped, ensure the reply_to subject is removed
    // from the client request inbox mapping.
    // NOTE: This is a blocking async block, but the only thing it blocks on
    // is the client's mutex, which should be fine.
    // When/if async drop becomes available we should use that instead.
    fn drop(&mut self) {
        if self.request_inbox_mapping_was_removed {
            return;
        }

        futures::executor::block_on(async {
            let mut client = self.wrapped_client.lock().await;

            client.request_inbox_mapping.remove(&self.reply_to);
        });
    }
}

//! Some documentation

mod codec;
#[cfg(test)]
mod tests;
mod types;
mod util;

use futures::{
    future::{self, Either, FutureExt},
    lock::Mutex,
    stream::StreamExt,
};
use log::{debug, error, info, trace};
use pin_utils::pin_mut;
use rand;
use rand::seq::SliceRandom;
use std::{
    collections::HashMap,
    io::ErrorKind,
    mem,
    net::Shutdown,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    codec::FramedRead,
    io::AsyncWriteExt,
    net::tcp::{
        split::{TcpStreamReadHalf, TcpStreamWriteHalf},
        TcpStream,
    },
    sync::{
        mpsc::{self, Receiver as MpscReceiver},
        oneshot,
        watch::{self, Receiver as WatchReceiver, Sender as WatchSender},
    },
    timer::{self, Timeout},
};
use uuid::Uuid;

use crate::{
    codec::Codec,
    error::{Error, Result},
    types::{
        ClientControl, ConnectionState, ServerMessage, StateTransition, StateTransitionResult,
        Subscription,
    },
};

pub use crate::types::{
    error, Address, Authorization, ClientState, Connect, Info, Msg, ProtocolError, Sid, Subject,
};

/// The type of a [`Client`](struct.Client.html)'s [`delay_generator`](struct.Client.html#method.delay_generator).
///
/// # Arguments
/// * `client` - A reference to the `Client` that is trying to connect
/// * `attempts` - The number of previous connect attempts
/// * `addresses` - The number of addresses the `Client` is aware of
pub type DelayGenerator = Box<dyn Fn(Arc<Mutex<Client>>, u64, u64) -> Duration + Send>;

/// Generate a [`Client`](struct.Client.html)'s [`delay_generator`](struct.Client.html#method.delay_generator).
///
/// A `Client`s `delay_generator` provides complete flexibility in determining delays between
/// connect attempts. However, often a reasonable `delay_generator` can be produced by defining
/// the few values required by this function.
///
/// # Arguments
///
/// * `connect_series_attempts_before_cool_down` - A `connect_series` is an attempt to try all
/// addresses: those explicitly [specified](struct.Client.html#method.addresses_mut) and
/// those received in an `INFO` messages's [`connect_urls`](struct.Info.html#method.connect_urls).
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
    Box::new(
        move |_: Arc<Mutex<Client>>, connect_attempts: u64, addresses: u64| {
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
        },
    )
}

/// The [NATS](https://nats.io/) client.
pub struct Client {
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
    subscriptions: HashMap<Sid, Subscription>,
}

impl Client {
    /// Create a new `Client` with a default [`Connect`](struct.Connect.html).
    ///
    /// # Arguments
    ///
    /// * `addresses` - the list of addresses to try and establish a connection to a server
    pub fn new(addresses: Vec<Address>) -> Arc<Mutex<Self>> {
        Self::with_connect(addresses, Connect::new())
    }

    /// Create a new `Client` with the provided [`Connect`](struct.Connect.html).
    ///
    /// # Arguments
    ///
    /// * `addresses` - the list of addresses to try and establish a connection to a server
    pub fn with_connect(addresses: Vec<Address>, connect: Connect) -> Arc<Mutex<Self>> {
        let state = ConnectionState::Disconnected;
        let (state_tx, state_rx) = watch::channel((&state).into());
        let (info_tx, info_rx) = watch::channel(Info::new());
        let (ping_tx, ping_rx) = watch::channel(());
        let (pong_tx, pong_rx) = watch::channel(());
        let (ok_tx, ok_rx) = watch::channel(());
        let (err_tx, err_rx) = watch::channel(ProtocolError::UnknownProtocolOperation);
        Arc::new(Mutex::new(Self {
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
            subscriptions: HashMap::new(),
        }))
    }

    /// Get the current state of the `Client`.
    pub fn state(&self) -> ClientState {
        self.state_rx.get_ref().clone()
    }

    /// Get a watch stream of `Client` state transitions.
    pub fn state_stream(&self) -> WatchReceiver<ClientState> {
        self.state_rx.clone()
    }

    /// Get a the most recent [`Info`](struct.Info.html) sent from the server.
    pub fn info(&self) -> Info {
        self.info_rx.get_ref().clone()
    }

    /// Get a mutable reference to this `Client`'s [`Connect`](struct.Connect.html).
    pub fn connect_mut(&mut self) -> &mut Connect {
        &mut self.connect
    }

    /// Get a mutable reference to the list of addresses used to try and establish a connection to a
    /// server.
    pub fn addresses_mut(&mut self) -> &mut [Address] {
        &mut self.addresses
    }

    /// Get the configured TCP connect timeout. [default = `10s`]
    ///
    /// This is the timeout of a single connect attempt. It is not the timeout of the
    /// [`connect`](struct.Client.html#method.connect) future which has no internal timeout.
    pub fn tcp_connect_timeout(&self) -> Duration {
        self.tcp_connect_timeout
    }

    /// Set the TCP connect timeout.
    pub fn set_tcp_connect_timeout(&mut self, tcp_connect_timeout: Duration) -> &mut Self {
        self.tcp_connect_timeout = tcp_connect_timeout;
        self
    }

    /// Get the [`DelayGenerator`](type.DelayGenerator.html)
    ///
    /// The default generator is generated with [`generate_delay_generator`](function.generate_delay_generator.html)
    /// with the following parameters:
    ///
    /// * `connect_series_attempts_before_cool_down` = `3`
    /// * `connect_delay` = `0s`
    /// * `connect_series_delay` = `5s`
    /// * `cool_down` = `60s`
    pub fn delay_generator(&mut self) -> &DelayGenerator {
        &self.delay_generator
    }

    /// Set the [`DelayGenerator`](type.DelayGenerator.html)
    pub fn set_delay_generator(&mut self, delay_generator: DelayGenerator) -> &mut Self {
        self.delay_generator = delay_generator;
        self
    }

    /// Send a `CONNECT` message to the server using the configured [`Connect`](struct.Connect.html).
    ///
    /// **Note**: [`connect`](struct.Client.html#method.connect) automatically sends a `CONNECT`
    /// message. This is only needed in the case that you want to change the connection parameters
    /// after already establishing a connection.
    pub async fn send_connect(&mut self) -> Result<()> {
        if let ConnectionState::Connected(address, writer) = &mut self.state {
            Self::send_connect_with_writer(writer, &self.connect, address).await
        } else {
            Err(Error::NotConnected)
        }
    }

    /// Continuously try and connect.
    /// TODO: comments
    pub async fn connect(wrapped_client: Arc<Mutex<Self>>) {
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
                .chain(client.info_rx.get_ref().connect_urls().iter())
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
                    Arc::clone(&wrapped_client),
                    connect_attempts,
                    addresses_len,
                );
                debug!(
                    "Delaying for {}s after {} connect attempts with {} addresses",
                    delay.as_secs_f32(),
                    connect_attempts,
                    addresses_len
                );
                timer::delay(Instant::now() + delay).await;
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
            let connect = Timeout::new(
                TcpStream::connect(address.address()),
                client.tcp_connect_timeout,
            );
            let (reader, mut writer) = match connect.await {
                Ok(Ok(sink_and_stream)) => sink_and_stream.split(),
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

            // Wait for the first server message. It should always be an info message.
            let wait_for_info = Timeout::new(reader.next(), client.tcp_connect_timeout);
            match wait_for_info.await {
                Ok(Some(Ok(Ok(message)))) => {
                    if let ServerMessage::Info(info) = message {
                        client.handle_info_message(info);
                    } else {
                        error!(
                            "First message should be {} instead received '{:?}'",
                            util::INFO_OP_NAME,
                            message
                        );
                        debug_assert!(false);
                        continue;
                    }
                }
                Ok(Some(Ok(Err(e)))) => {
                    error!("Received invalid server message, err: {}", e);
                    continue;
                }
                Ok(Some(Err(e))) => {
                    error!("TCP socket error, err: {}", e);
                    continue;
                }
                Ok(None) => {
                    error!("TCP socket was disconnected");
                    continue;
                }
                Err(_) => {
                    error!("Timed out waiting for {} message", util::INFO_OP_NAME);
                    continue;
                }
            }

            // Send a connect message
            if let Err(e) =
                Self::send_connect_with_writer(&mut writer, &client.connect, &address).await
            {
                error!("Failed to send connect message, err: {}", e);
                continue;
            }

            // Resubscribe to subscriptions
            let mut failed_to_resubscribe = Vec::new();
            for (sid, subscription) in &client.subscriptions {
                if let Err(e) =
                    Self::write_line(&mut writer, ClientControl::Sub(subscription)).await
                {
                    error!(
                        "Failed to resubscribe to sid '{}' with subject '{}', err: {}",
                        sid, subscription.subject, e
                    );
                    failed_to_resubscribe.push(*sid);
                }
            }
            // Remove all subscriptions that failed to resubscribe
            client
                .subscriptions
                .retain(|sid, _| !failed_to_resubscribe.contains(&sid));

            // Spawn the task to handle reading subsequent server messages
            tokio::spawn(Self::type_erased_server_messages_handler(
                Arc::clone(&wrapped_client),
                reader,
            ));

            client.state_transition(StateTransition::ToConnected(writer));
            return;
        }
    }

    /// Disconnect from the server.
    ///
    /// This future will resolve when we are completely disconnected.
    pub async fn disconnect(wrapped_client: Arc<Mutex<Self>>) {
        let (tx, rx) = oneshot::channel();
        {
            let mut client = wrapped_client.lock().await;

            // If we are already disconnected do not disconnect again
            if let ConnectionState::Disconnected = client.state {
                return;
            }

            let mut state_stream = client.state_stream();
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

    /// TODO: comments
    pub async fn publish(&mut self, subject: &Subject, payload: &[u8]) -> Result<()> {
        self.publish_with_optional_reply(subject, None, payload)
            .await
    }

    /// TODO: comments
    pub async fn publish_with_reply(
        &mut self,
        subject: &Subject,
        reply_to: &Subject,
        payload: &[u8],
    ) -> Result<()> {
        self.publish_with_optional_reply(subject, Some(reply_to), payload)
            .await
    }

    /// TODO: comments
    pub async fn publish_with_optional_reply(
        &mut self,
        subject: &Subject,
        reply_to: Option<&Subject>,
        payload: &[u8],
    ) -> Result<()> {
        // Check that payload's length does not exceed the servers max_payload
        let max_payload = self.info().max_payload;
        if payload.len() > max_payload {
            return Err(Error::ExceedsMaxPayload {
                tried: payload.len(),
                limit: max_payload,
            });
        }
        if let ConnectionState::Connected(_, writer) = &mut self.state {
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

    /// TODO: comments
    pub async fn request(
        wrapped_client: Arc<Mutex<Self>>,
        subject: &Subject,
        payload: &[u8],
    ) -> Result<Msg> {
        // This uses the old method of request reply. It creates a temporary subscription that is
        // immediately unsubscribed from.
        // See https://github.com/nats-io/nats.go/issues/294 for a different implementation.
        let inbox_uuid = Uuid::new_v4();
        let reply_to = format!("{}.{}", util::INBOX_PREFIX, inbox_uuid.to_simple()).parse()?;
        let mut rx = {
            let mut client = wrapped_client.lock().await;
            let (sid, rx) = client.subscribe(&reply_to, 1).await?;
            client.unsubscribe_with_max_msgs(sid, 1).await?;
            client
                .publish_with_reply(subject, &reply_to, payload)
                .await?;
            rx
        };
        Ok(rx.next().await.ok_or(Error::NoResponse)?)
    }

    /// TODO: comments
    pub async fn subscribe(
        &mut self,
        subject: &Subject,
        buffer: usize,
    ) -> Result<(Sid, MpscReceiver<Msg>)> {
        self.subscribe_optional_queue_group(subject, None, buffer)
            .await
    }

    /// TODO: comments
    pub async fn subscribe_with_queue_group(
        &mut self,
        subject: &Subject,
        queue_group: &str,
        buffer: usize,
    ) -> Result<(Sid, MpscReceiver<Msg>)> {
        self.subscribe_optional_queue_group(subject, Some(queue_group), buffer)
            .await
    }

    /// TODO: comments
    pub async fn subscribe_optional_queue_group(
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
            let sid = subscription.sid;
            self.subscriptions.insert(sid, subscription);
            Ok((sid, rx))
        } else {
            Err(Error::NotConnected)
        }
    }

    /// TODO: comments
    pub async fn unsubscribe(&mut self, sid: Sid) -> Result<()> {
        self.unsubscribe_optional_max_msgs(sid, None).await
    }

    /// TODO: comments
    pub async fn unsubscribe_with_max_msgs(&mut self, sid: Sid, max_msgs: u64) -> Result<()> {
        self.unsubscribe_optional_max_msgs(sid, Some(max_msgs))
            .await
    }

    /// TODO: comments
    pub async fn unsubscribe_optional_max_msgs(
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

    /// Get a watch stream of all received `INFO` messages.
    pub fn info_stream(&mut self) -> WatchReceiver<Info> {
        self.info_rx.clone()
    }

    /// Get a watch stream of all received `PING` messages.
    pub fn ping_stream(&mut self) -> WatchReceiver<()> {
        self.ping_rx.clone()
    }

    /// Get a watch stream of all received `PONG` messages.
    pub fn pong_stream(&mut self) -> WatchReceiver<()> {
        self.pong_rx.clone()
    }

    /// Get a watch stream of all received `+OK` messages.
    pub fn ok_stream(&mut self) -> WatchReceiver<()> {
        self.ok_rx.clone()
    }

    /// Get a watch stream of all received `-ERR` messages.
    pub fn err_stream(&mut self) -> WatchReceiver<ProtocolError> {
        self.err_rx.clone()
    }

    /// Send a `PING` to the server.
    ///
    /// This method, coupled with a `pong_stream`, can be a useful way to check that the client is
    /// still connected to the server.
    pub async fn ping(&mut self) -> Result<()> {
        if let ConnectionState::Connected(_, writer) = &mut self.state {
            Self::write_line(writer, ClientControl::Ping).await?;
            Ok(())
        } else {
            Err(Error::NotConnected)
        }
    }

    /// Send a `PONG` to the server.
    ///
    /// Note, you do not have to manually send a `PONG` as part of the servers ping/pong keep
    /// alive. The client library automatically handles replying to pings. You should not need
    /// to use this method.
    pub async fn pong(&mut self) -> Result<()> {
        if let ConnectionState::Connected(_, writer) = &mut self.state {
            Self::write_line(writer, ClientControl::Pong).await?;
            Ok(())
        } else {
            Err(Error::NotConnected)
        }
    }

    /// Send a ping and wait for a pong from the server
    pub async fn ping_pong(wrapped_client: Arc<Mutex<Self>>) -> Result<()> {
        let mut pong_stream = {
            let mut client = wrapped_client.lock().await;
            let mut pong_stream = client.pong_stream();
            // Clear the current value
            pong_stream.next().now_or_never();
            client.ping().await?;
            pong_stream
        };
        pong_stream.next().await;
        Ok(())
    }

    async fn server_messages_handler(
        wrapped_client: Arc<Mutex<Self>>,
        mut reader: FramedRead<TcpStreamReadHalf, Codec>,
    ) {
        let disconnecting = Self::disconnecting(Arc::clone(&wrapped_client));
        pin_mut!(disconnecting);
        loop {
            // Select between the next message and disconnecting
            let message_result = match future::select(reader.next(), disconnecting).await {
                Either::Left((Some(message), unresolved_disconnecting)) => {
                    disconnecting = unresolved_disconnecting;
                    message
                }
                Either::Left((None, _)) => {
                    error!("TCP socket was disconnected");
                    break;
                }
                Either::Right(((), _)) => break,
            };

            // Handle the message if it was valid
            match message_result {
                Ok(Ok(message)) => {
                    Self::handle_server_message(Arc::clone(&wrapped_client), message).await;
                }
                Ok(Err(e)) => {
                    error!("Received invalid server message, err: {}", e);
                    continue;
                }
                Err(e) => {
                    error!("TCP socket error, err: {}", e);
                    break;
                }
            };
        }
        // If we make it out of the above loop, we somehow disconnected
        let mut client = wrapped_client.lock().await;

        // If we are not in the disconnecting state, we did not intentionally disconnect
        // and should try to reconnect.
        let should_reconnect = !client.state().is_disconnecting();

        // Transition to the disconnected state and try and shutdown the TCP connection
        if let StateTransitionResult::Writer(writer) =
            client.state_transition(StateTransition::ToDisconnected)
        {
            match reader.into_inner().reunite(writer) {
                Ok(tcp) => {
                    if let Err(e) = tcp.shutdown(Shutdown::Both) {
                        if e.kind() != ErrorKind::NotConnected {
                            error!("Failed to shutdown TCP stream, err: {}", e);
                        }
                    }
                }
                Err(e) => error!("Failed to reunite TCP stream, err: {}", e),
            }
        } else {
            // We should always have a writer to close
            error!("Disconnected with no TCP writer. Unable to shutdown TCP stream.");
            debug_assert!(false);
        }

        if should_reconnect {
            tokio::spawn(Self::connect(Arc::clone(&wrapped_client)));
        }
    }

    #[allow(clippy::cognitive_complexity)]
    async fn handle_server_message(wrapped_client: Arc<Mutex<Self>>, message: ServerMessage) {
        match message {
            ServerMessage::Info(info) => {
                wrapped_client.lock().await.handle_info_message(info);
            }
            ServerMessage::Msg(msg) => {
                // Parse the sid
                let sid_str = &msg.sid();
                let sid_result = sid_str.parse::<Sid>();
                let sid = match sid_result {
                    Ok(sid) => sid,
                    Err(_) => {
                        // This should never happen as the only sid this client uses is of type
                        // `Sid`
                        error!("Received unknown sid '{}'", sid_str);
                        debug_assert!(false);
                        return;
                    }
                };
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
                let subject = msg.subject().clone();
                if let Err(e) = subscription.tx.try_send(msg) {
                    // If we could not send because the receiver is closed, we no longer
                    // care about this subscription and should unsubscribe
                    if e.is_closed() {
                        let wrapped_client = Arc::clone(&wrapped_client);
                        tokio::spawn(async move {
                            info!("Unsubscribing from closed sid '{}'", sid);
                            let mut client = wrapped_client.lock().await;
                            if let Err(e) = client.unsubscribe(sid).await {
                                error!("Failed to unsubscribe from sid '{}', err: {}", sid, e);
                            }
                        });
                    } else {
                        error!(
                            "Failed to send msg to sid '{}' with subject '{}', err: {}",
                            sid, subject, e
                        );
                    }
                }
                // If we have received all the messages we were waiting for, unsubscribe
                if let Some(unsubscribe_after) = &mut subscription.unsubscribe_after {
                    *unsubscribe_after -= 1;
                    if *unsubscribe_after == 0 {
                        client.subscriptions.remove(&sid);
                    }
                }
            }
            ServerMessage::Ping => {
                if let Err(e) = wrapped_client.lock().await.ping_tx.broadcast(()) {
                    error!("Failed to broadcast {}, err: {}", util::PING_OP_NAME, e);
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
                if let Err(e) = wrapped_client.lock().await.pong_tx.broadcast(()) {
                    error!("Failed to broadcast {}, err: {}", util::PONG_OP_NAME, e);
                }
            }
            ServerMessage::Ok => {
                if let Err(e) = wrapped_client.lock().await.ok_tx.broadcast(()) {
                    error!("Failed to broadcast {}, err: {}", util::OK_OP_NAME, e);
                }
            }
            ServerMessage::Err(e) => {
                error!("Protocol error, err: '{}'", e);
                if let Err(e) = wrapped_client.lock().await.err_tx.broadcast(e) {
                    error!("Failed to broadcast {}, err: {}", util::ERR_OP_NAME, e);
                }
            }
        }
    }

    fn handle_info_message(&mut self, info: Info) {
        if let Err(e) = self.info_tx.broadcast(info) {
            error!("Failed to broadcast {}, err: {}", util::INFO_OP_NAME, e);
        }
    }

    // We have to erase the type of `server_message_handler` in order to avoid a recursive future
    //
    // https://github.com/rust-lang/rust/issues/53690
    fn type_erased_server_messages_handler(
        wrapped_client: Arc<Mutex<Self>>,
        reader: FramedRead<TcpStreamReadHalf, Codec>,
    ) -> impl std::future::Future<Output = ()> + Send {
        Self::server_messages_handler(wrapped_client, reader)
    }

    // Create a future that waits for the disconnecting state.
    async fn disconnecting(wrapped_client: Arc<Mutex<Self>>) {
        let mut state_stream = wrapped_client.lock().await.state_stream();
        while let Some(state) = state_stream.next().await {
            if state.is_disconnecting() {
                break;
            }
        }
    }

    async fn write_line(
        writer: &mut TcpStreamWriteHalf,
        control_line: ClientControl<'_>,
    ) -> Result<()> {
        let line = control_line.to_line();
        Ok(writer.write_all(line.as_bytes()).await?)
    }

    async fn send_connect_with_writer(
        writer: &mut TcpStreamWriteHalf,
        connect: &Connect,
        address: &Address,
    ) -> Result<()> {
        let mut connect = connect.clone();
        // If the address has authorization information, override the default authorization
        if let Some(authorization) = address.authorization() {
            connect.set_authorization(Some(authorization.clone()));
        }
        Self::write_line(writer, ClientControl::Connect(&connect)).await?;
        Ok(())
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
        // If we can not broadcast the state transition, we could end up in an inconsistent
        // state. This would be very bad so instead panic. This should never happen.
        self.state_tx
            .broadcast(next_client_state)
            .expect("to broadcast state transition");
        result
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // TODO: Disconnect on drop
        trace!("Client was dropped");
    }
}

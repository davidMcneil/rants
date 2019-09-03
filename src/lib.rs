mod codec;
mod constants;
#[cfg(test)]
mod tests;
mod types;

use futures::{
    future::{self, Either},
    lock::Mutex,
    stream::StreamExt,
};
use log::{error, info};
use pin_utils::pin_mut;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::{
    codec::FramedRead,
    io::AsyncWriteExt,
    net::tcp::{split::TcpStreamReadHalf, TcpStream},
    sync::mpsc::{self, Receiver as MpscReceiver},
    sync::oneshot,
    sync::watch::{self, Receiver as WatchReceiver, Sender as WatchSender},
};

use crate::{
    codec::Codec,
    types::{
        ClientControl, ClientState, Connect, ConnectionState, Info, ProtocolError, RantsError,
        RantsResult, ServerMessage, Sid, Subject, Subscription,
    },
};

/// The [NATS](https://nats.io/) client.
pub struct Client {
    address: SocketAddr,
    info: Info,
    connect: Connect,
    state: ConnectionState,
    state_tx: WatchSender<ClientState>,
    state_rx: WatchReceiver<ClientState>,
    ping_tx: WatchSender<()>,
    ping_rx: WatchReceiver<()>,
    pong_tx: WatchSender<()>,
    pong_rx: WatchReceiver<()>,
    ok_tx: WatchSender<()>,
    ok_rx: WatchReceiver<()>,
    err_tx: WatchSender<ProtocolError>,
    err_rx: WatchReceiver<ProtocolError>,
    subscriptions: HashMap<Sid, Subscription>,
}

impl Client {
    /// Create a new `Client` with a default [Connect](struct.Connect.html).
    pub fn new(addr: &str) -> Arc<Mutex<Self>> {
        Self::with_connect(addr, Connect::new())
    }

    /// Create a new `Client` with the provided [Connect](struct.Connect.html).
    pub fn with_connect(addr: &str, connect: Connect) -> Arc<Mutex<Self>> {
        let address = addr.parse::<SocketAddr>().expect("TODO");
        let state = ConnectionState::Disconnected;
        let (state_tx, state_rx) = watch::channel((&state).into());
        let (ping_tx, ping_rx) = watch::channel(());
        let (pong_tx, pong_rx) = watch::channel(());
        let (ok_tx, ok_rx) = watch::channel(());
        let (err_tx, err_rx) = watch::channel(ProtocolError::UnknownProtocolOperation);
        Arc::new(Mutex::new(Self {
            address,
            info: Info::new(),
            connect,
            state,
            state_tx,
            state_rx,
            ping_tx,
            ping_rx,
            pong_tx,
            pong_rx,
            ok_tx,
            ok_rx,
            err_tx,
            err_rx,
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

    /// Get a reference to the current [Info](struct.Connect.html) sent from the server.
    pub fn info(&self) -> &Info {
        &self.info
    }

    /// Get a mutable reference to the current [Connect](struct.Connect.html) for this client.
    pub fn connect_mut(&mut self) -> &mut Connect {
        &mut self.connect
    }

    /// Send a connect message to the server using the current [Connect](struct.Connect.html).
    pub async fn send_connect(&mut self) -> RantsResult<()> {
        if let ConnectionState::Connected(_, writer) = &mut self.state {
            let line = ClientControl::Connect(&self.connect).to_line();
            writer.write_all(line.as_bytes()).await?;
            Ok(())
        } else {
            Err(RantsError::NotConnected)
        }
    }

    /// Continuously try and connect.
    /// TODO: comments
    pub async fn connect(wrapped_client: Arc<Mutex<Self>>) {
        let mut client = wrapped_client.lock().await;
        // If we are already connected do not connect again
        if let ConnectionState::Connected(_, _) = client.state {
            return;
        }

        // Continuously try and connect
        let sink_and_stream = loop {
            client.state_transition(ConnectionState::Connecting);
            // TODO: try all possible addresses
            // TODO: handle disconnect while trying to connect
            match TcpStream::connect(&client.address).await {
                Ok(sink_and_stream) => break sink_and_stream,
                Err(e) => {
                    client.state_transition(ConnectionState::Disconnected);
                    error!("Failed to reconnect, err: {}", e);
                }
            }
            // TODO: delay, timeout, back off, circuit breaker
        };

        // Store the writer half of the tcp stream and spawn the server messages handler with
        // the reader
        let (reader, writer) = sink_and_stream.split();
        let connected = ConnectionState::Connected(client.address.clone(), writer);
        // TODO: only transition after successfully sending connect and receiving info
        client.state_transition(connected);
        // TODO: reconnect subscriptions
        tokio::spawn(Self::type_erased_server_messages_handler(
            Arc::clone(&wrapped_client),
            reader,
        ));

        if let Err(e) = client.send_connect().await {
            error!("Failed to send connect message, err: {}", e);
        }
    }

    /// Disconnect from the server.
    ///
    /// This future will resolve when we are completely disconnected.
    pub async fn disconnect(wrapped_client: Arc<Mutex<Self>>) {
        let (tx, rx) = oneshot::channel();
        {
            let mut client = wrapped_client.lock().await;
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
            // breaking out of any continuos loops.
            client.state_transition(ConnectionState::Disconnecting);
        }
        // Wait till we are disconnected
        rx.await.expect("to receive disconnect signal");
    }

    /// TODO: comments
    pub async fn publish(&mut self, subject: &Subject, payload: &[u8]) -> RantsResult<()> {
        self.publish_with_optional_reply(subject, None, payload)
            .await
    }

    /// TODO: comments
    pub async fn publish_with_reply(
        &mut self,
        subject: &Subject,
        reply_to: &Subject,
        payload: &[u8],
    ) -> RantsResult<()> {
        self.publish_with_optional_reply(subject, Some(reply_to), payload)
            .await
    }

    /// TODO: comments
    pub async fn publish_with_optional_reply(
        &mut self,
        subject: &Subject,
        reply_to: Option<&Subject>,
        payload: &[u8],
    ) -> RantsResult<()> {
        if let ConnectionState::Connected(_, writer) = &mut self.state {
            let line = ClientControl::Pub(subject, reply_to, payload.len()).to_line();
            writer.write_all(line.as_bytes()).await?;
            writer.write_all(payload).await?;
            writer
                .write_all(constants::MESSAGE_TERMINATOR.as_bytes())
                .await?;
            Ok(())
        } else {
            Err(RantsError::NotConnected)
        }
    }

    /// TODO: comments
    pub async fn request_reply(&self) -> RantsResult<Vec<u8>> {
        unimplemented!()
    }

    /// Send a `PING` to the server.
    ///
    /// This method, coupled with a `pong_stream`, can be a useful way to check that the client is
    /// still connected to the server.
    pub async fn ping(&mut self) -> RantsResult<()> {
        if let ConnectionState::Connected(_, writer) = &mut self.state {
            let line = ClientControl::Ping.to_line();
            writer.write_all(line.as_bytes()).await?;
            Ok(())
        } else {
            Err(RantsError::NotConnected)
        }
    }

    /// Send a `PONG` to the server.
    ///
    /// Note, you do not have to manually send a `PONG` as part of the servers ping/pong keep
    /// alive. The client library automatically handles replying to pings. You should not need
    /// to use this method.
    pub async fn pong(&mut self) -> RantsResult<()> {
        if let ConnectionState::Connected(_, writer) = &mut self.state {
            let line = ClientControl::Pong.to_line();
            writer.write_all(line.as_bytes()).await?;
            Ok(())
        } else {
            Err(RantsError::NotConnected)
        }
    }

    /// TODO: comments
    pub async fn subscribe(
        &mut self,
        subject: &Subject,
        buffer: usize,
    ) -> RantsResult<MpscReceiver<Vec<u8>>> {
        self.subscribe_optional_queue_group(subject, None, buffer)
            .await
    }

    /// TODO: comments
    pub async fn subscribe_with_queue_group(
        &mut self,
        subject: &Subject,
        queue_group: &str,
        buffer: usize,
    ) -> RantsResult<MpscReceiver<Vec<u8>>> {
        self.subscribe_optional_queue_group(subject, Some(queue_group), buffer)
            .await
    }

    /// TODO: comments
    pub async fn subscribe_optional_queue_group(
        &mut self,
        subject: &Subject,
        queue_group: Option<&str>,
        buffer: usize,
    ) -> RantsResult<MpscReceiver<Vec<u8>>> {
        if let ConnectionState::Connected(_, writer) = &mut self.state {
            let (tx, rx) = mpsc::channel(buffer);
            let subscription =
                Subscription::new(subject.clone(), queue_group.map(String::from), tx);
            let line = ClientControl::Sub(&subscription).to_line();
            writer.write_all(line.as_bytes()).await?;
            self.subscriptions.insert(subscription.sid, subscription);
            Ok(rx)
        } else {
            Err(RantsError::NotConnected)
        }
    }

    /// TODO: comments
    pub async fn unsubscribe(&mut self, sid: Sid) -> RantsResult<()> {
        self.unsubscribe_optional_max_msgs(sid, None).await
    }

    /// TODO: comments
    pub async fn unsubscribe_with_max_msgs(&mut self, sid: Sid, max_msgs: u64) -> RantsResult<()> {
        self.unsubscribe_optional_max_msgs(sid, Some(max_msgs))
            .await
    }

    /// TODO: comments
    pub async fn unsubscribe_optional_max_msgs(
        &mut self,
        sid: Sid,
        max_msgs: Option<u64>,
    ) -> RantsResult<()> {
        if let ConnectionState::Connected(_, writer) = &mut self.state {
            let line = ClientControl::Unsub(sid, max_msgs).to_line();
            writer.write_all(line.as_bytes()).await?;
            self.subscriptions.remove(&sid);
            Ok(())
        } else {
            Err(RantsError::NotConnected)
        }
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

    async fn server_messages_handler(wrapped_client: Arc<Mutex<Self>>, reader: TcpStreamReadHalf) {
        let mut reader = FramedRead::new(reader, Codec::new());
        let mut state_stream = {
            let client = wrapped_client.lock().await;
            client.state_stream()
        };
        let disconnecting = Self::disconnecting(&mut state_stream);
        pin_mut!(disconnecting);
        loop {
            // Select between the next message and disconnecting
            let either = future::select(reader.next(), disconnecting).await;
            let message = match either {
                Either::Left((Some(message), d)) => {
                    disconnecting = d;
                    message
                }
                Either::Left((None, _)) => {
                    error!("TCP socket was disconnected");
                    break;
                }
                Either::Right(((), _)) => break,
            };

            // The codec never returns an error
            let message = message.expect("never codec error");

            // Check that we did not receive an invalid message
            let message = match message {
                Ok(message) => message,
                Err(e) => {
                    error!("Received invalid server message, err: {}", e);
                    continue;
                }
            };
            // Handle the different types of messages we could receive
            match message {
                ServerMessage::Info(info) => {
                    wrapped_client.lock().await.info = info;
                }
                ServerMessage::Msg(msg) => {
                    // Parse the sid
                    let sid_str = msg.sid;
                    let sid_result = sid_str.parse::<Sid>();
                    let sid = match sid_result {
                        Ok(sid) => sid,
                        Err(_) => {
                            // This should never happen as the only sid this client uses is of type
                            // `Sid`
                            error!("Received unknown sid '{}'", sid_str);
                            debug_assert!(false);
                            continue;
                        }
                    };
                    let mut client = wrapped_client.lock().await;
                    let subscription = match client.subscriptions.get_mut(&sid) {
                        Some(subscription) => subscription,
                        None => {
                            // If we do not know about this subscription, log an error and
                            // unsubscribe. This should never happen and means our subscription
                            // store got out of sync.
                            error!("Received unknown sid '{}'", sid_str);
                            debug_assert!(false);
                            let wrapped_client = Arc::clone(&wrapped_client);
                            tokio::spawn(async move {
                                info!("Unsubscribing from unknown sid '{}'", sid_str);
                                let mut client = wrapped_client.lock().await;
                                if let Err(e) = client.unsubscribe(sid).await {
                                    error!("Failed to unsubscribe from '{}', err: {}", sid, e);
                                }
                            });
                            continue;
                        }
                    };
                    // Try and send the message to the subscription receiver
                    if let Err(e) = subscription.tx.try_send(msg.payload) {
                        // If we could not send because the receiver is closed, we no longer
                        // care about this subscription and should unsubscribe
                        if e.is_closed() {
                            let wrapped_client = Arc::clone(&wrapped_client);
                            tokio::spawn(async move {
                                info!("Unsubscribing from closed sid '{}'", sid_str);
                                let mut client = wrapped_client.lock().await;
                                if let Err(e) = client.unsubscribe(sid).await {
                                    error!("Failed to unsubscribe from '{}', err: {}", sid, e);
                                }
                            });
                        } else {
                            error!(
                                "Failed to send msg with sid '{}' and subject '{}', err: {}",
                                sid, msg.subject, e
                            );
                        }
                    }
                }
                ServerMessage::Ping => {
                    if let Err(e) = wrapped_client.lock().await.ping_tx.broadcast(()) {
                        error!(
                            "Failed to broadcast {}, err: {}",
                            constants::PING_OP_NAME,
                            e
                        );
                    }
                    // Spawn a task to send a pong replying to the ping
                    let wrapped_client = Arc::clone(&wrapped_client);
                    tokio::spawn(async move {
                        let mut client = wrapped_client.lock().await;
                        if let Err(e) = client.pong().await {
                            error!("Failed to send {}, err: {}", constants::PONG_OP_NAME, e);
                        }
                    });
                }
                ServerMessage::Pong => {
                    if let Err(e) = wrapped_client.lock().await.pong_tx.broadcast(()) {
                        error!(
                            "Failed to broadcast {}, err: {}",
                            constants::PONG_OP_NAME,
                            e
                        );
                    }
                }
                ServerMessage::Ok => {
                    if let Err(e) = wrapped_client.lock().await.ok_tx.broadcast(()) {
                        error!("Failed to broadcast {}, err: {}", constants::OK_OP_NAME, e);
                    }
                }
                ServerMessage::Err(e) => {
                    error!("Protocol error, err: '{}'", e);
                    if let Err(e) = wrapped_client.lock().await.err_tx.broadcast(e) {
                        error!("Failed to broadcast {}, err: {}", constants::ERR_OP_NAME, e);
                    }
                }
            }
        }
        // TODO: combine the reader and writer and disconnect
        // Handle a disconnect
        let mut client = wrapped_client.lock().await;
        if let ConnectionState::Disconnecting = client.state {
            // We intentionally disconnected
            client.state_transition(ConnectionState::Disconnected);
        } else {
            // A network error occurred try to reconnect
            client.state_transition(ConnectionState::Disconnected);
            tokio::spawn(Self::connect(Arc::clone(&wrapped_client)));
        }
    }

    // We have to erase the type of `server_message_handler` in order to avoid a recursive future
    //
    // https://github.com/rust-lang/rust/issues/53690
    fn type_erased_server_messages_handler(
        wrapped_client: Arc<Mutex<Self>>,
        reader: TcpStreamReadHalf,
    ) -> impl std::future::Future<Output = ()> + Send {
        Self::server_messages_handler(wrapped_client, reader)
    }

    // Create a future that waits for the disconnecting state. We can only transition to
    // disconnecting from the connecting or connected states.
    async fn disconnecting(state_stream: &mut WatchReceiver<ClientState>) {
        // Immediatly remove the current state from the watch stream.
        let state = state_stream.next().await;
        debug_assert!({
            let state = state.unwrap();
            state.is_connecting() || state.is_connected()
        });
        // The next state is always a transition to the disconnecting state
        let state = state_stream.next().await;
        debug_assert!(state.unwrap().is_disconnecting());
    }

    fn state_transition(&mut self, state: ConnectionState) {
        self.state = state;
        let client_state = (&self.state).into();
        info!("Transitioned to state '{:?}'", client_state);
        // If we can not broadcast the state transition, we would end up in an inconsistent
        // state. This would be very bad so instead panic. This should never happen.
        self.state_tx
            .broadcast(client_state)
            .expect("to broadcast state transition");
    }
}

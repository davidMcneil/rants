mod codec;
mod constants;
#[cfg(test)]
mod tests;
mod types;

use futures::{lock::Mutex, stream::StreamExt};
use log::error;
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    codec::FramedRead,
    io::AsyncWriteExt,
    net::tcp::{split::TcpStreamReadHalf, TcpStream},
    sync::watch::{self, Receiver as WatchReceiver, Sender as WatchSender},
};

use crate::{
    codec::Codec,
    types::{ConnectionState, RantsResult, ServerMessage, Subject},
};

pub use crate::types::{ClientState, Connect, Info, ProtocolError, RantsError};

pub struct Client {
    address: SocketAddr,
    info: Info,
    connect: Connect,
    state: ConnectionState,
    state_sender: WatchSender<ClientState>,
    state_receiver: WatchReceiver<ClientState>,
    ping_sender: WatchSender<()>,
    ping_receiver: WatchReceiver<()>,
    pong_sender: WatchSender<()>,
    pong_receiver: WatchReceiver<()>,
    ok_sender: WatchSender<()>,
    ok_receiver: WatchReceiver<()>,
    err_sender: WatchSender<ProtocolError>,
    err_receiver: WatchReceiver<ProtocolError>,
}

impl Client {
    pub fn new(addr: &str) -> Arc<Mutex<Self>> {
        Self::with_connect(addr, Connect::new())
    }

    pub fn with_connect(addr: &str, connect: Connect) -> Arc<Mutex<Self>> {
        let address = addr.parse::<SocketAddr>().expect("TODO");
        let state = ConnectionState::Disconnected;
        let (state_sender, state_receiver) = watch::channel((&state).into());
        let (ping_sender, ping_receiver) = watch::channel(());
        let (pong_sender, pong_receiver) = watch::channel(());
        let (ok_sender, ok_receiver) = watch::channel(());
        let (err_sender, err_receiver) = watch::channel(ProtocolError::UnknownProtocolOperation);
        Arc::new(Mutex::new(Self {
            address,
            info: Info::new(),
            connect,
            state,
            state_sender,
            state_receiver,
            ping_sender,
            ping_receiver,
            pong_sender,
            pong_receiver,
            ok_sender,
            ok_receiver,
            err_sender,
            err_receiver,
        }))
    }

    pub fn state(&self) -> ClientState {
        self.state_receiver.get_ref().clone()
    }

    pub fn state_stream(&self) -> WatchReceiver<ClientState> {
        self.state_receiver.clone()
    }

    pub fn info(&self) -> &Info {
        &self.info
    }

    pub fn connect_mut(&mut self) -> &mut Connect {
        &mut self.connect
    }

    pub async fn send_connect(&mut self) -> RantsResult<()> {
        if let ConnectionState::Connected(_, writer) = &mut self.state {
            let message = format!(
                "{} {}{}",
                constants::CONNECT_OP_NAME,
                serde_json::to_string(&self.connect).expect("to serialize Connect"),
                constants::MESSAGE_TERMINATOR
            );
            writer.write_all(message.as_bytes()).await?;
            Ok(())
        } else {
            Err(RantsError::NotConnected)
        }
    }

    pub async fn connect(client_wrapper: Arc<Mutex<Self>>) {
        let mut client = client_wrapper.lock().await;
        // If we are already connected do not connect again
        if let ConnectionState::Connected(_, _) = client.state {
            client.state_transition(ConnectionState::Connecting);
            // Continuously try and connect
            let sink_and_stream = loop {
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
            client.state_transition(connected);
            tokio::spawn(Self::type_erased_server_messages_handler(
                Arc::clone(&client_wrapper),
                reader,
            ));
        }

        if let Err(e) = client.send_connect().await {
            error!("Failed to send connect message, err: {}", e);
        }
    }

    pub async fn disconnect(&mut self) -> RantsResult<()> {
        self.state_transition(ConnectionState::Disconnecting);
        unimplemented!()
    }

    pub async fn publish(&mut self, subject: &Subject, payload: &[u8]) -> RantsResult<()> {
        self.publish_with_optional_reply(subject, None, payload)
            .await
    }

    pub async fn publish_with_reply(
        &mut self,
        subject: &Subject,
        reply_to: &Subject,
        payload: &[u8],
    ) -> RantsResult<()> {
        self.publish_with_optional_reply(subject, Some(reply_to), payload)
            .await
    }

    pub async fn publish_with_optional_reply(
        &mut self,
        subject: &Subject,
        reply_to: Option<&Subject>,
        payload: &[u8],
    ) -> RantsResult<()> {
        if let ConnectionState::Connected(_, writer) = &mut self.state {
            let control_line = if let Some(reply_to) = reply_to {
                format!(
                    "{} {} {} {}{}",
                    constants::PUB_OP_NAME,
                    subject,
                    reply_to,
                    payload.len(),
                    constants::MESSAGE_TERMINATOR
                )
            } else {
                format!(
                    "{} {} {}{}",
                    constants::PUB_OP_NAME,
                    subject,
                    payload.len(),
                    constants::MESSAGE_TERMINATOR
                )
            };
            writer.write_all(control_line.as_bytes()).await?;
            writer.write_all(payload).await?;
            writer
                .write_all(constants::MESSAGE_TERMINATOR.as_bytes())
                .await?;
            Ok(())
        } else {
            Err(RantsError::NotConnected)
        }
    }

    pub async fn request_reply(&self) -> RantsResult<Vec<u8>> {
        unimplemented!()
    }

    pub async fn ping(&mut self) -> RantsResult<()> {
        if let ConnectionState::Connected(_, writer) = &mut self.state {
            let message = format!(
                "{}{}",
                constants::PING_OP_NAME,
                constants::MESSAGE_TERMINATOR
            );
            writer.write_all(message.as_bytes()).await?;
            Ok(())
        } else {
            Err(RantsError::NotConnected)
        }
    }

    pub async fn pong(&mut self) -> RantsResult<()> {
        if let ConnectionState::Connected(_, writer) = &mut self.state {
            let message = format!(
                "{}{}",
                constants::PONG_OP_NAME,
                constants::MESSAGE_TERMINATOR
            );
            writer.write_all(message.as_bytes()).await?;
            Ok(())
        } else {
            Err(RantsError::NotConnected)
        }
    }

    pub async fn watch_subscribe(&self) -> RantsResult<()> {
        unimplemented!()
    }

    pub async fn channel_subscribe(&self) -> RantsResult<()> {
        unimplemented!()
    }

    pub async fn unsubscribe(&self) -> RantsResult<()> {
        unimplemented!()
    }

    pub fn ping_stream(&mut self) -> WatchReceiver<()> {
        self.ping_receiver.clone()
    }

    pub fn pong_stream(&mut self) -> WatchReceiver<()> {
        self.pong_receiver.clone()
    }

    pub fn ok_stream(&mut self) -> WatchReceiver<()> {
        self.ok_receiver.clone()
    }

    pub fn err_stream(&mut self) -> WatchReceiver<ProtocolError> {
        self.err_receiver.clone()
    }

    async fn server_messages_handler(client_wrapper: Arc<Mutex<Self>>, reader: TcpStreamReadHalf) {
        let mut reader = FramedRead::new(reader, Codec::new());
        while let Some(message) = reader.next().await {
            // The codec can never return an error
            let message = message.expect("never codec error");

            // Check that we did not receive an invalid message
            if let Err(e) = message {
                error!("Received invalid server message, err: {}", e);
                continue;
            }
            match message.expect("valid message") {
                ServerMessage::Info(info) => {
                    client_wrapper.lock().await.info = info;
                }
                ServerMessage::Msg(msg) => println!("{:?}", msg),
                ServerMessage::Ping => {
                    if let Err(e) = client_wrapper.lock().await.ping_sender.broadcast(()) {
                        error!(
                            "Failed to broadcast {}, err: {}",
                            constants::PING_OP_NAME,
                            e
                        );
                    }
                    // Spawn a task to send a pong replying to the ping
                    let client_wrapper = Arc::clone(&client_wrapper);
                    tokio::spawn(async move {
                        let mut client = client_wrapper.lock().await;
                        if let Err(e) = client.pong().await {
                            error!("Failed to send {}, err: {}", constants::PONG_OP_NAME, e);
                        }
                    });
                }
                ServerMessage::Pong => {
                    if let Err(e) = client_wrapper.lock().await.pong_sender.broadcast(()) {
                        error!(
                            "Failed to broadcast {}, err: {}",
                            constants::PONG_OP_NAME,
                            e
                        );
                    }
                }
                ServerMessage::Ok => {
                    if let Err(e) = client_wrapper.lock().await.ok_sender.broadcast(()) {
                        error!("Failed to broadcast {}, err: {}", constants::OK_OP_NAME, e);
                    }
                }
                ServerMessage::Err(e) => {
                    error!("Protocol error, err: '{}'", e);
                    if let Err(e) = client_wrapper.lock().await.err_sender.broadcast(e) {
                        error!("Failed to broadcast {}, err: {}", constants::ERR_OP_NAME, e);
                    }
                }
            }
        }
        // If we make it here the tcp connection was somehow disconnected
        let mut client = client_wrapper.lock().await;
        if let ConnectionState::Disconnecting = client.state {
            // We intentionally disconnected
            client.state_transition(ConnectionState::Disconnected);
        } else {
            // A network error occurred try to reconnect
            client.state_transition(ConnectionState::Disconnected);
            Self::connect(Arc::clone(&client_wrapper)).await;
        }
    }

    // We have to type erase the `server_message_handler` in order to avoid a recursive future
    //
    // https://github.com/rust-lang/rust/issues/53690
    fn type_erased_server_messages_handler(
        client_wrapper: Arc<Mutex<Self>>,
        reader: TcpStreamReadHalf,
    ) -> impl std::future::Future<Output = ()> + Send {
        Self::server_messages_handler(client_wrapper, reader)
    }

    fn state_transition(&mut self, state: ConnectionState) {
        self.state = state;
        if let Err(e) = self.state_sender.broadcast((&self.state).into()) {
            error!("Failed to broadcast state transition, err: {}", e);
        }
    }
}

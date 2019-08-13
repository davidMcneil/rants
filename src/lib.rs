#![feature(async_await)]

mod constants;
mod types;

use futures::lock::Mutex;
use std::{net::SocketAddr,
          sync::Arc};
use tokio::{codec::{FramedRead,
                    LinesCodec},
            io::AsyncWriteExt,
            net::tcp::{split::{TcpStreamReadHalf,
                               TcpStreamWriteHalf},
                       TcpStream},
            stream::StreamExt};

use crate::{constants::MESSAGE_TERMINATOR,
            types::{header_for_publish_message,
                    Connect,
                    ConnectionState,
                    Subject}};

pub use crate::types::ServerMessage;

pub struct Gnat {
    addr:    SocketAddr,
    connect: Connect,
    state:   ConnectionState,
    reader:  TcpStreamReadHalf,
    writer:  TcpStreamWriteHalf,
}

impl Gnat {
    pub async fn new(addr: &str) -> Result<Arc<Mutex<Gnat>>, Box<dyn std::error::Error>> {
        let addr = addr.parse::<SocketAddr>()?;
        let stream = TcpStream::connect(&addr).await?;
        let (mut reader, writer) = stream.split();
        let mut client = Gnat { addr,
                                connect: Connect::default(),
                                state: ConnectionState::Connected,
                                reader,
                                writer };
        client.connect().await?;
        let client = Arc::new(Mutex::new(client));

        Ok(client)
    }

    pub async fn connect(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let message = self.connect.as_message();
        self.writer.write_all(&message.as_bytes()).await?;
        Ok(())
    }

    pub async fn publish(&mut self,
                         subject: &Subject,
                         reply_to: &Option<&Subject>,
                         payload: &[u8])
                         -> Result<(), Box<dyn std::error::Error>> {
        let len = payload.len();
        let header = header_for_publish_message(&subject, reply_to, len);
        self.writer.write_all(&header.as_bytes()).await?;
        self.writer.write_all(payload).await?;
        self.writer.write_all(MESSAGE_TERMINATOR.as_bytes()).await?;
        Ok(())
    }

    pub async fn publish_with_reply(&mut self,
                                    subject: &Subject,
                                    reply_to: &Subject,
                                    payload: &[u8])
                                    -> Result<(), Box<dyn std::error::Error>> {
        let reply_to = Some(reply_to);
        self.publish(subject, &reply_to, payload).await
    }

    pub async fn publish_no_reply(&mut self,
                                  subject: &Subject,
                                  payload: &[u8])
                                  -> Result<(), Box<dyn std::error::Error>> {
        self.publish(subject, &None, payload).await
    }

    // TODO
    pub async fn subscribe(&self) -> Result<(), Box<dyn std::error::Error>> { Ok(()) }

    // TODO
    pub async fn unsubscribe(&self) -> Result<(), Box<dyn std::error::Error>> { Ok(()) }

    // TODO
    fn server_messages_handler(&self) -> Result<(), Box<dyn std::error::Error>> { Ok(()) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{future::{self,
                           *},
                  lock::Mutex};
    use rand::Rng;
    use std::{convert::TryFrom,
              sync::Arc,
              thread,
              time};
    use tokio::{runtime::current_thread::Runtime,
                timer::Delay};
    #[test]
    fn it_works() {
        let f = async {
            let mut rng = rand::thread_rng();
            let client = Gnat::new("127.0.0.1:4222").await.unwrap();
            for i in 0..10 {
                let client = client.clone();
                let delay = rng.gen_range(0, 100);
                tokio::spawn(async move {
                    let s = Subject::try_from("test").unwrap();
                    let message = format!("this is iteration {}", i).into_bytes();
                    let till = time::Instant::now() + time::Duration::from_millis(delay);
                    Delay::new(till).await;
                    let mut lock = client.lock().await;
                    lock.publish_with_reply(&s, &s, &message).await.unwrap();
                });
            }
        };
        Runtime::new().unwrap().spawn(f).run().unwrap();
    }
}

#![feature(async_await)]

mod codec;
mod constants;
mod types;

use futures::{lock::Mutex,
              sink::SinkExt,
              stream::{SplitSink,
                       SplitStream,
                       StreamExt}};
use std::{net::SocketAddr,
          sync::Arc};
use tokio::{codec::Framed,
            io::AsyncWriteExt,
            net::tcp::TcpStream};

use crate::{codec::Codec,
            types::{ClientMessage,
                    Connect,
                    ConnectionState,
                    Msg,
                    Subject}};

pub struct Gnat {
    addr:    SocketAddr,
    connect: Connect,
    state:   ConnectionState,
    writer:  SplitSink<Framed<TcpStream, Codec>, ClientMessage>,
    reader:  SplitStream<Framed<TcpStream, Codec>>,
}

impl Gnat {
    pub async fn new(addr: &str) -> Result<Arc<Mutex<Gnat>>, Box<dyn std::error::Error>> {
        let addr = addr.parse::<SocketAddr>()?;
        let sink_and_stream = TcpStream::connect(&addr).await?;
        let sink_and_stream = Framed::new(sink_and_stream, Codec::new());
        let (writer, reader) = sink_and_stream.split();
        let mut client = Gnat { addr,
                                connect: Connect::default(),
                                state: ConnectionState::Connected,
                                writer,
                                reader };
        client.connect().await?;
        let client = Arc::new(Mutex::new(client));

        // tokio::spawn(Gnat::server_messages_handler(reader));

        Ok(client)
    }

    pub async fn connect(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.writer
            .send(ClientMessage::Connect(self.connect.clone()))
            .await?;
        Ok(())
    }

    pub async fn publish(&mut self,
                         subject: &Subject,
                         payload: Vec<u8>)
                         -> Result<(), Box<dyn std::error::Error>> {
        self.publish_with_optional_reply(subject, None, payload)
            .await
    }

    pub async fn publish_with_reply(&mut self,
                                    subject: &Subject,
                                    reply_to: &Subject,
                                    payload: Vec<u8>)
                                    -> Result<(), Box<dyn std::error::Error>> {
        self.publish_with_optional_reply(subject, Some(reply_to), payload)
            .await
    }

    pub async fn publish_with_optional_reply(&mut self,
                                             subject: &Subject,
                                             reply_to: Option<&Subject>,
                                             payload: Vec<u8>)
                                             -> Result<(), Box<dyn std::error::Error>> {
        self.writer
            .send(ClientMessage::Pub(Msg::new(subject.clone(),
                                              String::from("TODO"),
                                              reply_to.map(Clone::clone),
                                              payload)))
            .await?;
        Ok(())
    }

    pub async fn ping(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.writer.send(ClientMessage::Ping).await?;
        Ok(())
    }

    pub async fn pong(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.writer.send(ClientMessage::Pong).await?;
        Ok(())
    }

    // TODO
    pub async fn subscribe(&self) -> Result<(), Box<dyn std::error::Error>> { Ok(()) }

    // TODO
    pub async fn unsubscribe(&self) -> Result<(), Box<dyn std::error::Error>> { Ok(()) }

    async fn server_messages_handler(reader: ()) {
        // let codec = Codec::new();
        // let mut framed = FramedRead::new(reader, codec);
        // while let Some(line) = framed.next().await {
        //     println!("{:?}", line);
        // }
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use rand::Rng;
//     use std::{convert::TryFrom,
//               time};
//     use tokio::{runtime::current_thread::Runtime,
//                 timer::Delay};
//     #[test]
//     fn it_works() {
//         let f = async {
//             let mut rng = rand::thread_rng();
//             let client = Gnat::new("127.0.0.1:4222").await.unwrap();
//             for i in 0..10 {
//                 let client = client.clone();
//                 let delay = rng.gen_range(0, 100);
//                 tokio::spawn(async move {
//                     let s = "test".parse().unwrap();
//                     let message = format!("this is iteration {}", i).into_bytes();
//                     let till = time::Instant::now() + time::Duration::from_millis(delay);
//                     Delay::new(till).await;
//                     let mut lock = client.lock().await;
//                     lock.publish_with_reply(&s, &s, &message).await.unwrap();
//                 });
//             }
//         };
//         Runtime::new().unwrap().spawn(f).run().unwrap();
//     }
// }

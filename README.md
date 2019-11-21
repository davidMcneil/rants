# Rants

[![crates.io](https://img.shields.io/crates/v/rants.svg)](https://crates.io/crates/rants)
[![docs](https://docs.rs/rants/badge.svg)](https://docs.rs/rants)
![License](https://img.shields.io/badge/license-MIT-blue.svg)
![License](https://img.shields.io/badge/license-APACHE-blue.svg)

An async [NATS](https://nats.io/) client library for the Rust programming language.

The client aims to be an ergonomic, yet thin, wrapper over the NATS client protocol. The easiest way to learn to use the client is by reading the [NATS client protocol documentation](https://nats-io.github.io/docs/nats_protocol/nats-protocol.html). The main entry point into the library's API is the [`Client`](https://docs.rs/rants/latest/rants/struct.Client.html) struct.

# Example

```rust
use futures::stream::StreamExt;
use rants::Client;
use tokio::runtime::Runtime;

let main_future = async {
   // A NATS server must be running on `127.0.0.1:4222`
   let address = "127.0.0.1:4222".parse().unwrap();
   let client = Client::new(vec![address]);

   // Configure the client to receive messages even if it sent the message
   client.connect_mut().await.echo(true);

   // Connect to the server
   client.connect().await;

   // Create a new subject called "test"
   let subject = "test".parse().unwrap();

   // Subscribe to the "test" subject
   let (_, mut subscription) = client.subscribe(&subject, 1024).await.unwrap();

   // Publish a message to the "test" subject
   client
       .publish(&subject, b"This is a message!")
       .await
       .unwrap();

   // Read a message from the subscription
   let message = subscription.next().await.unwrap();
   let message = String::from_utf8(message.into_payload()).unwrap();
   println!("Received '{}'", message);

   // Disconnect from the server
   client.disconnect().await;
};

let runtime = Runtime::new().expect("to create Runtime");
runtime.spawn(main_future);
runtime.shutdown_on_idle();
```

## Development

The integration test suite requires the [NATS server](https://nats.io/download/nats-io/nats-server/) to be reachable at `127.0.0.1:4222`:

    > cargo test

The [`env_logger`](https://github.com/sebasmagri/env_logger/) crate is used in integration test. To enable it and run a single test run:

    > RUST_LOG=rants=trace cargo test ping_pong

## Alternatives

- [`rust-nats`](https://github.com/jedisct1/rust-nats)
- [`ratsio`](https://github.com/mnetship/ratsio)
- [`nitox`](https://github.com/YellowInnovation/nitox)

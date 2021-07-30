mod common;

use common::NatsServer;
use rants::Client;
use tokio::runtime::Runtime;

#[test]
fn blocking() {
    common::init();
    let rt = Runtime::new().unwrap();
    let _nats_server = rt.block_on(NatsServer::new(&[]));

    let address = "127.0.0.1".parse().unwrap();
    let client = Client::new(vec![address]);

    // Configure it to echo messages so we can receive messages we sent
    rt.block_on(client.connect_mut()).echo(true);

    rt.block_on(client.connect());

    let subject = "test".parse().unwrap();
    let (_, mut subscription) = rt.block_on(client.subscribe(&subject, 1)).unwrap();
    rt.block_on(client.publish(&subject, b"test")).unwrap();
    let result = rt.block_on(subscription.recv()).unwrap();
    assert_eq!(result.payload(), b"test");

    rt.block_on(client.disconnect());
}

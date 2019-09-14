use env_logger;
use std::future::Future;
use tokio::runtime::Runtime;

pub fn run_integration_test(future: impl Future<Output = ()> + Send + 'static) {
    env_logger::init();

    let runtime = Runtime::new().expect("to create Runtime");
    runtime.spawn(future);
    runtime.shutdown_on_idle();
}

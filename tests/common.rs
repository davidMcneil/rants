use env_logger;
use std::{
    env,
    future::Future,
    process::{Command, Stdio},
};
use tokio::runtime::Runtime;

const NATS_PATH_ENV: &str = "NATS_PATH";
const NATS_NO_CAPTURE_ENV: &str = "NATS_NO_CAPTURE";

pub fn run_integration_test(future: impl Future<Output = ()> + Send + 'static, args: &[&str]) {
    env_logger::init();

    // Get the path to the nats server binary
    let nats_path = env::var(NATS_PATH_ENV).unwrap_or_else(|_| {
        panic!(
            "Environment variable '{}' is not set. Set it to run the integration tests.",
            NATS_PATH_ENV
        );
    });

    let (stdout, stderr) = if env::var(NATS_NO_CAPTURE_ENV).unwrap_or_default().is_empty() {
        (Stdio::from(Stdio::null()), Stdio::null())
    } else {
        (Stdio::inherit(), Stdio::inherit())
    };

    // Try and start the nats server
    let mut child = Command::new(nats_path.clone())
        .args(args)
        .stdout(stdout)
        .stderr(stderr)
        .spawn()
        .unwrap_or_else(|e| {
            panic!(
                "Unable to run the integration tests. Failed to spawn nats server with command '{}', err: {}",
                format!("{} {}", nats_path, args.join(" ")),
                e
            );
        });

    let runtime = Runtime::new().expect("to create Runtime");
    runtime.spawn(future);
    runtime.shutdown_on_idle();

    let _ = child.kill();
}

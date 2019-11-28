use env_logger;
use futures::future::{self, Either};
use std::{env, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
    sync::oneshot::{self, Sender},
    time::timeout,
};

const NATS_PATH_ENV: &str = "NATS_PATH";
const NATS_READY_MESSAGE: &str = "Server is ready";

pub fn init() {
    env_logger::init();
}

pub struct NatsServer {
    kill: Option<Sender<()>>,
}

impl NatsServer {
    pub async fn new(args: &[&str]) -> Self {
        // Get the path to the nats executable
        let nats_path = env::var(NATS_PATH_ENV).unwrap_or_else(|_| {
            panic!(
                "Environment variable '{}' is not set. Set it to run the integration tests.",
                NATS_PATH_ENV
            );
        });

        let mut child = Command::new(nats_path.clone())
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect(&format!(
            "Unable to run the integration tests. Failed to spawn nats server with command '{} {}'",
            nats_path,
            args.join(" ")
        ));

        // Spawn a task to handle stdout
        let stdout = child
            .stdout()
            .take()
            .expect("child did not have a handle to stdout");
        tokio::spawn(async {
            let mut reader = BufReader::new(stdout).lines();
            while let Some(line) = reader.next_line().await.expect("valid stdout line") {
                println!("{}", line);
            }
        });

        // Spawn a task to handle stderr and check if nats is ready
        let (ready_tx, ready_rx) = oneshot::channel::<()>();
        let stderr = child
            .stderr()
            .take()
            .expect("child did not have a handle to stdout");
        tokio::spawn(async {
            let mut ready_tx = Some(ready_tx);
            let mut reader = BufReader::new(stderr).lines();
            while let Some(line) = reader.next_line().await.expect("valid stdout line") {
                println!("{}", line);
                if line.contains(NATS_READY_MESSAGE) {
                    if let Some(ready_tx) = ready_tx.take() {
                        ready_tx.send(()).expect("to send nats ready oneshot");
                    }
                }
            }
        });

        // Spawn a task to run the child and wait for the kill oneshot
        let (kill_tx, kill_rx) = oneshot::channel::<()>();
        tokio::spawn(async {
            match future::select(child, kill_rx).await {
                Either::Left((Ok(_exit_status), _)) => panic!("nats exited early"),
                Either::Left((Err(_), _)) => panic!("nats produced Err while running"),
                Either::Right((Ok(()), _)) => (),
                Either::Right((Err(_), _)) => panic!("failed to receive ready oneshot"),
            }
        });

        // Wait for nats to be ready or timeout
        if let Err(_) = timeout(Duration::from_secs(5), ready_rx).await {
            panic!("nats server failed to reach ready state within timeout");
        }

        Self {
            kill: Some(kill_tx),
        }
    }
}

impl Drop for NatsServer {
    fn drop(&mut self) {
        if let Some(kill) = self.kill.take() {
            kill.send(()).expect("to send kill oneshot")
        }
    }
}

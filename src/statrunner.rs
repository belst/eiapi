use std::path::PathBuf;

use serde::Serialize;
use tokio::{io::AsyncWriteExt, net::UnixStream, sync::mpsc::UnboundedReceiver, task::JoinHandle};

#[derive(Debug, thiserror::Error)]
#[error("failed to send stat: {0}")]
enum SendError {
    Io(#[from] std::io::Error),
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Serialize)]
pub enum Message {
    Success,
    Failure { path: PathBuf, log: String },
}

async fn send(msg: &Message, sock: &mut UnixStream) -> Result<(), SendError> {
    let msg = serde_json::to_string(msg)?;
    sock.write_all(msg.as_bytes()).await?;
    sock.write(b"\n").await?;
    sock.flush().await?;
    Ok(())
}

async fn connect() -> UnixStream {
    let mut backoff = 0;
    loop {
        match UnixStream::connect("/tmp/ei-uploads/stat.sock").await {
            Ok(sock) => return sock,
            Err(e) => {
                tracing::warn!(
                    "Could not connect to stat socket: {e}, Retrying in {} seconds.",
                    1 << backoff
                );
                tokio::time::sleep(std::time::Duration::from_secs(1 << backoff)).await;
                backoff += 1;
                backoff = backoff.clamp(0, 5);
            }
        }
    }
}

pub fn run(mut rx: UnboundedReceiver<Message>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut sock = connect().await;
        while let Some(msg) = rx.recv().await {
            if let Err(e) = send(&msg, &mut sock).await {
                tracing::error!("failed to send stat: {e}");
                if let SendError::Io(_e) = e {
                    tracing::error!("Reconnecting...");
                    sock = connect().await;
                }
            }
        }
    })
}

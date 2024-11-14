use std::{
    path::{Path, PathBuf},
    sync::atomic::Ordering,
    time::Duration,
};

use crate::QUEUE_SIZE;

pub fn run(mut rx: tokio::sync::mpsc::UnboundedReceiver<PathBuf>) {
    let mut interval = tokio::time::interval(Duration::from_secs(120));
    tokio::spawn(async move {
        loop {
            interval.tick().await;
            tracing::info!("Queue size: {}", QUEUE_SIZE.load(Ordering::SeqCst));
        }
    });
    tokio::spawn(async move {
        loop {
            tokio::select! {
                sp = rx.recv() => {
                    match sp {
                        Some(path) => {
                            QUEUE_SIZE.fetch_sub(1, Ordering::SeqCst);
                            tracing::info!("importing {path:?}");
                            match import_file(&path).await {
                                Ok(files) => {
                                    tracing::info!("imported {files:?}");
                                    for f in files {
                                        let _ = std::fs::remove_file(f)
                                            .map_err(|err| tracing::error!("failed to remove file: {err}"));
                                    }
                                    if std::fs::exists(&path).unwrap_or(false) {
                                        tracing::debug!("deleteing {path:?}");
                                        let _ = std::fs::remove_file(&path)
                                            .map_err(|err| tracing::error!("failed to remove file: {err}"));
                                    }
                                    // also delete the log file (maybe)
                                    if tokio::fs::try_exists(&path.with_extension("log"))
                                        .await
                                        .unwrap_or(false)
                                    {
                                        tracing::debug!("deleteing {path:?}.log");
                                        let _ = std::fs::remove_file(&path.with_extension("log"))
                                            .map_err(|err| tracing::error!("failed to remove file: {err}"));
                                    }
                                }
                                Err(err) => {
                                    tracing::error!("failed to import file: {err}");
                                }
                            }
                        }
                        None => break,
                    }
                }
            }
        }
    });
}

async fn import_file(path: impl AsRef<Path>) -> anyhow::Result<Vec<String>> {
    let cmd = tokio::process::Command::new(
        "/home/belst/GW2-Elite-Insights-Parser/GW2EIParserCLI/out/GuildWars2EliteInsights-CLI",
    )
    .current_dir("/home/belst/GW2-Elite-Insights-Parser/GW2EIParserCLI")
    .args(&[
        "-c",
        "/home/belst/GW2-Elite-Insights-Parser/GW2EIParser/settings.conf",
    ])
    .arg(tokio::fs::canonicalize(&path).await?.to_str().unwrap())
    .output()
    .await?;
    tracing::debug!(
        "path: {}",
        tokio::fs::canonicalize(path).await?.to_str().unwrap()
    );
    if !cmd.status.success() {
        return Err(anyhow::anyhow!("failed to run parser"));
    }
    let stdout = String::from_utf8(cmd.stdout)?;
    tracing::debug!("stdout: {}", stdout);
    tracing::debug!("stderr: {}", String::from_utf8(cmd.stderr)?);
    if stdout.contains("Error") {
        return Err(anyhow::anyhow!("parser failed"));
    }
    let generated: Vec<_> = stdout
        .lines()
        .filter(|l| {
            l.trim().starts_with("Parsing Successful -  ") || l.trim().starts_with("Generated: ")
        })
        .map(|l| {
            let s = l
                .trim()
                .trim_start_matches("Parsing Successful -  ")
                .trim_start_matches("Generated: ");
            match s.split_once(": ") {
                Some((path, _)) => path,
                None => s,
            }
            .into()
        })
        .collect();
    Ok(generated)
}

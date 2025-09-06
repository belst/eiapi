use std::{
    path::{Path, PathBuf},
    sync::atomic::Ordering,
    time::Duration,
};

use tokio::time::timeout;

use crate::QUEUE_SIZE;

pub async fn check_log_and_delete_if_exists(path: &Path) -> anyhow::Result<bool> {
    let mut ret = false;
    if tokio::fs::try_exists(path).await? {
        let log_content = tokio::fs::read_to_string(path).await?;
        let success_strings = [
            "Wingman: CheckUploadPossible successful: False", // cannot upload (maybe duplicate),
            "Wingman: UploadProcessed successful: True",
            "Wingman: UploadProcessed successful: Imported log is Fail Golem / WvW",
            "Program: Fight is too short",
            "Program: Log is too short",
            "Program: No Targets found",
        ];
        if success_strings.iter().any(|s| log_content.contains(s)) {
            ret = true;
            tracing::debug!("deleteing {}", path.display());
            let _ = std::fs::remove_file(&path.with_extension("log"))
                .map_err(|err| tracing::error!("failed to remove file: {err}"));
        }
    } else {
        tracing::debug!("log file does not exist");
        anyhow::bail!("log file does not exist");
    }
    Ok(ret)
}

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
            match rx.recv().await {
                Some(path) => {
                    QUEUE_SIZE.fetch_sub(1, Ordering::SeqCst);
                    tracing::info!("importing {path:?}");
                    match import_file(&path).await {
                        Ok(files) => {
                            let retrypath = Path::new("/tmp/ei-uploads/retry/");
                            if let Ok(check) =
                                check_log_and_delete_if_exists(&path.with_extension("log")).await
                            {
                                if check {
                                    tracing::info!("Successfully uploaded");
                                } else {
                                    tracing::error!("Failed to upload, moving file to retry queue");
                                    let _ = std::fs::rename(
                                        &path,
                                        retrypath.join(path.file_name().unwrap()),
                                    );
                                    let _ = std::fs::rename(
                                        path.with_extension("log"),
                                        retrypath
                                            .join(path.with_extension("log").file_name().unwrap()),
                                    );
                                    continue;
                                }
                            } else {
                                tracing::error!("failed to check log file");
                            }
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
                        }
                        Err(err) => {
                            tracing::error!("failed to import file: {err}");
                        }
                    }
                }
                None => break,
            }
        }
        tracing::error!("Import Task exited. Receiver closed.");
    });
}

async fn import_file(path: impl AsRef<Path>) -> anyhow::Result<Vec<String>> {
    let mut timeoutpath = PathBuf::from("/tmp/ei-uploads/timeouts/");
    let path = tokio::fs::canonicalize(&path).await?;

    let cmd = match timeout(
        Duration::from_secs(60 * 15),
        tokio::process::Command::new(
            "/home/belst/GW2-Elite-Insights-Parser/GW2EIParserCLI/out/GuildWars2EliteInsights-CLI",
        )
        .current_dir("/home/belst/GW2-Elite-Insights-Parser/GW2EIParserCLI")
        .args(&[
            "-c",
            "/home/belst/GW2-Elite-Insights-Parser/GW2EIParser/settings.conf",
        ])
        .arg(path.as_os_str())
        .kill_on_drop(true)
        .output(),
    )
    .await
    {
        Ok(cmd) => cmd?,
        Err(err) => {
            tracing::error!("Parser timeout: {err}");
            timeoutpath.push(path.file_name().unwrap());
            tracing::debug!("Moving file to {}", timeoutpath.display());
            tokio::fs::rename(&path, timeoutpath).await?;
            return Err(anyhow::anyhow!("failed to run parser"));
        }
    };

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

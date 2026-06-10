use std::{
    path::{Path, PathBuf},
    sync::atomic::Ordering,
    time::Duration,
};

use serde::Deserialize;
use tokio::{sync::mpsc::UnboundedReceiver, time::timeout};

use crate::QUEUE_SIZE;

const WINGMAN_SUCCSESS: &str = "Wingman: UploadProcessed successful: True";

// {
//   "fileName": "/tmp/ei-uploads/BerryDerpy5670_20260417-171254.zevtc",
//   "parsed": true,
//   "status": "Completed for failed StdGolem",
//   "generatedFiles": [
//     "/tmp/ei-uploads/BerryDerpy5670_20260417-171254.log"
//   ],
//   "dpsReportUploadTentative": false,
//   "dpsReportUploadFailed": false,
//   "wingmanUploadTentative": true,
//   "wingmanUploadFailed": false,
//   "wingmanUploadRefused": false,
//   "elapsed": 1679
// }
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EIResult {
    file_name: PathBuf,
    parsed: bool,
    status: String,
    generated_files: Vec<PathBuf>,
    dps_report_upload_tentative: bool,
    dps_report_upload_failed: bool,
    wingman_upload_tentative: bool,
    wingman_upload_failed: bool,
    wingman_upload_refused: bool,
    elapsed: u64,
}

fn parse_json(s: &str) -> anyhow::Result<EIResult> {
    if !s.starts_with("Processed - ") {
        anyhow::bail!("invalid log line");
    }
    let s = s.trim_start_matches("Processed - ").lines().next().unwrap();
    Ok(serde_json::from_str::<EIResult>(s)?)
}

pub fn check_output(output: &str) -> anyhow::Result<Option<String>> {
    for l in output.lines() {
        if l.starts_with("Processed - ") {
            let res = parse_json(l)?;
            if !res.parsed {
                return Ok(Some(res.status));
            }
            if !res.wingman_upload_tentative {
                return Ok(None);
            }
            if res.wingman_upload_refused {
                return Ok(None);
            }
            if !res.wingman_upload_refused && res.wingman_upload_failed {
                return Ok(Some(res.status));
            }
            if !res.wingman_upload_refused && !res.wingman_upload_failed {
                return Ok(Some(WINGMAN_SUCCSESS.to_owned()));
            }
        }
    }
    return Ok(Some("Could not find json".into()));
}

// pub async fn check_log_and_delete_if_exists(path: &Path) -> anyhow::Result<Option<String>> {
//     let ret;
//     if tokio::fs::try_exists(path).await? {
//         let log_content = tokio::fs::read_to_string(path).await?;
//         // success does not mean it actually got parsed and uploaded,
//         // but it's an error in the log and not the parser
//         let success_strings = [
//             WINGMAN_SUCCSESS,
//             "Wingman: UploadProcessed successful: True",
//             "Wingman: UploadProcessed successful: Imported log is Fail Golem / WvW",
//             "Program: Fight is too short",
//             "Program: Log is too short",
//             "Program: No Targets found",
//             "Program: Enervators not found",
//             "Program: Main target of the log not found",
//             "Program: Main target not found",
//             "Program: No active players",
//             "Program: No valid targets found for full log phase",
//             "Program: Sequence contains no matching element", // ???
//         ];
//         if let Some(&suc) = success_strings.iter().find(|&&s| log_content.contains(s)) {
//             if suc == WINGMAN_SUCCSESS {
//                 ret = Some(suc.to_owned());
//             } else {
//                 ret = None;
//             }
//             tracing::debug!("deleting {}", path.display());
//             let _ = std::fs::remove_file(&path.with_extension("log"))
//                 .map_err(|err| tracing::error!("failed to remove file: {err}"));
//         } else {
//             // TODO: filter out to the actual error
//             ret = Some(log_content);
//         }
//     } else {
//         tracing::debug!("log file does not exist");
//         anyhow::bail!("log file does not exist");
//     }
//     Ok(ret)
// }

pub fn run(mut rx: UnboundedReceiver<PathBuf>) {
    let mut interval = tokio::time::interval(Duration::from_secs(120));
    tokio::spawn(async move {
        loop {
            interval.tick().await;
            tracing::info!("Queue size: {}", QUEUE_SIZE.load(Ordering::SeqCst));
        }
    });
    tokio::spawn(async move {
        loop {
            // TODO: receive span from tracing, not just path
            match rx.recv().await {
                Some(path) => {
                    QUEUE_SIZE.fetch_sub(1, Ordering::SeqCst);
                    tracing::info!("importing {path:?}");
                    // TODO: get generated files from json
                    match import_file(&path).await {
                        Ok((stdout, files)) => {
                            let retrypath = Path::new("/tmp/ei-uploads/retry/");
                            match check_output(&stdout) {
                                Ok(check) => match check {
                                    Some(e) if e == WINGMAN_SUCCSESS => {
                                        tracing::info!("Successfully uploaded");
                                    }
                                    None => {
                                        tracing::info!("Not Uploaded but issue is with the log not with wingman/parser");
                                    }
                                    Some(err) => {
                                        tracing::error!(
                                        "Failed to upload, moving file to retry queue. Log: {err}"
                                    );
                                        let retry_path = retrypath.join(path.file_name().unwrap());
                                        let _ = std::fs::rename(&path, &retry_path);
                                        let _ = std::fs::remove_file(path.with_extension("log"));
                                        continue;
                                    }
                                },
                                Err(e) => {
                                    tracing::error!("failed to check log file: {e}");
                                    let retry_path = retrypath.join(path.file_name().unwrap());
                                    let _ = std::fs::rename(&path, &retry_path);
                                    let _ = std::fs::remove_file(path.with_extension("log"));
                                    continue;
                                }
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

async fn import_file(path: impl AsRef<Path>) -> anyhow::Result<(String, Vec<String>)> {
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
    Ok((stdout, generated))
}

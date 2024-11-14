use std::{
    collections::HashSet,
    error::Error,
    io,
    path::PathBuf,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use axum::{
    body::Bytes,
    extract::{
        multipart::{Field, MultipartError},
        DefaultBodyLimit, Multipart, State,
    },
    http::StatusCode,
    routing::{get, post},
    BoxError, Json, Router,
};
use derive_builder::Builder;
use futures::{stream, Stream, TryStreamExt};
use serde::Serialize;
use tokio::{fs::File, io::BufWriter, sync::Mutex};
use tokio_util::io::StreamReader;
use tower_http::limit::RequestBodyLimitLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod eirunner;

fn setup_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("{}=debug,tower_http=debug", env!("CARGO_CRATE_NAME")).into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}

struct AppState {
    seen: Mutex<HashSet<(u64, u64, String)>>,
    tx: tokio::sync::mpsc::UnboundedSender<PathBuf>,
}

static QUEUE_SIZE: AtomicUsize = AtomicUsize::new(0);

#[tokio::main]
async fn main() {
    setup_tracing();
    std::fs::create_dir_all(UPLOADS_DIRECTORY).unwrap();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    // tokio::spawn(async move {
    //     let mut interval = tokio::time::interval(Duration::from_secs(120));
    //
    //     loop {
    //         interval.tick().await;
    //         tracing::info!("Queue size: {}", rx.len());
    //     }
    // });
    tracing::info!("starting ei runner");
    eirunner::run(rx);
    let app = Router::new()
        .route("/", get(index))
        .route("/evtc", post(upload_evtc))
        .layer(DefaultBodyLimit::disable())
        .layer(RequestBodyLimitLayer::new(
            250 * 1024 * 1024, /* 250mb */
        ))
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .with_state(Arc::new(AppState {
            seen: Mutex::new(HashSet::new()),
            tx: tx.clone(),
        }));
    let port = env("PORT", 3334);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .unwrap();
    tracing::debug!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}

async fn index() -> &'static str {
    "Hello, world!"
}

fn map_err(err: impl Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

#[derive(Debug)]
struct OwnedField {
    name: Option<String>,
    file_name: Option<String>,
    content_type: Option<String>,
    headers: axum::http::HeaderMap,
    bytes: Bytes,
}

impl OwnedField {
    async fn from(field: Field<'_>) -> Result<Self, MultipartError> {
        let name = field.name().map(|s| s.to_string());
        let file_name = field.file_name().map(|s| s.to_string());
        let content_type = field.content_type().map(|s| s.to_string());
        let headers = field.headers().clone();
        let bytes = field.bytes().await?;
        Ok(Self {
            name,
            file_name,
            content_type,
            headers,
            bytes,
        })
    }
}

#[derive(Debug, Builder)]
#[builder(pattern = "owned")]
struct EvtcUpload {
    account: String,
    filesize: u64,
    trigger_id: u64,
    file: OwnedField,
}

#[derive(Debug, Serialize)]
struct EvtcUploadResponse {
    result: bool,
}

fn env<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

async fn upload_evtc(
    State(state): State<Arc<AppState>>,
    mut multipart: Multipart,
) -> Result<Json<EvtcUploadResponse>, (StatusCode, String)> {
    // For now accept same fields as uploadEVTC endpoint of wingman
    // fileds:
    // account: string
    // filesize: int
    // triggerID: int
    // file: bytes
    //
    // everything except file is a string though
    // Also file contains already filesize normally in multipart
    let mut evtc_upload = EvtcUploadBuilder::default();
    while let Some(field) = multipart.next_field().await.map_err(map_err)? {
        match field.name() {
            Some("account") => {
                evtc_upload = evtc_upload.account(field.text().await.map_err(map_err)?);
            }
            Some("filesize") => {
                evtc_upload = evtc_upload.filesize(
                    field
                        .text()
                        .await
                        .map_err(map_err)?
                        .parse()
                        .map_err(map_err)?,
                );
            }
            Some("triggerID") => {
                evtc_upload = evtc_upload.trigger_id(
                    field
                        .text()
                        .await
                        .map_err(map_err)?
                        .parse()
                        .map_err(map_err)?,
                );
            }
            Some("file") => {
                // TODO:
                // this is stupid, instead of loading the file into memory, it should be copied to
                // a temp file
                evtc_upload = evtc_upload.file(OwnedField::from(field).await.map_err(map_err)?);
            }
            Some(name) => {
                tracing::warn!("unknown field: {name}");
            }
            None => {
                tracing::warn!("field without name");
            }
        }
    }
    let evtc = evtc_upload.build().map_err(map_err)?;
    if !state
        .seen
        .lock()
        .await
        .insert((evtc.filesize, evtc.trigger_id, evtc.account.clone()))
    {
        return Err((StatusCode::CONFLICT, "duplicate".to_string()));
    }
    // TODO; parse file
    tracing::debug!(
        account = %evtc.account,
        filesize = %evtc.filesize,
        trigger_id = %evtc.trigger_id,
        file = %evtc.file.bytes.len(),
        "received evtc",
    );
    // prefix with account name to prevent collisions
    // collisions mostly happen when multiple users of the same squad are using the addon
    let path = evtc.account.replace('.', "")
        + "_"
        + &evtc.file.file_name.unwrap_or_else(|| {
            rand::random::<[char; 24]>()
                .iter()
                .filter(|c| c.is_ascii() && c.is_alphanumeric())
                .collect::<String>()
                + ".zevtc"
        });
    tracing::debug!("Storing file: {path}");
    let p = stream_to_file(
        &path,
        stream::once(async { Ok(evtc.file.bytes) as Result<Bytes, BoxError> }),
    )
    .await?;
    let _ = state
        .tx
        .send(p)
        .map(|_| QUEUE_SIZE.fetch_add(1, Ordering::SeqCst))
        .map_err(|e| tracing::error!("failed to send path: {e}"));
    Ok(Json(EvtcUploadResponse { result: true }))
}

const UPLOADS_DIRECTORY: &str = "/tmp/ei-uploads";

// Save a `Stream` to a file
async fn stream_to_file<S, E>(path: &str, stream: S) -> Result<PathBuf, (StatusCode, String)>
where
    S: Stream<Item = Result<Bytes, E>>,
    E: Into<BoxError>,
{
    if !path_is_valid(path) {
        return Err((StatusCode::BAD_REQUEST, "Invalid path".to_owned()));
    }

    async {
        // Convert the stream into an `AsyncRead`.
        let body_with_io_error = stream.map_err(|err| io::Error::new(io::ErrorKind::Other, err));
        let body_reader = StreamReader::new(body_with_io_error);
        futures::pin_mut!(body_reader);

        // Create the file. `File` implements `AsyncWrite`.
        let path = std::path::Path::new(UPLOADS_DIRECTORY).join(path);
        tracing::debug!("creating file at {path:?}");
        let mut file = BufWriter::new(File::create(&path).await?);

        // Copy the body into the file.
        tokio::io::copy(&mut body_reader, &mut file).await?;

        Ok::<_, io::Error>(path)
    }
    .await
    .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
}

// to prevent directory traversal attacks we ensure the path consists of exactly one normal
// component
fn path_is_valid(path: &str) -> bool {
    let path = std::path::Path::new(path);
    let mut components = path.components().peekable();

    if let Some(first) = components.peek() {
        if !matches!(first, std::path::Component::Normal(_)) {
            return false;
        }
    }

    components.count() == 1
}

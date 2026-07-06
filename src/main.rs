use std::{
    collections::{HashMap, HashSet},
    error::Error,
    io,
    path::PathBuf,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

use axum::{
    body::Bytes,
    extract::{
        multipart::{Field, MultipartError},
        DefaultBodyLimit, Multipart, Path, State,
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
use tracing_loki::{BackgroundTask, Layer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod eirunner;

fn setup_grafana_subscriber() -> (Layer, BackgroundTask) {
    // let user = env("GRAFANA_USER", "invalid".to_owned());
    // let api_key = env("GRAFANA_API_KEY", "invalid".to_owned());
    // let basic_auth = format!("{}:{}", user, api_key);
    //
    // let encoded = BASE64_STANDARD.encode(basic_auth);

    let url = url::Url::parse("http://localhost:3100").expect("invalid url");

    tracing_loki::builder()
        .label("application", "ei-runner")
        .unwrap()
        .extra_field("pid", format!("{}", std::process::id()))
        .unwrap()
        // .http_header("Authorization", encoded)
        // .unwrap()
        .build_url(url)
        .unwrap()
}

fn setup_tracing() {
    let (layer, task) = setup_grafana_subscriber();
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("{}=debug,tower_http=debug", env!("CARGO_CRATE_NAME")).into()
            }),
        )
        .with(layer)
        .with(tracing_subscriber::fmt::layer())
        .init();
    tokio::spawn(task);
}

// A queued job: the ticket handed to the client plus the stored file path.
type Job = (usize, PathBuf);

// Shared map of ticket -> latest status. Written by the worker (eirunner) and
// read by the GET /status/:ticket handler.
type Tickets = Arc<Mutex<HashMap<usize, TicketEntry>>>;

#[derive(Debug, Clone)]
struct TicketEntry {
    state: TicketState,
    // When this entry last changed, used to prune finished tickets.
    updated: Instant,
}

impl TicketEntry {
    fn new(state: TicketState) -> Self {
        Self {
            state,
            updated: Instant::now(),
        }
    }
}

// Per-upload processing status surfaced to the client.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum TicketState {
    // Still waiting in the queue; `position` in the response says how many are ahead.
    Queued,
    // Picked up by the worker: parsing + tentative wingman upload in progress.
    Processing,
    // Parsed and tentatively uploaded to wingman. The client can now start polling
    // gw2wingman's /checkUploadSuccessfulWithLog for the final result.
    Uploaded,
    // Parsed fine, but there was nothing to upload (a log issue, not a wingman/parser
    // fault). Nothing to poll.
    Skipped,
    // Parsing or upload failed; the file was moved to the retry queue server-side.
    Failed { message: String },
}

impl TicketState {
    // Terminal states no longer change and can be pruned after a grace period.
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            TicketState::Uploaded | TicketState::Skipped | TicketState::Failed { .. }
        )
    }
}

#[derive(Debug)]
struct AppState {
    seen: Mutex<HashSet<(u64, u64, String)>>,
    tx: tokio::sync::mpsc::UnboundedSender<Job>,
    tickets: Tickets,
}

// Current number of files waiting in the queue (enqueued but not yet picked up).
static QUEUE_SIZE: AtomicUsize = AtomicUsize::new(0);
// Monotonic counter of every accepted upload. Used to hand out a ticket number
// so the client can compute its position in the queue.
static ENQUEUED: AtomicUsize = AtomicUsize::new(0);
// Monotonic counter of every file the worker has picked up for processing.
// position_in_queue = ticket - DEQUEUED (<= 0 means it is being processed).
static DEQUEUED: AtomicUsize = AtomicUsize::new(0);

#[tokio::main]
async fn main() {
    setup_tracing();
    std::fs::create_dir_all(UPLOADS_DIRECTORY).unwrap();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Job>();
    let tickets: Tickets = Arc::new(Mutex::new(HashMap::new()));
    tracing::info!("starting ei runner");
    eirunner::run(rx, tickets.clone());
    let app = Router::new()
        .route("/", get(index))
        .route("/status", get(status))
        .route("/status/:ticket", get(ticket_status))
        .route("/evtc", post(upload_evtc))
        .layer(DefaultBodyLimit::disable())
        .layer(RequestBodyLimitLayer::new(
            250 * 1024 * 1024, /* 250mb */
        ))
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .with_state(Arc::new(AppState {
            seen: Mutex::new(HashSet::new()),
            tx: tx.clone(),
            tickets: tickets.clone(),
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

#[derive(Debug, Serialize)]
struct StatusResponse {
    // Files currently waiting in the queue (not yet picked up by the worker).
    queue_size: usize,
    // Total number of files the worker has picked up so far. A client that kept
    // the `ticket` from its upload can compute its position as `ticket - dequeued`
    // (a value <= 0 means the file is being processed or is already done).
    dequeued: usize,
}

async fn status() -> Json<StatusResponse> {
    Json(StatusResponse {
        queue_size: QUEUE_SIZE.load(Ordering::SeqCst),
        dequeued: DEQUEUED.load(Ordering::SeqCst),
    })
}

#[derive(Debug, Serialize)]
struct TicketStatusResponse {
    ticket: usize,
    // How many files are still ahead of this one in the queue. Only meaningful while
    // `state` is "queued"; 0 once it is being processed or finished.
    position: usize,
    #[serde(flatten)]
    state: TicketState,
}

// Lets the client poll for the position and outcome of a single upload.
// Returns 404 once the ticket has been pruned (some time after it finished).
async fn ticket_status(
    State(state): State<Arc<AppState>>,
    Path(ticket): Path<usize>,
) -> Result<Json<TicketStatusResponse>, StatusCode> {
    let entry = state
        .tickets
        .lock()
        .await
        .get(&ticket)
        .cloned()
        .ok_or(StatusCode::NOT_FOUND)?;
    let position = match entry.state {
        TicketState::Queued => ticket.saturating_sub(DEQUEUED.load(Ordering::SeqCst)),
        _ => 0,
    };
    Ok(Json(TicketStatusResponse {
        ticket,
        position,
        state: entry.state,
    }))
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
    // Monotonic ticket for this upload. Combine with `dequeued` from GET /status,
    // or poll GET /status/:ticket, to follow this file's position and outcome.
    // 0 means the file could not be enqueued.
    ticket: usize,
}

fn env<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

#[tracing::instrument(skip(state, multipart))]
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
    let seen_key = (evtc.filesize, evtc.trigger_id, evtc.account.clone());
    if !state.seen.lock().await.insert(seen_key.clone()) {
        tracing::info!(
            account = %evtc.account,
            filesize = %evtc.filesize,
            trigger_id = %evtc.trigger_id,
            file = %evtc.file.bytes.len(),
            "duplicate evtc"
        );
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
            use rand::distributions::{Alphanumeric, DistString};
            Alphanumeric.sample_string(&mut rand::thread_rng(), 24) + ".zevtc"
        });
    tracing::debug!("Storing file: {path}");
    let p = match stream_to_file(
        &path,
        stream::once(async { Ok(evtc.file.bytes) as Result<Bytes, BoxError> }),
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            state.seen.lock().await.remove(&seen_key);
            return Err(e);
        }
    };
    // Hand out a ticket and register it as queued before sending it to the worker,
    // so a fast worker can never set "processing" before the entry exists.
    let ticket = ENQUEUED.fetch_add(1, Ordering::SeqCst) + 1;
    state
        .tickets
        .lock()
        .await
        .insert(ticket, TicketEntry::new(TicketState::Queued));
    match state.tx.send((ticket, p)) {
        Ok(()) => {
            QUEUE_SIZE.fetch_add(1, Ordering::SeqCst);
            Ok(Json(EvtcUploadResponse {
                result: true,
                ticket,
            }))
        }
        Err(e) => {
            tracing::error!("failed to send path: {e}");
            state.tickets.lock().await.remove(&ticket);
            state.seen.lock().await.remove(&seen_key);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to enqueue".to_string(),
            ))
        }
    }
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

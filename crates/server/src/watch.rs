//! Implementation of the etcd `Watch` gRPC service.
//!
//! Live event streaming, historical replay from `start_revision`,
//! cancel and progress notify.
//!
//! Architecture:
//! - Each gRPC bidi stream owns a `WatchStreamState` tracking its
//!   watchers (by `watch_id`).
//! - One spawned task subscribes to `MvccStore::subscribe()` and
//!   forwards filtered events to the right watchers.
//! - One spawned task handles inbound `WatchRequest` messages
//!   (create / cancel / progress).
//! - Both tasks send `WatchResponse` messages through one outbound
//!   mpsc channel that feeds the gRPC response stream.
//!
//! **A watch never silently skips a revision (#16).** Every watcher
//! carries a cursor, `next_rev`: the first revision it has not yet been
//! given. Live delivery only sends batches at or above it. When the
//! forwarder cannot see every batch — the broadcast channel reports
//! `Lagged` because this stream fell behind, or a batch arrives whose
//! revision is not the successor of the last one (a follower installing
//! a raft snapshot, which emits no events) — each watcher is *resynced*:
//! its missed events are read back from MVCC history up to the current
//! revision, as etcd does for its unsynced watchers. If that history has
//! been compacted, the watch is cancelled with `compact_revision` set
//! (etcd's `ErrCompacted`), so the client re-lists and re-watches; it is
//! never left open with a hole in it. Revisions only advance with
//! events, so a gap in batch revisions is always a real miss.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::watch_server::Watch;
use fastetcd_proto::mvccpb;
use fastetcd_storage::mvcc::{EventBatch, EventKind, MvccEvent};
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

use crate::conv::record_to_kv;
use crate::state::{response_header, ServerState};

#[derive(Clone)]
pub struct WatchService {
    state: Arc<ServerState>,
}

impl WatchService {
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl Watch for WatchService {
    type WatchStream = Pin<Box<dyn Stream<Item = Result<pb::WatchResponse, Status>> + Send>>;

    async fn watch(
        &self,
        request: Request<Streaming<pb::WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let state = self.state.clone();
        let mut inbound = request.into_inner();

        // Outbound channel feeds the gRPC response stream.
        let (tx, rx) = mpsc::channel::<Result<pb::WatchResponse, Status>>(64);
        let stream_state = Arc::new(Mutex::new(WatchStreamState::new()));
        let event_rx = state.sm.mvcc().subscribe();
        // Read after subscribing: every batch above this revision is
        // guaranteed to reach `event_rx` (commits broadcast under the
        // same lock that publishes `current_rev`).
        let subscribed_at = state.sm.mvcc().current_revision().await;

        // Spawn the event-forwarder task.
        {
            let state = state.clone();
            let stream_state = stream_state.clone();
            let tx = tx.clone();
            tokio::spawn(forward_events(
                state,
                stream_state,
                tx,
                event_rx,
                subscribed_at,
            ));
        }

        // Spawn the progress-notify ticker. Sends a `ProgressNotify`
        // response on each tick if any watcher has `progress_notify`.
        {
            let state = state.clone();
            let stream_state = stream_state.clone();
            let tx = tx.clone();
            tokio::spawn(progress_notify_ticker(state, stream_state, tx));
        }

        // Inbound handler runs in this task.
        let state = state.clone();
        let stream_state = stream_state.clone();
        let tx_in = tx.clone();
        tokio::spawn(async move {
            while let Some(req) = inbound.next().await {
                let Ok(req) = req else { break };
                let Some(union) = req.request_union else { continue };
                match union {
                    pb::watch_request::RequestUnion::CreateRequest(create) => {
                        if let Err(_e) =
                            handle_create(&state, &stream_state, &tx_in, create).await
                        {
                            break;
                        }
                    }
                    pb::watch_request::RequestUnion::CancelRequest(cancel) => {
                        handle_cancel(&state, &stream_state, &tx_in, cancel.watch_id).await;
                    }
                    pb::watch_request::RequestUnion::ProgressRequest(_) => {
                        handle_progress(&state, &stream_state, &tx_in).await;
                    }
                }
            }
            // Connection ended; drop the stream_state so the forwarder
            // exits when the watcher set is empty.
        });

        let stream: Self::WatchStream = Box::pin(ReceiverStream::new(rx));
        Ok(Response::new(stream))
    }
}

/// Per-connection watcher state.
#[derive(Default)]
struct WatchStreamState {
    watchers: HashMap<i64, Watcher>,
    next_auto_id: AtomicI64,
}

impl WatchStreamState {
    fn new() -> Self {
        Self::default()
    }

    fn next_id(&self) -> i64 {
        self.next_auto_id.fetch_add(1, Ordering::Relaxed) + 1
    }
}

struct Watcher {
    key: Vec<u8>,
    range_end: Vec<u8>,
    progress_notify: bool,
    filter_no_put: bool,
    filter_no_delete: bool,
    prev_kv: bool,
    /// First revision this watcher has not yet been given. Live batches
    /// below it are already covered (by replay or resync) and skipped.
    next_rev: i64,
}

/// Times a watcher was caught up from history after missing live
/// events. Process-wide; exposed for tests and diagnostics.
static WATCH_RESYNCS: AtomicU64 = AtomicU64::new(0);
/// Times a watcher was cancelled because the history it had missed
/// was no longer available.
static WATCH_LAG_CANCELS: AtomicU64 = AtomicU64::new(0);

/// Number of watcher resyncs since process start (see module docs).
pub fn resync_count() -> u64 {
    WATCH_RESYNCS.load(Ordering::Relaxed)
}

/// Number of watchers cancelled because missed history was compacted
/// or unreadable.
pub fn lag_cancel_count() -> u64 {
    WATCH_LAG_CANCELS.load(Ordering::Relaxed)
}

impl Watcher {
    fn matches_key(&self, key: &[u8]) -> bool {
        if self.range_end.is_empty() {
            key == self.key.as_slice()
        } else if self.range_end == [0u8] {
            key >= self.key.as_slice()
        } else {
            key >= self.key.as_slice() && key < self.range_end.as_slice()
        }
    }

    fn passes_filter(&self, event: &MvccEvent) -> bool {
        match event.kind {
            EventKind::Put if self.filter_no_put => false,
            EventKind::Delete if self.filter_no_delete => false,
            _ => true,
        }
    }

    fn to_pb(&self, e: &MvccEvent) -> mvccpb::Event {
        let event_type = match e.kind {
            EventKind::Put => mvccpb::event::EventType::Put as i32,
            EventKind::Delete => mvccpb::event::EventType::Delete as i32,
        };
        mvccpb::Event {
            r#type: event_type,
            kv: Some(record_to_kv(&e.kv)),
            prev_kv: if self.prev_kv {
                e.prev_kv.as_ref().map(record_to_kv)
            } else {
                None
            },
        }
    }
}

/// Soft cap on the payload of one replay/resync response. History is
/// split between revisions (never inside one) so a large catch-up does
/// not produce a message over the client's gRPC receive limit.
const REPLAY_CHUNK_BYTES: usize = 1 << 20;

/// Outcome of sending a watcher its history.
enum Replay {
    /// Delivered everything up to the requested revision.
    Done,
    /// History below `compact_rev` is gone; the watcher must be
    /// cancelled.
    Compacted { compact_rev: i64 },
    /// History could not be read.
    Failed(String),
}

/// Send `watcher` every event in `[watcher.next_rev, upto]` that it
/// selects, in revision order. Does not touch `next_rev`.
async fn replay_history(
    state: &Arc<ServerState>,
    tx: &mpsc::Sender<Result<pb::WatchResponse, Status>>,
    watch_id: i64,
    watcher: &Watcher,
    upto: i64,
) -> Result<Replay, ()> {
    if watcher.next_rev > upto {
        return Ok(Replay::Done);
    }
    let compact_rev = state.sm.mvcc().compact_revision().await;
    if watcher.next_rev < compact_rev {
        return Ok(Replay::Compacted { compact_rev });
    }
    let events = match state
        .sm
        .mvcc()
        .range_events_until(&watcher.key, &watcher.range_end, watcher.next_rev - 1, upto)
        .await
    {
        Ok(events) => events,
        Err(fastetcd_storage::mvcc::MvccError::Compacted { compact_rev, .. }) => {
            return Ok(Replay::Compacted { compact_rev });
        }
        Err(e) => return Ok(Replay::Failed(e.to_string())),
    };

    let header = response_header(state, upto).await;
    let mut chunk: Vec<mvccpb::Event> = Vec::new();
    let mut chunk_bytes = 0usize;
    let mut chunk_rev = 0i64;
    for e in &events {
        if !watcher.passes_filter(e) {
            continue;
        }
        let rev = e.kv.mod_revision;
        if chunk_bytes >= REPLAY_CHUNK_BYTES && rev != chunk_rev {
            let resp = events_response(header, watch_id, std::mem::take(&mut chunk));
            tx.send(Ok(resp)).await.map_err(|_| ())?;
            chunk_bytes = 0;
        }
        chunk_rev = rev;
        chunk_bytes += e.kv.key.len()
            + e.kv.value.len()
            + e.prev_kv
                .as_ref()
                .filter(|_| watcher.prev_kv)
                .map_or(0, |p| p.key.len() + p.value.len());
        chunk.push(watcher.to_pb(e));
    }
    if !chunk.is_empty() {
        tx.send(Ok(events_response(header, watch_id, chunk)))
            .await
            .map_err(|_| ())?;
    }
    Ok(Replay::Done)
}

fn events_response(
    header: pb::ResponseHeader,
    watch_id: i64,
    events: Vec<mvccpb::Event>,
) -> pb::WatchResponse {
    pb::WatchResponse {
        header: Some(header),
        watch_id,
        created: false,
        canceled: false,
        compact_revision: 0,
        cancel_reason: String::new(),
        fragment: false,
        events,
    }
}

async fn handle_create(
    state: &Arc<ServerState>,
    stream_state: &Arc<Mutex<WatchStreamState>>,
    tx: &mpsc::Sender<Result<pb::WatchResponse, Status>>,
    create: pb::WatchCreateRequest,
) -> Result<(), ()> {
    use pb::watch_create_request::FilterType;

    // Take the stream lock *before* reading the revision: the forwarder
    // needs this lock to deliver any batch, so every batch above
    // `current_rev` is still ahead of it and reaches the new watcher.
    let mut ss = stream_state.lock().await;
    let current_rev = state.sm.mvcc().current_revision().await;
    let compact_rev = state.sm.mvcc().compact_revision().await;

    // Allocate / use watch_id.
    let watch_id = if create.watch_id > 0 {
        create.watch_id
    } else {
        ss.next_id()
    };

    let mut filter_no_put = false;
    let mut filter_no_delete = false;
    for f in &create.filters {
        match FilterType::try_from(*f).ok() {
            Some(FilterType::Noput) => filter_no_put = true,
            Some(FilterType::Nodelete) => filter_no_delete = true,
            None => {}
        }
    }

    // Compacted-watch detection: if the client wants history from a
    // revision that's been compacted, send a canceled response with
    // compact_revision set; do NOT register the watcher.
    if create.start_revision > 0 && create.start_revision < compact_rev {
        drop(ss);
        let header = response_header(state, current_rev).await;
        let resp = pb::WatchResponse {
            header: Some(header),
            watch_id,
            created: true,
            canceled: true,
            compact_revision: compact_rev,
            cancel_reason: format!(
                "watch revision {} has been compacted (compact_rev = {})",
                create.start_revision, compact_rev
            ),
            fragment: false,
            events: Vec::new(),
        };
        return tx.send(Ok(resp)).await.map_err(|_| ());
    }

    // The cursor: history (if any) covers everything up to
    // `current_rev`, live delivery takes over above it. A future
    // `start_revision` means nothing below it is wanted at all.
    let next_rev = if create.start_revision > 0 {
        create.start_revision
    } else {
        current_rev + 1
    };
    let mut watcher = Watcher {
        key: create.key.clone(),
        range_end: create.range_end.clone(),
        progress_notify: create.progress_notify,
        filter_no_put,
        filter_no_delete,
        prev_kv: create.prev_kv,
        next_rev,
    };

    // Acknowledge the create.
    let header = response_header(state, current_rev).await;
    let resp = pb::WatchResponse {
        header: Some(header),
        watch_id,
        created: true,
        canceled: false,
        compact_revision: 0,
        cancel_reason: String::new(),
        fragment: false,
        events: Vec::new(),
    };
    tx.send(Ok(resp)).await.map_err(|_| ())?;

    // Historical replay of `[start_revision, current_rev]`. The stream
    // lock is still held, so the forwarder cannot interleave live
    // events ahead of (or duplicate) the history being sent.
    match replay_history(state, tx, watch_id, &watcher, current_rev).await? {
        Replay::Done => {}
        Replay::Compacted { compact_rev } => {
            drop(ss);
            return send_cancel(
                state,
                tx,
                watch_id,
                compact_rev,
                format!(
                    "watch revision {} has been compacted (compact_rev = {})",
                    create.start_revision, compact_rev
                ),
            )
            .await;
        }
        Replay::Failed(e) => {
            drop(ss);
            tracing::warn!(target: "fastetcd::watch", watch_id, "historical replay error: {e}");
            return send_cancel(
                state,
                tx,
                watch_id,
                0,
                format!("watch history could not be read: {e}"),
            )
            .await;
        }
    }
    watcher.next_rev = watcher.next_rev.max(current_rev + 1);
    ss.watchers.insert(watch_id, watcher);
    Ok(())
}

async fn send_cancel(
    state: &Arc<ServerState>,
    tx: &mpsc::Sender<Result<pb::WatchResponse, Status>>,
    watch_id: i64,
    compact_revision: i64,
    cancel_reason: String,
) -> Result<(), ()> {
    let header = response_header(state, state.sm.mvcc().current_revision().await).await;
    tx.send(Ok(pb::WatchResponse {
        header: Some(header),
        watch_id,
        created: false,
        canceled: true,
        compact_revision,
        cancel_reason,
        fragment: false,
        events: Vec::new(),
    }))
    .await
    .map_err(|_| ())
}

async fn handle_cancel(
    state: &Arc<ServerState>,
    stream_state: &Arc<Mutex<WatchStreamState>>,
    tx: &mpsc::Sender<Result<pb::WatchResponse, Status>>,
    watch_id: i64,
) {
    let mut ss = stream_state.lock().await;
    let _existed = ss.watchers.remove(&watch_id).is_some();
    drop(ss);
    let header = response_header(state, state.sm.mvcc().current_revision().await).await;
    let _ = tx
        .send(Ok(pb::WatchResponse {
            header: Some(header),
            watch_id,
            created: false,
            canceled: true,
            compact_revision: 0,
            cancel_reason: String::new(),
            fragment: false,
            events: Vec::new(),
        }))
        .await;
}

async fn handle_progress(
    state: &Arc<ServerState>,
    stream_state: &Arc<Mutex<WatchStreamState>>,
    tx: &mpsc::Sender<Result<pb::WatchResponse, Status>>,
) {
    let ss = stream_state.lock().await;
    if ss.watchers.is_empty() {
        return;
    }
    drop(ss);
    let rev = state.sm.mvcc().current_revision().await;
    let header = response_header(state, rev).await;
    let _ = tx
        .send(Ok(pb::WatchResponse {
            header: Some(header),
            // -1 watch_id signals "progress for whole stream" per etcd convention.
            watch_id: -1,
            created: false,
            canceled: false,
            compact_revision: 0,
            cancel_reason: String::new(),
            fragment: false,
            events: Vec::new(),
        }))
        .await;
}

async fn forward_events(
    state: Arc<ServerState>,
    stream_state: Arc<Mutex<WatchStreamState>>,
    tx: mpsc::Sender<Result<pb::WatchResponse, Status>>,
    mut event_rx: tokio::sync::broadcast::Receiver<EventBatch>,
    subscribed_at: i64,
) {
    use tokio::sync::broadcast::error::RecvError;

    // Revision of the newest batch this stream has accounted for,
    // either seen on the broadcast or covered by a resync.
    let mut last_rev = subscribed_at;
    loop {
        let batch = match event_rx.recv().await {
            Ok(batch) => batch,
            Err(RecvError::Lagged(n)) => {
                tracing::warn!(
                    target: "fastetcd::watch",
                    lagged = n,
                    "watch stream fell behind the event broadcast; resyncing watchers from history"
                );
                match resync(&state, &stream_state, &tx).await {
                    Ok(rev) => last_rev = last_rev.max(rev),
                    Err(()) => return, // client disconnected
                }
                continue;
            }
            Err(RecvError::Closed) => return,
        };

        if batch.revision <= last_rev {
            // Predates this stream's subscription or an earlier
            // resync; each watcher's cursor filters it below.
        } else if batch.revision != last_rev + 1 {
            // Revisions advance only with events, so a hole means this
            // stream never saw some batches (e.g. a raft snapshot was
            // installed underneath it). Catch up from history; that
            // covers this batch too.
            tracing::warn!(
                target: "fastetcd::watch",
                expected = last_rev + 1,
                got = batch.revision,
                "watch stream saw a revision gap; resyncing watchers from history"
            );
            match resync(&state, &stream_state, &tx).await {
                Ok(rev) => last_rev = last_rev.max(rev),
                Err(()) => return,
            }
            continue;
        } else {
            last_rev = batch.revision;
        }

        let mut ss = stream_state.lock().await;
        if ss.watchers.is_empty() {
            continue;
        }
        // Collect (watch_id, events) per watcher, advancing cursors.
        let mut deliveries: Vec<(i64, Vec<mvccpb::Event>)> = Vec::new();
        for (watch_id, w) in ss.watchers.iter_mut() {
            if batch.revision < w.next_rev {
                continue;
            }
            w.next_rev = batch.revision + 1;
            let evts: Vec<mvccpb::Event> = batch
                .events
                .iter()
                .filter(|e| w.matches_key(&e.kv.key) && w.passes_filter(e))
                .map(|e| w.to_pb(e))
                .collect();
            if !evts.is_empty() {
                deliveries.push((*watch_id, evts));
            }
        }

        if deliveries.is_empty() {
            continue;
        }
        // Send while still holding the stream lock so a concurrent
        // cancel/create cannot slip a response between a cursor advance
        // and the events it accounts for.
        let header = response_header(&state, batch.revision).await;
        for (watch_id, events) in deliveries {
            if tx
                .send(Ok(events_response(header, watch_id, events)))
                .await
                .is_err()
            {
                return; // client disconnected
            }
        }
        drop(ss);
    }
}

/// Catch every watcher on this stream up to the current revision from
/// MVCC history. A watcher whose missed history has been compacted (or
/// cannot be read) is cancelled rather than left open with a hole.
/// Returns the revision every surviving watcher is now current to.
async fn resync(
    state: &Arc<ServerState>,
    stream_state: &Arc<Mutex<WatchStreamState>>,
    tx: &mpsc::Sender<Result<pb::WatchResponse, Status>>,
) -> Result<i64, ()> {
    let mut ss = stream_state.lock().await;
    let upto = state.sm.mvcc().current_revision().await;
    let mut cancelled: Vec<(i64, i64, String)> = Vec::new();
    for (watch_id, w) in ss.watchers.iter_mut() {
        if w.next_rev > upto {
            continue;
        }
        WATCH_RESYNCS.fetch_add(1, Ordering::Relaxed);
        match replay_history(state, tx, *watch_id, w, upto).await? {
            Replay::Done => w.next_rev = upto + 1,
            Replay::Compacted { compact_rev } => cancelled.push((
                *watch_id,
                compact_rev,
                format!(
                    "watcher fell behind and required revision {} has been compacted (compact_rev = {})",
                    w.next_rev, compact_rev
                ),
            )),
            Replay::Failed(e) => cancelled.push((
                *watch_id,
                0,
                format!("watcher fell behind and its history could not be read: {e}"),
            )),
        }
    }
    for (watch_id, compact_rev, reason) in cancelled {
        ss.watchers.remove(&watch_id);
        WATCH_LAG_CANCELS.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(target: "fastetcd::watch", watch_id, "cancelling watch: {reason}");
        send_cancel(state, tx, watch_id, compact_rev, reason).await?;
    }
    Ok(upto)
}

async fn progress_notify_ticker(
    state: Arc<ServerState>,
    stream_state: Arc<Mutex<WatchStreamState>>,
    tx: mpsc::Sender<Result<pb::WatchResponse, Status>>,
) {
    let mut ticker = tokio::time::interval(Duration::from_secs(10));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the immediate first tick.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let ss = stream_state.lock().await;
        let has_progress_subscriber = ss.watchers.values().any(|w| w.progress_notify);
        drop(ss);
        if !has_progress_subscriber {
            continue;
        }
        let rev = state.sm.mvcc().current_revision().await;
        let header = response_header(&state, rev).await;
        let resp = pb::WatchResponse {
            header: Some(header),
            watch_id: -1,
            created: false,
            canceled: false,
            compact_revision: 0,
            cancel_reason: String::new(),
            fragment: false,
            events: Vec::new(),
        };
        if tx.send(Ok(resp)).await.is_err() {
            return;
        }
    }
}

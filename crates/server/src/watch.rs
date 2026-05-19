//! Implementation of the etcd `Watch` gRPC service.
//!
//! This is **Phase 1**: live event streaming + cancel + progress notify.
//! Historical replay (a watch starting at a past `start_revision`) is
//! deferred to a follow-up commit; in this version, requests with
//! `start_revision > current_revision_at_create` are accepted but only
//! receive future events, and `start_revision <= compact_revision`
//! gets a `canceled` response with `compact_revision` set.
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

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
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

        // Spawn the event-forwarder task.
        {
            let state = state.clone();
            let stream_state = stream_state.clone();
            let tx = tx.clone();
            tokio::spawn(forward_events(state, stream_state, tx, event_rx));
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
}

async fn handle_create(
    state: &Arc<ServerState>,
    stream_state: &Arc<Mutex<WatchStreamState>>,
    tx: &mpsc::Sender<Result<pb::WatchResponse, Status>>,
    create: pb::WatchCreateRequest,
) -> Result<(), ()> {
    use pb::watch_create_request::FilterType;

    let current_rev = state.sm.mvcc().current_revision().await;
    let compact_rev = state.sm.mvcc().compact_revision().await;

    // Allocate / use watch_id.
    let mut ss = stream_state.lock().await;
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

    let watcher = Watcher {
        key: create.key.clone(),
        range_end: create.range_end.clone(),
        progress_notify: create.progress_notify,
        filter_no_put,
        filter_no_delete,
        prev_kv: create.prev_kv,
    };
    ss.watchers.insert(watch_id, watcher);
    drop(ss);

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

    // Historical replay is deferred to a follow-up commit. If the
    // client asked for a past revision in the present-or-future range
    // (start_revision <= current_revision), we don't backfill; we
    // just start forwarding new events. This is a known v0.1 gap and
    // documented in CHANGELOG.
    let _ = create.start_revision;

    Ok(())
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
) {
    loop {
        match event_rx.recv().await {
            Ok(batch) => {
                let ss = stream_state.lock().await;
                if ss.watchers.is_empty() {
                    continue;
                }
                // Collect (watch_id, events) per watcher.
                let mut deliveries: Vec<(i64, Vec<mvccpb::Event>)> = Vec::new();
                for (watch_id, w) in ss.watchers.iter() {
                    let mut evts: Vec<mvccpb::Event> = Vec::new();
                    for e in &batch.events {
                        if !w.matches_key(&e.kv.key) {
                            continue;
                        }
                        if !w.passes_filter(e) {
                            continue;
                        }
                        let event_type = match e.kind {
                            EventKind::Put => mvccpb::event::EventType::Put as i32,
                            EventKind::Delete => mvccpb::event::EventType::Delete as i32,
                        };
                        evts.push(mvccpb::Event {
                            r#type: event_type,
                            kv: Some(record_to_kv(&e.kv)),
                            prev_kv: if w.prev_kv {
                                e.prev_kv.as_ref().map(record_to_kv)
                            } else {
                                None
                            },
                        });
                    }
                    if !evts.is_empty() {
                        deliveries.push((*watch_id, evts));
                    }
                }
                drop(ss);

                if deliveries.is_empty() {
                    continue;
                }
                let header = response_header(&state, batch.revision).await;
                for (watch_id, events) in deliveries {
                    let resp = pb::WatchResponse {
                        header: Some(header.clone()),
                        watch_id,
                        created: false,
                        canceled: false,
                        compact_revision: 0,
                        cancel_reason: String::new(),
                        fragment: false,
                        events,
                    };
                    if tx.send(Ok(resp)).await.is_err() {
                        return; // client disconnected
                    }
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(
                    target: "fastetcd::watch",
                    lagged = n,
                    "watch broadcast lagged; subscribers may have missed events"
                );
                // Continue; subscribers should re-establish from a fresh
                // start_revision if they need exact history (v0.1 gap).
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
        }
    }
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

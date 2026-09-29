//! Traffic accounting for `/metrics` (fastetcd#29): what the member is
//! doing right now, as opposed to how big it is.
//!
//! [`Traffic`] lives in [`ServerState`](crate::state::ServerState) and is
//! updated where the work happens; `metrics::spawn_server` registers its
//! handles, so a scrape reads them directly. Names are etcd's, so
//! dashboards written against etcd work unchanged:
//!
//! - `grpc_server_started_total` / `grpc_server_handled_total`, labelled
//!   `grpc_type`, `grpc_service`, `grpc_method` (and `grpc_code` for
//!   handled), from [`grpc_middleware`] on the client port. A call is
//!   handled when its `grpc-status` is sent, in the headers of a
//!   trailers-only response or in the trailers; a call whose response
//!   is dropped before that (a client that goes away, which is how a
//!   `Watch` ends) is counted `Canceled`.
//! - `etcd_debugging_mvcc_watch_stream_total` / `..._watcher_total`
//!   (gauges): open `Watch` streams and the watchers on them.
//! - `etcd_debugging_mvcc_slow_watcher_total` (gauge): watchers that
//!   are behind the head: being caught up from history (a stream that
//!   lagged the event broadcast, or a create with a past
//!   `start_revision`), or on a stream whose client is not reading, so
//!   events are waiting on a full outbound buffer. Since #16 a watcher
//!   that falls behind is resynced rather than losing events, so this
//!   is the only sign that one is behind.
//! - `etcd_server_proposals_pending` (gauge): proposals this member is
//!   waiting on.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use http_body::Frame;
use hyper::body::Bytes;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;

/// Labels of `grpc_server_started_total`, as go-grpc-prometheus has them.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct GrpcLabels {
    pub grpc_type: String,
    pub grpc_service: String,
    pub grpc_method: String,
}

/// Labels of `grpc_server_handled_total`: the call and its status code
/// name (`OK`, `NotFound`, ...).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct GrpcHandledLabels {
    pub grpc_type: String,
    pub grpc_service: String,
    pub grpc_method: String,
    pub grpc_code: String,
}

/// Live traffic handles. Cloning a handle shares it.
#[derive(Default)]
pub struct Traffic {
    pub watch_streams: Gauge,
    pub watchers: Gauge,
    pub slow_watchers: Gauge,
    pub proposals_pending: Gauge,
    pub grpc_started: Family<GrpcLabels, Counter>,
    pub grpc_handled: Family<GrpcHandledLabels, Counter>,
}

/// Adds `n` to a gauge for as long as it is held.
pub struct GaugeHold {
    gauge: Gauge,
    n: i64,
}

impl GaugeHold {
    pub fn new(gauge: &Gauge, n: i64) -> Self {
        gauge.inc_by(n);
        Self {
            gauge: gauge.clone(),
            n,
        }
    }
}

impl Drop for GaugeHold {
    fn drop(&mut self) {
        self.gauge.dec_by(self.n);
    }
}

/// gRPC status code names, as `grpc_code` carries them.
const CODE_NAMES: [&str; 17] = [
    "OK",
    "Canceled",
    "Unknown",
    "InvalidArgument",
    "DeadlineExceeded",
    "NotFound",
    "AlreadyExists",
    "PermissionDenied",
    "ResourceExhausted",
    "FailedPrecondition",
    "Aborted",
    "OutOfRange",
    "Unimplemented",
    "Internal",
    "Unavailable",
    "DataLoss",
    "Unauthenticated",
];

fn code_name(status: &[u8]) -> &'static str {
    std::str::from_utf8(status)
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .and_then(|i| CODE_NAMES.get(i).copied())
        .unwrap_or("Unknown")
}

/// The call a request path names (`/etcdserverpb.KV/Range`), or `None`
/// for anything that is not one.
pub fn grpc_labels(path: &str) -> Option<GrpcLabels> {
    let (service, method) = path.strip_prefix('/')?.split_once('/')?;
    if service.is_empty() || method.is_empty() || method.contains('/') {
        return None;
    }
    let grpc_type = match (service, method) {
        ("etcdserverpb.Watch", "Watch") | ("etcdserverpb.Lease", "LeaseKeepAlive") => {
            "bidi_stream"
        }
        ("etcdserverpb.Maintenance", "Snapshot") | ("grpc.health.v1.Health", "Watch") => {
            "server_stream"
        }
        _ => "unary",
    };
    Some(GrpcLabels {
        grpc_type: grpc_type.to_string(),
        grpc_service: service.to_string(),
        grpc_method: method.to_string(),
    })
}

/// Counts every gRPC call on the client port (axum middleware; see
/// the module docs). Anything that is not gRPC passes through.
pub async fn grpc_middleware(
    State(traffic): State<Arc<Traffic>>,
    req: Request,
    next: Next,
) -> Response {
    let is_grpc = req
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .is_some_and(|v| v.as_bytes().starts_with(b"application/grpc"));
    let labels = match grpc_labels(req.uri().path()) {
        Some(l) if is_grpc => l,
        _ => return next.run(req).await,
    };
    traffic.grpc_started.get_or_create(&labels).inc();
    let mut handled = Handled {
        traffic,
        labels,
        done: false,
    };
    let resp = next.run(req).await;
    // A trailers-only response (every error from an interceptor, most
    // errors from a handler) carries the status in its headers.
    if let Some(status) = resp.headers().get("grpc-status") {
        handled.finish(code_name(status.as_bytes()));
    }
    let (parts, body) = resp.into_parts();
    Response::from_parts(
        parts,
        Body::new(CountingBody {
            inner: body,
            handled,
        }),
    )
}

/// Records one call in `grpc_server_handled_total`, once.
struct Handled {
    traffic: Arc<Traffic>,
    labels: GrpcLabels,
    done: bool,
}

impl Handled {
    fn finish(&mut self, code: &str) {
        if std::mem::replace(&mut self.done, true) {
            return;
        }
        let l = &self.labels;
        self.traffic
            .grpc_handled
            .get_or_create(&GrpcHandledLabels {
                grpc_type: l.grpc_type.clone(),
                grpc_service: l.grpc_service.clone(),
                grpc_method: l.grpc_method.clone(),
                grpc_code: code.to_string(),
            })
            .inc();
    }
}

impl Drop for Handled {
    fn drop(&mut self) {
        // The response went away before its status was sent.
        self.finish("Canceled");
    }
}

/// A response body that watches for the `grpc-status` trailer.
struct CountingBody {
    inner: Body,
    handled: Handled,
}

impl http_body::Body for CountingBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        let this = &mut *self;
        let polled = Pin::new(&mut this.inner).poll_frame(cx);
        match &polled {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(status) = frame.trailers_ref().and_then(|t| t.get("grpc-status")) {
                    this.handled.finish(code_name(status.as_bytes()));
                }
            }
            Poll::Ready(Some(Err(_))) => this.handled.finish("Unknown"),
            // Ended with no status at all: not a well-formed gRPC reply.
            Poll::Ready(None) => this.handled.finish("Unknown"),
            Poll::Pending => {}
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_parse_into_calls() {
        let l = grpc_labels("/etcdserverpb.KV/Range").unwrap();
        assert_eq!(
            (l.grpc_type.as_str(), l.grpc_service.as_str(), l.grpc_method.as_str()),
            ("unary", "etcdserverpb.KV", "Range")
        );
        assert_eq!(grpc_labels("/etcdserverpb.Watch/Watch").unwrap().grpc_type, "bidi_stream");
        assert_eq!(
            grpc_labels("/etcdserverpb.Lease/LeaseKeepAlive").unwrap().grpc_type,
            "bidi_stream"
        );
        assert_eq!(
            grpc_labels("/etcdserverpb.Maintenance/Snapshot").unwrap().grpc_type,
            "server_stream"
        );
        for bad in ["/health", "/", "", "/a/b/c", "//x", "/x/"] {
            assert!(grpc_labels(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn status_codes_have_names() {
        assert_eq!(code_name(b"0"), "OK");
        assert_eq!(code_name(b"5"), "NotFound");
        assert_eq!(code_name(b"16"), "Unauthenticated");
        assert_eq!(code_name(b"17"), "Unknown");
        assert_eq!(code_name(b"x"), "Unknown");
    }

    #[test]
    fn gauge_hold_releases_on_drop() {
        let g = Gauge::default();
        let a = GaugeHold::new(&g, 3);
        let b = GaugeHold::new(&g, 1);
        assert_eq!(g.get(), 4);
        drop(a);
        assert_eq!(g.get(), 1);
        drop(b);
        assert_eq!(g.get(), 0);
    }
}

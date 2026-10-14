//! WebSocket upgrades on the v3 JSON gateway (fastetcd#66), as etcd's
//! `tmc/grpc-websocket-proxy` serves them (etcd v3.6.11 wraps its gateway
//! in it, `server/embed/serve.go`): a browser cannot send a request body
//! that streams, so `/v3/watch` and `/v3/lease/keepalive` are reachable
//! from one only this way.
//!
//! A `/v3/` request with `Upgrade: websocket` becomes a `POST` to the
//! same route whose body is every message the client sends, each
//! followed by `\n`; every non-empty line of the response is sent back as
//! one text message. `Origin` and `Referer` are passed on. A token comes
//! from `Sec-WebSocket-Protocol: Bearer, <token>`, answered with the
//! `Bearer` protocol, and is passed on as the bare token (etcd passes
//! `Bearer <token>`, which its own auth then does not recognise).
//!
//! Two differences from etcd, decided on #66: an upgrade is accepted only
//! from a page of the gateway's own host, a client that sends no `Origin`
//! (not a browser), or an origin listed in `--gateway-ws-allowed-origins`
//! (`*`: any, as etcd); and etcd's `token` cookie is not read, since a
//! browser sends a cookie on its own and a page could then act as the
//! logged-in user without knowing the token.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{FromRequestParts, Request, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::body::Bytes;
use futures::{SinkExt, StreamExt};

/// Which pages may open a WebSocket to the gateway.
#[derive(Debug, Clone, Default)]
pub struct WsOrigins {
    any: bool,
    listed: Vec<String>,
}

impl WsOrigins {
    /// From `--gateway-ws-allowed-origins`: comma-separated origins
    /// (`https://console.example:8443`), or `*` for any.
    pub fn parse(flag: &str) -> Self {
        let listed: Vec<String> = flag
            .split(',')
            .map(|o| o.trim().trim_end_matches('/').to_ascii_lowercase())
            .filter(|o| !o.is_empty())
            .collect();
        Self { any: listed.iter().any(|o| o == "*"), listed }
    }

    /// Whether an upgrade with `origin` to a gateway reached as `host`
    /// may proceed.
    pub fn allows(&self, origin: Option<&str>, host: Option<&str>) -> bool {
        let Some(origin) = origin else {
            // Not a browser: it could send any Origin it liked anyway.
            return true;
        };
        if self.any {
            return true;
        }
        let origin = origin.trim_end_matches('/').to_ascii_lowercase();
        if self.listed.iter().any(|o| *o == origin) {
            return true;
        }
        // Same origin: the page was served by this host[:port].
        let origin_host = origin.split_once("://").map_or(origin.as_str(), |(_, h)| h);
        host.is_some_and(|h| h.eq_ignore_ascii_case(origin_host))
    }
}

fn is_upgrade(req: &Request) -> bool {
    let has = |name: header::HeaderName, word: &str| {
        req.headers()
            .get_all(name)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .any(|v| v.split(',').any(|t| t.trim().eq_ignore_ascii_case(word)))
    };
    has(header::UPGRADE, "websocket") && has(header::CONNECTION, "upgrade")
}

/// The token in `Sec-WebSocket-Protocol: Bearer, <token>`, as
/// grpc-websocket-proxy's `transformSubProtocolHeader` reads it.
fn subprotocol_token(protocols: &str) -> Option<String> {
    let (_, token) = protocols.split_once("Bearer,")?;
    Some(token.trim().to_string()).filter(|t| !t.is_empty())
}

/// Middleware for the gateway's routes: a WebSocket upgrade is served as
/// described in the module docs; anything else goes on as it came.
pub async fn layer(State(origins): State<Arc<WsOrigins>>, req: Request, next: Next) -> Response {
    if !is_upgrade(&req) {
        return next.run(req).await;
    }
    // In a block: a borrow of the request (its body is not `Sync`) must
    // not be held across an await.
    let allowed = {
        let header_str = |name: header::HeaderName| req.headers().get(name).and_then(|v| v.to_str().ok());
        origins.allows(header_str(header::ORIGIN), header_str(header::HOST))
    };
    if !allowed {
        return (
            StatusCode::FORBIDDEN,
            "WebSocket from this origin refused: see --gateway-ws-allowed-origins\n",
        )
            .into_response();
    }
    let (mut parts, _body) = req.into_parts();
    let upgrade = match WebSocketUpgrade::from_request_parts(&mut parts, &()).await {
        Ok(u) => u,
        Err(rejection) => return rejection.into_response(),
    };
    let protocols = parts
        .headers
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let token = protocols.as_deref().and_then(subprotocol_token);
    let bearer = protocols.as_deref().is_some_and(|p| p.starts_with("Bearer"));
    let upgrade = if bearer { upgrade.protocols(["Bearer"]) } else { upgrade };

    // The request the route sees: POST, the same URI, the TLS peer
    // certificates and the rest of the connection's extensions (a client
    // certificate's CN identifies the caller, #20), Origin and Referer,
    // and the token. Its body is filled from the socket.
    let mut inner = Request::builder().method(Method::POST).uri(parts.uri.clone());
    for name in [header::ORIGIN, header::REFERER] {
        if let Some(v) = parts.headers.get(&name) {
            inner = inner.header(name, v.clone());
        }
    }
    if let Some(t) = token.as_deref().and_then(|t| HeaderValue::from_str(t).ok()) {
        inner = inner.header(header::AUTHORIZATION, t);
    }
    let extensions = parts.extensions;
    upgrade.on_upgrade(move |socket| async move {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(16);
        let Ok(mut req) = inner.body(Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx)))
        else {
            return;
        };
        *req.extensions_mut() = extensions;
        proxy(socket, tx, next.run(req)).await;
    })
}

/// Pump the socket into the request body and the response's lines into
/// the socket, until either side ends. Dropping the response (when the
/// client goes away) ends the watch or keep-alive stream behind it.
async fn proxy(
    socket: WebSocket,
    body: tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
    response: impl std::future::Future<Output = Response>,
) {
    let (mut out, mut incoming) = socket.split();
    let reader = async move {
        while let Some(Ok(msg)) = incoming.next().await {
            let payload = match msg {
                Message::Text(t) => Bytes::from(t),
                Message::Binary(b) => Bytes::from(b),
                Message::Close(_) => break,
                Message::Ping(_) | Message::Pong(_) => continue,
            };
            let mut line = payload.to_vec();
            line.push(b'\n');
            if body.send(Ok(Bytes::from(line))).await.is_err() {
                break;
            }
        }
        // The client went away: the request body ends here.
    };
    let writer = async move {
        let mut data = response.await.into_body().into_data_stream();
        let mut pending: Vec<u8> = Vec::new();
        while let Some(Ok(chunk)) = data.next().await {
            pending.extend_from_slice(&chunk);
            while let Some(at) = pending.iter().position(|b| *b == b'\n') {
                let line: Vec<u8> = pending.drain(..=at).collect();
                let line = &line[..line.len() - 1];
                if line.is_empty() {
                    continue;
                }
                let text = String::from_utf8_lossy(line).into_owned();
                if out.send(Message::Text(text)).await.is_err() {
                    return;
                }
            }
        }
        if !pending.is_empty() {
            let _ = out.send(Message::Text(String::from_utf8_lossy(&pending).into_owned())).await;
        }
        let _ = out.send(Message::Close(None)).await;
    };
    // Either side ending ends both.
    tokio::select! {
        _ = reader => {}
        _ = writer => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_origin_listed_or_no_origin_only() {
        let none = WsOrigins::parse("");
        assert!(none.allows(None, Some("10.0.0.1:2379")), "not a browser");
        assert!(none.allows(Some("http://10.0.0.1:2379"), Some("10.0.0.1:2379")), "same origin");
        assert!(none.allows(Some("HTTPS://Etcd.Example:2379/"), Some("etcd.example:2379")));
        assert!(!none.allows(Some("http://evil.example"), Some("10.0.0.1:2379")));
        assert!(!none.allows(Some("http://10.0.0.1:2380"), Some("10.0.0.1:2379")), "another port");
        let listed = WsOrigins::parse("https://console.example:8443, http://other.example/");
        assert!(listed.allows(Some("https://console.example:8443"), Some("10.0.0.1:2379")));
        assert!(listed.allows(Some("http://other.example"), Some("10.0.0.1:2379")));
        assert!(!listed.allows(Some("https://console.example"), Some("10.0.0.1:2379")));
        assert!(WsOrigins::parse("*").allows(Some("http://anything.example"), None));
    }

    #[test]
    fn the_token_is_read_from_the_bearer_subprotocol_bare() {
        assert_eq!(subprotocol_token("Bearer, abc.123"), Some("abc.123".into()));
        assert_eq!(subprotocol_token("Bearer,abc"), Some("abc".into()));
        assert_eq!(subprotocol_token("Bearer"), None);
        assert_eq!(subprotocol_token("graphql-ws"), None);
    }
}

//! A start that makes no progress fails `/livez`; one that does stays ok
//! (fastetcd#219, finding d4d61f840472).
//!
//! The sampler read process-wide counters (`/proc/self/io`, `stat`), and
//! its own reads of them moved `rchar` every sample, so a start blocked
//! for good looked busy and `/livez` never failed. The listener and the
//! sampler now run on a thread the counters leave out. This file holds
//! one test, so nothing else runs in the process: the real sampler sees
//! only the "start" (this test's threads), with a 3 s stall instead of
//! 120 s.

use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fastetcd_server::startup::Startup;
use tokio::time::{sleep, Instant};

const STALL: Duration = Duration::from_secs(3);
const SAMPLE_EVERY: Duration = Duration::from_millis(200);

async fn livez(base: &str) -> (u16, String) {
    let r = reqwest::get(format!("{base}/livez")).await.expect("GET /livez");
    (r.status().as_u16(), r.text().await.unwrap())
}

/// Wait, doing nothing (no I/O, next to no CPU), until the start is seen
/// to stall; `None` if it never is within `within`.
async fn stalls_within(status: &fastetcd_server::startup::StartupStatus, within: Duration) -> Option<Duration> {
    let t = Instant::now();
    while t.elapsed() < within {
        sleep(Duration::from_millis(500)).await;
        let r = status.report();
        if !r.progressing() {
            return Some(r.since_progress);
        }
    }
    None
}

#[tokio::test]
async fn a_blocked_start_fails_livez_and_a_working_one_does_not() {
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
    let startup = Startup::with_timing(STALL, SAMPLE_EVERY);
    let status = startup.status();
    let _early = fastetcd_server::metrics::spawn_early_server(port, status.clone());
    let base = format!("http://{port}");

    // 1. Blocked: the start does nothing at all. The sampler keeps
    //    reading /proc and the listener is up, and still it stalls.
    let stalled = stalls_within(&status, STALL * 4).await;
    let r = status.report();
    assert!(stalled.is_some(), "a start doing nothing never stalled: {r:?}");
    assert!(r.since_start >= STALL, "{r:?}");
    let (code, body) = livez(&base).await;
    assert_eq!(code, 503, "{body}");
    assert!(body.starts_with("[-]starting failed: no progress for "), "{body}");

    // 2. Working: a thread reads a little every 300 ms, as a repair on a
    //    slow disk does. `/livez` stays ok for several stall periods.
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("data");
    std::fs::write(&file, vec![7u8; 64 * 1024]).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let worker = {
        let (stop, file) = (stop.clone(), file.clone());
        std::thread::spawn(move || {
            let mut buf = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                buf.clear();
                std::fs::File::open(&file).unwrap().read_to_end(&mut buf).unwrap();
                std::thread::sleep(Duration::from_millis(300));
            }
        })
    };
    sleep(Duration::from_secs(1)).await;
    let t = Instant::now();
    while t.elapsed() < STALL * 3 {
        let r = status.report();
        assert!(r.progressing(), "a start that reads was taken for a stall: {r:?}");
        assert!(r.since_progress < Duration::from_secs(1), "{r:?}");
        sleep(Duration::from_millis(500)).await;
    }
    let (code, body) = livez(&base).await;
    assert_eq!(code, 200, "{body}");
    assert!(body.starts_with("ok\nstarting for "), "{body}");

    // 3. The work stops: the stall is seen again.
    stop.store(true, Ordering::Relaxed);
    worker.join().unwrap();
    assert!(stalls_within(&status, STALL * 4).await.is_some(), "{:?}", status.report());
}

//! How long a task woken on the shared tokio runtime waits before it runs
//! (fastetcd#194).
//!
//! The raft apply loop (openraft's state-machine worker), the proposer,
//! watch fan-out and every gRPC stream share one multi-threaded runtime.
//! A task woken there (a channel send, an I/O readiness) waits in a run
//! queue until a worker thread polls it; under a busy watch or gRPC load
//! that wait is what a dedicated apply thread would remove. This probe
//! measures it: a plain OS thread, outside the runtime, spawns an empty
//! task every [`PROBE_EVERY`] and records the time from the spawn to the
//! task's first poll, in `fastetcd_runtime_schedule_delay_seconds` (a
//! histogram) and `fastetcd_runtime_schedule_delay_max_seconds`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

/// How often the probe spawns a task.
pub const PROBE_EVERY: Duration = Duration::from_millis(10);

/// 10 µs to 1 s.
const BUCKETS: [f64; 15] = [
    0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
    0.1, 0.25, 1.0,
];

/// The probe's readings.
#[derive(Debug)]
pub struct ScheduleProbe {
    delay: Histogram,
    max_seconds: Gauge<f64, AtomicU64>,
    max_nanos: AtomicU64,
    /// Held while a new maximum is stored and published, so the gauge
    /// cannot be set back to a smaller one by a slower writer
    /// (fastetcd#227).
    max_lock: Mutex<()>,
}

impl Default for ScheduleProbe {
    fn default() -> Self {
        Self {
            delay: Histogram::new(BUCKETS.iter().copied()),
            max_seconds: Gauge::default(),
            max_nanos: AtomicU64::new(0),
            max_lock: Mutex::new(()),
        }
    }
}

impl ScheduleProbe {
    /// Record one wait.
    pub fn observe(&self, waited: Duration) {
        self.delay.observe(waited.as_secs_f64());
        let nanos = waited.as_nanos().min(u64::MAX as u128) as u64;
        if self.max_nanos.load(Ordering::Relaxed) >= nanos {
            return; // not a new maximum: the common case takes no lock
        }
        let _held = self.max_lock.lock().unwrap_or_else(|e| e.into_inner());
        if self.max_nanos.fetch_max(nanos, Ordering::Relaxed) < nanos {
            self.max_seconds.set(waited.as_secs_f64());
        }
    }

    /// The longest wait seen.
    pub fn max(&self) -> Duration {
        Duration::from_nanos(self.max_nanos.load(Ordering::Relaxed))
    }

    pub fn register(&self, reg: &mut Registry) {
        reg.register(
            "fastetcd_runtime_schedule_delay_seconds",
            "Time a task spawned on the shared tokio runtime waited before its first poll, \
             sampled every 10 ms (what the raft apply task waits after a wake-up)",
            self.delay.clone(),
        );
        reg.register(
            "fastetcd_runtime_schedule_delay_max_seconds",
            "Longest wait of fastetcd_runtime_schedule_delay_seconds since the process started",
            self.max_seconds.clone(),
        );
    }

    /// Start the probe thread. It holds the probe weakly and stops once
    /// the probe is dropped (a server's metrics live as long as it).
    pub fn spawn(self: &Arc<Self>, runtime: tokio::runtime::Handle) -> std::io::Result<()> {
        let weak = Arc::downgrade(self);
        std::thread::Builder::new().name("runtime-probe".into()).spawn(move || loop {
            std::thread::sleep(PROBE_EVERY);
            let Some(probe) = weak.upgrade() else { break };
            let spawned = Instant::now();
            // A runtime that has shut down drops the task unpolled.
            drop(runtime.spawn(async move { probe.observe(spawned.elapsed()) }));
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observe_keeps_the_longest_wait() {
        let p = ScheduleProbe::default();
        p.observe(Duration::from_micros(30));
        p.observe(Duration::from_millis(3));
        p.observe(Duration::from_micros(10));
        assert_eq!(p.max(), Duration::from_millis(3));
        let mut reg = Registry::default();
        p.register(&mut reg);
        let mut text = String::new();
        prometheus_client::encoding::text::encode(&mut text, &reg).unwrap();
        assert!(text.contains("fastetcd_runtime_schedule_delay_seconds_count 3"), "{text}");
        assert!(text.contains("fastetcd_runtime_schedule_delay_max_seconds 0.003"), "{text}");
    }

    /// Concurrent observers leave the gauge at the longest wait, never a
    /// shorter one set after it (fastetcd#227).
    #[test]
    fn concurrent_observers_publish_the_longest_wait() {
        let p = Arc::new(ScheduleProbe::default());
        let threads: Vec<_> = (0..8u64)
            .map(|t| {
                let p = p.clone();
                std::thread::spawn(move || {
                    for i in 0..2000u64 {
                        p.observe(Duration::from_nanos((i * 8 + t) * 1000 + 1));
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(p.max(), Duration::from_nanos(((1999 * 8) + 7) * 1000 + 1));
        assert_eq!(p.max_seconds.get(), p.max().as_secs_f64());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_probe_measures_a_busy_runtime() {
        let p = Arc::new(ScheduleProbe::default());
        p.spawn(tokio::runtime::Handle::current()).unwrap();
        // Hold both workers in blocking code for 50 ms: a probe task
        // spawned meanwhile waits for one of them.
        let hogs: Vec<_> = (0..2)
            .map(|_| tokio::spawn(async { std::thread::sleep(Duration::from_millis(50)) }))
            .collect();
        for h in hogs {
            h.await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(p.max() >= Duration::from_millis(10), "max wait {:?}", p.max());
    }
}

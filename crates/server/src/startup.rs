//! Where a start's time goes (fastetcd#165).
//!
//! Each phase of startup logs one line under target `fastetcd::startup`
//! when it ends: `startup phase` with `phase`, `took_ms` and
//! `since_start_ms`. Once both ports listen, one summary line lists them
//! all; `raft_ready` (a leader is known, which `/readyz` waits for) is
//! logged when it happens, from a task. A slow start then says which
//! phase it spent its time in: redb's repair of a file not closed
//! cleanly, loading the RAM index, replaying the WAL, an election.
//!
//! [`StartupStatus`] is what a probe sees meanwhile (fastetcd#176): the
//! metrics listener is bound before the data file is opened, and until the
//! member serves, `/livez` answers `ok` while the start makes progress (a
//! phase ends, redb's repair reports, or the start's threads read, write or
//! use CPU) and fails once nothing has moved for [`STALL`]. The listener
//! and the sampler run on a thread of their own ([`spawn_monitor`]), which
//! is left out of the counters: watching the start, or answering a probe,
//! is not progress (fastetcd#219). `/readyz` and
//! `/health` fail until it serves. A kubelet's startup probe then waits
//! for a repair that is working, instead of killing it and having the
//! next start repair the file again from the beginning.

use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use fastetcd_raft::TypeConfig;
use openraft::Raft;

/// The phases of a start, in order: each is named when it ends.
pub const PHASES: [&str; 8] = [
    "data_file_open",
    "index_load",
    "state_machine_open",
    "upgrade_backup",
    "wal_open",
    "format_recovery",
    "raft_start",
    "listening",
];

/// How long a start may go without any progress before `/livez` fails:
/// no phase ended, no repair report, and the start's threads read, wrote
/// and ran nothing. A read of the disk blocked that long is a stall (#138), not a
/// slow disk: a repair on a saturated SMR drive still reads continuously.
pub const STALL: Duration = Duration::from_secs(120);

/// How often the counters are sampled.
pub const SAMPLE_EVERY: Duration = Duration::from_secs(2);

/// Read and write calls (`rchar` + `wchar`) that count as progress within
/// one sample. A parked tokio runtime reads and writes its eventfd, 8 bytes
/// a wake-up, with no work behind it (#219); a start reading its file from
/// the page cache moves megabytes.
pub const PROGRESS_CHARS: u64 = 64 * 1024;

/// CPU ticks (1/100 s) that count as progress within one sample: 50 ms. A
/// runtime waking for its timers uses microseconds; a start loading its
/// index or replaying its WAL uses most of a core.
pub const PROGRESS_TICKS: u64 = 5;

/// One sample of the start's threads (every thread but the monitor's).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
struct Counters {
    /// `rchar` + `wchar`: read and write calls, page cache or not.
    chars: u64,
    /// `read_bytes` + `write_bytes`: what reached the storage layer. No
    /// eventfd or /proc read counts here, so any growth is progress.
    storage: u64,
    /// utime + stime, in ticks.
    cpu: u64,
    /// redb's repair reports.
    repair: u64,
}

impl Counters {
    /// Moved since `prev` by more than an idle process does. A thread that
    /// ended takes its counts out of the sums: that alone is not progress.
    fn moved_since(&self, prev: &Counters) -> bool {
        self.storage > prev.storage
            || self.chars.saturating_sub(prev.chars) >= PROGRESS_CHARS
            || self.cpu.saturating_sub(prev.cpu) >= PROGRESS_TICKS
            || self.repair != prev.repair
    }
}

/// The phases of one start, in order.
pub struct Startup {
    start: Instant,
    last: Instant,
    phases: Vec<(&'static str, Duration)>,
    status: Arc<StartupStatus>,
}

/// What a probe sees while the member starts (#176). Shared with the
/// metrics listener, which is bound before the data file is opened.
#[derive(Debug)]
pub struct StartupStatus {
    start: Instant,
    stall: Duration,
    sample_every: Duration,
    /// The monitor thread ([`spawn_monitor`]): its reads of `/proc`, and
    /// the probes it answers, are not the start's progress (#219).
    monitor_tid: OnceLock<u64>,
    inner: Mutex<StatusInner>,
}

#[derive(Debug)]
struct StatusInner {
    /// The phase running now (`PHASES[i]`), or "serving" once done.
    phase: &'static str,
    phase_started: Instant,
    last_progress: Instant,
    /// The last sample.
    sample: Option<Counters>,
    done: bool,
}

/// One answer: where the start is and whether it is moving.
#[derive(Debug, Clone, PartialEq)]
pub struct StartupReport {
    pub phase: &'static str,
    pub since_start: Duration,
    pub in_phase: Duration,
    pub since_progress: Duration,
    /// redb's repair estimate, while it repairs the data file.
    pub repair: Option<f64>,
    pub done: bool,
    /// How long nothing may move ([`STALL`]).
    pub stall: Duration,
}

impl StartupReport {
    /// Making progress: something moved within [`STALL`].
    pub fn progressing(&self) -> bool {
        self.since_progress < self.stall
    }

    /// One line for a probe's body: `phase data_file_open for 45 s (redb
    /// repair 60%), last progress 2 s ago`.
    pub fn describe(&self) -> String {
        let repair = match self.repair {
            Some(p) if self.phase == "data_file_open" => {
                format!(" (redb repairing the data file: {:.0}%)", p * 100.0)
            }
            _ => String::new(),
        };
        format!(
            "starting for {} s: phase {} for {} s{repair}, last progress {} s ago",
            self.since_start.as_secs(),
            self.phase,
            self.in_phase.as_secs(),
            self.since_progress.as_secs()
        )
    }
}

impl StartupStatus {
    fn new(start: Instant, stall: Duration, sample_every: Duration) -> Arc<Self> {
        Arc::new(Self {
            start,
            stall,
            sample_every,
            monitor_tid: OnceLock::new(),
            inner: Mutex::new(StatusInner {
                phase: PHASES[0],
                phase_started: start,
                last_progress: start,
                sample: None,
                done: false,
            }),
        })
    }

    /// `ended` ended: the next phase runs from now.
    fn ended(&self, ended: &str) {
        let now = Instant::now();
        let mut s = self.inner.lock().unwrap();
        let next = PHASES.iter().position(|p| *p == ended).and_then(|i| PHASES.get(i + 1));
        s.phase = next.copied().unwrap_or("serving");
        s.phase_started = now;
        s.last_progress = now;
    }

    /// The member serves: the full routes answer from now on.
    pub fn done(&self) {
        let mut s = self.inner.lock().unwrap();
        s.done = true;
        s.phase = "serving";
    }

    pub fn is_done(&self) -> bool {
        self.inner.lock().unwrap().done
    }

    /// Look at the I/O and CPU counters of every thread but the
    /// monitor's, and redb's repair reports: storage I/O, a repair report,
    /// [`PROGRESS_CHARS`] of reads and writes or [`PROGRESS_TICKS`] of CPU
    /// since the last sample is progress. Called every [`SAMPLE_EVERY`] on
    /// the monitor thread.
    pub fn sample(&self) {
        let repair_steps = fastetcd_storage::redb_engine::RedbEngine::repair_progress().map_or(0, |(_, n)| n);
        // No /proc (not Linux), or no monitor thread to leave out: count
        // every sample as progress rather than fail a start we cannot see
        // into.
        let sample = self
            .monitor_tid
            .get()
            .and_then(|tid| threads_but(*tid))
            .map(|c| Counters { repair: repair_steps, ..c });
        self.observe(sample, Instant::now());
    }

    fn observe(&self, sample: Option<Counters>, now: Instant) {
        let mut s = self.inner.lock().unwrap();
        let moved = match (&sample, &s.sample) {
            (Some(cur), Some(prev)) => cur.moved_since(prev),
            _ => true,
        };
        if moved {
            s.last_progress = now;
        }
        s.sample = sample;
    }

    /// Where the start is now.
    pub fn report(&self) -> StartupReport {
        self.report_at(Instant::now())
    }

    fn report_at(&self, now: Instant) -> StartupReport {
        let s = self.inner.lock().unwrap();
        StartupReport {
            phase: s.phase,
            since_start: now.saturating_duration_since(self.start),
            in_phase: now.saturating_duration_since(s.phase_started),
            since_progress: now.saturating_duration_since(s.last_progress),
            repair: fastetcd_storage::redb_engine::RedbEngine::repair_progress().map(|(p, _)| p),
            done: s.done,
            stall: self.stall,
        }
    }
}

/// Run `serve` (the early metrics listener) and the progress sampler on
/// a thread of their own, with a runtime of its own, until the process
/// ends. That thread is left out of the counters (#219): before, the
/// sampler read process-wide counters, and its own reads of `/proc` moved
/// them, so a start that did nothing never stalled.
pub fn spawn_monitor<F>(status: Arc<StartupStatus>, serve: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    let spawned = std::thread::Builder::new().name("fastetcd-starting".into()).spawn(move || {
        if let Some(tid) = this_thread_id() {
            let _ = status.monitor_tid.set(tid);
        }
        let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
            Ok(rt) => rt,
            Err(e) => {
                tracing::error!(target: "fastetcd::startup", "the startup monitor's runtime: {e}");
                return;
            }
        };
        runtime.block_on(async move {
            let sampler = {
                let status = status.clone();
                async move {
                    // Progress is sampled, so a probe sees what moved since.
                    while !status.is_done() {
                        status.sample();
                        tokio::time::sleep(status.sample_every).await;
                    }
                }
            };
            tokio::join!(sampler, serve);
        });
    });
    if let Err(e) = spawned {
        tracing::error!(target: "fastetcd::startup", "the startup monitor's thread: {e}");
    }
}

/// This thread's id (`/proc/thread-self` names `<pid>/task/<tid>`).
fn this_thread_id() -> Option<u64> {
    let link = std::fs::read_link("/proc/thread-self").ok()?;
    link.file_name()?.to_str()?.parse().ok()
}

/// Every thread of this process but `skip`: what they read and wrote
/// (`/proc/self/task/<tid>/io`) and their user + system CPU ticks (`stat`
/// fields 14 and 15). A thread that ends between the listing and the
/// reads is left out.
fn threads_but(skip: u64) -> Option<Counters> {
    let mut sum = Counters::default();
    for entry in std::fs::read_dir("/proc/self/task").ok()? {
        let Ok(entry) = entry else { continue };
        let Some(tid) = entry.file_name().to_str().and_then(|n| n.parse::<u64>().ok()) else { continue };
        if tid == skip {
            continue;
        }
        let dir = entry.path();
        if let (Ok(i), Ok(st)) =
            (std::fs::read_to_string(dir.join("io")), std::fs::read_to_string(dir.join("stat")))
        {
            let (chars, storage) = io_counts(&i).unwrap_or((0, 0));
            sum.chars += chars;
            sum.storage += storage;
            sum.cpu += cpu_ticks(&st).unwrap_or(0);
        }
    }
    Some(sum)
}

/// (`rchar` + `wchar`, `read_bytes` + `write_bytes`) of an `io` file.
fn io_counts(io: &str) -> Option<(u64, u64)> {
    let field = |name: &str| {
        io.lines().find_map(|l| l.strip_prefix(name)?.trim().parse::<u64>().ok())
    };
    Some((field("rchar:")? + field("wchar:")?, field("read_bytes:")? + field("write_bytes:")?))
}

/// User + system CPU ticks of a `stat` file (fields 14 and 15).
fn cpu_ticks(stat: &str) -> Option<u64> {
    // The command name (field 2) may hold spaces; fields after it follow
    // its closing parenthesis.
    let rest = &stat[stat.rfind(')')? + 2..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // `rest` starts at field 3, so fields 14 and 15 are at 11 and 12.
    Some(fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?)
}

impl Default for Startup {
    fn default() -> Self {
        Self::new()
    }
}

impl Startup {
    pub fn new() -> Self {
        Self::with_timing(STALL, SAMPLE_EVERY)
    }

    /// A start whose `/livez` fails after `stall` without progress,
    /// sampled every `sample_every` (tests use seconds, not minutes).
    pub fn with_timing(stall: Duration, sample_every: Duration) -> Self {
        let now = Instant::now();
        Self { start: now, last: now, phases: Vec::new(), status: StartupStatus::new(now, stall, sample_every) }
    }

    /// What a probe sees while this start runs (#176).
    pub fn status(&self) -> Arc<StartupStatus> {
        self.status.clone()
    }

    /// End phase `name`, begun when the previous one ended, and log it.
    pub fn phase(&mut self, name: &'static str) {
        self.phase_with(name, "");
    }

    /// [`Startup::phase`] with a note on what the phase found (a repair,
    /// how many keys).
    pub fn phase_with(&mut self, name: &'static str, note: &str) {
        let now = Instant::now();
        let took = now - self.last;
        self.last = now;
        self.phases.push((name, took));
        self.status.ended(name);
        tracing::info!(
            target: "fastetcd::startup",
            phase = name,
            took_ms = took.as_millis() as u64,
            since_start_ms = (now - self.start).as_millis() as u64,
            note,
            "startup phase"
        );
    }

    /// When the timer was made: the start of startup.
    pub fn started(&self) -> Instant {
        self.start
    }

    /// Since the timer was made.
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    /// `name=Nms` for each phase, in order.
    pub fn summary(&self) -> String {
        self.phases
            .iter()
            .map(|(name, took)| format!("{name}={}ms", took.as_millis()))
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// End the `listening` phase (both ports bound) and log the summary.
    pub fn listening(mut self) {
        self.phase("listening");
        tracing::info!(
            target: "fastetcd::startup",
            version = env!("CARGO_PKG_VERSION"),
            since_start_ms = self.elapsed().as_millis() as u64,
            phases = %self.summary(),
            "startup: listening"
        );
    }
}

/// Log `raft_ready` once this member knows a leader, measured from
/// `start`. Gives up quietly if raft stops first.
pub fn log_raft_ready(raft: Raft<TypeConfig>, start: Instant) {
    tokio::spawn(async move {
        let mut metrics = raft.metrics();
        loop {
            let (leader, me, term) = {
                let m = metrics.borrow_and_update();
                (m.current_leader, m.id, m.current_term)
            };
            if let Some(leader) = leader {
                tracing::info!(
                    target: "fastetcd::startup",
                    phase = "raft_ready",
                    since_start_ms = start.elapsed().as_millis() as u64,
                    leader,
                    term,
                    is_leader = leader == me,
                    "startup phase"
                );
                return;
            }
            if metrics.changed().await.is_err() {
                return;
            }
        }
    });
}

/// Log etcd's `ready to serve client requests` once this member serves
/// clients (call it once the client listener is up) and knows a leader,
/// as etcd does after it has joined: tools that start etcd and wait for
/// the line (etcd's e2e and robustness harnesses, #195) can start
/// fastetcd. Gives up quietly if raft stops first.
pub fn log_ready_to_serve(raft: Raft<TypeConfig>) {
    tokio::spawn(async move {
        let mut metrics = raft.metrics();
        loop {
            if metrics.borrow_and_update().current_leader.is_some() {
                tracing::info!("ready to serve client requests");
                return;
            }
            if metrics.changed().await.is_err() {
                return;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phases_are_listed_in_order() {
        let mut s = Startup::new();
        s.phase("a");
        std::thread::sleep(Duration::from_millis(20));
        s.phase_with("b", "note");
        let summary = s.summary();
        assert!(summary.starts_with("a=") && summary.contains(" b="), "{summary}");
        assert!(s.phases[1].1 >= Duration::from_millis(20));
    }

    #[test]
    fn the_status_follows_the_phases() {
        let mut s = Startup::new();
        let status = s.status();
        assert_eq!(status.report().phase, "data_file_open");
        s.phase("data_file_open");
        assert_eq!(status.report().phase, "index_load");
        s.phase("raft_start");
        assert_eq!(status.report().phase, "listening");
        s.listening();
        assert_eq!(status.report().phase, "serving");
        assert!(!status.is_done());
        status.done();
        assert!(status.is_done());
    }

    fn c(chars: u64, storage: u64, cpu: u64, repair: u64) -> Option<Counters> {
        Some(Counters { chars, storage, cpu, repair })
    }

    #[test]
    fn a_start_with_nothing_moving_stalls() {
        let t0 = Instant::now();
        let status = StartupStatus::new(t0, STALL, SAMPLE_EVERY);
        status.observe(c(100, 0, 5, 0), t0);
        // An idle runtime's eventfd and a tick of CPU every sample, for
        // STALL: no progress (#219).
        let mut t = t0;
        for i in 1..=(STALL.as_secs() / 2) {
            t = t0 + SAMPLE_EVERY * i as u32;
            status.observe(c(100 + 16 * i, 0, 5 + i, 0), t);
        }
        let r = status.report_at(t0 + STALL);
        assert!(!r.progressing(), "{r:?}");
        assert_eq!(r.since_progress, STALL);
        // So is a thread that ended (the sums drop).
        status.observe(c(0, 0, 0, 0), t + SAMPLE_EVERY);
        assert!(!status.report_at(t0 + STALL + SAMPLE_EVERY).progressing());
        // Reads of PROGRESS_CHARS move it again.
        let t1 = t0 + STALL * 2;
        status.observe(c(PROGRESS_CHARS, 0, 0, 0), t1);
        let r = status.report_at(t1 + Duration::from_secs(1));
        assert!(r.progressing(), "{r:?}");
        assert_eq!(r.since_progress, Duration::from_secs(1));
        // So does any storage I/O, PROGRESS_TICKS of CPU, or a repair report.
        for (i, s) in [
            c(PROGRESS_CHARS, 4096, 0, 0),
            c(PROGRESS_CHARS, 4096, PROGRESS_TICKS, 0),
            c(PROGRESS_CHARS, 4096, PROGRESS_TICKS, 1),
        ]
        .into_iter()
        .enumerate()
        {
            let at = t1 + STALL * (i as u32 + 1);
            status.observe(s, at);
            assert_eq!(status.report_at(at).since_progress, Duration::ZERO, "{s:?}");
        }
        // Unknown counters count as progress.
        status.observe(None, t0 + STALL * 9);
        assert!(status.report_at(t0 + STALL * 9).progressing());
    }

    #[test]
    fn the_counters_parse() {
        if !cfg!(target_os = "linux") {
            return;
        }
        let me = this_thread_id().expect("/proc/thread-self");
        assert!(threads_but(me).is_some(), "/proc/self/task");
        let io = "rchar: 10\nwchar: 5\nsyscr: 1\nsyscw: 1\nread_bytes: 4096\nwrite_bytes: 8192\n";
        assert_eq!(io_counts(io), Some((15, 12288)));
        let stat = "123 (a b) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15";
        // utime is field 14, stime 15.
        assert_eq!(cpu_ticks(stat), Some(11 + 12));
    }

    #[test]
    fn the_description_names_the_repair() {
        let r = StartupReport {
            phase: "data_file_open",
            since_start: Duration::from_secs(50),
            in_phase: Duration::from_secs(45),
            since_progress: Duration::from_secs(2),
            repair: Some(0.6),
            done: false,
            stall: STALL,
        };
        assert_eq!(
            r.describe(),
            "starting for 50 s: phase data_file_open for 45 s (redb repairing the data file: 60%), \
             last progress 2 s ago"
        );
    }
}

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
//! phase ends, redb's repair reports, or the process reads, writes or uses
//! CPU) and fails once nothing has moved for [`STALL`]. `/readyz` and
//! `/health` fail until it serves. A kubelet's startup probe then waits
//! for a repair that is working, instead of killing it and having the
//! next start repair the file again from the beginning.

use std::sync::{Arc, Mutex};
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
/// no phase ended, no repair report, and the process read, wrote and ran
/// nothing. A read of the disk blocked that long is a stall (#138), not a
/// slow disk: a repair on a saturated SMR drive still reads continuously.
pub const STALL: Duration = Duration::from_secs(120);

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
    inner: Mutex<StatusInner>,
}

#[derive(Debug)]
struct StatusInner {
    /// The phase running now (`PHASES[i]`), or "serving" once done.
    phase: &'static str,
    phase_started: Instant,
    last_progress: Instant,
    /// The last sample: (I/O chars, CPU ticks, repair reports).
    sample: Option<(u64, u64, u64)>,
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
}

impl StartupReport {
    /// Making progress: something moved within [`STALL`].
    pub fn progressing(&self) -> bool {
        self.since_progress < STALL
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
    fn new(start: Instant) -> Arc<Self> {
        Arc::new(Self {
            start,
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

    /// Look at the process's I/O and CPU counters and redb's repair
    /// reports; any change is progress. Called every few seconds.
    pub fn sample(&self) {
        let repair_steps = fastetcd_storage::redb_engine::RedbEngine::repair_progress().map_or(0, |(_, n)| n);
        let sample = match (io_chars(), cpu_ticks()) {
            (Some(io), Some(cpu)) => Some((io, cpu, repair_steps)),
            // No /proc (not Linux): count every sample as progress rather
            // than fail a start we cannot see into.
            _ => None,
        };
        self.observe(sample, Instant::now());
    }

    fn observe(&self, sample: Option<(u64, u64, u64)>, now: Instant) {
        let mut s = self.inner.lock().unwrap();
        if sample.is_none() || sample != s.sample {
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
        }
    }
}

/// Characters read and written by this process (`/proc/self/io`'s
/// `rchar` + `wchar`: every read and write call, page cache or disk).
fn io_chars() -> Option<u64> {
    let io = std::fs::read_to_string("/proc/self/io").ok()?;
    let field = |name: &str| {
        io.lines().find_map(|l| l.strip_prefix(name)?.trim().parse::<u64>().ok())
    };
    Some(field("rchar:")? + field("wchar:")?)
}

/// User + system CPU ticks of this process (`/proc/self/stat` fields 14
/// and 15).
fn cpu_ticks() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
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
        let now = Instant::now();
        Self { start: now, last: now, phases: Vec::new(), status: StartupStatus::new(now) }
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

    #[test]
    fn a_start_with_nothing_moving_stalls() {
        let t0 = Instant::now();
        let status = StartupStatus::new(t0);
        status.observe(Some((100, 5, 0)), t0);
        // The same counters for STALL: no progress.
        status.observe(Some((100, 5, 0)), t0 + STALL);
        let r = status.report_at(t0 + STALL);
        assert!(!r.progressing(), "{r:?}");
        assert_eq!(r.since_progress, STALL);
        // A read moves it again.
        status.observe(Some((4196, 5, 0)), t0 + STALL + Duration::from_secs(1));
        let r = status.report_at(t0 + STALL + Duration::from_secs(2));
        assert!(r.progressing(), "{r:?}");
        assert_eq!(r.since_progress, Duration::from_secs(1));
        // So does CPU, or a repair report.
        status.observe(Some((4196, 6, 0)), t0 + STALL * 2);
        assert!(status.report_at(t0 + STALL * 2).progressing());
        status.observe(Some((4196, 6, 1)), t0 + STALL * 3);
        assert!(status.report_at(t0 + STALL * 3).progressing());
        // Unknown counters count as progress.
        status.observe(None, t0 + STALL * 5);
        assert!(status.report_at(t0 + STALL * 5).progressing());
    }

    #[test]
    fn this_process_has_counters() {
        if cfg!(target_os = "linux") {
            let io = io_chars().expect("rchar/wchar");
            cpu_ticks().expect("utime/stime");
            // A read is progress.
            let _ = std::fs::read_to_string("/proc/self/status");
            assert!(io_chars().unwrap() > io);
        }
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
        };
        assert_eq!(
            r.describe(),
            "starting for 50 s: phase data_file_open for 45 s (redb repairing the data file: 60%), \
             last progress 2 s ago"
        );
    }
}

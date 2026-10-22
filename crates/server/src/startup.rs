//! Where a start's time goes (fastetcd#165).
//!
//! Each phase of startup logs one line under target `fastetcd::startup`
//! when it ends: `startup phase` with `phase`, `took_ms` and
//! `since_start_ms`. Once both ports listen, one summary line lists them
//! all; `raft_ready` (a leader is known, which `/readyz` waits for) is
//! logged when it happens, from a task. A slow start then says which
//! phase it spent its time in: redb's repair of a file not closed
//! cleanly, loading the RAM index, replaying the WAL, an election.

use std::time::{Duration, Instant};

use fastetcd_raft::TypeConfig;
use openraft::Raft;

/// The phases of one start, in order.
pub struct Startup {
    start: Instant,
    last: Instant,
    phases: Vec<(&'static str, Duration)>,
}

impl Default for Startup {
    fn default() -> Self {
        Self::new()
    }
}

impl Startup {
    pub fn new() -> Self {
        let now = Instant::now();
        Self { start: now, last: now, phases: Vec::new() }
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
}

//! Results, the way stormcentral reads them (docs/test-standard.md): one
//! JSON object per test on stdout, then `{"summary": …}`. Exit 0 all
//! passed, 1 a test failed, 2 the suite could not run.

use serde_json::{json, Value};
use std::future::Future;
use std::io::Write;
use std::time::Instant;

/// What one test found.
pub enum Outcome {
    Pass(String),
    Fail(String),
    /// Not run, and why. Never a pass.
    Skip(String),
}

pub fn pass(d: impl Into<String>) -> Outcome {
    Outcome::Pass(d.into())
}
pub fn fail(d: impl Into<String>) -> Outcome {
    Outcome::Fail(d.into())
}
pub fn skip(d: impl Into<String>) -> Outcome {
    Outcome::Skip(d.into())
}

/// A test body's error is the test failing (fastetcd did not do what it
/// should), with the error as the detail. Only the environment (no work
/// directory, no binary) ends a suite as "could not run".
pub async fn attempt<F>(f: F) -> Outcome
where
    F: Future<Output = anyhow::Result<Outcome>>,
{
    match f.await {
        Ok(o) => o,
        Err(e) => Outcome::Fail(format!("{e:#}")),
    }
}

#[derive(Default)]
pub struct Report {
    pub pass: u32,
    pub fail: u32,
    pub skip: u32,
}

impl Report {
    pub fn line(&mut self, test: &str, status: &str, ms: u128, detail: &str, extra: Option<Value>) {
        match status {
            "pass" => self.pass += 1,
            "fail" => self.fail += 1,
            _ => self.skip += 1,
        }
        let mut v = json!({"test": test, "status": status, "ms": ms as u64, "detail": detail});
        if let Some(Value::Object(m)) = extra {
            for (k, x) in m {
                v[k] = x;
            }
        }
        emit(&v);
    }

    /// Run one test and record it; an error inside it is a failure.
    pub async fn check<F>(&mut self, test: &str, f: F)
    where
        F: Future<Output = anyhow::Result<Outcome>>,
    {
        let t = Instant::now();
        let o = attempt(f).await;
        let ms = t.elapsed().as_millis();
        match o {
            Outcome::Pass(d) => self.line(test, "pass", ms, &d, None),
            Outcome::Fail(d) => self.line(test, "fail", ms, &d, None),
            Outcome::Skip(d) => self.line(test, "skip", ms, &d, None),
        }
    }

    pub fn finish(&self) -> i32 {
        self.summary();
        if self.fail > 0 {
            1
        } else {
            0
        }
    }

    /// The suite could not run: say why, as a skip (it is not a pass), and
    /// exit 2. stormcentral never counts a 2 as a pass.
    pub fn abort(&mut self, why: &anyhow::Error) -> i32 {
        self.line("could-not-run", "skip", 0, &format!("{why:#}"), None);
        self.summary();
        2
    }

    fn summary(&self) {
        emit(&json!({"summary": {"pass": self.pass, "fail": self.fail, "skip": self.skip}}));
    }
}

fn emit(v: &Value) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{v}");
    let _ = out.flush();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_and_exit_codes() {
        let mut r = Report::default();
        r.line("a", "pass", 1, "", None);
        assert_eq!(r.finish(), 0);
        r.line("b", "skip", 1, "", None);
        assert_eq!(r.finish(), 0, "a skip is not a failure");
        r.line("c", "fail", 1, "", None);
        assert_eq!(r.finish(), 1);
        assert_eq!((r.pass, r.fail, r.skip), (1, 1, 1));
        assert_eq!(r.abort(&anyhow::anyhow!("x")), 2);
    }

    #[tokio::test]
    async fn an_error_in_a_test_is_a_failure() {
        let mut r = Report::default();
        r.check("e", async { anyhow::bail!("boom") }).await;
        assert_eq!(r.fail, 1);
    }
}

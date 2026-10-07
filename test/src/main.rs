//! fastetcd-test: fastetcd on a running node, tested from a pod, per
//! stormcentral `docs/test-standard.md` (#36).
//!
//! `/test short|medium|long` prints one JSON object per test and a
//! summary, and exits 0 if everything passed, 1 if a test failed, and 2 if
//! the suite could not run. See test/README.md for what each suite covers.

mod client;
mod env;
mod long;
mod medium;
mod member;
mod report;
mod short;

use report::Report;

#[tokio::main]
async fn main() {
    let suite = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("STORM_SUITE").ok())
        .unwrap_or_else(|| "short".into());
    std::process::exit(run(&suite).await);
}

async fn run(suite: &str) -> i32 {
    let mut rep = Report::default();
    if !matches!(suite, "short" | "medium" | "long") {
        return rep.abort(&anyhow::anyhow!("unknown suite {suite:?}: use short, medium or long"));
    }
    let env = match env::Env::discover(suite) {
        Ok(e) => e,
        Err(e) => return rep.abort(&e),
    };
    eprintln!(
        "fastetcd-test {}: node {}, run {}, commit {}, binary {}, work {}, {:?} to run",
        env.suite,
        env.node.as_deref().unwrap_or("-"),
        env.run_id,
        if env.commit.is_empty() { "?" } else { &env.commit },
        env.bin.display(),
        env.work.display(),
        env.left()
    );
    let r = match suite {
        "short" => short::run(&env, &mut rep).await,
        "medium" => medium::run(&env, &mut rep).await,
        _ => long::run(&env, &mut rep).await,
    };
    // The members this run started are gone (each test kills its own);
    // their data goes too unless a test failed, when it is kept for a look.
    if rep.fail == 0 {
        let _ = std::fs::remove_dir_all(&env.work);
    }
    match r {
        Ok(()) => rep.finish(),
        Err(e) => rep.abort(&e),
    }
}

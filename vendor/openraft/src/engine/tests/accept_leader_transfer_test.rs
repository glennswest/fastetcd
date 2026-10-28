//! fastetcd: `Engine::accept_leader_transfer` and the lease waiver it sets.

use std::sync::Arc;

use maplit::btreeset;
use pretty_assertions::assert_eq;

use crate::core::ServerState;
use crate::engine::testing::UTConfig;
use crate::engine::Command;
use crate::engine::Engine;
use crate::engine::LogIdList;
use crate::raft::VoteRequest;
use crate::raft::VoteResponse;
use crate::testing::log_id;
use crate::utime::UTime;
use crate::EffectiveMembership;
use crate::Membership;
use crate::TokioInstant;
use crate::Vote;

fn m012() -> Membership<u64, ()> {
    Membership::<u64, ()>::new(vec![btreeset! {0,1,2}], None)
}

/// Node `id` follows leader 1 (vote committed (2,1), updated just now, so its lease is live), with
/// log up to (2,1,3).
fn eng(id: u64) -> Engine<UTConfig> {
    let mut eng = Engine::default();
    eng.state.enable_validation(false); // Disable validation for incomplete state

    eng.config.id = id;
    eng.state.vote = UTime::new(TokioInstant::now(), Vote::new_committed(2, 1));
    eng.state.server_state = ServerState::Follower;
    eng.state.log_ids = LogIdList::new(vec![log_id(2, 1, 3)]);
    eng.state
        .membership_state
        .set_effective(Arc::new(EffectiveMembership::new(Some(log_id(1, 1, 1)), m012())));
    eng.output.take_commands();
    eng
}

fn vote_req_from_2() -> VoteRequest<u64> {
    VoteRequest {
        vote: Vote::new(3, 2),
        last_log_id: Some(log_id(2, 1, 3)),
    }
}

#[test]
fn test_lease_refuses_a_vote_without_a_transfer() -> anyhow::Result<()> {
    let mut eng = eng(0);

    let resp = eng.handle_vote_req(vote_req_from_2());

    assert_eq!(
        VoteResponse::new(Vote::new_committed(2, 1), Some(log_id(2, 1, 3)), false),
        resp
    );
    assert_eq!(Vote::new_committed(2, 1), *eng.state.vote_ref());
    Ok(())
}

#[test]
fn test_transfer_waives_the_lease_of_that_vote() -> anyhow::Result<()> {
    let mut eng = eng(0);

    assert!(eng.accept_leader_transfer(Vote::new_committed(2, 1), 2, Some(log_id(2, 1, 3))));
    assert!(eng.output.take_commands().is_empty(), "not the target: no election");

    let resp = eng.handle_vote_req(vote_req_from_2());

    assert_eq!(VoteResponse::new(Vote::new(3, 2), Some(log_id(2, 1, 3)), true), resp);
    assert_eq!(Vote::new(3, 2), *eng.state.vote_ref());
    Ok(())
}

#[test]
fn test_transfer_from_another_vote_is_ignored() -> anyhow::Result<()> {
    let mut eng = eng(0);

    assert!(!eng.accept_leader_transfer(Vote::new_committed(1, 1), 2, Some(log_id(2, 1, 3))));
    assert!(eng.lease_waived.is_none());

    let resp = eng.handle_vote_req(vote_req_from_2());
    assert!(!resp.vote_granted);
    assert_eq!(Vote::new_committed(2, 1), *eng.state.vote_ref());
    Ok(())
}

#[test]
fn test_waiver_does_not_cover_a_later_vote() -> anyhow::Result<()> {
    let mut eng = eng(0);

    assert!(eng.accept_leader_transfer(Vote::new_committed(2, 1), 2, Some(log_id(2, 1, 3))));
    // A new leader of a later term: its lease is enforced again.
    eng.state.vote.update(TokioInstant::now(), Vote::new_committed(3, 1));

    let resp = eng.handle_vote_req(VoteRequest {
        vote: Vote::new(4, 2),
        last_log_id: Some(log_id(2, 1, 3)),
    });
    assert!(!resp.vote_granted);
    assert_eq!(Vote::new_committed(3, 1), *eng.state.vote_ref());
    Ok(())
}

#[test]
fn test_waiver_does_not_skip_the_log_check() -> anyhow::Result<()> {
    let mut eng = eng(0);

    assert!(eng.accept_leader_transfer(Vote::new_committed(2, 1), 2, Some(log_id(2, 1, 3))));

    let resp = eng.handle_vote_req(VoteRequest {
        vote: Vote::new(3, 2),
        last_log_id: Some(log_id(2, 1, 2)),
    });
    assert!(!resp.vote_granted);
    assert_eq!(Vote::new_committed(2, 1), *eng.state.vote_ref());
    Ok(())
}

#[test]
fn test_target_with_the_leaders_log_elects() -> anyhow::Result<()> {
    let mut eng = eng(2);

    assert!(eng.accept_leader_transfer(Vote::new_committed(2, 1), 2, Some(log_id(2, 1, 3))));

    assert_eq!(Vote::new(3, 2), *eng.state.vote_ref());
    assert_eq!(ServerState::Candidate, eng.state.server_state);
    assert_eq!(
        vec![
            Command::SaveVote { vote: Vote::new(3, 2) },
            Command::SendVote {
                vote_req: VoteRequest {
                    vote: Vote::new(3, 2),
                    last_log_id: Some(log_id(2, 1, 3)),
                },
            },
        ],
        eng.output.take_commands()
    );
    Ok(())
}

#[test]
fn test_target_behind_the_leader_does_not_elect() -> anyhow::Result<()> {
    let mut eng = eng(2);

    assert!(eng.accept_leader_transfer(Vote::new_committed(2, 1), 2, Some(log_id(2, 1, 4))));

    assert_eq!(Vote::new_committed(2, 1), *eng.state.vote_ref());
    assert_eq!(ServerState::Follower, eng.state.server_state);
    assert!(eng.output.take_commands().is_empty());
    Ok(())
}

#[test]
fn test_target_not_a_voter_does_not_elect() -> anyhow::Result<()> {
    let mut eng = eng(3);

    assert!(eng.accept_leader_transfer(Vote::new_committed(2, 1), 3, Some(log_id(2, 1, 3))));

    assert_eq!(Vote::new_committed(2, 1), *eng.state.vote_ref());
    assert!(eng.output.take_commands().is_empty());
    Ok(())
}

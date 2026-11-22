use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use maplit::btreeset;

use crate::Membership;
use crate::Vote;
use crate::election_observer::AutomaticElectionDecision;
use crate::election_observer::CampaignOrigin;
use crate::election_observer::CampaignPhase;
use crate::election_observer::ElectionEvent;
use crate::election_observer::ElectionEventKind;
use crate::election_observer::ElectionObserver;
use crate::election_observer::VoteResponseDisposition;
use crate::election_observer::handle::ObserverHandle;
use crate::engine::Engine;
use crate::engine::testing::UTConfig;
use crate::engine::testing::log_id;
use crate::raft::VoteResponse;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::StoredMembershipOf;
use crate::utime::Leased;

#[derive(Default)]
struct Recorder(Mutex<Vec<ElectionEvent<UTConfig>>>);

impl ElectionObserver<UTConfig> for Recorder {
    fn on_event(&self, event: ElectionEvent<UTConfig>) {
        self.0.try_lock().unwrap().push(event);
    }
}

/// Native oracle: a matching raw grant bit from a non-voter is never an observed consumed grant.
#[test]
fn non_voter_reply_cannot_produce_an_observed_grant_or_quorum() {
    let mut engine: Engine<UTConfig> = Engine::testing_default(1);
    engine.state.enable_validation(false);
    engine.state.membership_state.set_effective(Arc::new(StoredMembershipOf::<UTConfig>::new(
        Some(log_id(0, 1, 0)),
        Membership::new_with_defaults(vec![btreeset! {1,2,3}], []),
    )));
    let recorder = Arc::new(Recorder::default());
    engine.election_observer = Some(ObserverHandle::new(recorder.clone()));
    engine.elect();
    let vote = *engine.candidate_ref().unwrap().vote_ref();
    engine.handle_vote_resp(1, VoteResponse::new(vote, None, true));
    engine.handle_vote_resp(9, VoteResponse::new(vote, None, true));
    assert!(engine.leader.is_none());
    {
        let events = recorder.0.lock().unwrap();
        assert!(events.iter().any(|e| matches!(e.kind, ElectionEventKind::VoteResponse {
            target: 9,
            response_granted: true,
            disposition: VoteResponseDisposition::IgnoredNonVoter,
            ..
        })));
        assert!(!events.iter().any(|e| matches!(e.kind, ElectionEventKind::QuorumGranted { .. })));
    }
    engine.handle_vote_resp(2, VoteResponse::new(vote, None, true));
    assert!(engine.leader.is_some());
    assert!(
        recorder.0.lock().unwrap().iter().any(|e| matches!(e.kind, ElectionEventKind::QuorumGranted {
        granters: Some(ref voters), ..
    } if voters == &vec![2,1] || voters == &vec![1,2]))
    );
}

/// The deadline path reports `AutomaticTimeout`; a manual tick runs the same gate but reports
/// `ExternalElect`. The origin never changes which campaign starts.
#[test]
fn due_election_reports_the_caller_origin() {
    for (origin, pre_vote, phase) in [
        (CampaignOrigin::AutomaticTimeout, true, CampaignPhase::PreVote),
        (CampaignOrigin::ExternalElect, true, CampaignPhase::PreVote),
        (CampaignOrigin::ExternalElect, false, CampaignPhase::Vote),
    ] {
        let mut engine: Engine<UTConfig> = Engine::testing_default(1);
        engine.state.enable_validation(false);
        engine.state.membership_state.set_effective(Arc::new(StoredMembershipOf::<UTConfig>::new(
            Some(log_id(0, 1, 0)),
            Membership::new_with_defaults(vec![btreeset! {1,2}], []),
        )));
        let now = UTConfig::<()>::now();
        engine.state.vote = Leased::new(now, Duration::ZERO, Vote::new(1, 2));
        let recorder = Arc::new(Recorder::default());
        engine.election_observer = Some(ObserverHandle::new(recorder.clone()));

        let due = now + engine.election_timeout() + Duration::from_millis(1);
        if origin == CampaignOrigin::AutomaticTimeout {
            engine.elect_if_due(due, true, pre_vote);
        } else {
            engine.elect_if_due_with_origin(due, true, pre_vote, origin);
        }

        let events = recorder.0.lock().unwrap();
        assert!(
            events.iter().any(|e| matches!(e.kind, ElectionEventKind::AutomaticElection {
                decision: AutomaticElectionDecision::Campaign,
                election_enabled: Some(true),
                ..
            }))
        );
        let started = events
            .iter()
            .filter_map(|e| match e.kind {
                ElectionEventKind::CampaignStarted { origin, phase, .. } => Some((origin, phase)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(started, vec![(origin, phase)]);
    }
}

/// A due automatic Pre-Vote always starts its campaign, so the `Campaign` gate event is always
/// followed by `CampaignStarted`: the deadline includes the leader lease that `pre_elect()`
/// checks again, so a due deadline implies an expired lease. This holds for a follower with a
/// real lease and for a Leader whose leadership transfer expired.
#[test]
fn due_pre_vote_gate_is_followed_by_its_campaign() {
    for leader_with_expired_transfer in [false, true] {
        let mut engine: Engine<UTConfig> = Engine::testing_default(1);
        engine.state.enable_validation(false);
        engine.state.membership_state.set_effective(Arc::new(StoredMembershipOf::<UTConfig>::new(
            Some(log_id(0, 1, 0)),
            Membership::new_with_defaults(vec![btreeset! {1,2}], []),
        )));

        if leader_with_expired_transfer {
            engine.state.vote = Leased::without_last_update(Vote::new_committed(1, 2));
            engine.elect();
            engine.candidate_mut().unwrap().grant_by(&1);
            engine.handle_vote_resp(2, VoteResponse::new(Vote::new(2, 1), None, true));
            assert!(engine.leader.is_some());
            engine.leader.as_mut().unwrap().mark_transfer(2, Duration::ZERO);
        }

        // The lease and the election timeout both ended in the past, as at a real deadline.
        let vote = *engine.state.vote_ref();
        let past = UTConfig::<()>::now() - (Duration::from_millis(100) + engine.election_timeout()) * 2;
        engine.state.vote = Leased::new(past, Duration::from_millis(100), vote);

        let recorder = Arc::new(Recorder::default());
        engine.election_observer = Some(ObserverHandle::new(recorder.clone()));
        engine.elect_if_due(UTConfig::<()>::now(), true, true);

        let events = recorder.0.lock().unwrap();
        let kinds = events
            .iter()
            .filter_map(|e| match e.kind {
                ElectionEventKind::AutomaticElection { decision, .. } => Some((Some(decision), None)),
                ElectionEventKind::CampaignStarted { origin, phase, .. } => Some((None, Some((origin, phase)))),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            vec![
                (Some(AutomaticElectionDecision::Campaign), None),
                (None, Some((CampaignOrigin::AutomaticTimeout, CampaignPhase::PreVote))),
            ],
            "leader_with_expired_transfer={leader_with_expired_transfer}"
        );
    }
}

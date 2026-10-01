use std::sync::Arc;
use std::sync::Mutex;

use maplit::btreeset;

use crate::Membership;
use crate::election_observer::ElectionEvent;
use crate::election_observer::ElectionEventKind;
use crate::election_observer::ElectionObserver;
use crate::election_observer::VoteResponseDisposition;
use crate::election_observer::handle::ObserverHandle;
use crate::engine::Engine;
use crate::engine::testing::UTConfig;
use crate::engine::testing::log_id;
use crate::raft::VoteResponse;
use crate::type_config::alias::StoredMembershipOf;

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

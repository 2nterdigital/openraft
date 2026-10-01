use std::time::Duration;

use crate::RaftTypeConfig;
use crate::core::ServerState;
use crate::election_observer::AutomaticElectionDecision;
use crate::election_observer::CampaignOrigin;
use crate::election_observer::CampaignPhase;
use crate::election_observer::ElectionEvent;
use crate::election_observer::ElectionEventKind;
use crate::election_observer::ElectionTiming;
use crate::election_observer::MAX_OBSERVED_VOTERS;
use crate::election_observer::VoteRequestDisposition;
use crate::election_observer::VoteResponseDisposition;
use crate::engine::Engine;
use crate::raft::VoteRequest;
use crate::raft::VoteResponse;
use crate::raft_state::LogStateReader;
use crate::storage::RaftStateMachine;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::CommittedVoteOf;
use crate::type_config::alias::InstantOf;
use crate::type_config::alias::UncommittedVoteOf;
use crate::type_config::alias::VoteOf;

impl<C, SM> Engine<C, SM>
where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C>,
{
    fn observing(&self) -> bool {
        self.election_observer.as_ref().is_some_and(|o| o.enabled())
    }

    fn timing(&self) -> ElectionTiming<C> {
        let (last_vote_update, lease, lease_enabled) = self.state.vote.lease_info();
        ElectionTiming {
            last_vote_update,
            lease,
            lease_enabled,
            election_timeout: self.config.timer_config.election_timeout,
            seen_greater_log: self.seen_greater_log,
            greater_log_delay: self.config.timer_config.smaller_log_timeout,
        }
    }

    fn observe(&mut self, at: InstantOf<C>, kind: ElectionEventKind<C>) {
        let event = ElectionEvent {
            at,
            local_vote: self.state.vote_ref().clone(),
            kind,
        };
        self.election_observer.as_mut().unwrap().emit(event);
    }

    pub(crate) fn observe_started(&mut self) {
        if !self.observing() {
            return;
        }
        self.observe(C::now(), ElectionEventKind::Started { timing: self.timing() });
    }

    pub(crate) fn observed_request_context(&self) -> Option<(VoteOf<C>, ElectionTiming<C>)> {
        self.observing().then(|| (self.state.vote_ref().clone(), self.timing()))
    }

    pub(crate) fn observe_request(
        &mut self,
        at: InstantOf<C>,
        phase: CampaignPhase,
        request: &VoteRequest<C>,
        context: Option<(VoteOf<C>, ElectionTiming<C>)>,
        disposition: VoteRequestDisposition,
    ) {
        let Some((previous_vote, timing)) = context else {
            return;
        };
        self.observe(at, ElectionEventKind::VoteRequestProcessed {
            phase,
            vote: request.vote.clone(),
            last_log_id: request.last_log_id.clone(),
            leadership_transfer: request.leadership_transfer,
            previous_vote,
            local_last_log_id: self.state.last_log_id().cloned(),
            timing,
            disposition,
        });
    }

    pub(crate) fn observe_automatic(
        &mut self,
        at: InstantOf<C>,
        decision: AutomaticElectionDecision,
        election_enabled: Option<bool>,
        pre_vote_enabled: Option<bool>,
        voter_count: usize,
    ) {
        if !self.observing() {
            return;
        }
        self.observe(at, ElectionEventKind::AutomaticElection {
            timing: self.timing(),
            is_voter: true, // The actual gate already accepted this voter.
            voter_count,
            is_leader: self.state.server_state == ServerState::Leader,
            election_enabled,
            pre_vote_enabled,
            decision,
        });
    }

    pub(crate) fn observe_campaign(
        &mut self,
        at: InstantOf<C>,
        phase: CampaignPhase,
        origin: CampaignOrigin,
        vote: &VoteOf<C>,
        election_timeout_before: Duration,
    ) {
        if !self.observing() {
            return;
        }
        let Some(campaign_id) = self.election_observer.as_mut().unwrap().start_campaign(phase, origin) else {
            return;
        };
        let effective = self.state.membership_state.effective();
        let configs = effective.membership().get_joint_config();
        let entries = configs.iter().try_fold(0usize, |total, config| {
            total.checked_add(config.len()).filter(|n| *n <= MAX_OBSERVED_VOTERS)
        });
        let joint_voters = entries.map(|_| configs.clone());
        self.observe(at, ElectionEventKind::CampaignStarted {
            campaign_id,
            origin,
            phase,
            vote: vote.clone(),
            last_log_id: self.state.last_log_id().cloned(),
            membership_log_id: effective.log_id().clone(),
            joint_voters,
            election_timeout_before,
            election_timeout_after: self.config.timer_config.election_timeout,
        });
    }

    pub(crate) fn observed_pre_vote_origin(&self) -> CampaignOrigin {
        self.election_observer.as_ref().map(|o| o.pre_vote_origin).unwrap_or(CampaignOrigin::ExternalElect)
    }

    fn source_campaign(&self, phase: CampaignPhase) -> Option<u64> {
        self.election_observer.as_ref().and_then(|o| o.campaign(phase))
    }

    pub(crate) fn observe_pre_vote_self_grant(&mut self, quorum: bool) {
        if !self.observing() {
            return;
        }
        self.observe(C::now(), ElectionEventKind::SelfPreVoteGranted {
            campaign_id: self.source_campaign(CampaignPhase::PreVote),
            vote: self.pre_candidate_ref().unwrap().vote_ref().clone(),
            quorum,
        });
    }

    pub(crate) fn observe_consumed_response(
        &mut self,
        phase: CampaignPhase,
        target: &C::NodeId,
        response: &VoteResponse<C>,
        quorum: bool,
        consumed: bool,
    ) {
        if !self.observing() {
            return;
        }
        // The status comes from the actual native progress.update result, not an eligibility oracle.
        let disposition = if consumed {
            VoteResponseDisposition::Granted { quorum }
        } else {
            VoteResponseDisposition::IgnoredNonVoter
        };
        self.observe_response(phase, target, response, disposition);
    }

    pub(crate) fn observe_response(
        &mut self,
        phase: CampaignPhase,
        target: &C::NodeId,
        response: &VoteResponse<C>,
        disposition: VoteResponseDisposition,
    ) {
        if !self.observing() {
            return;
        }
        let candidate_vote = match phase {
            CampaignPhase::Vote => self.candidate_ref(),
            CampaignPhase::PreVote => self.pre_candidate_ref(),
        }
        .unwrap()
        .vote_ref()
        .clone();
        self.observe_reply(C::now(), phase, target, &candidate_vote, response, disposition, true);
    }

    /// Observe a notification rejected before the native tally. Never link it to the current round.
    pub(crate) fn observe_ignored_response(
        &mut self,
        at: Option<InstantOf<C>>,
        phase: CampaignPhase,
        target: &C::NodeId,
        candidate_vote: &UncommittedVoteOf<C>,
        response: &VoteResponse<C>,
        disposition: VoteResponseDisposition,
    ) {
        if !self.observing() {
            return;
        }
        self.observe_reply(
            at.unwrap_or_else(C::now),
            phase,
            target,
            &candidate_vote.clone().into_vote(),
            response,
            disposition,
            false,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn observe_reply(
        &mut self,
        at: InstantOf<C>,
        phase: CampaignPhase,
        target: &C::NodeId,
        candidate_vote: &VoteOf<C>,
        response: &VoteResponse<C>,
        disposition: VoteResponseDisposition,
        associated: bool,
    ) {
        self.observe(at, ElectionEventKind::VoteResponse {
            campaign_id: associated.then(|| self.source_campaign(phase)).flatten(),
            phase,
            target: target.clone(),
            candidate_vote: candidate_vote.clone(),
            response_vote: response.vote.clone(),
            response_last_log_id: response.last_log_id.clone(),
            response_granted: response.vote_granted,
            disposition,
        });
    }

    pub(crate) fn observe_quorum(&mut self, phase: CampaignPhase) {
        if !self.observing() {
            return;
        }
        let candidate = match phase {
            CampaignPhase::Vote => self.candidate_ref(),
            CampaignPhase::PreVote => self.pre_candidate_ref(),
        }
        .unwrap();
        let granters = (candidate.progress().iter().len() <= MAX_OBSERVED_VOTERS)
            .then(|| candidate.granters().collect::<Vec<_>>());
        self.observe(C::now(), ElectionEventKind::QuorumGranted {
            campaign_id: self.source_campaign(phase),
            phase,
            vote: candidate.vote_ref().clone(),
            granters,
        });
    }

    pub(crate) fn observe_leader(&mut self, vote: &CommittedVoteOf<C>) {
        if !self.observing() {
            return;
        }
        self.observe(C::now(), ElectionEventKind::LeaderEstablished {
            campaign_id: self.source_campaign(CampaignPhase::Vote),
            vote: vote.clone().into_vote(),
        });
    }
}

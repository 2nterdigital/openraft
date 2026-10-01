use std::collections::BTreeSet;
use std::time::Duration;

use openraft_macros::since;

use super::AutomaticElectionDecision;
use super::CampaignOrigin;
use super::CampaignPhase;
use super::ElectionTiming;
use super::VoteRequestDisposition;
use super::VoteResponseDisposition;
use crate::RaftTypeConfig;
use crate::type_config::alias::LogIdOf;
use crate::type_config::alias::VoteOf;

/// Typed native election facts. Missing or delayed events must not be reconstructed as facts.
#[since(version = "0.10.0")]
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ElectionEventKind<C>
where C: RaftTypeConfig
{
    /// Observer installed before core startup, with the already-selected timer.
    Started {
        /// Initial native timer state.
        timing: ElectionTiming<C>,
    },
    /// The automatic gate proceeds to a campaign, before resetting greater-log state.
    /// Skipped and unexpired ticks do not emit events.
    AutomaticElection {
        /// Actual timer state used by this gate.
        timing: ElectionTiming<C>,
        /// Whether the local node is an effective voter.
        is_voter: bool,
        /// Effective voter count.
        voter_count: usize,
        /// Whether the native server state is leader.
        is_leader: bool,
        /// Actual automatic switch read by this gate, absent if an earlier branch returned.
        election_enabled: Option<bool>,
        /// Actual Pre-Vote switch read by this gate, absent if an earlier branch returned.
        pre_vote_enabled: Option<bool>,
        /// Actual branch result; no observer repeats eligibility checks.
        decision: AutomaticElectionDecision,
    },
    /// A real campaign or Pre-Vote actually started.
    CampaignStarted {
        /// Instance-local transient sequence, independent of term and ballot identity.
        campaign_id: u64,
        /// Entry that caused this campaign.
        origin: CampaignOrigin,
        /// Whether this is a Pre-Vote or real vote.
        phase: CampaignPhase,
        /// Complete campaign vote, which need not yet be the local persisted vote.
        vote: VoteOf<C>,
        /// Candidate's actual log tip.
        last_log_id: Option<LogIdOf<C>>,
        /// Effective membership's log identity at campaign creation.
        membership_log_id: Option<LogIdOf<C>>,
        /// Actual joint voter sets, absent if copying would exceed `MAX_OBSERVED_VOTERS`.
        joint_voters: Option<Vec<BTreeSet<C::NodeId>>>,
        /// Timeout selected before the campaign.
        election_timeout_before: Duration,
        /// Newly sampled timeout for the subsequent campaign.
        election_timeout_after: Duration,
    },
    /// An inbound request's actual native decision, before its IO-dependent RPC reply.
    VoteRequestProcessed {
        /// Real vote or hypothetical Pre-Vote.
        phase: CampaignPhase,
        /// Full request ballot.
        vote: VoteOf<C>,
        /// Request log tip.
        last_log_id: Option<LogIdOf<C>>,
        /// Whether the request carried native leadership-transfer authorization.
        leadership_transfer: bool,
        /// Complete local vote before processing this request.
        previous_vote: VoteOf<C>,
        /// Local log tip used by the native comparison.
        local_last_log_id: Option<LogIdOf<C>>,
        /// Native leased-vote timing before this request was processed.
        timing: ElectionTiming<C>,
        /// Actual decision branch; does not assert persistence or RPC completion.
        disposition: VoteRequestDisposition,
    },
    /// A reply reached the native consumer, which either consumed or ignored it.
    VoteResponse {
        /// Current matching source campaign, or absent when no association is known.
        campaign_id: Option<u64>,
        /// Tally being addressed.
        phase: CampaignPhase,
        /// Replying voter (the local voter is also reported here after its SaveVote IO).
        target: C::NodeId,
        /// Ballot to which the notification was addressed.
        candidate_vote: VoteOf<C>,
        /// Ballot returned by the voter, distinct from `candidate_vote`.
        response_vote: VoteOf<C>,
        /// Log tip returned by the voter.
        response_last_log_id: Option<LogIdOf<C>>,
        /// Raw reply grant bit, not an accepted native grant.
        response_granted: bool,
        /// Actual native response disposition.
        disposition: VoteResponseDisposition,
    },
    /// Pre-Vote grants its own vote directly, without a synthetic network or IO response.
    SelfPreVoteGranted {
        /// Matching source campaign if known.
        campaign_id: Option<u64>,
        /// Complete hypothetical Pre-Vote ballot.
        vote: VoteOf<C>,
        /// Actual local grant's native tally result.
        quorum: bool,
    },
    /// The native tally reported a quorum, before leader establishment or a real post-Pre-Vote
    /// vote.
    QuorumGranted {
        /// Matching source campaign if known.
        campaign_id: Option<u64>,
        /// Native tally phase.
        phase: CampaignPhase,
        /// Complete campaign vote.
        vote: VoteOf<C>,
        /// Actual granting voters, absent if the native tally exceeds `MAX_OBSERVED_VOTERS`.
        granters: Option<Vec<C::NodeId>>,
    },
    /// Native leader state was established successfully.
    ///
    /// This is not a local IO reply, durable quorum acknowledgement, committed blank log, or
    /// application readiness. Those boundaries remain separate.
    LeaderEstablished {
        /// Matching source campaign if known.
        campaign_id: Option<u64>,
        /// Complete committed leader vote.
        vote: VoteOf<C>,
    },
}

use openraft_macros::since;

/// The branch actually taken by the automatic election gate.
#[since(version = "0.10.0")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AutomaticElectionDecision {
    /// This node is already a leader.
    Leader,
    /// This node is not an effective voter.
    NotVoter,
    /// Automatic elections are disabled.
    Disabled,
    /// A single voter already has an active campaign.
    SingleVoterPending,
    /// The vote's lease plus election timeout has not expired.
    NotExpired,
    /// A recent Pre-Vote is still in flight.
    PreVotePending,
    /// The gate proceeds to an actual campaign.
    Campaign,
}

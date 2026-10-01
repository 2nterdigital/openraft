use openraft_macros::since;

/// Distinguishes a nonpersistent Pre-Vote probe from a real vote.
#[since(version = "0.10.0")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CampaignPhase {
    /// Probe without advancing the persisted vote.
    PreVote,
    /// Real election.
    Vote,
}

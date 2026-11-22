use openraft_macros::since;

/// How native code consumed a reply, distinct from its network grant bit.
#[since(version = "0.10.0")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VoteResponseDisposition {
    /// The native tally accepted this voter; `quorum` is the actual tally result.
    Granted {
        /// Whether this tally reached the campaign's native quorum.
        quorum: bool,
    },
    /// The current campaign processed a rejection (including a mismatched reply vote).
    Rejected,
    /// No campaign was present when the reply reached the native consumer.
    IgnoredNoCampaign,
    /// The reply was addressed to an obsolete campaign ballot.
    IgnoredStaleCampaign,
    /// The reply did not belong to the campaign's native voter set.
    IgnoredNonVoter,
}

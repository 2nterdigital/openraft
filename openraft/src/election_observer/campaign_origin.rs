use openraft_macros::since;

/// The actual entry that started a campaign, retained across a Pre-Vote success.
#[since(version = "0.10.0")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CampaignOrigin {
    /// Successful initialization immediately starts a campaign.
    Initialize,
    /// The automatic tick path allowed a campaign (including the single-voter shortcut).
    AutomaticTimeout,
    /// An external election command was accepted.
    ExternalElect,
    /// A leadership transfer addressed to this node was accepted.
    LeadershipTransfer,
}

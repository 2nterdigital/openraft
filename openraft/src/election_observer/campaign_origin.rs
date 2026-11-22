use openraft_macros::since;

/// The actual entry that started a campaign, retained across a Pre-Vote success.
#[since(version = "0.10.0")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CampaignOrigin {
    /// Successful initialization immediately starts a campaign.
    Initialize,
    /// The election deadline notifier allowed a campaign (including the single-voter shortcut and
    /// a Leader whose leadership transfer expired).
    AutomaticTimeout,
    /// An external election command was accepted, or a manual `Trigger::tick()` found an election
    /// due.
    ExternalElect,
    /// A leadership transfer addressed to this node was accepted.
    LeadershipTransfer,
}

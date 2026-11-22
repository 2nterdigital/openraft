use std::time::Duration;

use openraft_macros::since;

use crate::RaftTypeConfig;
use crate::type_config::alias::InstantOf;

/// The actual native timer state, copied at a source boundary.
///
/// Instants are only comparable within the same runtime clock; `None` is an absent update, not
/// zero. `election_timeout` is the selected random value, not a new sample or a configured range.
#[since(version = "0.10.0")]
#[derive(Clone, Debug)]
pub struct ElectionTiming<C>
where C: RaftTypeConfig
{
    /// Actual last update of the leased local vote.
    pub last_vote_update: Option<InstantOf<C>>,
    /// Actual current vote lease duration.
    pub lease: Duration,
    /// Whether touch operations can extend that lease.
    pub lease_enabled: bool,
    /// Actual sampled timeout governing the next automatic campaign.
    pub election_timeout: Duration,
    /// Whether a greater log has been observed by this engine.
    pub seen_greater_log: bool,
    /// Additional native delay used when `seen_greater_log` is true.
    pub greater_log_delay: Duration,
}

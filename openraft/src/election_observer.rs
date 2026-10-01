//! Optional source facts about native elections.
//!
//! Install an observer with
//! [`Raft::new_with_election_observer`](crate::Raft::new_with_election_observer). Bind application/
//! run context in the observer; OpenRaft does not interpret it. Events are local facts, not a
//! second election oracle, persistence acknowledgements, or permission to serve clients.

mod campaign_origin;
mod campaign_phase;
mod decision;
mod event;
mod event_kind;
pub(crate) mod handle;
mod timing;
mod vote_request_disposition;
mod vote_response_disposition;

use openraft_macros::since;

pub use self::campaign_origin::CampaignOrigin;
pub use self::campaign_phase::CampaignPhase;
pub use self::decision::AutomaticElectionDecision;
pub use self::event::ElectionEvent;
pub use self::event_kind::ElectionEventKind;
pub use self::timing::ElectionTiming;
pub use self::vote_request_disposition::VoteRequestDisposition;
pub use self::vote_response_disposition::VoteResponseDisposition;
use crate::OptionalSend;
use crate::OptionalSync;
use crate::RaftTypeConfig;

/// Maximum total voter entries copied into one observer event.
///
/// Larger native memberships continue operating normally; their collection metadata is `None`.
#[since(version = "0.10.0")]
pub const MAX_OBSERVED_VOTERS: usize = 1024;

/// Receives facts synchronously at their native source.
///
/// Implementations **must** return promptly: use a bounded, nonblocking queue rather than IO,
/// waiting, or expensive serialization. OpenRaft adds no consumer task. A panic is contained and
/// disables this instance's observer when unwinding is enabled. With `panic = "abort"` the process
/// aborts as usual. Absence of events after a panic must be treated as missing coverage.
///
/// Events can precede the constructor's return. The callback's result never affects consensus.
#[since(version = "0.10.0", change = "optional native election source observer")]
pub trait ElectionObserver<C>: OptionalSend + OptionalSync + 'static
where C: RaftTypeConfig
{
    /// Consume a native fact, without blocking or making consensus decisions.
    #[since(version = "0.10.0")]
    fn on_event(&self, event: ElectionEvent<C>);
}

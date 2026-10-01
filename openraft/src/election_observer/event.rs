use openraft_macros::since;

use super::ElectionEventKind;
use crate::RaftTypeConfig;
use crate::type_config::alias::InstantOf;
use crate::type_config::alias::VoteOf;

/// One native source event. Contains only election metadata, never application payloads.
#[since(version = "0.10.0")]
#[derive(Clone, Debug)]
pub struct ElectionEvent<C>
where C: RaftTypeConfig
{
    /// Source timestamp in this Raft instance's native clock.
    pub at: InstantOf<C>,
    /// Complete local vote at the event boundary.
    pub local_vote: VoteOf<C>,
    /// The fact emitted by native code.
    pub kind: ElectionEventKind<C>,
}

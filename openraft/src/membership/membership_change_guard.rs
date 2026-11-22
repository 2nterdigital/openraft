use openraft_macros::since;

use crate::RaftTypeConfig;
use crate::type_config::alias::CommittedLeaderIdOf;
use crate::type_config::alias::StoredMembershipOf;

/// Authorizes one membership append against an exact leader and membership incarnation.
///
/// Capture the committed leader before the application authorization read and reuse that identity
/// throughout the operation. Membership snapshots include both log IDs and complete configurations.
/// The core compares all fields synchronously before applying the change and appending its log.
/// This guards Raft identities; application-level authorization and serialization remain the
/// application's responsibility.
#[since(version = "0.10.0", change = "dispatch-time membership authorization guard")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipChangeGuard<C>
where C: RaftTypeConfig
{
    /// The committed leader incarnation that authorized the operation.
    pub expected_leader: CommittedLeaderIdOf<C>,
    /// The exact committed membership observed by the authorizing application.
    pub committed: StoredMembershipOf<C>,
    /// The exact effective membership observed by the authorizing application.
    pub effective: StoredMembershipOf<C>,
}

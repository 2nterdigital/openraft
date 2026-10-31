use openraft_macros::since;

/// Selects membership authorization before a Raft core starts processing requests.
///
/// Applications must persist their selection and restore it on every construction. This policy
/// does not authorize initialization; applications must separately authorize pristine bootstrap.
#[since(version = "0.10.0", change = "construction-only membership authorization policy")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MembershipChangePolicy {
    /// Accept the existing unguarded membership APIs.
    #[default]
    Unrestricted,
    /// Reject membership mutations without an explicit dispatch-time guard.
    RequireGuard,
}

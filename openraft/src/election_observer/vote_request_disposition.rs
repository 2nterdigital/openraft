use openraft_macros::since;

/// The actual branch deciding an inbound vote or Pre-Vote request.
#[since(version = "0.10.0")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VoteRequestDisposition {
    /// The request was granted by the native engine, before persistence or RPC completion.
    Granted,
    /// A live leader lease rejected the request: an active committed vote's lease, or, on a
    /// Leader, its quorum-ack lease.
    LeaseNotExpired,
    /// The request's log tip was behind the native local tip.
    LogBehind,
    /// The native vote comparison or update rejected the request.
    VoteRejected,
}

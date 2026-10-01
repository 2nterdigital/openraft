# Native election source observation

Applications can optionally install an [`ElectionObserver`](crate::election_observer::ElectionObserver)
with [`Raft::new_with_election_observer`](crate::Raft::new_with_election_observer). The existing
`Raft::new` remains equivalent to supplying `None`. Bind instance, process, run, or application group
context in the observer itself. No application identifiers or payloads enter the native API.

The observer is installed before core startup. Its initial event contains the already-selected
random timeout; subsequent automatic gate events copy the actual leased vote, sampled timeout,
greater-log state, and the branch actually taken. Only a gate that proceeds to a campaign is emitted;
skipped and unexpired ticks produce no callback or snapshot. Observers must not recompute
eligibility and present it as a native fact.

Campaign events distinguish initialization, automatic timeout, external election commands, and
leadership transfer. A Pre-Vote success preserves its originating entry when it starts a real
election. Each actual campaign receives an instance-local transient ID, including repeated Pre-Votes
at the same ballot. These IDs are neither persisted nor globally unique. Full votes remain necessary;
a term alone does not identify a ballot in every supported `LeaderId` implementation.

Inbound decisions expose the actual lease, log, or vote branch before its IO-dependent reply.
Response events distinguish raw replies from grants consumed by the native tally and from obsolete
notifications that were ignored. A response's campaign ID identifies the tally that consumed it,
not the remote request's globally unique origin. In particular, native repeated Pre-Vote requests
can share a ballot. An ignored response has no invented association to the current round.

`QuorumGranted` reads the native tally after it returns success. `LeaderEstablished` reports the
accepted native leader state. Neither event asserts that the committed vote or blank log has been
persisted, that an RPC reply has been delivered, that a client write succeeded, or that the
application is ready. The local real vote is tallied only after its real SaveVote IO notification;
the local Pre-Vote grant is a separate event and is never presented as a synthetic RPC reply.

Membership and granter collections are bounded by
[`MAX_OBSERVED_VOTERS`](crate::election_observer::MAX_OBSERVED_VOTERS). If copying would exceed that
bound, the collection is `None`, while the native membership and quorum continue unchanged. All
timestamps use the instance's native clock; do not compare monotonic instants across processes.
Missing, dropped, unavailable, or post-shutdown events remain missing coverage, not zero activity.

Callbacks run synchronously on the source task and must be bounded and nonblocking. Use a bounded
queue with explicit loss accounting; do not perform IO, serialization, waiting, or business work in
the callback. OpenRaft adds no task. Unwinding consumer panics are caught and disable that instance's
observer, with a warning; aborting panics still abort the process. The callback's result never
controls native consensus. With no observer there are no event snapshots, collection clones,
campaign counters, callbacks, or observer tasks. The election algorithm, timing samples, vote and
quorum rules, runtime defaults, and persistent formats are unchanged.

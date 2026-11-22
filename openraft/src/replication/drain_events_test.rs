use std::fmt::Debug;
use std::future::Future;
use std::io;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::Duration;

use super::ReplicationCore;
use crate::Config;
use crate::OptionalSend;
use crate::RaftLogReader;
use crate::RaftNetworkFactory;
use crate::async_runtime::WatchSender;
use crate::core::SharedReplicateBatch;
use crate::engine::testing::UTConfig;
use crate::engine::testing::log_id;
use crate::errors::RPCError;
use crate::errors::ReplicationClosed;
use crate::errors::StreamingError;
use crate::log_id_range::LogIdRange;
use crate::network::RPCOption;
use crate::network::RaftNetworkV2;
use crate::progress::inflight_id::InflightId;
use crate::progress::stream_id::StreamId;
use crate::raft::AppendEntriesRequest;
use crate::raft::AppendResult;
use crate::raft::SnapshotResponse;
use crate::raft::VoteRequest;
use crate::raft::VoteResponse;
use crate::raft_state::IOId;
use crate::replication::backoff_state::BackoffState;
use crate::replication::event_watcher::EventWatcher;
use crate::replication::payload::Payload;
use crate::replication::replicate::Replicate;
use crate::replication::replication_context::ReplicationContext;
use crate::replication::replication_progress::ReplicationProgress;
use crate::replication::stream_state::StreamState;
use crate::storage::IOFlushed;
use crate::storage::LogState;
use crate::storage::RaftLogStorage;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::EntryOf;
use crate::type_config::alias::LogIdOf;
use crate::type_config::alias::SnapshotOf;
use crate::type_config::alias::VoteOf;
use crate::vote::raft_vote::RaftVoteExt;

type C = UTConfig;

/// `drain_events()` touches neither the network nor the log store.
struct Unused;

impl RaftNetworkFactory<C> for Unused {
    type Network = Self;

    async fn new_client(&mut self, _target: u64, _node: &()) -> Self {
        unreachable!()
    }
}

impl RaftNetworkV2<C> for Unused {
    type SnapshotData = ();

    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> Result<AppendResult<C>, RPCError<C>> {
        unreachable!()
    }

    async fn vote(&mut self, _rpc: VoteRequest<C>, _option: RPCOption) -> Result<VoteResponse<C>, RPCError<C>> {
        unreachable!()
    }

    async fn full_snapshot(
        &mut self,
        _vote: VoteOf<C>,
        _snapshot: SnapshotOf<C, ()>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
        unreachable!()
    }
}

impl RaftLogReader<C> for Unused {
    async fn try_get_log_entries<R>(&mut self, _range: R) -> Result<Vec<EntryOf<C>>, io::Error>
    where R: RangeBounds<u64> + Clone + Debug + OptionalSend {
        unreachable!()
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, io::Error> {
        unreachable!()
    }
}

impl RaftLogStorage<C> for Unused {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error> {
        unreachable!()
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        unreachable!()
    }

    async fn save_vote(&mut self, _vote: &VoteOf<C>) -> Result<(), io::Error> {
        unreachable!()
    }

    async fn append<I>(&mut self, _entries: I, _callback: IOFlushed<C>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<C>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        unreachable!()
    }

    async fn truncate_after(&mut self, _last_log_id: Option<LogIdOf<C>>) -> Result<(), io::Error> {
        unreachable!()
    }

    async fn purge(&mut self, _log_id: LogIdOf<C>) -> Result<(), io::Error> {
        unreachable!()
    }
}

/// With `broadcast_commit = false`, a commit change alone leaves an idle replication task waiting
/// with no payload, but it records the commit and does not hide the next replicate command.
/// With `true`, the same change yields an entry-less `Probe` at once.
#[test]
fn drain_events_follows_broadcast_commit() {
    const PENDING: Duration = Duration::from_millis(100);

    C::run(async {
        for broadcast_commit in [true, false] {
            let vote = VoteOf::<C>::new_committed(1, 1).to_committed();
            let matched = Some(log_id(1, 1, 5));
            let new_commit = Some(log_id(1, 1, 5));

            // The initial values are already seen, as when the task finished its last payload.
            let initial = Replicate::new_logs_since(matched, InflightId::new(1));
            let (replicate_tx, replicate_rx) = C::watch_channel(initial);
            let (committed_tx, committed_rx) = C::watch_channel(Some(log_id(1, 1, 4)));
            let (_io_tx, io_rx) = C::watch_channel(IOId::new_log_io(vote.clone(), matched));
            let (_cancel_tx, cancel_rx) = C::watch_channel(());
            let (_backoff_reset_tx, backoff_reset_rx) = C::watch_channel(());
            let (_probe_tx, quorum_loss_probe_rx) = C::watch_channel(None);
            let (tx_notify, _rx_notify) = C::mpsc(1);

            let replication_context = ReplicationContext {
                id: 1,
                target: 2,
                leader_vote: vote,
                stream_id: StreamId::new(1),
                config: Arc::new(Config {
                    broadcast_commit,
                    ..Default::default()
                }),
                tx_notify,
                cancel_rx,
                replicate_batch: SharedReplicateBatch::new(),
                quorum_loss_probe_rx,
            };
            let event_watcher = EventWatcher {
                replicate_rx,
                committed_rx,
                io_accepted_rx: io_rx.clone(),
                io_submitted_rx: io_rx,
            };
            let stream_state = StreamState::<C, Unused> {
                replication_context: replication_context.clone(),
                event_watcher: event_watcher.clone(),
                log_reader: Unused,
                payload: None,
                inflight_id: None,
                backoff_consumer: BackoffState::new().consumer(backoff_reset_rx),
                after_leader_check: None,
            };
            let mut core = ReplicationCore::<C, Unused, Unused> {
                replication_context,
                stream_state: Arc::new(C::mutex(stream_state)),
                event_watcher,
                next_action: None,
                inflight_id: None,
                network: None,
                replication_progress: ReplicationProgress {
                    local_committed: Some(log_id(1, 1, 4)),
                    remote_matched: matched,
                },
                backoff_state: BackoffState::new(),
            };

            tracing::info!(broadcast_commit, "--- only the commit advances");
            assert!(committed_tx.send_if_greater(new_commit));
            let res = C::timeout(PENDING, core.drain_events()).await;
            assert_eq!(new_commit, core.replication_progress.local_committed);

            if broadcast_commit {
                res.expect("a commit-only change yields a payload").unwrap();
                let log_id_range = LogIdRange::new(matched, matched);
                assert_eq!(Some(Payload::Probe { log_id_range }), core.next_action);
                assert_eq!(None, core.inflight_id);
                continue;
            }

            assert!(res.is_err(), "a commit-only change must not yield a payload");
            assert_eq!(None, core.next_action);

            tracing::info!("--- the next replicate command is still delivered");
            let inflight_id = InflightId::new(2);
            replicate_tx.send(Replicate::new_logs_since(matched, inflight_id)).unwrap();
            C::timeout(PENDING, core.drain_events())
                .await
                .expect("a replicate command must wake the task")
                .unwrap();
            assert_eq!(Some(Payload::LogsSince { prev: matched }), core.next_action);
            assert_eq!(Some(inflight_id), core.inflight_id);
        }
    });
}

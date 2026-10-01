//! Independent consumer of the public observer constructor and actual routed Raft RPCs.

#[macro_use]
#[path = "fixtures/mod.rs"]
mod fixtures;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Result;
use fixtures::RaftRouter;
use fixtures::ut_harness;
use maplit::btreeset;
use openraft::Config;
use openraft::RPCTypes;
use openraft::ServerState;
use openraft::election_observer::AutomaticElectionDecision;
use openraft::election_observer::CampaignOrigin;
use openraft::election_observer::CampaignPhase;
use openraft::election_observer::ElectionEvent;
use openraft::election_observer::ElectionEventKind;
use openraft::election_observer::ElectionObserver;
use openraft::election_observer::MAX_OBSERVED_VOTERS;
use openraft::election_observer::VoteRequestDisposition;
use openraft::election_observer::VoteResponseDisposition;
use openraft::type_config::TypeConfigExt;
use openraft_memstore::TypeConfig;

struct Recorder {
    events: Mutex<Vec<ElectionEvent<TypeConfig>>>,
    dropped: AtomicU64,
    callback_max_ns: AtomicU64,
}

impl Recorder {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::with_capacity(4096)),
            dropped: AtomicU64::new(0),
            callback_max_ns: AtomicU64::new(0),
        })
    }

    async fn wait(&self, predicate: impl Fn(&ElectionEvent<TypeConfig>) -> bool) {
        for _ in 0..200 {
            if self.events.lock().unwrap().iter().any(&predicate) {
                return;
            }
            TypeConfig::sleep(Duration::from_millis(10)).await;
        }
        panic!("expected native event was not delivered");
    }

    fn snapshot(&self) -> Vec<ElectionEvent<TypeConfig>> {
        self.events.lock().unwrap().clone()
    }
}

impl ElectionObserver<TypeConfig> for Recorder {
    fn on_event(&self, event: ElectionEvent<TypeConfig>) {
        let start = std::time::Instant::now();
        if let Ok(mut events) = self.events.try_lock()
            && events.len() < events.capacity()
        {
            events.push(event);
        } else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        self.callback_max_ns.fetch_max(
            start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }
}

fn config() -> Arc<Config> {
    Arc::new(
        Config {
            heartbeat_interval: 20,
            election_timeout_min: 80,
            election_timeout_max: 160,
            enable_elect: false,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    )
}

async fn shutdown(router: &RaftRouter, ids: &[u64]) -> Result<()> {
    for id in ids {
        router.get_raft_handle(id)?.shutdown().await?;
    }
    Ok(())
}

#[test_harness::test(harness = ut_harness)]
async fn initialize_reports_actual_quorum_and_delayed_reply_is_not_a_grant() -> Result<()> {
    let mut router = RaftRouter::new(config());
    let recorder = Recorder::new();
    router.new_raft_node_with_election_observer(0, recorder.clone()).await;
    assert!(
        matches!(recorder.snapshot()[0].kind, ElectionEventKind::Started { .. }),
        "observer must be installed before constructor return"
    );
    router.new_raft_node(1).await;
    router.new_raft_node(2).await;
    router
        .set_rpc_post_hook(RPCTypes::Vote, |_, _, _, from, to| {
            Box::pin(async move {
                if from == 0 && to == 2 {
                    TypeConfig::sleep(Duration::from_millis(40)).await;
                }
                Ok(())
            })
        })
        .await;
    router.initialize(0).await?;
    let raft = router.get_raft_handle(&0)?;
    raft.wait(Some(Duration::from_secs(2))).state(ServerState::Leader, "real native quorum").await?;
    recorder
        .wait(|e| {
            matches!(e.kind, ElectionEventKind::VoteResponse {
                target: 2,
                disposition: VoteResponseDisposition::IgnoredNoCampaign,
                ..
            })
        })
        .await;
    router.client_request(0, "observer", 1).await?;
    shutdown(&router, &[0, 1, 2]).await?;
    let events = recorder.snapshot();
    let campaign = events
        .iter()
        .find_map(|e| match &e.kind {
            ElectionEventKind::CampaignStarted {
                campaign_id,
                origin,
                joint_voters,
                ..
            } => {
                assert_eq!(*origin, CampaignOrigin::Initialize);
                assert_eq!(joint_voters.as_ref().unwrap(), &vec![btreeset! {0,1,2}]);
                Some(*campaign_id)
            }
            _ => None,
        })
        .unwrap();
    let quorum_pos = events
        .iter()
        .position(|e| {
            matches!(&e.kind,
        ElectionEventKind::QuorumGranted { campaign_id: Some(id), granters: Some(granters), .. }
        if *id == campaign && granters.contains(&0) && granters.contains(&1) && granters.len() == 2)
        })
        .unwrap();
    let leader_pos = events
        .iter()
        .position(|e| {
            matches!(&e.kind,
        ElectionEventKind::LeaderEstablished { campaign_id: Some(id), vote } if *id == campaign && vote.is_committed())
        })
        .unwrap();
    assert!(quorum_pos < leader_pos);
    assert!(
        events.iter().any(|e| matches!(e.kind, ElectionEventKind::VoteResponse {
            target: 0,
            disposition: VoteResponseDisposition::Granted { .. },
            ..
        })),
        "local SaveVote completion must really reach the native tally"
    );
    let final_len = events.len();
    TypeConfig::sleep(Duration::from_millis(60)).await;
    assert_eq!(
        final_len,
        recorder.snapshot().len(),
        "shutdown joins native source callbacks"
    );
    Ok(())
}

struct PanickingObserver(AtomicU64);

impl ElectionObserver<TypeConfig> for PanickingObserver {
    fn on_event(&self, _event: ElectionEvent<TypeConfig>) {
        self.0.fetch_add(1, Ordering::Relaxed);
        panic!("consumer failure");
    }
}

#[test_harness::test(harness = ut_harness)]
async fn observer_panic_is_disabled_without_breaking_initialization_or_writes() -> Result<()> {
    let mut router = RaftRouter::new(config());
    let observer = Arc::new(PanickingObserver(AtomicU64::new(0)));
    router.new_raft_node_with_election_observer(0, observer.clone()).await;
    router.initialize(0).await?;
    let raft = router.get_raft_handle(&0)?;
    raft.wait(Some(Duration::from_secs(2)))
        .state(ServerState::Leader, "panic independent leader")
        .await?;
    router.client_request(0, "panic", 1).await?;
    assert_eq!(observer.0.load(Ordering::Relaxed), 1, "disable after the first panic");
    shutdown(&router, &[0]).await
}

#[test_harness::test(harness = ut_harness)]
async fn healthy_rf3_period_has_no_per_tick_callbacks() -> Result<()> {
    let mut native_config = (*config()).clone();
    native_config.enable_elect = true;
    let mut router = RaftRouter::new(Arc::new(native_config));
    let recorders = [Recorder::new(), Recorder::new(), Recorder::new()];
    for (id, recorder) in recorders.iter().enumerate() {
        router.new_raft_node_with_election_observer(id as u64, recorder.clone()).await;
    }
    router.initialize(0).await?;
    for id in 0..3 {
        router
            .get_raft_handle(&id)?
            .wait(Some(Duration::from_secs(2)))
            .applied_index_at_least(Some(1), "initialized")
            .await?;
    }
    TypeConfig::sleep(Duration::from_millis(100)).await;
    let before = recorders.iter().map(|r| r.snapshot().len()).collect::<Vec<_>>();
    TypeConfig::sleep(Duration::from_millis(200)).await;
    let after = recorders.iter().map(|r| r.snapshot().len()).collect::<Vec<_>>();
    assert_eq!(
        before, after,
        "healthy ticks and lease refreshes must add no source event"
    );
    shutdown(&router, &[0, 1, 2]).await?;
    for (id, recorder) in recorders.iter().enumerate() {
        assert_eq!(recorder.dropped.load(Ordering::Relaxed), 0);
        println!(
            "node={id} native_events={} healthy_200ms_events=0 consumer_callback_max_ns={}",
            after[id],
            recorder.callback_max_ns.load(Ordering::Relaxed)
        );
    }
    Ok(())
}

#[test_harness::test(harness = ut_harness)]
async fn transfer_is_a_distinct_native_origin_without_waiting_for_lease() -> Result<()> {
    let mut router = RaftRouter::new(config());
    let recorder = Recorder::new();
    router.new_raft_node(0).await;
    router.new_raft_node_with_election_observer(1, recorder.clone()).await;
    router.new_raft_node(2).await;
    router.initialize(0).await?;
    let old = router.get_raft_handle(&0)?;
    old.wait(Some(Duration::from_secs(2))).state(ServerState::Leader, "initial leader").await?;
    old.trigger().transfer_leader(1).await?;
    router
        .get_raft_handle(&1)?
        .wait(Some(Duration::from_secs(2)))
        .state(ServerState::Leader, "transferred")
        .await?;
    recorder.wait(|e| matches!(e.kind, ElectionEventKind::LeaderEstablished { .. })).await;
    shutdown(&router, &[0, 1, 2]).await?;
    assert!(
        recorder.snapshot().iter().any(|e| matches!(e.kind, ElectionEventKind::CampaignStarted {
            origin: CampaignOrigin::LeadershipTransfer,
            phase: CampaignPhase::Vote,
            ..
        }))
    );
    Ok(())
}

#[test_harness::test(harness = ut_harness)]
async fn automatic_campaign_reports_the_actual_expired_gate_and_sample() -> Result<()> {
    let mut native_config = (*config()).clone();
    native_config.enable_pre_vote = Some(true);
    let mut router = RaftRouter::new(Arc::new(native_config));
    let recorder = Recorder::new();
    router.new_raft_node(0).await;
    router.new_raft_node_with_election_observer(1, recorder.clone()).await;
    router.new_raft_node(2).await;
    router.initialize(0).await?;
    let old = router.get_raft_handle(&0)?;
    old.wait(Some(Duration::from_secs(2))).state(ServerState::Leader, "initial leader").await?;
    router
        .get_raft_handle(&1)?
        .wait(Some(Duration::from_secs(2)))
        .state(ServerState::Follower, "effective voter")
        .await?;
    old.runtime_config().heartbeat(false);
    router.get_raft_handle(&1)?.runtime_config().elect(true);
    router
        .get_raft_handle(&1)?
        .wait(Some(Duration::from_secs(2)))
        .state(ServerState::Leader, "natural timeout")
        .await?;
    shutdown(&router, &[0, 1, 2]).await?;
    let events = recorder.snapshot();
    let gate = events
        .iter()
        .find(|e| {
            matches!(e.kind, ElectionEventKind::AutomaticElection {
                decision: AutomaticElectionDecision::Campaign,
                ..
            })
        })
        .unwrap();
    if let ElectionEventKind::AutomaticElection {
        timing,
        election_enabled,
        ..
    } = &gate.kind
    {
        assert_eq!(*election_enabled, Some(true));
        assert!(gate.at > timing.last_vote_update.unwrap() + timing.lease + timing.election_timeout);
        assert!(timing.election_timeout >= Duration::from_millis(80));
        assert!(timing.election_timeout < Duration::from_millis(160));
    }
    assert!(
        events.iter().any(|e| matches!(e.kind, ElectionEventKind::CampaignStarted {
            origin: CampaignOrigin::AutomaticTimeout,
            ..
        }))
    );
    assert!(
        events.iter().any(|e| matches!(e.kind, ElectionEventKind::CampaignStarted {
            origin: CampaignOrigin::AutomaticTimeout,
            phase: CampaignPhase::PreVote,
            ..
        }))
    );
    assert!(
        events.iter().any(|e| matches!(e.kind, ElectionEventKind::QuorumGranted {
            phase: CampaignPhase::PreVote,
            granters: Some(_),
            ..
        }))
    );
    Ok(())
}

#[test_harness::test(harness = ut_harness)]
async fn obsolete_real_campaign_reply_keeps_unknown_round_and_is_not_counted() -> Result<()> {
    let mut router = RaftRouter::new(config());
    let recorder = Recorder::new();
    router.new_raft_node(0).await;
    router.new_raft_node_with_election_observer(1, recorder.clone()).await;
    router.new_raft_node(2).await;
    router.initialize(0).await?;
    router
        .get_raft_handle(&0)?
        .wait(Some(Duration::from_secs(2)))
        .state(ServerState::Leader, "initial leader")
        .await?;
    let follower = router.get_raft_handle(&1)?;
    follower
        .wait(Some(Duration::from_secs(2)))
        .state(ServerState::Follower, "initialized follower")
        .await?;
    // Keep established-leader replication from canceling either deliberately overlapping campaign.
    router.set_network_error(0, true);
    let entered = Arc::new(AtomicU64::new(0));
    let hook_entered = entered.clone();
    router
        .set_rpc_post_hook(RPCTypes::Vote, move |_, _, _, from, to| {
            let entered = hook_entered.clone();
            Box::pin(async move {
                if from == 1 && to == 2 {
                    entered.fetch_add(1, Ordering::Relaxed);
                    TypeConfig::sleep(Duration::from_millis(40)).await;
                }
                Ok(())
            })
        })
        .await;
    follower.trigger().elect(false).await?;
    for _ in 0..100 {
        if entered.load(Ordering::Relaxed) > 0 {
            break;
        }
        TypeConfig::sleep(Duration::from_millis(1)).await;
    }
    assert!(entered.load(Ordering::Relaxed) > 0);
    follower.trigger().elect(false).await?;
    recorder
        .wait(|e| {
            matches!(e.kind, ElectionEventKind::VoteResponse {
                campaign_id: None,
                disposition: VoteResponseDisposition::IgnoredStaleCampaign,
                ..
            })
        })
        .await;
    shutdown(&router, &[0, 1, 2]).await?;
    assert!(!recorder.snapshot().iter().any(|e| matches!(e.kind, ElectionEventKind::LeaderEstablished { .. })));
    Ok(())
}

#[test_harness::test(harness = ut_harness)]
async fn repeated_manual_pre_votes_have_distinct_rounds_at_the_same_ballot() -> Result<()> {
    let mut router = RaftRouter::new(config());
    let recorder = Recorder::new();
    let voter = Recorder::new();
    router.new_raft_node_with_election_observer(0, voter.clone()).await;
    router.new_raft_node_with_election_observer(1, recorder.clone()).await;
    router.new_raft_node(2).await;
    router.initialize(0).await?;
    router
        .get_raft_handle(&0)?
        .wait(Some(Duration::from_secs(2)))
        .state(ServerState::Leader, "initial leader")
        .await?;
    let follower = router.get_raft_handle(&1)?;
    follower.wait(Some(Duration::from_secs(2))).state(ServerState::Follower, "follower").await?;
    for _ in 0..2 {
        follower.trigger().elect(true).await?;
        TypeConfig::sleep(Duration::from_millis(40)).await;
    }
    shutdown(&router, &[0, 1, 2]).await?;
    let campaigns = recorder
        .snapshot()
        .into_iter()
        .filter_map(|e| match e.kind {
            ElectionEventKind::CampaignStarted {
                campaign_id,
                origin,
                phase: CampaignPhase::PreVote,
                vote,
                ..
            } => {
                assert_eq!(origin, CampaignOrigin::ExternalElect);
                Some((campaign_id, vote))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(campaigns.len(), 2);
    assert_ne!(campaigns[0].0, campaigns[1].0);
    assert_eq!(campaigns[0].1, campaigns[1].1);
    assert!(
        voter.snapshot().iter().any(|e| matches!(e.kind, ElectionEventKind::VoteRequestProcessed {
            phase: CampaignPhase::PreVote,
            disposition: VoteRequestDisposition::LeaseNotExpired,
            ..
        })),
        "native rejection reason must come from the lease branch"
    );
    Ok(())
}

#[test_harness::test(harness = ut_harness)]
async fn oversized_membership_is_explicitly_absent_without_changing_native_membership() -> Result<()> {
    let mut router = RaftRouter::new(config());
    let recorder = Recorder::new();
    router.new_raft_node_with_election_observer(0, recorder.clone()).await;
    let raft = router.get_raft_handle(&0)?;
    let members = (0..=(MAX_OBSERVED_VOTERS as u64)).collect::<std::collections::BTreeSet<_>>();
    raft.initialize(members.clone()).await?;
    recorder
        .wait(|e| matches!(e.kind, ElectionEventKind::CampaignStarted { joint_voters: None, .. }))
        .await;
    raft.wait(Some(Duration::from_secs(2)))
        .voter_ids(members, "native membership was not truncated")
        .await?;
    shutdown(&router, &[0]).await
}

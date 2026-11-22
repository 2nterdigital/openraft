use std::time::Duration;

use anyhow::Result;
use openraft::Config;
use openraft::Instant;
use openraft::async_runtime::WatchReceiver;
use openraft::type_config::TypeConfigExt;
use openraft_memstore::TypeConfig;

use crate::fixtures::RaftRouter;
use crate::fixtures::ut_harness;

const HEARTBEAT_INTERVAL: u64 = 50;

fn config(heartbeat_min_interval: u64) -> Config {
    Config {
        broadcast_commit: false,
        heartbeat_interval: HEARTBEAT_INTERVAL,
        heartbeat_min_interval: Some(heartbeat_min_interval),
        election_timeout_min: 500,
        election_timeout_max: 1000,
        ..Default::default()
    }
}

/// Wait until both followers have committed and applied `index`, all within `within` of the
/// call: every wait gets only what is left of that one budget.
async fn followers_apply(router: &RaftRouter, index: u64, within: Duration) -> Result<()> {
    let deadline = TypeConfig::now() + within;
    let remaining = || deadline.saturating_duration_since(TypeConfig::now());
    for id in [1, 2] {
        router
            .wait_with_timeout(&id, remaining())
            .metrics(
                |m| {
                    let committed = m.local_committed.as_ref().map(|l| l.index) == Some(index);
                    committed && m.last_applied.as_ref().map(|l| l.index) == Some(index)
                },
                format!("node-{id} committed and applied {index}"),
            )
            .await?;
    }
    Ok(())
}

/// With `broadcast_commit = false` the leader does not push a commit-only AppendEntries:
/// followers receive the last entries but learn their commit only from a heartbeat.
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn broadcast_commit_off() -> Result<()> {
    let mut router = RaftRouter::new(config(0))?;

    let mut log_index = router.new_cluster([0, 1, 2], []).await?;

    tracing::info!(
        log_index,
        "--- with heartbeat: followers commit and apply within a few heartbeats"
    );
    {
        log_index += router.client_request_many(0, "foo", 10).await?.count;
        followers_apply(&router, log_index, Duration::from_millis(500)).await?;
    }

    tracing::info!(
        log_index,
        "--- without heartbeat: the last commit is not pushed to followers"
    );
    {
        let leader = router.get_raft_handle(&0)?;
        leader.runtime_config().heartbeat(false);

        log_index += router.client_request_many(0, "foo", 1).await?.count;
        router.wait(&0).applied_index(Some(log_index), "leader applied").await?;

        for id in [1, 2] {
            router.wait(&id).log_index(Some(log_index), "follower received the log").await?;
        }

        TypeConfig::sleep(Duration::from_millis(300)).await;

        // The follower whose ack committed the entry got it in a request built before the commit.
        // The other one may have got a later request that already carried it.
        let mut not_learned = vec![];
        for id in [1, 2] {
            let m = router.get_raft_handle(&id)?.metrics().borrow_watched().clone();
            if m.local_committed.as_ref().map(|l| l.index) < Some(log_index) {
                not_learned.push(id);
            }
        }
        assert!(
            !not_learned.is_empty(),
            "a follower must not learn the last commit without a heartbeat"
        );

        leader.runtime_config().heartbeat(true);
        followers_apply(&router, log_index, Duration::from_millis(500)).await?;
    }

    Ok(())
}

/// Heartbeat suppression does not hold the last commit back: once writes stop, every follower
/// learns it within `heartbeat_interval + heartbeat_min_interval`, plus scheduling slack.
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn broadcast_commit_off_with_heartbeat_suppression() -> Result<()> {
    const MIN_INTERVAL: u64 = 200;
    const SLACK: u64 = 150;

    let mut router = RaftRouter::new(config(MIN_INTERVAL))?;

    let mut log_index = router.new_cluster([0, 1, 2], []).await?;

    for round in 0..3 {
        tracing::info!(round, log_index, "--- a burst of writes, then the cluster goes idle");
        log_index += router.client_request_many(0, "foo", 10).await?.count;

        let bound = Duration::from_millis(HEARTBEAT_INTERVAL + MIN_INTERVAL + SLACK);
        followers_apply(&router, log_index, bound).await?;
    }

    Ok(())
}

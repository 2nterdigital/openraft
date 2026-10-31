use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use openraft::Config;
use openraft::MembershipChangePolicy;
use openraft::Raft;
use openraft::errors::ChangeMembershipError;
use openraft::errors::ClientWriteError;
use openraft::errors::RaftError;

use crate::fixtures::RaftRouter;
use crate::fixtures::ut_harness;

#[test_harness::test(harness = ut_harness)]
async fn controlled_instance_rejects_legacy_mutations_without_append() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_tick: false,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config.clone());
    let (store, sm) = router.new_store();
    let raft = Raft::new_with_election_observer_and_membership_policy(
        0,
        config,
        router,
        store,
        sm,
        None,
        MembershipChangePolicy::RequireGuard,
    )
    .await?;
    raft.initialize(btreeset! {0}).await?;
    raft.wait(Some(Duration::from_secs(2))).applied_index(Some(1), "bootstrap").await?;
    let before = raft.with_raft_state(|state| state.log_ids.last().cloned()).await?;
    for result in [
        raft.add_learner(1, (), false).await,
        raft.change_membership(btreeset! {0}, true).await,
    ] {
        assert!(
            matches!(
                result,
                Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(
                    ChangeMembershipError::GuardRequired
                )))
            ),
            "legacy mutation should require guard: {result:?}"
        );
    }
    assert_eq!(
        raft.with_raft_state(|state| state.log_ids.last().cloned()).await?,
        before,
        "rejected calls must not append"
    );
    raft.shutdown().await?;
    Ok(())
}

#[test_harness::test(harness = ut_harness)]
async fn guard_rejects_stale_membership_identity_without_append() -> Result<()> {
    use openraft::ChangeMembers;
    use openraft::MembershipChangeGuard;
    use openraft::vote::RaftLeaderId;

    let config = Arc::new(
        Config {
            enable_tick: false,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config.clone());
    let (store, sm) = router.new_store();
    let raft = Raft::new_with_election_observer_and_membership_policy(
        0,
        config,
        router,
        store,
        sm,
        None,
        MembershipChangePolicy::RequireGuard,
    )
    .await?;
    raft.initialize(btreeset! {0}).await?;
    raft.wait(Some(Duration::from_secs(2))).applied_index(Some(1), "bootstrap").await?;
    let guard = raft
        .with_raft_state(|state| MembershipChangeGuard {
            expected_leader: state.vote_ref().leader_id().to_committed(),
            committed: state.membership_state.committed().as_ref().clone(),
            effective: state.membership_state.effective().as_ref().clone(),
        })
        .await?;
    // Independently mismatch each complete membership identity before any accepted append.
    let before = raft.with_raft_state(|state| state.log_ids.last().cloned()).await?;
    for committed in [true, false] {
        let mut wrong = guard.clone();
        let membership = if committed {
            &mut wrong.committed
        } else {
            &mut wrong.effective
        };
        *membership = openraft::StoredMembership::new(None, membership.membership().clone());
        let result = raft.change_membership_guarded(ChangeMembers::AddVoterIds(Default::default()), true, wrong).await;
        assert!(
            matches!(
                result,
                Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(
                    ChangeMembershipError::GuardMismatch
                )))
            ),
            "each log identity must match, committed={committed}: {result:?}"
        );
    }
    for committed in [true, false] {
        let mut wrong = guard.clone();
        let membership = if committed {
            &mut wrong.committed
        } else {
            &mut wrong.effective
        };
        *membership = openraft::StoredMembership::new(
            *membership.log_id(),
            openraft::Membership::new_with_defaults(vec![btreeset! {0}], btreeset! {1}),
        );
        let result = raft.change_membership_guarded(ChangeMembers::AddVoterIds(Default::default()), true, wrong).await;
        assert!(
            matches!(
                result,
                Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(
                    ChangeMembershipError::GuardMismatch
                )))
            ),
            "each full config must match, committed={committed}: {result:?}"
        );
    }
    assert_eq!(
        raft.with_raft_state(|state| state.log_ids.last().cloned()).await?,
        before
    );
    // Same voter set, but a new membership log identity invalidates the old authorization.
    let response = raft
        .change_membership_guarded(ChangeMembers::AddVoterIds(Default::default()), true, guard.clone())
        .await?;
    assert_eq!(response.membership.unwrap().get_joint_config(), &vec![btreeset! {0}]);
    let before = raft.with_raft_state(|state| state.log_ids.last().cloned()).await?;
    let result = raft.change_membership_guarded(ChangeMembers::AddVoterIds(Default::default()), true, guard).await;
    assert!(
        matches!(
            result,
            Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(
                ChangeMembershipError::GuardMismatch
            )))
        ),
        "stale authorization must be rejected: {result:?}"
    );
    assert_eq!(
        raft.with_raft_state(|state| state.log_ids.last().cloned()).await?,
        before
    );
    raft.shutdown().await?;
    Ok(())
}

#[test_harness::test(harness = ut_harness)]
async fn guarded_joint_requires_explicit_fresh_finalization() -> Result<()> {
    use openraft::ChangeMembers;
    use openraft::MembershipChangeGuard;
    use openraft::vote::RaftLeaderId;
    use openraft_memstore::TypeConfig;

    let config = Arc::new(
        Config {
            enable_tick: false,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config.clone());
    router.new_raft_node(1).await;
    let (store, sm) = router.new_store();
    let raft = Raft::new_with_election_observer_and_membership_policy(
        0,
        config.clone(),
        router.clone(),
        store.clone(),
        sm.clone(),
        None,
        MembershipChangePolicy::RequireGuard,
    )
    .await?;
    raft.initialize(btreeset! {0}).await?;
    raft.wait(Some(Duration::from_secs(2))).applied_index(Some(1), "bootstrap").await?;
    let snapshot = |state: &openraft::RaftState<TypeConfig>| MembershipChangeGuard {
        expected_leader: state.vote_ref().leader_id().to_committed(),
        committed: state.membership_state.committed().as_ref().clone(),
        effective: state.membership_state.effective().as_ref().clone(),
    };
    raft.change_membership_guarded(
        ChangeMembers::AddNodes(maplit::btreemap! {1 => ()}),
        true,
        raft.with_raft_state(snapshot).await?,
    )
    .await?;
    let before_joint = raft.with_raft_state(snapshot).await?;
    let joint = raft.change_membership_guarded(btreeset! {0, 1}, true, before_joint.clone()).await?;
    assert_eq!(joint.membership.as_ref().unwrap().get_joint_config(), &vec![
        btreeset! {0},
        btreeset! {0, 1}
    ]);
    let observed = raft.with_raft_state(snapshot).await?;
    assert_eq!(observed.committed.log_id(), &Some(joint.log_id));
    assert_eq!(
        observed.effective, observed.committed,
        "single-step call must leave joint committed"
    );
    let stale = raft
        .change_membership_guarded(ChangeMembers::AddVoterIds(Default::default()), true, before_joint)
        .await;
    assert!(matches!(
        stale,
        Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(
            ChangeMembershipError::GuardMismatch
        )))
    ));
    raft.shutdown().await?;
    let raft = Raft::new_with_election_observer_and_membership_policy(
        0,
        config,
        router,
        store,
        sm,
        None,
        MembershipChangePolicy::RequireGuard,
    )
    .await?;
    raft.wait_for_recovery(Some(Duration::from_secs(2))).await?;
    raft.trigger().elect(false).await?;
    raft.wait(Some(Duration::from_secs(2))).current_leader(0, "joint recovery").await?;
    let recovered = raft.with_raft_state(snapshot).await?;
    assert_eq!(
        recovered.committed, observed.committed,
        "restart preserves the joint configuration"
    );
    assert!(matches!(
        raft.add_learner(2, (), false).await,
        Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(
            ChangeMembershipError::GuardRequired
        )))
    ));
    let uniform = raft
        .change_membership_guarded(ChangeMembers::AddVoterIds(Default::default()), true, recovered)
        .await?;
    assert_eq!(uniform.membership.unwrap().get_joint_config(), &vec![btreeset! {0, 1}]);
    assert!(uniform.log_id.index > joint.log_id.index);
    raft.shutdown().await?;
    Ok(())
}

#[test_harness::test(harness = ut_harness)]
async fn queued_guard_checks_effective_membership_and_leader_at_dispatch() -> Result<()> {
    use openraft::ChangeMembers;
    use openraft::MembershipChangeGuard;
    use openraft::Vote;
    use openraft::raft::VoteRequest;
    use openraft::vote::RaftLeaderId;

    let config = Arc::new(
        Config {
            enable_tick: false,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config.clone());
    let (store, sm) = router.new_store();
    let raft = Raft::new_with_election_observer_and_membership_policy(
        0,
        config,
        router,
        store,
        sm,
        None,
        MembershipChangePolicy::RequireGuard,
    )
    .await?;
    raft.initialize(btreeset! {0}).await?;
    raft.wait(Some(Duration::from_secs(2))).applied_index(Some(1), "bootstrap").await?;
    let guard = raft
        .with_raft_state(|state| MembershipChangeGuard {
            expected_leader: state.vote_ref().leader_id().to_committed(),
            committed: state.membership_state.committed().as_ref().clone(),
            effective: state.membership_state.effective().as_ref().clone(),
        })
        .await?;
    // join polls the first submission before the second. Both carry the original snapshot;
    // the second dispatch must observe the effective membership changed by the first.
    let (first, second) = futures::join!(
        raft.change_membership_guarded(ChangeMembers::AddVoterIds(Default::default()), true, guard.clone()),
        raft.change_membership_guarded(ChangeMembers::AddVoterIds(Default::default()), true, guard),
    );
    first?;
    assert!(matches!(
        second,
        Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(
            ChangeMembershipError::GuardMismatch
        )))
    ));
    let guard = raft
        .with_raft_state(|state| MembershipChangeGuard {
            expected_leader: state.vote_ref().leader_id().to_committed(),
            committed: state.membership_state.committed().as_ref().clone(),
            effective: state.membership_state.effective().as_ref().clone(),
        })
        .await?;
    let before = raft.with_raft_state(|state| state.log_ids.last().cloned()).await?;
    let mut request = VoteRequest::new(Vote::new_committed(10, 1), *guard.effective.log_id());
    request.leadership_transfer = true;
    let old_guard = guard.clone();
    let (vote, stale) = futures::join!(
        raft.vote(request),
        raft.change_membership_guarded(ChangeMembers::AddVoterIds(Default::default()), true, guard),
    );
    vote?;
    assert!(matches!(
        stale,
        Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(
            ChangeMembershipError::GuardMismatch
        )))
    ));
    assert_eq!(
        raft.with_raft_state(|state| state.log_ids.last().cloned()).await?,
        before
    );
    raft.trigger().elect(false).await?;
    raft.wait(Some(Duration::from_secs(2))).current_leader(0, "regain leadership").await?;
    let current = raft
        .with_raft_state(|state| MembershipChangeGuard {
            expected_leader: state.vote_ref().leader_id().to_committed(),
            committed: state.membership_state.committed().as_ref().clone(),
            effective: state.membership_state.effective().as_ref().clone(),
        })
        .await?;
    assert_ne!(current.expected_leader, old_guard.expected_leader);
    assert_eq!(
        current.committed, old_guard.committed,
        "membership itself did not change"
    );
    let before = raft.with_raft_state(|state| state.log_ids.last().cloned()).await?;
    let stale = raft
        .change_membership_guarded(ChangeMembers::AddVoterIds(Default::default()), true, old_guard)
        .await;
    assert!(
        matches!(
            stale,
            Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(
                ChangeMembershipError::GuardMismatch
            )))
        ),
        "regained leadership must not revive an old incarnation: {stale:?}"
    );
    assert_eq!(
        raft.with_raft_state(|state| state.log_ids.last().cloned()).await?,
        before
    );
    raft.change_membership_guarded(ChangeMembers::AddVoterIds(Default::default()), true, current)
        .await?;
    raft.shutdown().await?;
    Ok(())
}

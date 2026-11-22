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
use openraft::type_config::alias::StoredMembershipOf;

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
    let mut router = RaftRouter::new((*config).clone())?;
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
    let current = raft.with_raft_state(|state| state.membership_state.effective().membership().clone()).await?;
    for result in [
        raft.add_learner(1, (), false).await,
        raft.change_membership(btreeset! {0}, true).await,
        raft.append_membership(current, openraft::EntryPayload::Blank, []).await,
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
    let mut router = RaftRouter::new((*config).clone())?;
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
        let membership: &mut StoredMembershipOf<openraft_memstore::TypeConfig> = if committed {
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
    let mut router = RaftRouter::new((*config).clone())?;
    router.new_raft_node(1).await?;
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
    // v0.10.1 rejects a membership change with `UncommittedLeaderLog` until the new term's blank
    // log commits, which here needs a remote ack.
    let blank = raft.with_raft_state(|state| state.log_ids.last().cloned()).await?;
    raft.wait(Some(Duration::from_secs(2)))
        .applied_index_at_least(blank.map(|log_id| log_id.index), "blank log of the recovered term")
        .await?;
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
    let mut router = RaftRouter::new((*config).clone())?;
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
    // v0.10.1 lets a transfer bypass the lease only when the Leader it names authorized it.
    request.leadership_transfer_from = Some(raft.with_raft_state(|state| *state.vote_ref()).await?);
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
    // v0.10.1 rejects a membership change with `UncommittedLeaderLog` until the regained leader's
    // blank log commits; the guard alone does not wait for it.
    let blank = raft.with_raft_state(|state| state.log_ids.last().cloned()).await?;
    raft.wait(Some(Duration::from_secs(2)))
        .applied_index_at_least(blank.map(|log_id| log_id.index), "blank log of the regained term")
        .await?;
    raft.change_membership_guarded(ChangeMembers::AddVoterIds(Default::default()), true, current)
        .await?;
    raft.shutdown().await?;
    Ok(())
}

/// The guard is checked before the v0.10.1 leader-log barrier: a fresh guard still gets
/// `UncommittedLeaderLog` while the new leader's blank log is uncommitted, a stale one gets
/// `GuardMismatch`, an unguarded call gets `GuardRequired`, and none of them appends.
///
/// As in `t20_change_membership`, dropping every AppendEntries that carries entries keeps node 1's
/// blank log uncommitted while heartbeats keep its lease valid.
#[test_harness::test(harness = ut_harness)]
async fn guard_composes_with_the_uncommitted_leader_log_barrier() -> Result<()> {
    use openraft::ChangeMembers;
    use openraft::MembershipChangeGuard;
    use openraft::ServerState;
    use openraft::errors::NetworkError;
    use openraft::errors::RPCError;
    use openraft::vote::RaftLeaderId;
    use openraft_memstore::TypeConfig;

    use crate::fixtures::rpc_kind::RpcKind;
    use crate::fixtures::rpc_request::RpcRequest;

    let config = Config {
        enable_elect: false,
        heartbeat_interval: 50,
        election_timeout_min: 500,
        election_timeout_max: 501,
        ..Default::default()
    };
    let mut router = RaftRouter::new(config)?;
    router.new_raft_node(0).await?;
    router.new_raft_node_with_policy(1, None, MembershipChangePolicy::RequireGuard).await;
    router.new_raft_node(2).await?;
    router.initialize(0).await?;
    for id in [0, 1, 2] {
        router.wait(&id).applied_index_at_least(Some(1), "initialized").await?;
    }

    router
        .set_rpc_pre_hook(RpcKind::AppendEntries, |call| {
            let carries_entries = matches!(&call.request, RpcRequest::AppendEntries(a) if !a.entries.is_empty());
            let res = if carries_entries {
                let msg = format!("blocked: {}->{} append-entries with entries", call.from, call.to);
                Err(RPCError::Network(NetworkError::<TypeConfig>::from_string(msg)))
            } else {
                Ok(())
            };
            futures::future::ready(res)
        })
        .await;

    let leader = router.get_raft_handle(&1)?;
    router.get_raft_handle(&0)?.trigger().transfer_leader(1).await?;
    leader
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |m| m.state == ServerState::Leader && m.cluster_committed.is_none() && m.last_quorum_acked.is_some(),
            "node 1 leads with its blank log uncommitted",
        )
        .await?;

    let guard = leader
        .with_raft_state(|state| MembershipChangeGuard {
            expected_leader: state.vote_ref().leader_id().to_committed(),
            committed: state.membership_state.committed().as_ref().clone(),
            effective: state.membership_state.effective().as_ref().clone(),
        })
        .await?;
    let before = leader.with_raft_state(|state| state.log_ids.last().cloned()).await?;

    let fresh = leader
        .change_membership_guarded(ChangeMembers::AddVoterIds(Default::default()), true, guard.clone())
        .await;
    assert!(
        matches!(
            fresh,
            Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(
                ChangeMembershipError::UncommittedLeaderLog(_)
            )))
        ),
        "a matching guard does not bypass the barrier: {fresh:?}"
    );

    let mut stale = guard;
    stale.committed = openraft::StoredMembership::new(None, stale.committed.membership().clone());
    let stale = leader.change_membership_guarded(ChangeMembers::AddVoterIds(Default::default()), true, stale).await;
    assert!(
        matches!(
            stale,
            Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(
                ChangeMembershipError::GuardMismatch
            )))
        ),
        "the guard is checked before the barrier: {stale:?}"
    );

    let unguarded = leader.add_learner(3, (), false).await;
    assert!(
        matches!(
            unguarded,
            Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(
                ChangeMembershipError::GuardRequired
            )))
        ),
        "the policy is checked before the barrier: {unguarded:?}"
    );

    assert_eq!(
        leader.with_raft_state(|state| state.log_ids.last().cloned()).await?,
        before,
        "rejected calls must not append"
    );
    Ok(())
}

/// Guarded voter changes append the joint step even where an unguarded change goes direct
/// because every old quorum intersects every new one: replacing one of two voters, and adding a
/// voter. The replaced voter stays in the joint until an explicit finalization.
#[test_harness::test(harness = ut_harness)]
async fn guarded_voter_changes_always_enter_joint() -> Result<()> {
    use std::collections::BTreeSet;

    use openraft::ChangeMembers;
    use openraft::MembershipChangeGuard;
    use openraft::vote::RaftLeaderId;
    use openraft_memstore::TypeConfig;

    let config = Config {
        enable_tick: false,
        ..Default::default()
    }
    .validate()?;
    let mut router = RaftRouter::new(config.clone())?;
    for id in [1, 2, 3] {
        router.new_raft_node(id).await?;
    }
    let (store, sm) = router.new_store();
    let raft = Raft::new_with_election_observer_and_membership_policy(
        0,
        Arc::new(config),
        router,
        store,
        sm,
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
    let guarded = |changes: ChangeMembers<u64, ()>| {
        let raft = raft.clone();
        async move {
            let guard = raft.with_raft_state(snapshot).await?;
            let resp = raft.change_membership_guarded(changes, true, guard).await?;
            anyhow::Ok(resp.membership.unwrap().get_joint_config().clone())
        }
    };
    let finalize = || guarded(ChangeMembers::AddVoterIds(BTreeSet::new()));

    guarded(ChangeMembers::AddNodes(maplit::btreemap! {1 => (), 2 => (), 3 => ()})).await?;
    guarded(ChangeMembers::ReplaceAllVoters(btreeset! {0, 1})).await?;
    assert_eq!(finalize().await?, vec![btreeset! {0, 1}]);

    // {0, 1} -> {0, 2}: both majorities contain 0, so an unguarded change goes direct.
    let joint = guarded(ChangeMembers::ReplaceAllVoters(btreeset! {0, 2})).await?;
    assert_eq!(joint, vec![btreeset! {0, 1}, btreeset! {0, 2}]);
    let committed = raft.with_raft_state(snapshot).await?.committed;
    assert!(
        committed.voter_ids().any(|id| id == 1),
        "the replaced voter stays in the committed joint until finalization"
    );
    assert_eq!(finalize().await?, vec![btreeset! {0, 2}]);

    // {0, 2} -> {0, 2, 3}: every majority of the two intersects, so an unguarded change goes
    // direct.
    let joint = guarded(ChangeMembers::AddVoterIds(btreeset! {3})).await?;
    assert_eq!(joint, vec![btreeset! {0, 2}, btreeset! {0, 2, 3}]);
    assert_eq!(finalize().await?, vec![btreeset! {0, 2, 3}]);

    raft.shutdown().await?;
    Ok(())
}

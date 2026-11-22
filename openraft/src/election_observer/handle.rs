use std::fmt;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use super::CampaignOrigin;
use super::CampaignPhase;
use super::ElectionEvent;
use super::ElectionObserver;
use crate::RaftTypeConfig;

/// Callback and transient source IDs; this state never governs protocol decisions.
pub(crate) struct ObserverHandle<C>
where C: RaftTypeConfig
{
    observer: Arc<dyn ElectionObserver<C>>,
    enabled: bool,
    next_campaign: u64,
    pub(crate) vote_campaign: Option<u64>,
    pub(crate) pre_vote_campaign: Option<u64>,
    pub(crate) pre_vote_origin: CampaignOrigin,
}

impl<C> fmt::Debug for ObserverHandle<C>
where C: RaftTypeConfig
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObserverHandle").field("enabled", &self.enabled).finish_non_exhaustive()
    }
}

impl<C> ObserverHandle<C>
where C: RaftTypeConfig
{
    pub(crate) fn new(observer: Arc<dyn ElectionObserver<C>>) -> Self {
        Self {
            observer,
            enabled: true,
            next_campaign: 0,
            vote_campaign: None,
            pre_vote_campaign: None,
            pre_vote_origin: CampaignOrigin::AutomaticTimeout,
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn start_campaign(&mut self, phase: CampaignPhase, origin: CampaignOrigin) -> Option<u64> {
        let Some(next) = self.next_campaign.checked_add(1) else {
            self.enabled = false;
            tracing::warn!("native election observer campaign ID exhausted; disabling observer");
            return None;
        };
        self.next_campaign = next;
        match phase {
            CampaignPhase::Vote => self.vote_campaign = Some(self.next_campaign),
            CampaignPhase::PreVote => {
                self.pre_vote_campaign = Some(self.next_campaign);
                self.pre_vote_origin = origin;
            }
        }
        Some(self.next_campaign)
    }

    pub(crate) fn campaign(&self, phase: CampaignPhase) -> Option<u64> {
        match phase {
            CampaignPhase::Vote => self.vote_campaign,
            CampaignPhase::PreVote => self.pre_vote_campaign,
        }
    }

    pub(crate) fn emit(&mut self, event: ElectionEvent<C>) {
        if std::panic::catch_unwind(AssertUnwindSafe(|| self.observer.on_event(event))).is_err() {
            self.enabled = false;
            tracing::warn!("native election observer panicked; disabling observer for this Raft instance");
        }
    }
}

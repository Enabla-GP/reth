use crate::{BackfillJobFactory, ExExNotification, StreamBackfillJob, WalHandle};
use alloy_consensus::BlockHeader;
use alloy_eips::BlockNumHash;
use futures::{Stream, StreamExt};
use reth_ethereum_primitives::EthPrimitives;
use reth_evm::ConfigureEvm;
use reth_exex_types::ExExHead;
use reth_node_api::NodePrimitives;
use reth_provider::{BlockNumReader, BlockReader, Chain, HeaderProvider, StateProviderFactory};
use reth_stages_api::ExecutionStageThresholds;
use reth_tracing::tracing::debug;
use std::{
    collections::VecDeque,
    fmt::Debug,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};
use tokio::sync::mpsc::Receiver;

/// A stream of [`ExExNotification`]s. The stream will emit notifications for all blocks. If the
/// stream is configured with a head via [`ExExNotifications::set_with_head`] or
/// [`ExExNotifications::with_head`], it will run backfill jobs to catch up to the node head.
#[derive(Debug)]
pub struct ExExNotifications<P, E>
where
    E: ConfigureEvm,
{
    inner: ExExNotificationsInner<P, E>,
}

/// A trait, that represents a stream of [`ExExNotification`]s. The stream will emit notifications
/// for all blocks. If the stream is configured with a head via [`ExExNotifications::set_with_head`]
/// or [`ExExNotifications::with_head`], it will run backfill jobs to catch up to the node head.
pub trait ExExNotificationsStream<N: NodePrimitives = EthPrimitives>:
    Stream<Item = eyre::Result<ExExNotification<N>>> + Unpin
{
    /// Sets [`ExExNotificationsStream`] to a stream of [`ExExNotification`]s without a head.
    ///
    /// It's a no-op if the stream has already been configured without a head.
    ///
    /// See the documentation of [`ExExNotificationsWithoutHead`] for more details.
    fn set_without_head(&mut self);

    /// Sets [`ExExNotificationsStream`] to a stream of [`ExExNotification`]s with the provided
    /// head.
    ///
    /// It's a no-op if the stream has already been configured with a head.
    ///
    /// See the documentation of [`ExExNotificationsWithHead`] for more details.
    fn set_with_head(&mut self, exex_head: ExExHead);

    /// Returns a new [`ExExNotificationsStream`] without a head.
    ///
    /// See the documentation of [`ExExNotificationsWithoutHead`] for more details.
    fn without_head(self) -> Self
    where
        Self: Sized;

    /// Returns a new [`ExExNotificationsStream`] with the provided head.
    ///
    /// See the documentation of [`ExExNotificationsWithHead`] for more details.
    fn with_head(self, exex_head: ExExHead) -> Self
    where
        Self: Sized;

    /// Sets custom thresholds for the backfill job.
    ///
    /// These thresholds control how many blocks are included in each backfill notification.
    /// Only takes effect when the stream is configured with a head.
    ///
    /// By default, the backfill job uses [`BackfillJobFactory`] defaults (up to 500,000 blocks
    /// per batch, bounded by 30s execution time).
    fn set_backfill_thresholds(&mut self, _thresholds: ExecutionStageThresholds) {}
}

#[derive(Debug)]
enum ExExNotificationsInner<P, E>
where
    E: ConfigureEvm,
{
    /// A stream of [`ExExNotification`]s. The stream will emit notifications for all blocks.
    WithoutHead(ExExNotificationsWithoutHead<P, E>),
    /// A stream of [`ExExNotification`]s. The stream will only emit notifications for blocks that
    /// are committed or reverted after the given head.
    WithHead(Box<ExExNotificationsWithHead<P, E>>),
    /// Internal state used when transitioning between [`ExExNotificationsInner::WithoutHead`] and
    /// [`ExExNotificationsInner::WithHead`].
    Invalid,
}

impl<P, E> ExExNotificationsInner<P, E>
where
    E: ConfigureEvm,
{
    /// Returns the provider of the underlying stream.
    fn provider(&self) -> &P {
        match self {
            Self::WithoutHead(n) => &n.provider,
            Self::WithHead(n) => &n.provider,
            Self::Invalid => unreachable!(),
        }
    }
}

impl<P, E> ExExNotifications<P, E>
where
    E: ConfigureEvm,
{
    /// Creates a new stream of [`ExExNotifications`] without a head.
    pub const fn new(
        node_head: BlockNumHash,
        provider: P,
        evm_config: E,
        notifications: Receiver<ExExNotification<E::Primitives>>,
        wal_handle: WalHandle<E::Primitives>,
    ) -> Self {
        Self {
            inner: ExExNotificationsInner::WithoutHead(ExExNotificationsWithoutHead::new(
                node_head,
                provider,
                evm_config,
                notifications,
                wal_handle,
            )),
        }
    }

    /// As [`set_with_head`](ExExNotificationsStream::set_with_head), but backfills up to the
    /// node's current canonical head rather than the head captured at construction.
    pub fn catch_up_with_head(&mut self, exex_head: ExExHead) -> eyre::Result<()>
    where
        P: BlockNumReader,
    {
        // Resolve the current canonical head before tearing down the stream state, so a failed
        // lookup leaves the stream untouched.
        let local_head: BlockNumHash = self.inner.provider().chain_info()?.into();

        let current = std::mem::replace(&mut self.inner, ExExNotificationsInner::Invalid);
        let (provider, evm_config, notifications, wal_handle, backfill_thresholds) = match current {
            ExExNotificationsInner::WithoutHead(n) => {
                (n.provider, n.evm_config, n.notifications, n.wal_handle, None)
            }
            ExExNotificationsInner::WithHead(n) => {
                (n.provider, n.evm_config, n.notifications, n.wal_handle, n.backfill_thresholds)
            }
            ExExNotificationsInner::Invalid => unreachable!(),
        };
        let mut with_head = ExExNotificationsWithHead::new(
            local_head,
            provider,
            evm_config,
            notifications,
            wal_handle,
            exex_head,
        );
        // Preserve any custom backfill thresholds so the catch-up backfill respects the limits
        // the ExEx already configured.
        with_head.backfill_thresholds = backfill_thresholds;
        self.inner = ExExNotificationsInner::WithHead(Box::new(with_head));
        Ok(())
    }
}

impl<P, E> ExExNotificationsStream<E::Primitives> for ExExNotifications<P, E>
where
    P: BlockReader + HeaderProvider + StateProviderFactory + Clone + Unpin + 'static,
    E: ConfigureEvm<Primitives: NodePrimitives<Block = P::Block>> + Clone + Unpin + 'static,
{
    fn set_without_head(&mut self) {
        let current = std::mem::replace(&mut self.inner, ExExNotificationsInner::Invalid);
        self.inner = ExExNotificationsInner::WithoutHead(match current {
            ExExNotificationsInner::WithoutHead(notifications) => notifications,
            ExExNotificationsInner::WithHead(notifications) => ExExNotificationsWithoutHead::new(
                notifications.initial_local_head,
                notifications.provider,
                notifications.evm_config,
                notifications.notifications,
                notifications.wal_handle,
            ),
            ExExNotificationsInner::Invalid => unreachable!(),
        });
    }

    fn set_with_head(&mut self, exex_head: ExExHead) {
        let current = std::mem::replace(&mut self.inner, ExExNotificationsInner::Invalid);
        self.inner = ExExNotificationsInner::WithHead(match current {
            ExExNotificationsInner::WithoutHead(notifications) => {
                Box::new(notifications.with_head(exex_head))
            }
            ExExNotificationsInner::WithHead(notifications) => {
                Box::new(ExExNotificationsWithHead::new(
                    notifications.initial_local_head,
                    notifications.provider,
                    notifications.evm_config,
                    notifications.notifications,
                    notifications.wal_handle,
                    exex_head,
                ))
            }
            ExExNotificationsInner::Invalid => unreachable!(),
        });
    }

    fn without_head(mut self) -> Self {
        self.set_without_head();
        self
    }

    fn with_head(mut self, exex_head: ExExHead) -> Self {
        self.set_with_head(exex_head);
        self
    }

    fn set_backfill_thresholds(&mut self, thresholds: ExecutionStageThresholds) {
        if let ExExNotificationsInner::WithHead(notifications) = &mut self.inner {
            notifications.backfill_thresholds = Some(thresholds);
        }
    }
}

impl<P, E> Stream for ExExNotifications<P, E>
where
    P: BlockReader + HeaderProvider + StateProviderFactory + Clone + Unpin + 'static,
    E: ConfigureEvm<Primitives: NodePrimitives<Block = P::Block>> + 'static,
{
    type Item = eyre::Result<ExExNotification<E::Primitives>>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match &mut self.get_mut().inner {
            ExExNotificationsInner::WithoutHead(notifications) => {
                notifications.poll_next_unpin(cx).map(|result| result.map(Ok))
            }
            ExExNotificationsInner::WithHead(notifications) => notifications.poll_next_unpin(cx),
            ExExNotificationsInner::Invalid => unreachable!(),
        }
    }
}

/// A stream of [`ExExNotification`]s. The stream will emit notifications for all blocks.
pub struct ExExNotificationsWithoutHead<P, E>
where
    E: ConfigureEvm,
{
    node_head: BlockNumHash,
    provider: P,
    evm_config: E,
    notifications: Receiver<ExExNotification<E::Primitives>>,
    wal_handle: WalHandle<E::Primitives>,
}

impl<P: Debug, E> Debug for ExExNotificationsWithoutHead<P, E>
where
    E: ConfigureEvm + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExExNotifications")
            .field("provider", &self.provider)
            .field("evm_config", &self.evm_config)
            .field("notifications", &self.notifications)
            .finish()
    }
}

impl<P, E> ExExNotificationsWithoutHead<P, E>
where
    E: ConfigureEvm,
{
    /// Creates a new instance of [`ExExNotificationsWithoutHead`].
    const fn new(
        node_head: BlockNumHash,
        provider: P,
        evm_config: E,
        notifications: Receiver<ExExNotification<E::Primitives>>,
        wal_handle: WalHandle<E::Primitives>,
    ) -> Self {
        Self { node_head, provider, evm_config, notifications, wal_handle }
    }

    /// Subscribe to notifications with the given head.
    fn with_head(self, head: ExExHead) -> ExExNotificationsWithHead<P, E> {
        ExExNotificationsWithHead::new(
            self.node_head,
            self.provider,
            self.evm_config,
            self.notifications,
            self.wal_handle,
            head,
        )
    }
}

impl<P: Unpin, E> Stream for ExExNotificationsWithoutHead<P, E>
where
    E: ConfigureEvm,
{
    type Item = ExExNotification<E::Primitives>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().notifications.poll_recv(cx)
    }
}

/// A stream of [`ExExNotification`]s synchronized with the ExEx's latest view of the host chain.
/// Ordinary commits covered by the startup head are initially skipped. For example, with a head
/// at block 10, the first ordinary commit starts at block 11 unless a reversal occurs first.
///
/// Raw reorgs and reverts are always retained, including below the startup head. After observing
/// one, startup coverage no longer suppresses subsequent ordinary commits.
#[derive(Debug)]
pub struct ExExNotificationsWithHead<P, E>
where
    E: ConfigureEvm,
{
    /// The node's local head at launch.
    initial_local_head: BlockNumHash,
    provider: P,
    evm_config: E,
    notifications: Receiver<ExExNotification<E::Primitives>>,
    wal_handle: WalHandle<E::Primitives>,
    /// The exex head at launch
    initial_exex_head: ExExHead,
    /// Whether ordinary raw commits can still be covered by startup synchronization.
    /// Permanently retired when a raw reorg or revert is observed.
    startup_coverage_valid: bool,

    /// If true, then we need to check if the ExEx head is on the canonical chain and if not,
    /// revert its head.
    pending_check_canonical: bool,
    /// If true, then we need to check if the ExEx head is behind the node head and if so, backfill
    /// the missing blocks.
    pending_check_backfill: bool,
    /// The backfill job to run before consuming any notifications.
    backfill_job: Option<StreamBackfillJob<E, P, Chain<E::Primitives>>>,
    /// Custom thresholds for the backfill job, if set.
    backfill_thresholds: Option<ExecutionStageThresholds>,
    /// Notifications received during backfill that must be delivered after it completes.
    /// Includes all raw notifications after a reversal invalidates startup coverage.
    pending_notifications: VecDeque<ExExNotification<E::Primitives>>,
}

impl<P, E> ExExNotificationsWithHead<P, E>
where
    E: ConfigureEvm,
{
    /// Creates a new [`ExExNotificationsWithHead`].
    const fn new(
        node_head: BlockNumHash,
        provider: P,
        evm_config: E,
        notifications: Receiver<ExExNotification<E::Primitives>>,
        wal_handle: WalHandle<E::Primitives>,
        exex_head: ExExHead,
    ) -> Self {
        Self {
            initial_local_head: node_head,
            provider,
            evm_config,
            notifications,
            wal_handle,
            initial_exex_head: exex_head,
            startup_coverage_valid: true,
            pending_check_canonical: true,
            pending_check_backfill: true,
            backfill_job: None,
            backfill_thresholds: None,
            pending_notifications: VecDeque::new(),
        }
    }

    /// Sets custom thresholds for the backfill job.
    ///
    /// These thresholds control how many blocks are included in each backfill notification.
    /// By default, the backfill job uses [`BackfillJobFactory`] defaults (up to 500,000 blocks
    /// per batch, bounded by 30s execution time).
    ///
    /// If your ExEx is memory-constrained, consider setting a lower `max_blocks` value to
    /// reduce the size of each backfill notification.
    pub const fn with_backfill_thresholds(mut self, thresholds: ExecutionStageThresholds) -> Self {
        self.backfill_thresholds = Some(thresholds);
        self
    }
}

impl<P, E> ExExNotificationsWithHead<P, E>
where
    P: BlockReader + HeaderProvider + StateProviderFactory + Clone + Unpin + 'static,
    E: ConfigureEvm<Primitives: NodePrimitives<Block = P::Block>> + Clone + Unpin + 'static,
{
    /// Checks if the ExEx head is on the canonical chain.
    ///
    /// If the head block is not found in the database or it's ahead of the node head, it means
    /// we're not on the canonical chain and we need to revert the notification with the ExEx
    /// head block.
    fn check_canonical(&mut self) -> eyre::Result<Option<ExExNotification<E::Primitives>>> {
        if self.provider.is_known(self.initial_exex_head.block.hash)? &&
            self.initial_exex_head.block.number <= self.initial_local_head.number
        {
            // we have the targeted block and that block is below the current head
            debug!(target: "exex::notifications", "ExEx head is on the canonical chain");
            return Ok(None)
        }

        // If the head block is not found in the database, it means we're not on the canonical
        // chain.

        // Get the committed notification for the head block from the WAL.
        let Some(notification) = self
            .wal_handle
            .get_committed_notification_by_block_hash(&self.initial_exex_head.block.hash)?
        else {
            // it's possible that the exex head is further ahead
            if self.initial_exex_head.block.number > self.initial_local_head.number {
                debug!(target: "exex::notifications", "ExEx head is ahead of the canonical chain");
                return Ok(None);
            }

            return Err(eyre::eyre!(
                "Could not find notification for block hash {:?} in the WAL",
                self.initial_exex_head.block.hash
            ))
        };

        // Update the head block hash to the parent hash of the first committed block.
        let committed_chain = notification.committed_chain().unwrap();
        let new_exex_head =
            (committed_chain.first().parent_hash(), committed_chain.first().number() - 1).into();
        debug!(target: "exex::notifications", old_exex_head = ?self.initial_exex_head.block, new_exex_head = ?new_exex_head, "ExEx head updated");
        self.initial_exex_head.block = new_exex_head;

        // Return an inverted notification. See the documentation for
        // `ExExNotification::into_inverted`.
        Ok(Some(notification.into_inverted()))
    }

    /// Compares the node head against the ExEx head, and backfills if needed.
    ///
    /// CAUTION: This method assumes that the ExEx head is <= the node head, and that it's on the
    /// canonical chain.
    ///
    /// Possible situations are:
    /// - ExEx is behind the node head (`exex_head.number < node_head.number`). Backfill from the
    ///   node database.
    /// - ExEx is at the same block number as the node head (`exex_head.number ==
    ///   node_head.number`). Nothing to do.
    fn check_backfill(&mut self) -> eyre::Result<()> {
        let mut backfill_job_factory =
            BackfillJobFactory::new(self.evm_config.clone(), self.provider.clone());
        if let Some(thresholds) = self.backfill_thresholds.clone() {
            backfill_job_factory = backfill_job_factory.with_thresholds(thresholds);
        }
        match self.initial_exex_head.block.number.cmp(&self.initial_local_head.number) {
            std::cmp::Ordering::Less => {
                // ExEx is behind the node head, start backfill
                debug!(target: "exex::notifications", "ExEx is behind the node head and on the canonical chain, starting backfill");
                let backfill = backfill_job_factory
                    .backfill(
                        self.initial_exex_head.block.number + 1..=self.initial_local_head.number,
                    )
                    .into_stream();
                self.backfill_job = Some(backfill);
            }
            std::cmp::Ordering::Equal => {
                debug!(target: "exex::notifications", "ExEx is at the node head");
            }
            std::cmp::Ordering::Greater => {
                debug!(target: "exex::notifications", "ExEx is ahead of the node head");
            }
        };

        Ok(())
    }
}

impl<P, E> Stream for ExExNotificationsWithHead<P, E>
where
    P: BlockReader + HeaderProvider + StateProviderFactory + Clone + Unpin + 'static,
    E: ConfigureEvm<Primitives: NodePrimitives<Block = P::Block>> + Clone + Unpin + 'static,
{
    type Item = eyre::Result<ExExNotification<E::Primitives>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // 1. Check once whether we need to retrieve a notification gap from the WAL.
        if this.pending_check_canonical {
            if let Some(canonical_notification) = this.check_canonical()? {
                return Poll::Ready(Some(Ok(canonical_notification)))
            }

            // ExEx head is on the canonical chain, we no longer need to check it
            this.pending_check_canonical = false;
        }

        // 2. Check once whether we need to trigger backfill sync
        if this.pending_check_backfill {
            this.check_backfill()?;
            this.pending_check_backfill = false;
        }

        // 3. If backfill is in progress yield new notifications
        if let Some(backfill_job) = &mut this.backfill_job {
            debug!(target: "exex::notifications", "Polling backfill job");

            // Drain the notification channel to prevent backpressure from stalling the
            // ExExManager. During backfill, the ExEx is not consuming from the channel,
            // so the capacity-1 channel fills up, which blocks the manager's PollSender,
            // which fills the manager's 1024-entry buffer, which blocks all upstream
            // senders. Ordinary commits covered by the backfill range are discarded while
            // startup coverage remains valid. A raw reversal invalidates that coverage for all
            // following notifications, which must be buffered even within the backfill range.
            while let Poll::Ready(Some(notification)) = this.notifications.poll_recv(cx) {
                // Always buffer revert-containing notifications (ChainReverted,
                // ChainReorged) because the backfill job only re-delivers
                // ChainCommitted from the database. Discarding a reorg here would
                // leave the ExEx unaware of the fork switch.
                if notification.reverted_chain().is_some() {
                    // Retire before receiving another item, not when the pending item is yielded.
                    this.startup_coverage_valid = false;
                    this.pending_notifications.push_back(notification);
                    continue;
                }
                if this.startup_coverage_valid &&
                    let Some(committed) = notification.committed_chain() &&
                    committed.tip().number() <= this.initial_local_head.number
                {
                    // Covered by backfill range, safe to discard
                    continue;
                }
                // Beyond the backfill range — buffer for delivery after backfill
                this.pending_notifications.push_back(notification);
            }

            if let Some(chain) = ready!(backfill_job.poll_next_unpin(cx)).transpose()? {
                debug!(target: "exex::notifications", range = ?chain.range(), "Backfill job returned a chain");
                return Poll::Ready(Some(Ok(ExExNotification::ChainCommitted {
                    new: Arc::new(chain),
                })))
            }

            // Backfill job is done, remove it
            this.backfill_job = None;
        }

        // 4. Deliver any notifications that were buffered during backfill
        if let Some(notification) = this.pending_notifications.pop_front() {
            return Poll::Ready(Some(Ok(notification)))
        }

        // 5. Otherwise advance the regular event stream
        loop {
            let Some(notification) = ready!(this.notifications.poll_recv(cx)) else {
                return Poll::Ready(None)
            };

            // Only raw receiver reversals invalidate coverage; WAL inversion reconciles startup.
            if notification.reverted_chain().is_some() {
                this.startup_coverage_valid = false;
            }

            // 6. Skip commits covered by the startup head only while that coverage remains valid.
            if this.startup_coverage_valid &&
                let Some(committed) = notification.committed_chain()
            {
                // inclusive check because we should start with `exex.head + 1`
                if this.initial_exex_head.block.number >= committed.tip().number() {
                    continue
                }
            }

            return Poll::Ready(Some(Ok(notification)))
        }
    }
}

#[cfg(test)]
mod tests;

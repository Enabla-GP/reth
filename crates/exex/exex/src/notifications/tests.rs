use super::*;
use crate::Wal;
use alloy_consensus::Header;
use alloy_eips::BlockNumHash;
use eyre::OptionExt;
use futures::StreamExt;
use reth_db_common::init::init_genesis;
use reth_ethereum_primitives::Block;
use reth_evm_ethereum::EthEvmConfig;
use reth_primitives_traits::Block as _;
use reth_provider::{
    providers::BlockchainProvider, test_utils::create_test_provider_factory, BlockWriter, Chain,
    DBProvider, DatabaseProviderFactory,
};
use reth_testing_utils::generators::{self, random_block, BlockParams};
use std::collections::BTreeMap;
use tokio::sync::mpsc;

#[tokio::test]
async fn exex_notifications_behind_head_canonical() -> eyre::Result<()> {
    let mut rng = generators::rng();

    let temp_dir = tempfile::tempdir().unwrap();
    let wal = Wal::new(temp_dir.path()).unwrap();

    let provider_factory = create_test_provider_factory();
    let genesis_hash = init_genesis(&provider_factory)?;
    let genesis_block = provider_factory
        .block(genesis_hash.into())?
        .ok_or_else(|| eyre::eyre!("genesis block not found"))?;

    let provider = BlockchainProvider::new(provider_factory.clone())?;

    let node_head_block = random_block(
        &mut rng,
        genesis_block.number + 1,
        BlockParams { parent: Some(genesis_hash), tx_count: Some(0), ..Default::default() },
    )
    .try_recover()?;
    let node_head = node_head_block.num_hash();
    let provider_rw = provider_factory.provider_rw()?;
    provider_rw.insert_block(&node_head_block)?;
    provider_rw.commit()?;
    let exex_head =
        ExExHead { block: BlockNumHash { number: genesis_block.number, hash: genesis_hash } };

    let notification = ExExNotification::ChainCommitted {
        new: Arc::new(Chain::new(
            vec![random_block(
                &mut rng,
                node_head.number + 1,
                BlockParams { parent: Some(node_head.hash), ..Default::default() },
            )
            .try_recover()?],
            Default::default(),
            BTreeMap::new(),
        )),
    };

    let (notifications_tx, notifications_rx) = mpsc::channel(1);

    notifications_tx.send(notification.clone()).await?;

    let mut notifications = ExExNotificationsWithoutHead::new(
        node_head,
        provider,
        EthEvmConfig::mainnet(),
        notifications_rx,
        wal.handle(),
    )
    .with_head(exex_head);

    // First notification is the backfill of missing blocks from the canonical chain
    assert_eq!(
        notifications.next().await.transpose()?,
        Some(ExExNotification::ChainCommitted {
            new: Arc::new(
                BackfillJobFactory::new(
                    notifications.evm_config.clone(),
                    notifications.provider.clone()
                )
                .backfill(1..=1)
                .next()
                .ok_or_eyre("failed to backfill")??
            )
        })
    );

    // Second notification is the actual notification that we sent before
    assert_eq!(notifications.next().await.transpose()?, Some(notification));

    Ok(())
}

#[tokio::test]
async fn catch_up_with_head_after_pause_backfills_missed_blocks() -> eyre::Result<()> {
    let mut rng = generators::rng();

    let temp_dir = tempfile::tempdir().unwrap();
    let wal = Wal::new(temp_dir.path()).unwrap();

    let provider_factory = create_test_provider_factory();
    let genesis_hash = init_genesis(&provider_factory)?;
    let genesis_block = provider_factory
        .block(genesis_hash.into())?
        .ok_or_else(|| eyre::eyre!("genesis block not found"))?;
    let provider = BlockchainProvider::new(provider_factory.clone())?;

    let exex_head =
        ExExHead { block: BlockNumHash { number: genesis_block.number, hash: genesis_hash } };
    let (notifications_tx, notifications_rx) = mpsc::channel(1);

    let evm_config = EthEvmConfig::mainnet();
    let mut notifications = ExExNotifications::new(
        BlockNumHash { number: genesis_block.number, hash: genesis_hash },
        provider.clone(),
        evm_config.clone(),
        notifications_rx,
        wal.handle(),
    );
    // The ExEx configures its head at launch, as usual.
    notifications.set_with_head(exex_head);

    // Block 1 is delivered live and consumed, but the ExEx fails to durably process it.
    let node_head_block = random_block(
        &mut rng,
        genesis_block.number + 1,
        BlockParams { parent: Some(genesis_hash), tx_count: Some(0), ..Default::default() },
    )
    .try_recover()?;
    let node_head = node_head_block.num_hash();
    let block_1_notification = ExExNotification::ChainCommitted {
        new: Arc::new(Chain::new(
            vec![node_head_block.clone()],
            Default::default(),
            BTreeMap::new(),
        )),
    };
    notifications_tx.send(block_1_notification.clone()).await?;
    assert_eq!(notifications.next().await.transpose()?, Some(block_1_notification));

    // Meanwhile the node commits block 1 and advances its canonical head past the
    // launch-time head.
    let provider_rw = provider_factory.provider_rw()?;
    provider_rw.insert_block(&node_head_block)?;
    provider_rw.commit()?;
    provider.canonical_in_memory_state().set_canonical_head(node_head_block.clone_sealed_header());

    // The ExEx recovers still at genesis and catches up, it needs block 1 again, but that
    // notification is long gone from the channel.
    notifications.catch_up_with_head(exex_head)?;

    let block_2_notification = ExExNotification::ChainCommitted {
        new: Arc::new(Chain::new(
            vec![random_block(
                &mut rng,
                node_head.number + 1,
                BlockParams { parent: Some(node_head.hash), ..Default::default() },
            )
            .try_recover()?],
            Default::default(),
            BTreeMap::new(),
        )),
    };
    notifications_tx.send(block_2_notification.clone()).await?;

    // Backfill re-delivers block 1 up to the node's current head
    assert_eq!(
        notifications.next().await.transpose()?,
        Some(ExExNotification::ChainCommitted {
            new: Arc::new(
                BackfillJobFactory::new(evm_config, provider)
                    .backfill(1..=1)
                    .next()
                    .ok_or_eyre("failed to backfill")??
            )
        })
    );
    // followed by the live notification for block 2.
    assert_eq!(notifications.next().await.transpose()?, Some(block_2_notification));

    Ok(())
}

#[tokio::test]
async fn exex_notifications_same_head_canonical() -> eyre::Result<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let wal = Wal::new(temp_dir.path()).unwrap();

    let provider_factory = create_test_provider_factory();
    let genesis_hash = init_genesis(&provider_factory)?;
    let genesis_block = provider_factory
        .block(genesis_hash.into())?
        .ok_or_else(|| eyre::eyre!("genesis block not found"))?;

    let provider = BlockchainProvider::new(provider_factory)?;

    let node_head = BlockNumHash { number: genesis_block.number, hash: genesis_hash };
    let exex_head = ExExHead { block: node_head };

    let notification = ExExNotification::ChainCommitted {
        new: Arc::new(Chain::new(
            vec![Block {
                header: Header {
                    parent_hash: node_head.hash,
                    number: node_head.number + 1,
                    ..Default::default()
                },
                ..Default::default()
            }
            .seal_slow()
            .try_recover()?],
            Default::default(),
            BTreeMap::new(),
        )),
    };

    let (notifications_tx, notifications_rx) = mpsc::channel(1);

    notifications_tx.send(notification.clone()).await?;

    let mut notifications = ExExNotificationsWithoutHead::new(
        node_head,
        provider,
        EthEvmConfig::mainnet(),
        notifications_rx,
        wal.handle(),
    )
    .with_head(exex_head);

    let new_notification = notifications.next().await.transpose()?;
    assert_eq!(new_notification, Some(notification));

    Ok(())
}

#[tokio::test]
async fn exex_notifications_same_head_non_canonical() -> eyre::Result<()> {
    let mut rng = generators::rng();

    let temp_dir = tempfile::tempdir().unwrap();
    let wal = Wal::new(temp_dir.path()).unwrap();

    let provider_factory = create_test_provider_factory();
    let genesis_hash = init_genesis(&provider_factory)?;
    let genesis_block = provider_factory
        .block(genesis_hash.into())?
        .ok_or_else(|| eyre::eyre!("genesis block not found"))?;

    let provider = BlockchainProvider::new(provider_factory)?;

    let node_head_block = random_block(
        &mut rng,
        genesis_block.number + 1,
        BlockParams { parent: Some(genesis_hash), tx_count: Some(0), ..Default::default() },
    )
    .try_recover()?;
    let node_head = node_head_block.num_hash();
    let provider_rw = provider.database_provider_rw()?;
    provider_rw.insert_block(&node_head_block)?;
    provider_rw.commit()?;
    let node_head_notification = ExExNotification::ChainCommitted {
        new: Arc::new(
            BackfillJobFactory::new(EthEvmConfig::mainnet(), provider.clone())
                .backfill(node_head.number..=node_head.number)
                .next()
                .ok_or_else(|| eyre::eyre!("failed to backfill"))??,
        ),
    };

    let exex_head_block = random_block(
        &mut rng,
        genesis_block.number + 1,
        BlockParams { parent: Some(genesis_hash), tx_count: Some(0), ..Default::default() },
    );
    let exex_head = ExExHead { block: exex_head_block.num_hash() };
    let exex_head_notification = ExExNotification::ChainCommitted {
        new: Arc::new(Chain::new(
            vec![exex_head_block.clone().try_recover()?],
            Default::default(),
            BTreeMap::new(),
        )),
    };
    wal.commit(&exex_head_notification)?;

    let new_notification = ExExNotification::ChainCommitted {
        new: Arc::new(Chain::new(
            vec![random_block(
                &mut rng,
                node_head.number + 1,
                BlockParams { parent: Some(node_head.hash), ..Default::default() },
            )
            .try_recover()?],
            Default::default(),
            BTreeMap::new(),
        )),
    };

    let (notifications_tx, notifications_rx) = mpsc::channel(1);

    notifications_tx.send(new_notification.clone()).await?;

    let mut notifications = ExExNotificationsWithoutHead::new(
        node_head,
        provider,
        EthEvmConfig::mainnet(),
        notifications_rx,
        wal.handle(),
    )
    .with_head(exex_head);

    // First notification is the revert of the ExEx head block to get back to the canonical
    // chain
    assert_eq!(
        notifications.next().await.transpose()?,
        Some(exex_head_notification.into_inverted())
    );
    // Second notification is the backfilled block from the canonical chain to get back to the
    // canonical tip
    assert_eq!(notifications.next().await.transpose()?, Some(node_head_notification));
    // Third notification is the actual notification that we sent before
    assert_eq!(notifications.next().await.transpose()?, Some(new_notification));

    Ok(())
}

#[tokio::test]
async fn test_notifications_ahead_of_head() -> eyre::Result<()> {
    reth_tracing::init_test_tracing();
    let mut rng = generators::rng();

    let temp_dir = tempfile::tempdir().unwrap();
    let wal = Wal::new(temp_dir.path()).unwrap();

    let provider_factory = create_test_provider_factory();
    let genesis_hash = init_genesis(&provider_factory)?;
    let genesis_block = provider_factory
        .block(genesis_hash.into())?
        .ok_or_else(|| eyre::eyre!("genesis block not found"))?;

    let provider = BlockchainProvider::new(provider_factory)?;

    let exex_head_block = random_block(
        &mut rng,
        genesis_block.number + 1,
        BlockParams { parent: Some(genesis_hash), tx_count: Some(0), ..Default::default() },
    );
    let exex_head_notification = ExExNotification::ChainCommitted {
        new: Arc::new(Chain::new(
            vec![exex_head_block.clone().try_recover()?],
            Default::default(),
            BTreeMap::new(),
        )),
    };
    wal.commit(&exex_head_notification)?;

    let node_head = BlockNumHash { number: genesis_block.number, hash: genesis_hash };
    let exex_head = ExExHead {
        block: BlockNumHash { number: exex_head_block.number, hash: exex_head_block.hash() },
    };

    let new_notification = ExExNotification::ChainCommitted {
        new: Arc::new(Chain::new(
            vec![random_block(
                &mut rng,
                genesis_block.number + 1,
                BlockParams { parent: Some(genesis_hash), ..Default::default() },
            )
            .try_recover()?],
            Default::default(),
            BTreeMap::new(),
        )),
    };

    let (notifications_tx, notifications_rx) = mpsc::channel(1);

    notifications_tx.send(new_notification.clone()).await?;

    let mut notifications = ExExNotificationsWithoutHead::new(
        node_head,
        provider,
        EthEvmConfig::mainnet(),
        notifications_rx,
        wal.handle(),
    )
    .with_head(exex_head);

    // First notification is the revert of the ExEx head block to get back to the canonical
    // chain
    assert_eq!(
        notifications.next().await.transpose()?,
        Some(exex_head_notification.into_inverted())
    );

    // Second notification is the actual notification that we sent before
    assert_eq!(notifications.next().await.transpose()?, Some(new_notification));

    Ok(())
}

/// Regression test for <https://github.com/paradigmxyz/reth/issues/19665>.
///
/// During backfill, `poll_next` must drain the notification channel so that
/// the upstream `ExExManager` is never blocked by a full channel. Without
/// the drain loop the capacity-1 channel stays full for the entire backfill
/// duration, which stalls the manager's `PollSender` and eventually blocks
/// all upstream senders once the 1024-entry buffer fills up.
///
/// The key assertion is the `try_send` after the first `poll_next`: it
/// proves the channel was drained during the backfill poll. Without the
/// fix this `try_send` fails because the notification is still sitting in
/// the channel.
#[tokio::test]
async fn exex_notifications_backfill_drains_channel() -> eyre::Result<()> {
    let mut rng = generators::rng();

    let temp_dir = tempfile::tempdir().unwrap();
    let wal = Wal::new(temp_dir.path()).unwrap();

    let provider_factory = create_test_provider_factory();
    let genesis_hash = init_genesis(&provider_factory)?;
    let genesis_block = provider_factory
        .block(genesis_hash.into())?
        .ok_or_else(|| eyre::eyre!("genesis block not found"))?;

    let provider = BlockchainProvider::new(provider_factory.clone())?;

    // Insert block 1 into the DB so there's something to backfill
    let node_head_block = random_block(
        &mut rng,
        genesis_block.number + 1,
        BlockParams { parent: Some(genesis_hash), tx_count: Some(0), ..Default::default() },
    )
    .try_recover()?;
    let node_head = node_head_block.num_hash();
    let provider_rw = provider_factory.provider_rw()?;
    provider_rw.insert_block(&node_head_block)?;
    provider_rw.commit()?;

    // ExEx head is at genesis — backfill will run for block 1
    let exex_head =
        ExExHead { block: BlockNumHash { number: genesis_block.number, hash: genesis_hash } };

    // Notification for a block AFTER the backfill range (block 2).
    let post_backfill_notification = ExExNotification::ChainCommitted {
        new: Arc::new(Chain::new(
            vec![random_block(
                &mut rng,
                node_head.number + 1,
                BlockParams { parent: Some(node_head.hash), ..Default::default() },
            )
            .try_recover()?],
            Default::default(),
            BTreeMap::new(),
        )),
    };

    // Another notification (block 3) used to probe channel capacity.
    let probe_notification = ExExNotification::ChainCommitted {
        new: Arc::new(Chain::new(
            vec![random_block(
                &mut rng,
                node_head.number + 2,
                BlockParams { parent: None, ..Default::default() },
            )
            .try_recover()?],
            Default::default(),
            BTreeMap::new(),
        )),
    };

    let (notifications_tx, notifications_rx) = mpsc::channel(1);

    // Fill the capacity-1 channel.
    notifications_tx.send(post_backfill_notification.clone()).await?;

    // Confirm the channel is full — this is the precondition that causes the
    // stall in production: the ExExManager's PollSender would block here.
    assert!(
        notifications_tx.try_send(probe_notification.clone()).is_err(),
        "channel should be full before backfill poll"
    );

    let mut notifications = ExExNotificationsWithoutHead::new(
        node_head,
        provider,
        EthEvmConfig::mainnet(),
        notifications_rx,
        wal.handle(),
    )
    .with_head(exex_head);

    // Poll once — this returns the backfill result for block 1. Crucially,
    // the drain loop in poll_next runs in this same call, consuming the
    // notification from the channel and buffering it.
    let backfill_result = notifications.next().await.transpose()?;
    assert_eq!(
        backfill_result,
        Some(ExExNotification::ChainCommitted {
            new: Arc::new(
                BackfillJobFactory::new(
                    notifications.evm_config.clone(),
                    notifications.provider.clone()
                )
                .backfill(1..=1)
                .next()
                .ok_or_eyre("failed to backfill")??
            )
        })
    );

    // KEY ASSERTION: the channel was drained during the backfill poll above.
    // Without the drain loop this try_send fails because the original
    // notification is still occupying the capacity-1 channel.
    assert!(
        notifications_tx.try_send(probe_notification.clone()).is_ok(),
        "channel should have been drained during backfill poll"
    );

    // The first buffered notification (block 2) was drained from the channel
    // during backfill and is delivered now.
    let buffered = notifications.next().await.transpose()?;
    assert_eq!(buffered, Some(post_backfill_notification));

    // The probe notification (block 3) that we just sent is delivered next.
    let probe = notifications.next().await.transpose()?;
    assert_eq!(probe, Some(probe_notification));

    Ok(())
}

#[tokio::test]
async fn startup_fence_lower_reorg_preserves_recommit() -> eyre::Result<()> {
    let fixture = StartupFenceFixture::new()?;
    let replacement = startup_fence_branch(fixture.at(4), 1, 1)?;
    let block6 = startup_fence_branch(replacement.tip().num_hash(), 1, 1)?;
    let block7 = startup_fence_branch(block6.tip().num_hash(), 1, 1)?;
    let expected = vec![
        ExExNotification::ChainReorged { old: fixture.suffix(5), new: replacement },
        ExExNotification::ChainCommitted { new: block6 },
        ExExNotification::ChainCommitted { new: block7 },
    ];
    let mut input = vec![ExExNotification::ChainCommitted { new: fixture.suffix(5) }];
    input.extend(expected.clone());

    assert_eq!(
        startup_fence_collect(fixture.stream(fixture.at(6), &input)?).await?,
        expected,
        "lower reorg and its recommit at the startup height must both survive"
    );
    Ok(())
}

#[tokio::test]
async fn startup_fence_equal_reorg_preserves_both_sides() -> eyre::Result<()> {
    let fixture = StartupFenceFixture::new()?;
    let replacement = startup_fence_branch(fixture.at(4), 2, 1)?;
    let block7 = startup_fence_branch(replacement.tip().num_hash(), 1, 1)?;
    let expected = vec![
        ExExNotification::ChainReorged { old: fixture.suffix(5), new: replacement },
        ExExNotification::ChainCommitted { new: block7 },
    ];
    let mut input = vec![ExExNotification::ChainCommitted { new: fixture.suffix(5) }];
    input.extend(expected.clone());

    assert_eq!(
        startup_fence_collect(fixture.stream(fixture.at(6), &input)?).await?,
        expected,
        "equal-tip reorg must retain both original sides before the next commit"
    );
    Ok(())
}

#[tokio::test]
async fn startup_fence_pure_revert_preserves_recommits() -> eyre::Result<()> {
    let fixture = StartupFenceFixture::new()?;
    let replacement = startup_fence_branch(fixture.at(4), 3, 1)?;
    let mut expected = vec![ExExNotification::ChainReverted { old: fixture.suffix(5) }];
    expected.extend(replacement.blocks_iter().map(|block| ExExNotification::ChainCommitted {
        new: Arc::new(Chain::new(
            vec![block.as_ref().clone()],
            Default::default(),
            BTreeMap::new(),
        )),
    }));
    let mut input = vec![ExExNotification::ChainCommitted { new: fixture.suffix(5) }];
    input.extend(expected.clone());

    assert_eq!(
        startup_fence_collect(fixture.stream(fixture.at(6), &input)?).await?,
        expected,
        "pure revert to4 must allow the new commits5 and6 below the startup fence"
    );
    Ok(())
}

#[tokio::test]
async fn startup_fence_backfill_preserves_reversal_continuation() -> eyre::Result<()> {
    let fixture = StartupFenceFixture::new()?;
    let replacement = startup_fence_branch(fixture.at(4), 1, 1)?;
    let block6 = startup_fence_branch(replacement.tip().num_hash(), 1, 1)?;
    let block7 = startup_fence_branch(block6.tip().num_hash(), 1, 1)?;
    let expected = vec![
        ExExNotification::ChainReorged { old: fixture.suffix(5), new: replacement },
        ExExNotification::ChainCommitted { new: block6 },
        ExExNotification::ChainCommitted { new: block7 },
    ];
    // All raw items are available to the same drain before the real backfill is polled.
    let mut input = vec![ExExNotification::ChainCommitted { new: fixture.canonical.clone() }];
    input.extend(expected.clone());
    let actual = startup_fence_collect(fixture.stream(fixture.genesis, &input)?).await?;

    assert_eq!(
        actual[0]
            .committed_chain()
            .unwrap()
            .blocks_iter()
            .map(|block| block.num_hash())
            .collect::<Vec<_>>(),
        fixture.canonical.blocks_iter().map(|block| block.num_hash()).collect::<Vec<_>>()
    );
    assert_eq!(
        &actual[1..],
        &expected,
        "backfill drain must retain commit6 following its buffered raw reorg"
    );
    Ok(())
}

#[tokio::test]
async fn startup_fence_wal_inversion_keeps_covered_commits_suppressed() -> eyre::Result<()> {
    let fixture = StartupFenceFixture::new()?;
    let orphan = startup_fence_branch(fixture.genesis, 6, 2)?;
    let orphan_head = orphan.tip().num_hash();
    let orphan_commit = ExExNotification::ChainCommitted { new: orphan };
    fixture.wal.commit(&orphan_commit)?;
    let block7 =
        ExExNotification::ChainCommitted { new: startup_fence_branch(fixture.at(6), 1, 1)? };
    let input =
        vec![ExExNotification::ChainCommitted { new: fixture.canonical.clone() }, block7.clone()];
    let actual = startup_fence_collect(fixture.stream(orphan_head, &input)?).await?;

    assert_eq!(
        actual.len(),
        3,
        "WAL inversion must retain suppression of the raw commit covered by backfill"
    );
    assert_eq!(actual[0], orphan_commit.into_inverted());
    assert_eq!(
        actual[1]
            .committed_chain()
            .unwrap()
            .blocks_iter()
            .map(|block| block.num_hash())
            .collect::<Vec<_>>(),
        fixture.canonical.blocks_iter().map(|block| block.num_hash()).collect::<Vec<_>>()
    );
    assert_eq!(actual[2], block7);
    Ok(())
}

struct StartupFenceFixture {
    provider: BlockchainProvider<reth_provider::test_utils::MockNodeTypesWithDB>,
    genesis: BlockNumHash,
    canonical: Arc<Chain>,
    wal: Wal,
    _wal_dir: tempfile::TempDir,
}

impl StartupFenceFixture {
    fn new() -> eyre::Result<Self> {
        let wal_dir = tempfile::tempdir()?;
        let wal = Wal::new(wal_dir.path())?;
        let factory = create_test_provider_factory();
        let genesis_hash = init_genesis(&factory)?;
        let genesis_block =
            factory.block(genesis_hash.into())?.ok_or_eyre("genesis block not found")?;
        let genesis = BlockNumHash { number: genesis_block.number, hash: genesis_hash };
        let canonical = startup_fence_branch(genesis, 6, 0)?;
        let provider_rw = factory.provider_rw()?;
        for block in canonical.blocks_iter() {
            provider_rw.insert_block(block.as_ref())?;
        }
        provider_rw.commit()?;
        Ok(Self {
            provider: BlockchainProvider::new(factory)?,
            genesis,
            canonical,
            wal,
            _wal_dir: wal_dir,
        })
    }

    fn at(&self, number: u64) -> BlockNumHash {
        self.canonical.blocks()[&number].num_hash()
    }

    fn suffix(&self, first: u64) -> Arc<Chain> {
        Arc::new(Chain::new(
            self.canonical
                .blocks_iter()
                .filter(|block| block.number() >= first)
                .map(|block| block.as_ref().clone())
                .collect::<Vec<_>>(),
            Default::default(),
            BTreeMap::new(),
        ))
    }

    fn stream(
        &self,
        head: BlockNumHash,
        input: &[ExExNotification],
    ) -> eyre::Result<
        ExExNotificationsWithHead<
            BlockchainProvider<reth_provider::test_utils::MockNodeTypesWithDB>,
            EthEvmConfig,
        >,
    > {
        let (sender, receiver) = mpsc::channel(input.len().max(1));
        for notification in input {
            sender.try_send(notification.clone())?;
        }
        // A lost item must cause finite assertion failure, not a receive timeout.
        drop(sender);
        Ok(ExExNotificationsWithoutHead::new(
            self.at(6),
            self.provider.clone(),
            EthEvmConfig::mainnet(),
            receiver,
            self.wal.handle(),
        )
        .with_head(ExExHead { block: head }))
    }
}

fn startup_fence_branch(
    mut parent: BlockNumHash,
    count: u64,
    branch: u8,
) -> eyre::Result<Arc<Chain>> {
    let mut blocks = Vec::new();
    for _ in 0..count {
        let block = Block {
            header: Header {
                parent_hash: parent.hash,
                number: parent.number + 1,
                extra_data: vec![branch].into(),
                ..Default::default()
            },
            ..Default::default()
        }
        .seal_slow()
        .try_recover()?;
        parent = block.num_hash();
        blocks.push(block);
    }
    Ok(Arc::new(Chain::new(blocks, Default::default(), BTreeMap::new())))
}

async fn startup_fence_collect(
    notifications: impl Stream<Item = eyre::Result<ExExNotification>> + Unpin,
) -> eyre::Result<Vec<ExExNotification>> {
    notifications.collect::<Vec<_>>().await.into_iter().collect()
}

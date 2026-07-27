//! Check that chain-level block and envelope import converges to the same fork choice state
//! regardless of delivery order, using the production envelope import path.

#![cfg(not(debug_assertions))]

use std::sync::Arc;

use beacon_chain::test_utils::{BeaconChainHarness, EphemeralHarnessType, test_spec};
use beacon_chain::{AvailabilityProcessingStatus, NotifyExecutionLayer};
use eth2::types::SignedBlockContentsTuple;
use types::{
    BeaconState, BlockImportSource, EthSpec, Hash256, MainnetEthSpec,
    SignedExecutionPayloadEnvelope, Slot,
};

type E = MainnetEthSpec;
type Harness = BeaconChainHarness<EphemeralHarnessType<E>>;

const VALIDATOR_COUNT: usize = 32;

struct SegmentBlock {
    slot: Slot,
    root: Hash256,
    contents: SignedBlockContentsTuple<E>,
    envelope: SignedExecutionPayloadEnvelope<E>,
}

fn make_harness() -> Harness {
    let spec = test_spec::<E>();
    BeaconChainHarness::builder(E::default())
        .spec(Arc::new(spec))
        .deterministic_keypairs(VALIDATOR_COUNT)
        .fresh_ephemeral_store()
        .mock_execution_layer()
        .build()
}

/// A consumer with the same genesis as the producer: the spec and slot clock must be shared or
/// the builder derives different fork times from the wall clock.
fn make_consumer(producer: &Harness) -> Harness {
    BeaconChainHarness::builder(E::default())
        .spec(producer.spec.clone())
        .deterministic_keypairs(VALIDATOR_COUNT)
        .fresh_ephemeral_store()
        .mock_execution_layer()
        .testing_slot_clock(producer.chain.slot_clock.clone())
        .build()
}

/// Build a two-block gloas segment. With `full_parent` the producer imports each envelope
/// before producing the next block, so the child bids on the full parent; without it the child
/// bids on the empty parent and envelope delivery order is unconstrained.
async fn build_segment(producer: &Harness, full_parent: bool) -> Vec<SegmentBlock> {
    let mut state: BeaconState<E> = producer.get_current_state();
    let mut segment = vec![];

    for slot in 1..=2u64 {
        let slot = Slot::new(slot);
        producer.advance_slot();
        let (contents, envelope, post_state) =
            producer.make_block_with_envelope(state.clone(), slot).await;
        let root = contents.0.canonical_root();
        let envelope = envelope.expect("gloas block should have envelope");

        producer
            .process_block(slot, root, contents.clone())
            .await
            .unwrap();
        if full_parent {
            let block_state_root = contents.0.state_root();
            producer
                .process_envelope(root, envelope.clone(), &post_state, block_state_root)
                .await;
        }

        segment.push(SegmentBlock {
            slot,
            root,
            contents,
            envelope,
        });
        state = post_state;
    }
    segment
}

async fn import_block(consumer: &Harness, block: &SegmentBlock) {
    consumer
        .process_block(block.slot, block.root, block.contents.clone())
        .await
        .unwrap();
}

async fn import_block_duplicate(consumer: &Harness, block: &SegmentBlock) {
    let _ = consumer
        .process_block(block.slot, block.root, block.contents.clone())
        .await;
}

async fn import_block_expect_parent_unknown(consumer: &Harness, block: &SegmentBlock) {
    let err = consumer
        .process_block(block.slot, block.root, block.contents.clone())
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("ParentUnknown"),
        "full-parent child without parent envelope should be rejected, got: {err:?}"
    );
}

/// Import an envelope via the production path. Returns `Err` if gossip verification rejects it.
async fn import_envelope(consumer: &Harness, block: &SegmentBlock) -> Result<(), String> {
    let verified = consumer
        .chain
        .verify_envelope_for_gossip(Arc::new(block.envelope.clone()))
        .await
        .map_err(|e| format!("{e:?}"))?;

    let status = consumer
        .chain
        .process_execution_payload_envelope(
            block.root,
            verified,
            NotifyExecutionLayer::Yes,
            BlockImportSource::Gossip,
            || Ok(()),
        )
        .await
        .map_err(|e| format!("{e:?}"))?;

    if matches!(status, AvailabilityProcessingStatus::MissingComponents(..)) {
        consumer
            .process_gossip_columns(&block.contents.0, None)
            .await;
        assert!(
            consumer.chain.envelope_is_known_to_fork_choice(&block.root),
            "envelope import did not complete for {:?}",
            block.root
        );
    }

    consumer.chain.recompute_head_at_current_slot().await;
    Ok(())
}

fn fork_choice_summary(consumer: &Harness, segment: &[SegmentBlock]) -> Vec<String> {
    let head = consumer.chain.canonical_head.cached_head();
    let mut summary = vec![format!(
        "head {:?} {:?}",
        head.head_block_root(),
        head.head_payload_status()
    )];
    for block in segment {
        summary.push(format!(
            "{:?} received={}",
            block.root,
            consumer.chain.envelope_is_known_to_fork_choice(&block.root)
        ));
    }
    summary
}

#[tokio::test]
async fn chain_import_order_convergence_full_parent() {
    let spec = test_spec::<E>();
    if !spec.fork_name_at_slot::<E>(Slot::new(0)).gloas_enabled() {
        return;
    }

    let producer = make_harness();
    let segment = build_segment(&producer, true).await;
    let (b1, b2) = (&segment[0], &segment[1]);

    // Canonical order: block, envelope, block, envelope.
    let canonical = make_consumer(&producer);
    import_block(&canonical, b1).await;
    import_envelope(&canonical, b1).await.unwrap();
    import_block(&canonical, b2).await;
    import_envelope(&canonical, b2).await.unwrap();
    let expected = fork_choice_summary(&canonical, &segment);

    // A full-parent child before its parent's envelope must be rejected, and retrying after the
    // envelope converges (the reprocess-queue path at the chain API level).
    let child_early = make_consumer(&producer);
    import_block(&child_early, b1).await;
    import_block_expect_parent_unknown(&child_early, b2).await;
    import_envelope(&child_early, b1).await.unwrap();
    import_block(&child_early, b2).await;
    import_envelope(&child_early, b2).await.unwrap();
    assert_eq!(
        fork_choice_summary(&child_early, &segment),
        expected,
        "full-parent child retry"
    );

    // An envelope before its block must be rejected, and retrying after the block converges.
    let envelope_early = make_consumer(&producer);
    let err = import_envelope(&envelope_early, b1).await.unwrap_err();
    assert!(
        err.contains("BlockRootUnknown"),
        "early envelope should be rejected for unknown block, got: {err}"
    );
    import_block(&envelope_early, b1).await;
    import_envelope(&envelope_early, b1).await.unwrap();
    import_block(&envelope_early, b2).await;
    import_envelope(&envelope_early, b2).await.unwrap();
    assert_eq!(
        fork_choice_summary(&envelope_early, &segment),
        expected,
        "early envelope retry"
    );

    // Duplicate deliveries must not change the outcome. A duplicate envelope import may error;
    // only the final state matters.
    let duplicated = make_consumer(&producer);
    import_block(&duplicated, b1).await;
    import_block_duplicate(&duplicated, b1).await;
    import_envelope(&duplicated, b1).await.unwrap();
    let _ = import_envelope(&duplicated, b1).await;
    import_block(&duplicated, b2).await;
    import_envelope(&duplicated, b2).await.unwrap();
    let _ = import_envelope(&duplicated, b2).await;
    assert_eq!(
        fork_choice_summary(&duplicated, &segment),
        expected,
        "duplicate deliveries"
    );
}

#[tokio::test]
async fn chain_import_order_convergence_empty_parent() {
    let spec = test_spec::<E>();
    if !spec.fork_name_at_slot::<E>(Slot::new(0)).gloas_enabled() {
        return;
    }

    let producer = make_harness();
    let segment = build_segment(&producer, false).await;
    let (b1, b2) = (&segment[0], &segment[1]);

    // Canonical order.
    let canonical = make_consumer(&producer);
    import_block(&canonical, b1).await;
    import_envelope(&canonical, b1).await.unwrap();
    import_block(&canonical, b2).await;
    import_envelope(&canonical, b2).await.unwrap();
    let expected = fork_choice_summary(&canonical, &segment);

    // Empty-parent children have no envelope dependency: both blocks first, then envelopes late.
    let late = make_consumer(&producer);
    import_block(&late, b1).await;
    import_block(&late, b2).await;
    import_envelope(&late, b1).await.unwrap();
    import_envelope(&late, b2).await.unwrap();
    assert_eq!(
        fork_choice_summary(&late, &segment),
        expected,
        "late envelopes"
    );

    // The child's envelope before the parent's.
    let reversed = make_consumer(&producer);
    import_block(&reversed, b1).await;
    import_block(&reversed, b2).await;
    import_envelope(&reversed, b2).await.unwrap();
    import_envelope(&reversed, b1).await.unwrap();
    assert_eq!(
        fork_choice_summary(&reversed, &segment),
        expected,
        "reversed envelopes"
    );
}

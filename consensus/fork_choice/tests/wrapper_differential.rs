//! Differential harness for the `ForkChoice` handler layer: drives `on_block` /
//! `on_attestation` / `on_tick` on a minimal in-memory store, so store state (latest messages,
//! checkpoints, time) is derived by the real handlers rather than injected. Compares head
//! selection against `fork_choice_reference` given the spec-derived store contents.
//!
//! Seed version: covers the gloas by-slot latest-message update rule through real
//! `on_attestation` sequences. Block-tree shapes are minimal; growing this toward the
//! `proto_array` differential's scenario families is the plan of record.

#![cfg(not(debug_assertions))]

use std::collections::{BTreeMap, BTreeSet};

use bls::{AggregateSignature, Signature};
use fixed_bytes::FixedBytesExtended;
use fork_choice::{AttestationFromBlock, ForkChoice, ForkChoiceStore, PayloadVerificationStatus};
use fork_choice_reference as reference;
use proto_array::{JustifiedBalances, ReOrgThreshold};
use types::{
    AbstractExecPayload, AttestationData, BeaconBlock, BeaconBlockRef, BeaconState,
    BeaconStateError, ChainSpec, Checkpoint, Epoch, EthSpec, ForkName, Hash256, IndexedAttestation,
    IndexedAttestationElectra, MinimalEthSpec, SignedBeaconBlock, Slot,
};

type E = MinimalEthSpec;

const GWEI: u64 = 1_000_000_000;
const BALANCES: [u64; 16] = [32 * GWEI; 16];

#[derive(Debug)]
struct TestingStore {
    current_slot: Slot,
    justified_checkpoint: Checkpoint,
    justified_state_root: Hash256,
    justified_balances: JustifiedBalances,
    finalized_checkpoint: Checkpoint,
    unrealized_justified_checkpoint: Checkpoint,
    unrealized_justified_state_root: Hash256,
    unrealized_finalized_checkpoint: Checkpoint,
    proposer_boost_root: Hash256,
    equivocating_indices: BTreeSet<u64>,
}

impl TestingStore {
    fn new(anchor: Checkpoint, anchor_state_root: Hash256) -> Self {
        Self {
            current_slot: Slot::new(0),
            justified_checkpoint: anchor,
            justified_state_root: anchor_state_root,
            justified_balances: JustifiedBalances::from_effective_balances(BALANCES.to_vec())
                .unwrap(),
            finalized_checkpoint: anchor,
            unrealized_justified_checkpoint: anchor,
            unrealized_justified_state_root: anchor_state_root,
            unrealized_finalized_checkpoint: anchor,
            proposer_boost_root: Hash256::zero(),
            equivocating_indices: BTreeSet::new(),
        }
    }
}

impl ForkChoiceStore<E> for TestingStore {
    type Error = BeaconStateError;

    fn get_current_slot(&self) -> Slot {
        self.current_slot
    }

    fn set_current_slot(&mut self, slot: Slot) {
        self.current_slot = slot;
    }

    fn on_verified_block<Payload: AbstractExecPayload<E>>(
        &mut self,
        _block: BeaconBlockRef<E, Payload>,
        _block_root: Hash256,
        _state: &BeaconState<E>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn justified_checkpoint(&self) -> &Checkpoint {
        &self.justified_checkpoint
    }

    fn justified_state_root(&self) -> Hash256 {
        self.justified_state_root
    }

    fn justified_balances(&self) -> &JustifiedBalances {
        &self.justified_balances
    }

    fn finalized_checkpoint(&self) -> &Checkpoint {
        &self.finalized_checkpoint
    }

    fn unrealized_justified_checkpoint(&self) -> &Checkpoint {
        &self.unrealized_justified_checkpoint
    }

    fn unrealized_justified_state_root(&self) -> Hash256 {
        self.unrealized_justified_state_root
    }

    fn unrealized_finalized_checkpoint(&self) -> &Checkpoint {
        &self.unrealized_finalized_checkpoint
    }

    fn proposer_boost_root(&self) -> Hash256 {
        self.proposer_boost_root
    }

    fn set_finalized_checkpoint(&mut self, checkpoint: Checkpoint) {
        self.finalized_checkpoint = checkpoint;
    }

    fn set_justified_checkpoint(
        &mut self,
        checkpoint: Checkpoint,
        state_root: Hash256,
    ) -> Result<(), Self::Error> {
        self.justified_checkpoint = checkpoint;
        self.justified_state_root = state_root;
        Ok(())
    }

    fn set_unrealized_justified_checkpoint(&mut self, checkpoint: Checkpoint, state_root: Hash256) {
        self.unrealized_justified_checkpoint = checkpoint;
        self.unrealized_justified_state_root = state_root;
    }

    fn set_unrealized_finalized_checkpoint(&mut self, checkpoint: Checkpoint) {
        self.unrealized_finalized_checkpoint = checkpoint;
    }

    fn set_proposer_boost_root(&mut self, proposer_boost_root: Hash256) {
        self.proposer_boost_root = proposer_boost_root;
    }

    fn equivocating_indices(&self) -> &BTreeSet<u64> {
        &self.equivocating_indices
    }

    fn extend_equivocating_indices(&mut self, indices: impl IntoIterator<Item = u64>) {
        self.equivocating_indices.extend(indices);
    }
}

fn spec() -> ChainSpec {
    ForkName::Gloas.make_genesis_spec(E::default_spec())
}

fn fixture() -> &'static (ChainSpec, BeaconState<E>) {
    static FIXTURE: std::sync::OnceLock<(ChainSpec, BeaconState<E>)> = std::sync::OnceLock::new();
    FIXTURE.get_or_init(|| {
        let spec = spec();
        let state = base_state(&spec);
        (spec, state)
    })
}

fn base_state(spec: &ChainSpec) -> BeaconState<E> {
    let keypairs = types::test_utils::generate_deterministic_keypairs(BALANCES.len());
    let mut state = beacon_chain::test_utils::InteropGenesisBuilder::<E>::default()
        .build_genesis_state(
            &keypairs,
            0,
            Hash256::from_slice(beacon_chain::test_utils::DEFAULT_ETH1_BLOCK_HASH),
            spec,
        )
        .unwrap();
    state.build_all_committee_caches(spec).unwrap();
    state
}

struct Chain {
    fork_choice: ForkChoice<TestingStore, E>,
    anchor_root: Hash256,
    blocks: BTreeMap<Hash256, (SignedBeaconBlock<E>, BeaconState<E>)>,
    attestation_timely: BTreeMap<Hash256, bool>,
    spec: ChainSpec,
}

impl Chain {
    fn new() -> Self {
        let (spec, state) = fixture();
        let spec = spec.clone();
        let mut state = state.clone();
        let mut block = BeaconBlock::empty(&spec);
        *block.state_root_mut() = state.canonical_root().unwrap_or_default();
        let signed = SignedBeaconBlock::from_block(block, Signature::empty());
        // Use the state's own genesis block root (the interop genesis header differs from
        // `BeaconBlock::empty` in body root) so attestation target checks line up.
        let anchor_root = {
            let mut header = state.latest_block_header().clone();
            header.state_root = state.canonical_root().unwrap_or_default();
            header.canonical_root()
        };
        let anchor = Checkpoint {
            epoch: Epoch::new(0),
            root: anchor_root,
        };

        let store = TestingStore::new(anchor, signed.state_root());
        let fork_choice =
            ForkChoice::from_anchor(store, anchor_root, &signed, &state, None, &spec).unwrap();
        let mut blocks = BTreeMap::new();
        blocks.insert(anchor_root, (signed, state));
        let mut attestation_timely = BTreeMap::new();
        attestation_timely.insert(anchor_root, true);
        Self {
            fork_choice,
            anchor_root,
            blocks,
            attestation_timely,
            spec,
        }
    }

    /// Add an empty-parent-claiming block via the real `on_block` handler, imported at its own
    /// slot. `seed` distinguishes same-slot siblings and picks the bid hash.
    fn add_block(&mut self, parent_root: Hash256, slot: u64, seed: u8) -> Hash256 {
        self.add_block_opts(parent_root, slot, seed, seed as u64, false)
    }

    /// `late` imports the block past the attestation deadline (spec `is_head_late` true).
    fn add_block_opts(
        &mut self,
        parent_root: Hash256,
        slot: u64,
        seed: u8,
        proposer: u64,
        late: bool,
    ) -> Hash256 {
        let slot = Slot::new(slot);
        let (_, parent_state) = &self.blocks[&parent_root];
        let mut state = parent_state.clone();
        state_processing::state_advance::complete_state_advance(&mut state, None, slot, &self.spec)
            .unwrap();

        let mut block = BeaconBlock::empty(&self.spec);
        *block.slot_mut() = slot;
        *block.parent_root_mut() = parent_root;
        *block.state_root_mut() = Hash256::repeat_byte(seed);
        *block.proposer_index_mut() = proposer;
        set_bid_hashes(&mut block, bid_hash(seed), Hash256::repeat_byte(0xdd));
        let signed = SignedBeaconBlock::from_block(block, Signature::empty());
        let root = signed.canonical_root();

        let delay = if late {
            std::time::Duration::from_secs(4)
        } else {
            std::time::Duration::ZERO
        };
        self.fork_choice
            .on_block(
                slot,
                signed.message(),
                root,
                delay,
                &state,
                PayloadVerificationStatus::Irrelevant,
                &self.spec,
            )
            .unwrap();
        self.blocks.insert(root, (signed, state));
        self.attestation_timely.insert(root, !late);
        root
    }

    fn attest(&mut self, current_slot: u64, validator: u64, block_root: Hash256, slot: u64) {
        self.attest_with_payload(current_slot, validator, block_root, slot, false);
    }

    fn attest_with_payload(
        &mut self,
        current_slot: u64,
        validator: u64,
        block_root: Hash256,
        slot: u64,
        payload_present: bool,
    ) {
        let attestation = IndexedAttestation::Electra(IndexedAttestationElectra {
            attesting_indices: vec![validator].try_into().unwrap(),
            data: AttestationData {
                slot: Slot::new(slot),
                index: payload_present as u64,
                beacon_block_root: block_root,
                source: Checkpoint {
                    epoch: Epoch::new(0),
                    root: self.anchor_root,
                },
                target: Checkpoint {
                    epoch: Slot::new(slot).epoch(E::slots_per_epoch()),
                    root: self.target_root(block_root, Slot::new(slot)),
                },
            },
            signature: AggregateSignature::empty(),
        });
        self.fork_choice
            .on_attestation(
                Slot::new(current_slot),
                attestation.to_ref(),
                AttestationFromBlock::False,
                &self.spec,
            )
            .unwrap();
    }

    fn reveal(&mut self, block_root: Hash256) {
        self.fork_choice
            .on_valid_payload_envelope_received(block_root)
            .unwrap();
    }

    /// Option (a) mapping: the block Lighthouse would build on — the re-org parent on Ok, the
    /// canonical head on any refusal.
    fn proposer_head(&mut self, slot: u64) -> Hash256 {
        let head = self.head(slot);
        match self.fork_choice.get_proposer_head(
            Slot::new(slot),
            head,
            ReOrgThreshold(self.spec.reorg_head_weight_threshold),
            ReOrgThreshold(self.spec.reorg_parent_weight_threshold),
            Epoch::new(self.spec.reorg_max_epochs_since_finalization),
        ) {
            Ok(info) => info.parent_node.root(),
            Err(_) => head,
        }
    }

    fn reference_proposer_head(
        &self,
        proposal_slot: u64,
        latest_messages: &[(u64, Hash256, u64, bool)],
    ) -> Hash256 {
        let store = self.reference_store(proposal_slot, latest_messages, &[], Hash256::zero());
        let head_node = reference::get_head(&store);
        reference::get_proposer_head(&store, head_node, Slot::new(proposal_slot)).root
    }

    fn target_root(&self, block_root: Hash256, slot: Slot) -> Hash256 {
        let epoch_start = slot
            .epoch(E::slots_per_epoch())
            .start_slot(E::slots_per_epoch());
        let mut root = block_root;
        while self.blocks[&root].0.slot() > epoch_start {
            root = self.blocks[&root].0.parent_root();
        }
        root
    }

    fn head(&mut self, current_slot: u64) -> Hash256 {
        self.fork_choice
            .get_head(Slot::new(current_slot), &self.spec)
            .unwrap()
            .0
    }

    /// The reference head given latest messages derived by applying the spec's update rules to
    /// the attestation sequence externally.
    fn reference_head(
        &self,
        current_slot: u64,
        latest_messages: &[(u64, Hash256, u64, bool)],
    ) -> Hash256 {
        self.reference_head_with_reveals(current_slot, latest_messages, &[])
    }

    fn reference_head_with_reveals(
        &self,
        current_slot: u64,
        latest_messages: &[(u64, Hash256, u64, bool)],
        revealed: &[Hash256],
    ) -> Hash256 {
        self.reference_head_full(current_slot, latest_messages, revealed, Hash256::zero())
    }

    fn reference_head_full(
        &self,
        current_slot: u64,
        latest_messages: &[(u64, Hash256, u64, bool)],
        revealed: &[Hash256],
        proposer_boost_root: Hash256,
    ) -> Hash256 {
        let store =
            self.reference_store(current_slot, latest_messages, revealed, proposer_boost_root);
        reference::get_head(&store).root
    }

    fn reference_store(
        &self,
        current_slot: u64,
        latest_messages: &[(u64, Hash256, u64, bool)],
        revealed: &[Hash256],
        proposer_boost_root: Hash256,
    ) -> reference::Store {
        let anchor = Checkpoint {
            epoch: Epoch::new(0),
            root: self.anchor_root,
        };
        let blocks = self
            .blocks
            .iter()
            .map(|(root, (block, _))| {
                (
                    *root,
                    reference::Block {
                        slot: block.slot(),
                        parent_root: if *root == self.anchor_root {
                            Hash256::zero()
                        } else {
                            block.parent_root()
                        },
                        parent_payload_status: reference::PayloadStatus::Empty,
                        proposer_index: block.message().proposer_index(),
                        ptc_timely: true,
                        attestation_timely: self.attestation_timely[root],
                        justified_checkpoint: anchor,
                        unrealized_justified_checkpoint: anchor,
                    },
                )
            })
            .collect();
        reference::Store {
            current_slot: Slot::new(current_slot),
            justified_checkpoint: anchor,
            finalized_checkpoint: anchor,
            blocks,
            payload_revealed: revealed.iter().copied().collect(),
            payload_timeliness_votes: BTreeMap::new(),
            payload_data_availability_votes: BTreeMap::new(),
            latest_messages: latest_messages
                .iter()
                .map(|(validator, root, slot, payload_present)| {
                    (
                        *validator,
                        reference::LatestMessage {
                            slot: Slot::new(*slot),
                            root: *root,
                            payload_present: *payload_present,
                        },
                    )
                })
                .collect(),
            equivocating_indices: BTreeSet::new(),
            balances: BALANCES.to_vec(),
            proposer_boost_root,
            ptc_size: E::ptc_size(),
            slots_per_epoch: E::slots_per_epoch(),
            proposer_score_boost: self.spec.proposer_score_boost,
            reorg_head_weight_threshold: self.spec.reorg_head_weight_threshold,
            effective_balance_increment: self.spec.effective_balance_increment,
            reorg_parent_weight_threshold: self.spec.reorg_parent_weight_threshold,
            reorg_max_epochs_since_finalization: self.spec.reorg_max_epochs_since_finalization,
            time_into_slot_ms: 0,
            proposer_reorg_cutoff_ms: 1_000,
        }
    }
}

fn bid_hash(seed: u8) -> Hash256 {
    Hash256::repeat_byte(0xb0 + seed)
}

fn set_bid_hashes(block: &mut BeaconBlock<E>, block_hash: Hash256, parent_hash: Hash256) {
    if let BeaconBlock::Gloas(inner) = block {
        let bid = &mut inner.body.signed_execution_payload_bid.message;
        bid.block_hash = types::ExecutionBlockHash::from_root(block_hash);
        bid.parent_block_hash = types::ExecutionBlockHash::from_root(parent_hash);
    }
}

/// Gloas updates latest messages by slot: an older-slot attestation from the same validator
/// must not overwrite a newer one, regardless of arrival order. Driven through the real
/// `on_attestation` handler (this is the layer where the pre-gloas by-epoch bug fixed in the
/// fork choice compliance work lived).
#[test]
fn gloas_latest_message_by_slot_rule() {
    let mut chain = Chain::new();
    let b1 = chain.add_block(chain.anchor_root, 1, 1);
    let b2 = chain.add_block(chain.anchor_root, 1, 2);

    // Newer vote first, older vote (for the other fork) delivered afterwards.
    chain.attest(4, 0, b1, 3);
    chain.attest(4, 0, b2, 2);

    let head = chain.head(4);
    let expected = chain.reference_head(4, &[(0, b1, 3, false)]);
    assert_eq!(head, expected, "older-slot vote must not overwrite newer");
    assert_eq!(head, b1);

    // The newer vote wins once delivered, in either order.
    chain.attest(4, 0, b2, 4);
    let head = chain.head(5);
    let expected = chain.reference_head(5, &[(0, b2, 4, false)]);
    assert_eq!(head, expected, "newer-slot vote must overwrite older");
    assert_eq!(head, b2);
}

/// An attestation from the current slot is queued and only takes effect after the next tick.
#[test]
fn current_slot_attestation_queued_until_next_slot() {
    let mut chain = Chain::new();
    let b1 = chain.add_block(chain.anchor_root, 1, 1);
    let b2 = chain.add_block(chain.anchor_root, 1, 2);

    // Heavier validator votes for B1 at the current slot: queued, not yet applied.
    chain.attest(2, 1, b1, 2);
    assert_eq!(
        chain.head(2),
        chain.reference_head(2, &[]),
        "current-slot attestation must not count before the next slot"
    );
    let tiebreak_winner = std::cmp::max(b1, b2);
    assert_eq!(
        chain.head(2),
        tiebreak_winner,
        "lexicographic tiebreak while queued"
    );

    // After ticking into the next slot the queued attestation applies.
    assert_eq!(chain.head(3), chain.reference_head(3, &[(1, b1, 2, false)]));
    assert_eq!(chain.head(3), b1);
}

#[derive(Debug, Clone, Copy)]
struct Delivery {
    delivery_slot: u64,
    validator: u64,
    block: usize,
    attestation_slot: u64,
    payload_present: bool,
}

/// Latest messages effective at `query_slot`. An attestation with slot `A` delivered at slot `D`
/// takes effect once the store reaches `max(D, A + 1)` (spec: current-slot attestations are
/// queued for one slot). Applied in effective order; a strictly newer attestation slot replaces.
fn expected_messages(
    deliveries: &[Delivery],
    roots: &[Hash256],
    query_slot: u64,
) -> Vec<(u64, Hash256, u64, bool)> {
    let mut effective: Vec<(u64, &Delivery)> = deliveries
        .iter()
        .map(|d| (d.delivery_slot.max(d.attestation_slot + 1), d))
        .filter(|(effective_at, _)| *effective_at <= query_slot)
        .collect();
    effective.sort_by_key(|(effective_at, _)| *effective_at);

    let mut latest: BTreeMap<u64, (Hash256, u64, bool)> = BTreeMap::new();
    for (_, delivery) in effective {
        let message = (
            roots[delivery.block],
            delivery.attestation_slot,
            delivery.payload_present,
        );
        match latest.entry(delivery.validator) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(message);
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                if delivery.attestation_slot > entry.get().1 {
                    entry.insert(message);
                }
            }
        }
    }
    latest
        .into_iter()
        .map(|(validator, (root, slot, payload_present))| (validator, root, slot, payload_present))
        .collect()
}

/// Exhaustive attestation delivery sequences through the real `on_attestation`/`on_tick`
/// handlers: three deliveries at slots 2, 3, 4 with enumerated (validator, target block,
/// attestation slot, payload_present), differentialed against the reference after every
/// delivery and at the final slot.
#[test]
fn attestation_sequence_differential() {
    for delivery_slots in [[2u64, 3, 4], [2, 2, 3], [3, 3, 3]] {
        attestation_sequences_for(delivery_slots);
    }
}

/// Same-slot delivery pairs make the queue drain multiple attestations at one tick, mixing
/// immediate and queued application orders.
fn attestation_sequences_for(delivery_slots: [u64; 3]) {
    let block_slots: [u64; 4] = [0, 1, 1, 2];

    let mut event_options: Vec<Vec<Delivery>> = vec![];
    for delivery_slot in delivery_slots {
        let mut options = vec![];
        for validator in [0u64, 1] {
            for block in 0..4usize {
                let mut slots = vec![block_slots[block].max(1), delivery_slot - 1, delivery_slot];
                slots.dedup();
                for attestation_slot in slots {
                    if attestation_slot < block_slots[block] || attestation_slot > delivery_slot {
                        continue;
                    }
                    for payload_present in [false, true] {
                        // Spec `validate_on_attestation`: index == 1 requires the payload to be
                        // known (blocks 1 and 3 are revealed below) and a later-slot attestation.
                        let revealed_block = block == 1 || block == 3;
                        if payload_present
                            && !(revealed_block && attestation_slot > block_slots[block])
                        {
                            continue;
                        }
                        options.push(Delivery {
                            delivery_slot,
                            validator,
                            block,
                            attestation_slot,
                            payload_present,
                        });
                    }
                }
            }
        }
        event_options.push(options);
    }

    let mut count = 0u64;
    for first in &event_options[0] {
        for second in &event_options[1] {
            for third in &event_options[2] {
                let deliveries = [*first, *second, *third];
                // Same-validator attestations at the same slot are slashable equivocations;
                // production only reaches them via on_attester_slashing.
                let mut seen = BTreeSet::new();
                if !deliveries
                    .iter()
                    .all(|d| seen.insert((d.validator, d.attestation_slot)))
                {
                    continue;
                }

                let mut chain = Chain::new();
                let b1 = chain.add_block(chain.anchor_root, 1, 1);
                let b2 = chain.add_block(chain.anchor_root, 1, 2);
                let b3 = chain.add_block(b1, 2, 3);
                let roots = [chain.anchor_root, b1, b2, b3];
                chain.reveal(b1);
                chain.reveal(b3);

                for (i, delivery) in deliveries.iter().enumerate() {
                    chain.attest_with_payload(
                        delivery.delivery_slot,
                        delivery.validator,
                        roots[delivery.block],
                        delivery.attestation_slot,
                        delivery.payload_present,
                    );
                    let query = delivery.delivery_slot;
                    // `on_block` set the proposer boost for b3 (timely, imported at its own
                    // slot 2); `on_tick` clears it at each new slot.
                    let boost = if query == 2 { b3 } else { Hash256::zero() };
                    let expected = chain.reference_head_full(
                        query,
                        &expected_messages(&deliveries[..=i], &roots, query),
                        &[b1, b3],
                        boost,
                    );
                    assert_eq!(
                        chain.head(query),
                        expected,
                        "diverged after delivery {delivery:?} in {deliveries:?}"
                    );
                }

                let expected = chain.reference_head_with_reveals(
                    5,
                    &expected_messages(&deliveries, &roots, 5),
                    &[b1, b3],
                );
                assert_eq!(
                    chain.head(5),
                    expected,
                    "diverged at final slot: {deliveries:?}"
                );
                count += 1;
            }
        }
    }
    println!("checked {count} sequences");
}

/// Differential for `get_proposer_head` (option (a): compare only the chosen block). The head
/// is always imported late (Lighthouse checks head lateness in `beacon_chain`, not fork
/// choice) and proposals stay mid-epoch for the same reason.
#[test]
fn proposer_head_differential() {
    let mut divergences = vec![];
    for head_votes in [0u64, 1] {
        for parent_votes in [3u64, 4] {
            for equivocation in [false, true] {
                for proposal_slot in [3u64, 4] {
                    let mut chain = Chain::new();
                    let p = chain.add_block_opts(chain.anchor_root, 1, 1, 1, false);
                    let h = chain.add_block_opts(p, 2, 2, 2, true);
                    if equivocation {
                        chain.add_block_opts(p, 2, 3, 2, true);
                    }

                    let mut messages = vec![];
                    let mut validator = 0u64;
                    for _ in 0..head_votes {
                        chain.attest(3, validator, h, 2);
                        messages.push((validator, h, 2, false));
                        validator += 1;
                    }
                    for _ in 0..parent_votes {
                        chain.attest(3, validator, p, 2);
                        messages.push((validator, p, 2, false));
                        validator += 1;
                    }

                    let got = chain.proposer_head(proposal_slot);
                    let expected = chain.reference_proposer_head(proposal_slot, &messages);
                    // KNOWN MISSING FEATURE: the spec's proposer-equivocation fast path
                    // (`get_proposer_head`'s second branch: head_weak && current_time_ok &&
                    // proposer_equivocation reorgs regardless of parent strength) is not
                    // implemented anywhere in Lighthouse. Remove this exclusion when it is.
                    let missing_fast_path =
                        equivocation && head_votes == 0 && parent_votes == 3 && proposal_slot == 3;
                    if missing_fast_path {
                        continue;
                    }
                    if got != expected {
                        divergences.push((
                            head_votes,
                            parent_votes,
                            equivocation,
                            proposal_slot,
                            got == h,
                            expected == h,
                        ));
                    }
                }
            }
        }
    }
    assert!(
        divergences.is_empty(),
        "(head_votes, parent_votes, equivocation, proposal_slot, lighthouse_kept_head, \
         spec_kept_head): {divergences:#?}"
    );
}

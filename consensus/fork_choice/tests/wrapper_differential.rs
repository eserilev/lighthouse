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
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::json;

use bls::{AggregateSignature, Signature};
use fixed_bytes::FixedBytesExtended;
use fork_choice::{AttestationFromBlock, ForkChoice, ForkChoiceStore, PayloadVerificationStatus};
use fork_choice_reference as reference;
use proto_array::{JustifiedBalances, ReOrgThreshold};
use state_processing::common::update_progressive_balances_cache::initialize_progressive_balances_cache;
use types::consts::altair::TIMELY_TARGET_FLAG_INDEX;
use types::{
    AbstractExecPayload, AttestationData, BeaconBlock, BeaconBlockRef, BeaconState,
    BeaconStateError, ChainSpec, Checkpoint, Epoch, EthSpec, ForkName, Hash256, IndexedAttestation,
    IndexedAttestationElectra, MinimalEthSpec, ProgressiveBalancesCache, SignedBeaconBlock, Slot,
};

type E = MinimalEthSpec;

const GWEI: u64 = 1_000_000_000;
const BALANCES: [u64; 16] = [32 * GWEI; 16];

/// When active, every `Chain` records its handler calls and head queries so scenarios can be
/// exported for pyspec handler replay (see `export_handler_sequences`).
static EXPORT_ACTIVE: AtomicBool = AtomicBool::new(false);
static EXPORTER: Mutex<Option<Exporter>> = Mutex::new(None);

struct Exporter {
    out: Vec<serde_json::Value>,
    stride: u64,
    counter: u64,
}

fn set_export_stride(stride: u64) {
    let mut guard = EXPORTER.lock().unwrap();
    let exporter = guard.get_or_insert(Exporter {
        out: vec![],
        stride,
        counter: 0,
    });
    exporter.stride = stride;
    exporter.counter = 0;
    EXPORT_ACTIVE.store(true, Ordering::Relaxed);
}

fn export_scenario(chain: &Chain, family: &str) {
    if !EXPORT_ACTIVE.load(Ordering::Relaxed) {
        return;
    }
    let mut guard = EXPORTER.lock().unwrap();
    if let Some(exporter) = guard.as_mut() {
        exporter.counter += 1;
        if exporter.counter % exporter.stride != 0 {
            return;
        }
        exporter.out.push(json!({
            "family": family,
            "anchor_root": format!("{:?}", chain.anchor_root),
            "balances": chain.balances,
            "events": chain.events,
        }));
    }
}

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
    fn new(anchor: Checkpoint, anchor_state_root: Hash256, balances: Vec<u64>) -> Self {
        Self {
            current_slot: Slot::new(0),
            justified_checkpoint: anchor,
            justified_state_root: anchor_state_root,
            justified_balances: JustifiedBalances::from_effective_balances(balances).unwrap(),
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
    /// Per block: (realized justified, unrealized justified) as the spec derives them from the
    /// block's post-state — the expected side of what `on_block` computes.
    block_checkpoints: BTreeMap<Hash256, (Checkpoint, Checkpoint)>,
    balances: Vec<u64>,
    events: Vec<serde_json::Value>,
    spec: ChainSpec,
}

impl Chain {
    fn new() -> Self {
        Self::new_with_balances(BALANCES.to_vec())
    }

    /// The fork choice store's justified balances are independent of the genesis state's
    /// validators; tests can use crafted balance vectors.
    fn new_with_balances(balances: Vec<u64>) -> Self {
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

        let store = TestingStore::new(anchor, signed.state_root(), balances.clone());
        let fork_choice =
            ForkChoice::from_anchor(store, anchor_root, &signed, &state, None, &spec).unwrap();
        let mut blocks = BTreeMap::new();
        blocks.insert(anchor_root, (signed, state));
        let mut attestation_timely = BTreeMap::new();
        attestation_timely.insert(anchor_root, true);
        let mut block_checkpoints = BTreeMap::new();
        block_checkpoints.insert(anchor_root, (anchor, anchor));
        Self {
            fork_choice,
            anchor_root,
            blocks,
            attestation_timely,
            block_checkpoints,
            balances,
            events: vec![],
            spec,
        }
    }

    fn record(&mut self, event: serde_json::Value) {
        if EXPORT_ACTIVE.load(Ordering::Relaxed) {
            self.events.push(event);
        }
    }

    fn anchor_checkpoint(&self) -> Checkpoint {
        Checkpoint {
            epoch: Epoch::new(0),
            root: self.anchor_root,
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
        self.add_block_full(parent_root, slot, slot, seed, proposer, late, false)
    }

    /// `import_slot >= slot` delivers the block while the wall clock is past its slot —
    /// `import_slot` in a later epoch reaches `on_block`'s checkpoint pull-up branch.
    /// `justify_previous_epoch` crafts full previous-epoch target participation in the post-state
    /// so the block carries unrealized justification of its previous epoch.
    fn add_block_full(
        &mut self,
        parent_root: Hash256,
        slot: u64,
        import_slot: u64,
        seed: u8,
        proposer: u64,
        late: bool,
        justify_previous_epoch: bool,
    ) -> Hash256 {
        let slot = Slot::new(slot);
        let (_, parent_state) = &self.blocks[&parent_root];
        let mut state = parent_state.clone();
        state.build_slashings_cache().unwrap();
        state_processing::state_advance::complete_state_advance(&mut state, None, slot, &self.spec)
            .unwrap();

        let mut block = BeaconBlock::empty(&self.spec);
        *block.slot_mut() = slot;
        *block.parent_root_mut() = parent_root;
        *block.proposer_index_mut() = proposer;
        set_bid_hashes(&mut block, bid_hash(seed), Hash256::repeat_byte(0xdd));

        // Mirror `process_block_header` + the deferred state-root patch: install the block's
        // header (state root zeroed) and give the block the resulting post-state root, so
        // descendants advancing this state record the block's real root in `block_roots`.
        *state.latest_block_header_mut() = block.temporary_block_header();
        if justify_previous_epoch {
            let previous_epoch = state.previous_epoch();
            let current_epoch = state.current_epoch();
            let participation = state
                .get_epoch_participation_mut(previous_epoch, previous_epoch, current_epoch)
                .unwrap();
            for i in 0..participation.len() {
                participation
                    .get_mut(i)
                    .unwrap()
                    .add_flag(TIMELY_TARGET_FLAG_INDEX)
                    .unwrap();
            }
            *state.progressive_balances_cache_mut() = ProgressiveBalancesCache::default();
            initialize_progressive_balances_cache(&mut state, &self.spec).unwrap();
            state.build_total_active_balance_cache(&self.spec).unwrap();
        }
        *block.state_root_mut() = state.canonical_root().unwrap();
        let signed = SignedBeaconBlock::from_block(block, Signature::empty());
        let root = signed.canonical_root();

        let realized = state.current_justified_checkpoint();
        let unrealized = if justify_previous_epoch {
            let previous_epoch = state.previous_epoch();
            Checkpoint {
                epoch: previous_epoch,
                root: *state
                    .get_block_root(previous_epoch.start_slot(E::slots_per_epoch()))
                    .unwrap(),
            }
        } else {
            realized
        };

        let delay = if late {
            std::time::Duration::from_secs(4)
        } else {
            std::time::Duration::ZERO
        };
        self.fork_choice
            .on_block(
                Slot::new(import_slot),
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
        self.block_checkpoints.insert(root, (realized, unrealized));
        self.record(json!({
            "type": "block",
            "root": format!("{root:?}"),
            "slot": slot.as_u64(),
            "import_slot": import_slot,
            "parent_root": format!("{parent_root:?}"),
            "proposer": proposer,
            "late": late,
            "justify_previous_epoch": justify_previous_epoch,
            "bid_block_hash": format!("{:?}", bid_hash(seed)),
            "bid_parent_block_hash": format!("{:?}", Hash256::repeat_byte(0xdd)),
        }));
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
        let target_epoch = Slot::new(slot).epoch(E::slots_per_epoch());
        let target_root = self.target_root(block_root, Slot::new(slot));
        self.record(json!({
            "type": "attestation",
            "delivery_slot": current_slot,
            "validator": validator,
            "beacon_block_root": format!("{block_root:?}"),
            "slot": slot,
            "index": payload_present as u64,
            "target_epoch": target_epoch.as_u64(),
            "target_root": format!("{target_root:?}"),
        }));
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
        self.record(json!({
            "type": "reveal",
            "root": format!("{block_root:?}"),
        }));
        self.fork_choice
            .on_valid_payload_envelope_received(block_root)
            .unwrap();
    }

    fn slash(&mut self, validator: u64) {
        let data = |root: Hash256| AttestationData {
            slot: Slot::new(1),
            index: 0,
            beacon_block_root: root,
            source: Checkpoint {
                epoch: Epoch::new(0),
                root: self.anchor_root,
            },
            target: Checkpoint {
                epoch: Epoch::new(0),
                root: self.anchor_root,
            },
        };
        let attestation = |root: Hash256| types::IndexedAttestationElectra::<E> {
            attesting_indices: vec![validator].try_into().unwrap(),
            data: data(root),
            signature: AggregateSignature::empty(),
        };
        let slashing = types::AttesterSlashingElectra {
            attestation_1: attestation(Hash256::repeat_byte(0xe1)),
            attestation_2: attestation(Hash256::repeat_byte(0xe2)),
        };
        self.record(json!({
            "type": "attester_slashing",
            "validator": validator,
        }));
        self.fork_choice
            .on_attester_slashing(types::AttesterSlashingRef::Electra(&slashing));
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
        let head = self
            .fork_choice
            .get_head(Slot::new(current_slot), &self.spec)
            .unwrap()
            .0;
        let justified = *self.fork_choice.fc_store().justified_checkpoint();
        self.record(json!({
            "type": "query",
            "slot": current_slot,
            "head_root": format!("{head:?}"),
            "justified_epoch": justified.epoch.as_u64(),
            "justified_root": format!("{:?}", justified.root),
        }));
        head
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

    fn reference_head_with_justified(
        &self,
        current_slot: u64,
        latest_messages: &[(u64, Hash256, u64, bool)],
        proposer_boost_root: Hash256,
        justified: Checkpoint,
    ) -> Hash256 {
        let store = self.reference_store_with_equivocating(
            current_slot,
            latest_messages,
            &[],
            proposer_boost_root,
            &[],
            justified,
        );
        reference::get_head(&store).root
    }

    fn reference_store(
        &self,
        current_slot: u64,
        latest_messages: &[(u64, Hash256, u64, bool)],
        revealed: &[Hash256],
        proposer_boost_root: Hash256,
    ) -> reference::Store {
        self.reference_store_with_equivocating(
            current_slot,
            latest_messages,
            revealed,
            proposer_boost_root,
            &[],
            self.anchor_checkpoint(),
        )
    }

    fn reference_store_with_equivocating(
        &self,
        current_slot: u64,
        latest_messages: &[(u64, Hash256, u64, bool)],
        revealed: &[Hash256],
        proposer_boost_root: Hash256,
        equivocating: &[u64],
        justified: Checkpoint,
    ) -> reference::Store {
        let anchor = self.anchor_checkpoint();
        let blocks = self
            .blocks
            .iter()
            .map(|(root, (block, _))| {
                let (justified_checkpoint, unrealized_justified_checkpoint) =
                    self.block_checkpoints[root];
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
                        justified_checkpoint,
                        unrealized_justified_checkpoint,
                    },
                )
            })
            .collect();
        reference::Store {
            current_slot: Slot::new(current_slot),
            justified_checkpoint: justified,
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
            equivocating_indices: equivocating.iter().copied().collect(),
            balances: self.balances.clone(),
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
                export_scenario(&chain, "attestation");
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

#[derive(Debug, Clone, Copy)]
enum SlashingEvent {
    Attest {
        validator: u64,
        block: usize,
        attestation_slot: u64,
    },
    Slash {
        validator: u64,
    },
}

/// Sequences mixing attestations and attester slashings through the real handlers. Covers
/// slash-after-vote (weight must come off), slash-before-vote (vote must not count), vote-again
/// -after-slash (must stay ignored), and double slashing (idempotent). Blocks are imported late
/// so the proposer boost never engages (`is_head_weak`'s equivocation term is a known
/// three-way deviation and only runs under boost).
#[test]
fn attester_slashing_sequence_differential() {
    let block_slots: [u64; 4] = [0, 1, 1, 2];
    let delivery_slots: [u64; 3] = [2, 3, 4];

    let mut event_options: Vec<Vec<SlashingEvent>> = vec![];
    for delivery_slot in delivery_slots {
        let mut options = vec![];
        for validator in [0u64, 1] {
            options.push(SlashingEvent::Slash { validator });
            for block in 0..4usize {
                for attestation_slot in [block_slots[block].max(1), delivery_slot - 1] {
                    if attestation_slot < block_slots[block] || attestation_slot > delivery_slot {
                        continue;
                    }
                    options.push(SlashingEvent::Attest {
                        validator,
                        block,
                        attestation_slot,
                    });
                }
            }
        }
        event_options.push(options.into_iter().collect());
    }

    let mut count = 0u64;
    for first in &event_options[0] {
        for second in &event_options[1] {
            for third in &event_options[2] {
                let events = [*first, *second, *third];
                // Keep one attestation per (validator, slot): same-slot double votes only
                // arise through the slashing path itself.
                let mut seen = BTreeSet::new();
                if !events.iter().all(|event| match event {
                    SlashingEvent::Attest {
                        validator,
                        attestation_slot,
                        ..
                    } => seen.insert((*validator, *attestation_slot)),
                    SlashingEvent::Slash { .. } => true,
                }) {
                    continue;
                }
                // Require at least one slashing, otherwise this space is already covered.
                if !events
                    .iter()
                    .any(|event| matches!(event, SlashingEvent::Slash { .. }))
                {
                    continue;
                }

                let mut chain = Chain::new();
                let b1 = chain.add_block_opts(chain.anchor_root, 1, 1, 1, true);
                let b2 = chain.add_block_opts(chain.anchor_root, 1, 2, 2, true);
                let b3 = chain.add_block_opts(b1, 2, 3, 3, true);
                let roots = [chain.anchor_root, b1, b2, b3];

                let mut deliveries: Vec<Delivery> = vec![];
                let mut slashed: Vec<u64> = vec![];
                for (event, delivery_slot) in events.iter().zip(delivery_slots) {
                    match event {
                        SlashingEvent::Attest {
                            validator,
                            block,
                            attestation_slot,
                        } => {
                            chain.attest(
                                delivery_slot,
                                *validator,
                                roots[*block],
                                *attestation_slot,
                            );
                            deliveries.push(Delivery {
                                delivery_slot,
                                validator: *validator,
                                block: *block,
                                attestation_slot: *attestation_slot,
                                payload_present: false,
                            });
                        }
                        SlashingEvent::Slash { validator } => {
                            chain.slash(*validator);
                            if !slashed.contains(validator) {
                                slashed.push(*validator);
                            }
                        }
                    }

                    let query = delivery_slot;
                    let store = chain.reference_store_with_equivocating(
                        query,
                        &expected_messages(&deliveries, &roots, query),
                        &[],
                        Hash256::zero(),
                        &slashed,
                        chain.anchor_checkpoint(),
                    );
                    let expected = reference::get_head(&store).root;
                    assert_eq!(
                        chain.head(query),
                        expected,
                        "diverged after {event:?} in {events:?}"
                    );
                }
                export_scenario(&chain, "slashing");
                count += 1;
            }
        }
    }
    println!("checked {count} slashing sequences");
}

/// Isolate the deliberate `is_parent_strong` deviation (spec issue #5305): the spec measures
/// the parent's whole-subtree weight (the head's own votes included), Lighthouse uses the
/// parent's pending attestation score. Balances are crafted so the head's small vote is the
/// margin that crosses the 160% parent threshold.
#[test]
fn proposer_head_parent_strength_subtree_votes() {
    let balances: Vec<u64> = [3, 32, 32, 32, 32, 29, 2, 2]
        .into_iter()
        .map(|eth: u64| eth * GWEI)
        .collect();
    let mut chain = Chain::new_with_balances(balances);
    let p = chain.add_block_opts(chain.anchor_root, 1, 1, 1, true);
    let h = chain.add_block_opts(p, 2, 2, 2, true);

    // Validator 0 (3 ETH) votes the head: head weight 3 < 4.1 threshold (weak), and per the
    // spec this vote also counts toward the parent. Validator 1 (32 ETH) votes the parent:
    // spec parent weight 35 > 32.8 threshold (strong, reorg); Lighthouse counts 32 (refuse).
    chain.attest(3, 0, h, 2);
    chain.attest(3, 1, p, 2);

    let got = chain.proposer_head(3);
    let messages = [(0, h, 2, false), (1, p, 2, false)];
    let expected = chain.reference_proposer_head(3, &messages);

    assert_eq!(
        expected, p,
        "spec re-orgs: subtree weight crosses the parent threshold"
    );
    // Lighthouse's `attestation_score(Pending)` on the parent turns out to include the weight
    // routed up from the head's vote, so its parent-strength metric agrees with the spec here
    // despite the differently-worded implementation (spec issue #5305 concerns a weight split
    // this scenario cannot produce). Pinning agreement.
    assert_eq!(got, expected, "both re-org to the parent");
    let _ = h;
}

/// A block at slot 17 (epoch 2) carrying unrealized justification of epoch 1 (crafted
/// previous-epoch participation), delivered at import slots inside its own epoch and after it.
/// The store's justified checkpoint must move at the spec's moment — the next epoch-boundary
/// tick (`on_tick` pull-up) for same-epoch imports, immediately at import (`on_block`'s
/// `block_epoch < current_epoch` pull-up branch) for past-epoch imports — and the head must flip
/// from the vote-heavy fork off the anchor to the justified branch exactly then, because the
/// justified root moves to b1 and the filtered tree drops everything else.
#[test]
fn justification_pull_up_timing_differential() {
    for justify in [false, true] {
        for import_slot in [17u64, 20, 23, 24, 25, 31] {
            let mut chain = Chain::new();
            let b1 = chain.add_block(chain.anchor_root, 8, 1);
            let c1 = chain.add_block(chain.anchor_root, 9, 3);
            chain.attest(10, 0, c1, 9);
            chain.attest(10, 1, c1, 9);
            let b2 = chain.add_block_full(b1, 17, import_slot, 2, 2, false, justify);

            let msgs = [(0, c1, 9, false), (1, c1, 9, false)];
            let anchor = chain.anchor_checkpoint();
            let justified_from = import_slot.max(24);

            for query in import_slot..=33 {
                let head = chain.head(query);
                let expected_justified = if justify && query >= justified_from {
                    Checkpoint {
                        epoch: Epoch::new(1),
                        root: b1,
                    }
                } else {
                    anchor
                };
                let ctx = format!("justify {justify} import {import_slot} query {query}");
                assert_eq!(
                    *chain.fork_choice.fc_store().justified_checkpoint(),
                    expected_justified,
                    "store justified checkpoint, {ctx}"
                );
                // A timely own-slot import earns the boost (both branches share the anchor as
                // the epoch-2 dependent root); the next tick clears it. Past-slot imports never
                // earn it.
                let expected_boost = if import_slot == 17 && query == 17 {
                    b2
                } else {
                    Hash256::zero()
                };
                assert_eq!(
                    chain.fork_choice.fc_store().proposer_boost_root(),
                    expected_boost,
                    "derived proposer boost, {ctx}"
                );
                let expected_head = if expected_justified.epoch == Epoch::new(1) {
                    b2
                } else {
                    c1
                };
                assert_eq!(head, expected_head, "concrete head, {ctx}");
                assert_eq!(
                    head,
                    chain.reference_head_with_justified(
                        query,
                        &msgs,
                        expected_boost,
                        expected_justified
                    ),
                    "reference head, {ctx}"
                );
            }
            export_scenario(&chain, "pull_up");
        }
    }
}

/// Export the wrapper families' handler-call sequences (strided) as versioned JSONL for pyspec
/// replay: `certify_handlers.py` re-runs each sequence through the real pyspec handlers and
/// store-mutation helpers and checks the head and justified checkpoint at every query. This
/// closes the trust gap on the hand-written expected sides above (`expected_messages`, the
/// derived-boost model, pull-up timing).
#[test]
#[ignore = "exporter for `make certify-fork-choice-handlers`"]
fn export_handler_sequences() {
    let out_path = std::env::var("CERTIFY_OUT").unwrap_or_else(|_| "handler_sequences.jsonl".into());
    let spec = spec();

    set_export_stride(43);
    for delivery_slots in [[2u64, 3, 4], [2, 2, 3], [3, 3, 3]] {
        attestation_sequences_for(delivery_slots);
    }
    set_export_stride(8);
    attester_slashing_sequence_differential();
    set_export_stride(1);
    justification_pull_up_timing_differential();
    EXPORT_ACTIVE.store(false, Ordering::Relaxed);

    let scenarios = EXPORTER.lock().unwrap().take().unwrap().out;
    let mut lines = vec![json!({
        "format_version": 1u32,
        "kind": "handler_sequences",
        "spec_version": "v1.7.0-alpha.11",
        "ptc_size": E::ptc_size(),
        "slots_per_epoch": E::slots_per_epoch(),
        "proposer_score_boost": spec.proposer_score_boost,
        "scenario_count": scenarios.len(),
    })];
    lines.extend(scenarios);
    let mut file = std::fs::File::create(&out_path).unwrap();
    for line in &lines {
        use std::io::Write;
        writeln!(file, "{line}").unwrap();
    }
    println!("exported {} handler sequences to {out_path}", lines.len() - 1);
}

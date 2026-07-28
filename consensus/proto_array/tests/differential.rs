//! Differential test: exhaustively compare `ProtoArrayForkChoice::find_head` against the naive
//! spec transcription in `fork_choice_reference` over small gloas scenarios.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use fixed_bytes::FixedBytesExtended;
use fork_choice_reference as reference;
use proto_array::{Block, ExecutionStatus, JustifiedBalances, ProtoArrayForkChoice};
use types::{
    AttestationShufflingId, ChainSpec, Checkpoint, Epoch, EthSpec, ExecutionBlockHash, ForkName,
    Hash256, MinimalEthSpec, Slot,
};

type E = MinimalEthSpec;

const ANCHOR_ROOT: Hash256 = Hash256::repeat_byte(0xa0);

#[derive(Debug, Clone, Copy, PartialEq)]
enum ParentStatus {
    Empty,
    Full,
}

#[derive(Debug, Clone, Copy)]
struct ScenarioBlock {
    parent: usize,
    slot_offset: u64,
    parent_status: ParentStatus,
    proposer: u64,
}

#[derive(Debug, Clone, Copy)]
struct Vote {
    block: usize,
    same_slot: bool,
    payload_present: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum PtcPattern {
    NoVotes,
    Timely,
    Late,
    AtThreshold,
    TimelyNotAvailable,
    Contested,
}

#[derive(Debug, Clone)]
struct Scenario {
    blocks: Vec<ScenarioBlock>,
    revealed: Vec<bool>,
    votes: Vec<Option<Vote>>,
    boost: Option<usize>,
    ptc: PtcPattern,
    extra_slot: bool,
    balances: Vec<u64>,
    /// Per-block `(justified, unrealized_justified)` checkpoints.
    checkpoints: Vec<(Checkpoint, Checkpoint)>,
    store_justified: Checkpoint,
    equivocating: Vec<u64>,
    /// Apply PTC votes to unrevealed blocks too (PTC saw the payload, this node did not).
    ptc_unrevealed: bool,
}

fn block_root(index: usize) -> Hash256 {
    if index == 0 {
        ANCHOR_ROOT
    } else {
        Hash256::repeat_byte(index as u8)
    }
}

fn bid_hash(index: usize) -> ExecutionBlockHash {
    ExecutionBlockHash::from_root(Hash256::repeat_byte(0xb0 + index as u8))
}

/// Absolute slot of each block: anchor at 0, others offset from their parent.
fn block_slots(blocks: &[ScenarioBlock]) -> Vec<Slot> {
    let mut slots = vec![Slot::new(0)];
    for block in &blocks[1..] {
        slots.push(slots[block.parent] + block.slot_offset);
    }
    slots
}

fn checkpoint() -> Checkpoint {
    Checkpoint {
        epoch: Epoch::new(0),
        root: ANCHOR_ROOT,
    }
}

fn ptc_votes(pattern: PtcPattern) -> Vec<(bool, bool)> {
    let threshold = E::ptc_size() / 2;
    match pattern {
        PtcPattern::NoVotes => vec![],
        PtcPattern::Timely => vec![(true, true); threshold + 1],
        PtcPattern::Late => vec![(false, false); threshold + 1],
        PtcPattern::AtThreshold => vec![(true, true); threshold],
        PtcPattern::TimelyNotAvailable => vec![(true, false); threshold + 1],
        PtcPattern::Contested => {
            let mut votes = vec![(true, true); threshold + 1];
            votes.extend(vec![(false, false); E::ptc_size() - threshold - 1]);
            votes
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Op {
    Block(usize),
    Reveal(usize),
    PtcVote {
        block: usize,
        ptc_index: usize,
        timely: bool,
        available: bool,
    },
    Attestation {
        validator: usize,
        vote: Vote,
    },
}

/// Ops in the canonical order: blocks, then reveals + PTC votes, then attestations.
fn scenario_ops(scenario: &Scenario) -> Vec<Op> {
    let mut ops = vec![];
    for i in 1..scenario.blocks.len() {
        ops.push(Op::Block(i));
    }
    for (i, revealed) in scenario.revealed.iter().enumerate() {
        if *revealed {
            ops.push(Op::Reveal(i));
        }
        // PTC members vote based on what they saw; local reveal status is independent.
        if *revealed || scenario.ptc_unrevealed {
            for (ptc_index, (timely, available)) in ptc_votes(scenario.ptc).iter().enumerate() {
                ops.push(Op::PtcVote {
                    block: i,
                    ptc_index,
                    timely: *timely,
                    available: *available,
                });
            }
        }
    }
    for (validator, vote) in scenario.votes.iter().enumerate() {
        if let Some(vote) = vote {
            ops.push(Op::Attestation {
                validator,
                vote: *vote,
            });
        }
    }
    ops
}

/// Reorderings that must not change the head. Blocks stay topologically ordered and reveals/PTC
/// votes stay after their block (production guarantees both); everything else is fair game.
fn op_orders(scenario: &Scenario) -> Vec<Vec<Op>> {
    let canonical = scenario_ops(scenario);

    // Attestations delivered before any block exists.
    let mut votes_first = canonical.clone();
    votes_first.sort_by_key(|op| !matches!(op, Op::Attestation { .. }));

    // Reveals and PTC votes for later blocks land before earlier ones.
    let mut reveals_reversed: Vec<Op> = canonical
        .iter()
        .filter(|op| matches!(op, Op::Block(_)))
        .copied()
        .collect();
    let mut reveal_groups: Vec<Vec<Op>> = vec![];
    for op in &canonical {
        match op {
            Op::Reveal(block) => match reveal_groups.last() {
                Some(group)
                    if group
                        .iter()
                        .any(|o| matches!(o, Op::PtcVote { block: b, .. } if b == block)) =>
                {
                    reveal_groups.last_mut().unwrap().push(*op);
                }
                _ => reveal_groups.push(vec![*op]),
            },
            Op::PtcVote { block, .. } => match reveal_groups.last_mut() {
                Some(group)
                    if group.iter().any(|o| {
                        matches!(o, Op::Reveal(b) if b == block)
                            || matches!(o, Op::PtcVote { block: b, .. } if b == block)
                    }) =>
                {
                    group.push(*op);
                }
                _ => reveal_groups.push(vec![*op]),
            },
            _ => {}
        }
    }
    reveals_reversed.extend(reveal_groups.into_iter().rev().flatten());
    reveals_reversed.extend(
        canonical
            .iter()
            .filter(|op| matches!(op, Op::Attestation { .. }))
            .copied(),
    );

    // PTC votes land before the reveal they concern (production allows either order).
    let mut ptc_first: Vec<Op> = canonical
        .iter()
        .filter(|op| matches!(op, Op::Block(_)))
        .copied()
        .collect();
    for op in &canonical {
        if let Op::Reveal(block) = op {
            ptc_first.extend(
                canonical
                    .iter()
                    .copied()
                    .filter(|other| matches!(other, Op::PtcVote { block: b, .. } if b == block)),
            );
            ptc_first.push(*op);
        }
    }
    ptc_first.extend(
        canonical
            .iter()
            .filter(|op| matches!(op, Op::Attestation { .. }))
            .copied(),
    );

    // Everything delivered twice: imports must be idempotent.
    let mut duplicated = canonical.clone();
    duplicated.extend(canonical.iter().copied());

    vec![
        canonical,
        votes_first,
        reveals_reversed,
        ptc_first,
        duplicated,
    ]
}

fn find_head_with_balances(
    fork_choice: &mut ProtoArrayForkChoice,
    scenario: &Scenario,
    spec: &ChainSpec,
    current_slot: Slot,
    balances: Vec<u64>,
) -> (Hash256, u8) {
    let balances = JustifiedBalances::from_effective_balances(balances).unwrap();
    let equivocating = scenario
        .equivocating
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let (root, payload_status) = fork_choice
        .find_head::<E>(
            scenario.store_justified,
            checkpoint(),
            &balances,
            scenario.boost.map_or(Hash256::zero(), block_root),
            &equivocating,
            current_slot,
            spec,
        )
        .unwrap();
    (root, payload_status as u8)
}

fn run_proto_array(
    scenario: &Scenario,
    ops: &[Op],
    spec: &ChainSpec,
    current_slot: Slot,
) -> ((Hash256, u8), ProtoArrayForkChoice) {
    let junk_shuffling_id = AttestationShufflingId::from_components(Epoch::new(0), Hash256::zero());
    let slots = block_slots(&scenario.blocks);

    let mut fork_choice = ProtoArrayForkChoice::new::<E>(
        current_slot,
        Slot::new(0),
        Hash256::zero(),
        checkpoint(),
        checkpoint(),
        junk_shuffling_id.clone(),
        junk_shuffling_id.clone(),
        ExecutionStatus::irrelevant(),
        Some(ExecutionBlockHash::zero()),
        Some(bid_hash(0)),
        0,
        spec,
    )
    .unwrap();

    for op in ops {
        match op {
            Op::Block(i) => {
                let block = &scenario.blocks[*i];
                let payload_parent_hash = match block.parent_status {
                    ParentStatus::Full => bid_hash(block.parent),
                    ParentStatus::Empty => ExecutionBlockHash::zero(),
                };
                fork_choice
                    .process_block::<E>(
                        Block {
                            slot: slots[*i],
                            root: block_root(*i),
                            parent_root: Some(block_root(block.parent)),
                            state_root: block_root(*i),
                            target_root: ANCHOR_ROOT,
                            current_epoch_shuffling_id: junk_shuffling_id.clone(),
                            next_epoch_shuffling_id: junk_shuffling_id.clone(),
                            justified_checkpoint: scenario.checkpoints[*i].0,
                            finalized_checkpoint: checkpoint(),
                            execution_status: ExecutionStatus::irrelevant(),
                            unrealized_justified_checkpoint: Some(scenario.checkpoints[*i].1),
                            unrealized_finalized_checkpoint: Some(checkpoint()),
                            execution_payload_parent_hash: Some(payload_parent_hash),
                            execution_payload_block_hash: Some(bid_hash(*i)),
                            proposer_index: Some(block.proposer),
                            payload_received: false,
                        },
                        // Import at the block's own slot: production processes timely blocks
                        // when they arrive, and the reference models them all as PTC-timely.
                        slots[*i],
                        spec,
                        Duration::ZERO,
                    )
                    .unwrap();
            }
            Op::Reveal(i) => {
                fork_choice
                    .on_valid_payload_envelope_received(block_root(*i))
                    .unwrap();
            }
            Op::PtcVote {
                block,
                ptc_index,
                timely,
                available,
            } => {
                fork_choice
                    .process_payload_attestation(
                        block_root(*block),
                        *ptc_index,
                        *timely,
                        *available,
                    )
                    .unwrap();
            }
            Op::Attestation { validator, vote } => {
                let attestation_slot = if vote.same_slot {
                    slots[vote.block]
                } else {
                    slots[vote.block] + 1
                };
                fork_choice
                    .process_attestation(
                        *validator,
                        block_root(vote.block),
                        attestation_slot,
                        vote.payload_present,
                    )
                    .unwrap();
            }
        }
    }

    let result = find_head_of(&mut fork_choice, scenario, spec, current_slot);
    (result, fork_choice)
}

fn find_head_of(
    fork_choice: &mut ProtoArrayForkChoice,
    scenario: &Scenario,
    spec: &ChainSpec,
    current_slot: Slot,
) -> (Hash256, u8) {
    find_head_with_balances(
        fork_choice,
        scenario,
        spec,
        current_slot,
        scenario.balances.clone(),
    )
}

fn run_reference(scenario: &Scenario, spec: &ChainSpec, current_slot: Slot) -> (Hash256, u8) {
    let slots = block_slots(&scenario.blocks);

    let mut blocks = BTreeMap::new();
    for (i, block) in scenario.blocks.iter().enumerate() {
        blocks.insert(
            block_root(i),
            reference::Block {
                slot: slots[i],
                parent_root: if i == 0 {
                    Hash256::zero()
                } else {
                    block_root(block.parent)
                },
                parent_payload_status: match block.parent_status {
                    ParentStatus::Empty => reference::PayloadStatus::Empty,
                    ParentStatus::Full => reference::PayloadStatus::Full,
                },
                proposer_index: block.proposer,
                ptc_timely: true,
                attestation_timely: true,
                justified_checkpoint: scenario.checkpoints[i].0,
                unrealized_justified_checkpoint: scenario.checkpoints[i].1,
            },
        );
    }

    let mut payload_revealed = BTreeSet::new();
    let mut timeliness_votes = BTreeMap::new();
    let mut availability_votes = BTreeMap::new();
    for (i, revealed) in scenario.revealed.iter().enumerate() {
        if *revealed {
            payload_revealed.insert(block_root(i));
        }
        if *revealed || scenario.ptc_unrevealed {
            let votes = ptc_votes(scenario.ptc);
            let mut timeliness = vec![None; E::ptc_size()];
            let mut availability = vec![None; E::ptc_size()];
            for (ptc_index, (timely, available)) in votes.iter().enumerate() {
                timeliness[ptc_index] = Some(*timely);
                availability[ptc_index] = Some(*available);
            }
            timeliness_votes.insert(block_root(i), timeliness);
            availability_votes.insert(block_root(i), availability);
        }
    }

    let mut latest_messages = BTreeMap::new();
    for (validator, vote) in scenario.votes.iter().enumerate() {
        if let Some(vote) = vote {
            let attestation_slot = if vote.same_slot {
                slots[vote.block]
            } else {
                slots[vote.block] + 1
            };
            latest_messages.insert(
                validator as u64,
                reference::LatestMessage {
                    slot: attestation_slot,
                    root: block_root(vote.block),
                    payload_present: vote.payload_present,
                },
            );
        }
    }

    let store = reference::Store {
        current_slot,
        justified_checkpoint: scenario.store_justified,
        finalized_checkpoint: checkpoint(),
        blocks,
        payload_revealed,
        payload_timeliness_votes: timeliness_votes,
        payload_data_availability_votes: availability_votes,
        latest_messages,
        equivocating_indices: scenario.equivocating.iter().copied().collect(),
        balances: scenario.balances.clone(),
        proposer_boost_root: scenario.boost.map_or(Hash256::zero(), block_root),
        ptc_size: E::ptc_size(),
        slots_per_epoch: E::slots_per_epoch(),
        proposer_score_boost: spec.proposer_score_boost,
        reorg_head_weight_threshold: spec.reorg_head_weight_threshold,
        effective_balance_increment: spec.effective_balance_increment,
        reorg_parent_weight_threshold: spec.reorg_parent_weight_threshold,
        reorg_max_epochs_since_finalization: spec.reorg_max_epochs_since_finalization,
        time_into_slot_ms: 0,
        proposer_reorg_cutoff_ms: 1_000,
    };

    let head = reference::get_head(&store);
    (head.root, head.payload_status as u8)
}

/// KNOWN DEVIATION: with proposer boost engaged, `is_head_weak` counts equivocator weight
/// three different ways — the spec uses head-slot committee membership, Lighthouse uses
/// `equivocating_attestation_score` (only equivocators who voted for the node; see the TODO on
/// `ProtoArray::is_head_weak`), and the reference counts all equivocators. Remove this
/// exclusion if the committee-based computation is implemented.
fn is_known_equivocation_deviation(scenario: &Scenario) -> bool {
    !scenario.equivocating.is_empty() && scenario.boost.is_some()
}

fn check(scenario: &Scenario, spec: &ChainSpec, current_slot: Slot) {
    if is_known_equivocation_deviation(scenario) {
        return;
    }

    let reference = run_reference(scenario, spec, current_slot);
    for (order, ops) in op_orders(scenario).iter().enumerate() {
        let (proto, fork_choice) = run_proto_array(scenario, ops, spec, current_slot);
        assert_eq!(
            proto, reference,
            "proto_array (left, op order {order}) disagrees with spec reference (right) for \
             {scenario:#?} at current_slot {current_slot}"
        );

        // Balance transition: a prior find_head with different balances must not change the
        // final answer (exercises compute_deltas' old-vs-new balance path).
        if order == 0 {
            let (_, mut transitioned) =
                run_proto_array(scenario, &op_orders(scenario)[0], spec, current_slot);
            let mut perturbed = scenario.balances.clone();
            perturbed.reverse();
            perturbed[0] = perturbed[0].saturating_add(32 * GWEI);
            let _ =
                find_head_with_balances(&mut transitioned, scenario, spec, current_slot, perturbed);
            let zeroed = {
                let mut b = scenario.balances.clone();
                b[0] = 0;
                b
            };
            let _ =
                find_head_with_balances(&mut transitioned, scenario, spec, current_slot, zeroed);
            let warm = find_head_of(&mut transitioned, scenario, spec, current_slot);
            assert_eq!(
                warm, reference,
                "balance-transition run (left) disagrees with spec reference (right) for \
                 {scenario:#?} at current_slot {current_slot}"
            );
        }

        // Warm/cold: an instance rebuilt from its SSZ encoding must agree with the live one.
        if order == 0 {
            let balances =
                JustifiedBalances::from_effective_balances(scenario.balances.clone()).unwrap();
            let mut restored =
                ProtoArrayForkChoice::from_bytes(&fork_choice.as_bytes(), balances).unwrap();
            let cold = find_head_of(&mut restored, scenario, spec, current_slot);
            assert_eq!(
                cold, reference,
                "SSZ-rebuilt proto_array (left) disagrees with spec reference (right) for \
                 {scenario:#?} at current_slot {current_slot}"
            );
        }
    }
}

fn spec() -> ChainSpec {
    ForkName::Gloas.make_genesis_spec(E::default_spec())
}

fn vote_options(num_blocks: usize) -> Vec<Option<Vote>> {
    let mut options = vec![None];
    for block in 0..num_blocks {
        for payload_present in [false, true] {
            options.push(Some(Vote {
                block,
                same_slot: false,
                payload_present,
            }));
        }
        options.push(Some(Vote {
            block,
            same_slot: true,
            payload_present: false,
        }));
    }
    options
}

const GWEI: u64 = 1_000_000_000;

fn spec_increment() -> u64 {
    E::default_spec().effective_balance_increment
}

/// Enumerate every valid scenario in the bounded space and call `f` with it and its current
/// slot. Scenarios whose proposer boost root is not a current-slot block are skipped (spec
/// `on_block` only ever boosts current-slot blocks).
fn for_each_scenario(mut f: impl FnMut(&Scenario, Slot)) {
    // Attestations cannot come from the future: a message slot beyond the store's current slot
    // is production-unreachable, and implementations legitimately differ on it.
    let mut f = |scenario: &Scenario, current_slot: Slot| {
        let slots = block_slots(&scenario.blocks);
        let votes_valid = scenario.votes.iter().flatten().all(|vote| {
            let slot = if vote.same_slot {
                slots[vote.block]
            } else {
                slots[vote.block] + 1
            };
            slot <= current_slot
        });
        if votes_valid {
            f(scenario, current_slot);
        }
    };

    let mut shapes = vec![];
    for b1_offset in [1, 2] {
        for b1_status in [ParentStatus::Empty, ParentStatus::Full] {
            for b2_parent in [0, 1] {
                for b2_offset in [1, 2] {
                    for b2_status in [ParentStatus::Empty, ParentStatus::Full] {
                        shapes.push(vec![
                            ScenarioBlock {
                                parent: 0,
                                slot_offset: 0,
                                parent_status: ParentStatus::Empty,
                                proposer: 0,
                            },
                            ScenarioBlock {
                                parent: 0,
                                slot_offset: b1_offset,
                                parent_status: b1_status,
                                proposer: 1,
                            },
                            ScenarioBlock {
                                parent: b2_parent,
                                slot_offset: b2_offset,
                                parent_status: b2_status,
                                proposer: 2,
                            },
                        ]);
                    }
                }
            }
        }
    }

    let votes = vote_options(3);
    for blocks in &shapes {
        for revealed_mask in 0u32..8 {
            let revealed = (0..3)
                .map(|i| revealed_mask & (1 << i) != 0)
                .collect::<Vec<_>>();
            for vote_a in &votes {
                for vote_b in &votes {
                    for boost in [None, Some(0), Some(1), Some(2)] {
                        for ptc in [
                            PtcPattern::NoVotes,
                            PtcPattern::Timely,
                            PtcPattern::Late,
                            PtcPattern::AtThreshold,
                            PtcPattern::TimelyNotAvailable,
                            PtcPattern::Contested,
                        ] {
                            for extra_slot in [false, true] {
                                for balances in [
                                    vec![32 * GWEI, 32 * GWEI],
                                    vec![32 * GWEI, 64 * GWEI],
                                    vec![5, 3],
                                ] {
                                    for equivocating in [vec![], vec![0u64]] {
                                        let scenario = Scenario {
                                            blocks: blocks.clone(),
                                            revealed: revealed.clone(),
                                            votes: vec![*vote_a, *vote_b],
                                            boost,
                                            ptc,
                                            extra_slot,
                                            balances: balances.clone(),
                                            checkpoints: vec![(checkpoint(), checkpoint()); 3],
                                            store_justified: checkpoint(),
                                            equivocating,
                                            ptc_unrevealed: false,
                                        };

                                        let slots = block_slots(&scenario.blocks);
                                        let max_slot = slots.iter().copied().max().unwrap();
                                        let current_slot = if scenario.extra_slot {
                                            max_slot + 1
                                        } else {
                                            max_slot
                                        };
                                        if let Some(boost) = scenario.boost
                                            && slots[boost] != current_slot
                                        {
                                            continue;
                                        }
                                        // KNOWN DEVIATION: `calculate_committee_fraction` is
                                        // missing the spec's EFFECTIVE_BALANCE_INCREMENT floor
                                        // (pyspec-confirmed), so dust totals with proposer boost
                                        // engaged diverge. Remove this exclusion when fixed.
                                        let dust = scenario.balances.iter().sum::<u64>()
                                            < spec_increment();
                                        if dust && scenario.boost.is_some() {
                                            continue;
                                        }

                                        f(&scenario, current_slot);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    non_finality_scenarios(&mut f);
    void_scenarios(&mut f);
    proposer_equivocation_scenarios(&mut f);
    extend_payload_arm_scenarios(&mut f);
}

/// Proposer equivocation at slot 1 (B1 and B2: same slot, same proposer), a boost child B3 of
/// B1 at slot 2, and an optionally-equivocating attester. `should_apply_proposer_boost` then
/// depends on `is_head_weak(B1)`, whose equivocating-weight term is where Lighthouse
/// approximates the spec.
fn proposer_equivocation_scenarios(f: &mut impl FnMut(&Scenario, Slot)) {
    for b1_status in [ParentStatus::Empty, ParentStatus::Full] {
        for b2_status in [ParentStatus::Empty, ParentStatus::Full] {
            for b3_status in [ParentStatus::Empty, ParentStatus::Full] {
                let blocks = vec![
                    ScenarioBlock {
                        parent: 0,
                        slot_offset: 0,
                        parent_status: ParentStatus::Empty,
                        proposer: 0,
                    },
                    ScenarioBlock {
                        parent: 0,
                        slot_offset: 1,
                        parent_status: b1_status,
                        proposer: 1,
                    },
                    ScenarioBlock {
                        parent: 0,
                        slot_offset: 1,
                        parent_status: b2_status,
                        proposer: 1,
                    },
                    ScenarioBlock {
                        parent: 1,
                        slot_offset: 1,
                        parent_status: b3_status,
                        proposer: 3,
                    },
                ];
                for equivocating in [vec![], vec![0u64]] {
                    for vote_a in void_vote_options(4) {
                        for vote_b in void_vote_options(4) {
                            for boost in [None, Some(3)] {
                                for revealed_mask in [0u32, 0b1111] {
                                    let revealed = (0..4)
                                        .map(|i| revealed_mask & (1 << i) != 0)
                                        .collect::<Vec<_>>();
                                    let scenario = Scenario {
                                        blocks: blocks.clone(),
                                        revealed,
                                        votes: vec![vote_a, vote_b],
                                        boost,
                                        ptc: PtcPattern::NoVotes,
                                        extra_slot: false,
                                        balances: vec![32 * GWEI, 64 * GWEI],
                                        checkpoints: vec![(checkpoint(), checkpoint()); 4],
                                        store_justified: checkpoint(),
                                        equivocating: equivocating.clone(),
                                        ptc_unrevealed: false,
                                    };
                                    f(&scenario, Slot::new(2));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// `should_extend_payload` OR-arm isolation. B (block 1, slot 1) is revealed and
/// payload-pending at slot 2, with balances [5, 5, 90] ETH making the proposer boost
/// (total/20 = 5 ETH) exactly one vote's weight, so B's empty and full virtual nodes can tie
/// and the payload-status tiebreaker decides the head. The slot-2 block attaches to B's empty
/// node, B's full node, or a fork off the anchor, so across the enumeration each OR-arm
/// (PTC timely+available / no boost / boost not on B's child / boosted child already full) is
/// the sole deciding condition in some scenario.
fn extend_payload_arm_scenarios(f: &mut impl FnMut(&Scenario, Slot)) {
    let shapes = [
        (1, 1, ParentStatus::Empty),
        (1, 1, ParentStatus::Full),
        (0, 2, ParentStatus::Empty),
    ];
    for (parent, slot_offset, parent_status) in shapes {
        let blocks = vec![
            ScenarioBlock {
                parent: 0,
                slot_offset: 0,
                parent_status: ParentStatus::Empty,
                proposer: 0,
            },
            ScenarioBlock {
                parent: 0,
                slot_offset: 1,
                parent_status: ParentStatus::Empty,
                proposer: 1,
            },
            ScenarioBlock {
                parent,
                slot_offset,
                parent_status,
                proposer: 2,
            },
        ];
        for ptc in [
            PtcPattern::NoVotes,
            PtcPattern::Timely,
            PtcPattern::Late,
            PtcPattern::AtThreshold,
            PtcPattern::TimelyNotAvailable,
            PtcPattern::Contested,
        ] {
            for boost in [None, Some(2)] {
                for vote_a in [
                    None,
                    Some(Vote {
                        block: 1,
                        same_slot: false,
                        payload_present: false,
                    }),
                ] {
                    for vote_b in [
                        None,
                        Some(Vote {
                            block: 1,
                            same_slot: false,
                            payload_present: true,
                        }),
                    ] {
                        for balances in [
                            vec![5 * GWEI, 5 * GWEI, 90 * GWEI],
                            vec![32 * GWEI, 32 * GWEI, 32 * GWEI],
                        ] {
                            let scenario = Scenario {
                                blocks: blocks.clone(),
                                revealed: vec![false, true, false],
                                votes: vec![vote_a, vote_b],
                                boost,
                                ptc,
                                extra_slot: false,
                                balances,
                                checkpoints: vec![(checkpoint(), checkpoint()); 3],
                                store_justified: checkpoint(),
                                equivocating: vec![],
                                ptc_unrevealed: false,
                            };
                            f(&scenario, Slot::new(2));
                        }
                    }
                }
            }
        }
    }
}

fn cp(epoch: u64, root_index: usize) -> Checkpoint {
    Checkpoint {
        epoch: Epoch::new(epoch),
        root: block_root(root_index),
    }
}

/// Index of the ancestor of `block` at or before `slot`, walking parent pointers.
fn ancestor_at(blocks: &[ScenarioBlock], slots: &[Slot], mut index: usize, slot: Slot) -> usize {
    while slots[index] > slot {
        index = blocks[index].parent;
    }
    index
}

/// Multi-epoch chains under non-finality: finalization stays at the genesis anchor while
/// justification advances to epoch 1 on some branch, engaging the viability filter. Blocks:
/// anchor(slot 0), B1(slot 1), B2(slot 8, epoch 1), B3(slot 16, epoch 2).
fn non_finality_scenarios(f: &mut impl FnMut(&Scenario, Slot)) {
    let cp0 = checkpoint();

    for b2_parent in [0usize, 1] {
        for b3_parent in [0usize, 1, 2] {
            for b1_status in [ParentStatus::Empty, ParentStatus::Full] {
                for b2_status in [ParentStatus::Empty, ParentStatus::Full] {
                    for b3_status in [ParentStatus::Empty, ParentStatus::Full] {
                        for (b2_slot, b3_slot) in [(8u64, 16u64), (9, 16), (8, 17), (9, 17)] {
                            let b2_parent_slot = if b2_parent == 0 { 0 } else { 1 };
                            let b3_parent_slot = match b3_parent {
                                0 => 0,
                                1 => 1,
                                _ => b2_slot,
                            };
                            let blocks = vec![
                                ScenarioBlock {
                                    parent: 0,
                                    slot_offset: 0,
                                    parent_status: ParentStatus::Empty,
                                    proposer: 0,
                                },
                                ScenarioBlock {
                                    parent: 0,
                                    slot_offset: 1,
                                    parent_status: b1_status,
                                    proposer: 1,
                                },
                                ScenarioBlock {
                                    parent: b2_parent,
                                    slot_offset: b2_slot - b2_parent_slot,
                                    parent_status: b2_status,
                                    proposer: 2,
                                },
                                ScenarioBlock {
                                    parent: b3_parent,
                                    slot_offset: b3_slot - b3_parent_slot,
                                    parent_status: b3_status,
                                    proposer: 3,
                                },
                            ];
                            let slots = block_slots(&blocks);
                            let epoch1_boundary = Slot::new(8);
                            let b2_cp1 = cp(1, ancestor_at(&blocks, &slots, 2, epoch1_boundary));
                            let b3_cp1 = cp(1, ancestor_at(&blocks, &slots, 3, epoch1_boundary));

                            for b2_unrealized in [cp0, b2_cp1] {
                                for (b3_justified, b3_unrealized) in
                                    [(cp0, cp0), (cp0, b3_cp1), (b3_cp1, b3_cp1)]
                                {
                                    // Unrealized justification cannot regress along a chain.
                                    if b3_parent == 2 && b3_unrealized.epoch < b2_unrealized.epoch {
                                        continue;
                                    }

                                    let checkpoints = vec![
                                        (cp0, cp0),
                                        (cp0, cp0),
                                        (cp0, b2_unrealized),
                                        (b3_justified, b3_unrealized),
                                    ];

                                    let mut store_candidates = checkpoints
                                        .iter()
                                        .flat_map(|(j, u)| [*j, *u])
                                        .filter(|c| c.epoch == Epoch::new(1))
                                        .collect::<Vec<_>>();
                                    store_candidates.dedup();
                                    if store_candidates.is_empty() {
                                        store_candidates.push(cp0);
                                    }

                                    for store_justified in store_candidates {
                                        for current_slot in [16u64, 17, 23, 24, 25] {
                                            if current_slot < b3_slot {
                                                continue;
                                            }
                                            let current_slot = Slot::new(current_slot);
                                            for boost in [None, Some(3)] {
                                                if let Some(boost) = boost
                                                    && slots[boost] != current_slot
                                                {
                                                    continue;
                                                }
                                                for revealed_mask in [0u32, 0b1111, 0b1100, 0b1000]
                                                {
                                                    let revealed = (0..4)
                                                        .map(|i| revealed_mask & (1 << i) != 0)
                                                        .collect::<Vec<_>>();
                                                    for ptc in
                                                        [PtcPattern::NoVotes, PtcPattern::Timely]
                                                    {
                                                        for vote_a in deep_vote_options() {
                                                            for vote_b in deep_vote_options() {
                                                                let scenario = Scenario {
                                                                    blocks: blocks.clone(),
                                                                    revealed: revealed.clone(),
                                                                    votes: vec![vote_a, vote_b],
                                                                    boost,
                                                                    ptc,
                                                                    extra_slot: false,
                                                                    balances: vec![
                                                                        32 * GWEI,
                                                                        64 * GWEI,
                                                                    ],
                                                                    checkpoints: checkpoints
                                                                        .clone(),
                                                                    store_justified,
                                                                    equivocating: vec![],
                                                                    ptc_unrevealed: false,
                                                                };
                                                                f(&scenario, current_slot);
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Long stretches of empty slots under non-finality, then late arrivals: an anchor alone, a
/// single block appearing epochs later, a late block plus a child across another gap, and a
/// two-branch fork after a void. Justification can advance to `{1, anchor}` (the epoch-1
/// boundary ancestor of everything here is the anchor).
fn void_scenarios(f: &mut impl FnMut(&Scenario, Slot)) {
    let cp0 = checkpoint();
    let cp1 = cp(1, 0);

    let mut shapes: Vec<Vec<(usize, u64)>> = vec![vec![]];
    for b1_slot in [9u64, 17, 24] {
        shapes.push(vec![(0, b1_slot)]);
    }
    for gap in [1u64, 8] {
        shapes.push(vec![(0, 17), (1, 17 + gap)]);
    }
    shapes.push(vec![(0, 9), (0, 10)]);

    for shape in &shapes {
        for status_mask in 0u32..(1 << shape.len()) {
            let mut blocks = vec![ScenarioBlock {
                parent: 0,
                slot_offset: 0,
                parent_status: ParentStatus::Empty,
                proposer: 0,
            }];
            for (i, (parent, slot)) in shape.iter().enumerate() {
                let parent_slot = if *parent == 0 {
                    0
                } else {
                    shape[*parent - 1].1
                };
                blocks.push(ScenarioBlock {
                    parent: *parent,
                    slot_offset: slot - parent_slot,
                    parent_status: if status_mask & (1 << i) != 0 {
                        ParentStatus::Full
                    } else {
                        ParentStatus::Empty
                    },
                    proposer: i as u64 + 1,
                });
            }
            let slots = block_slots(&blocks);
            let max_slot = slots.iter().copied().max().unwrap();

            // `(cp0, cp1)` (unrealized ahead of realized) is only reachable through epoch 2: a
            // later state realizes it at the epoch transition, and post-EIP-7045 the epoch-1
            // attestations sourcing it cannot be included from epoch 3 onwards.
            let checkpoint_options = |slot: Slot| -> Vec<(Checkpoint, Checkpoint)> {
                let epoch = slot.epoch(E::slots_per_epoch());
                let mut options = vec![(cp0, cp0)];
                if slot >= Slot::new(8) && epoch <= Epoch::new(2) {
                    options.push((cp0, cp1));
                }
                if epoch >= Epoch::new(2) {
                    options.push((cp1, cp1));
                }
                options
            };

            let per_block_options = slots
                .iter()
                .map(|slot| checkpoint_options(*slot))
                .collect::<Vec<_>>();
            let mut assignments = vec![vec![]];
            for options in &per_block_options {
                assignments = assignments
                    .into_iter()
                    .flat_map(|prefix: Vec<(Checkpoint, Checkpoint)>| {
                        options.iter().map(move |option| {
                            let mut assignment = prefix.clone();
                            assignment.push(*option);
                            assignment
                        })
                    })
                    .collect();
            }

            for checkpoints in assignments {
                // A child crossing an epoch boundary realizes its parent's unrealized
                // justification; unrealized justification never regresses along a chain.
                let valid = blocks.iter().enumerate().skip(1).all(|(i, block)| {
                    let parent = block.parent;
                    let child_epoch = slots[i].epoch(E::slots_per_epoch());
                    let parent_epoch = slots[parent].epoch(E::slots_per_epoch());
                    let crossing_realizes = child_epoch <= parent_epoch
                        || checkpoints[i].0.epoch >= checkpoints[parent].1.epoch;
                    let unrealized_monotonic =
                        checkpoints[i].1.epoch >= checkpoints[parent].1.epoch;
                    crossing_realizes && unrealized_monotonic
                });
                if !valid {
                    continue;
                }

                let mut store_candidates = checkpoints
                    .iter()
                    .flat_map(|(j, u)| [*j, *u])
                    .filter(|c| c.epoch == Epoch::new(1))
                    .collect::<Vec<_>>();
                store_candidates.dedup();
                if store_candidates.is_empty() {
                    store_candidates.push(cp0);
                }

                for store_justified in store_candidates {
                    for current_slot in [max_slot, max_slot + 1, Slot::new(25), Slot::new(33)] {
                        if current_slot < max_slot {
                            continue;
                        }
                        for boost in std::iter::once(None).chain((1..blocks.len()).map(Some)) {
                            if let Some(boost) = boost
                                && slots[boost] != current_slot
                            {
                                continue;
                            }
                            for revealed_mask in [0u32, u32::MAX, 1] {
                                let revealed = (0..blocks.len())
                                    .map(|i| revealed_mask & (1 << i) != 0)
                                    .collect::<Vec<_>>();
                                for ptc in [PtcPattern::NoVotes, PtcPattern::Timely] {
                                    for vote_a in void_vote_options(blocks.len()) {
                                        for vote_b in void_vote_options(blocks.len()) {
                                            let scenario = Scenario {
                                                blocks: blocks.clone(),
                                                revealed: revealed.clone(),
                                                votes: vec![vote_a, vote_b],
                                                boost,
                                                ptc,
                                                extra_slot: false,
                                                balances: vec![32 * GWEI, 64 * GWEI],
                                                checkpoints: checkpoints.clone(),
                                                store_justified,
                                                equivocating: vec![],
                                                ptc_unrevealed: true,
                                            };
                                            f(&scenario, current_slot);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn void_vote_options(num_blocks: usize) -> Vec<Option<Vote>> {
    let mut options = vec![None];
    for block in 0..num_blocks {
        for payload_present in [false, true] {
            options.push(Some(Vote {
                block,
                same_slot: false,
                payload_present,
            }));
        }
    }
    options
}

fn deep_vote_options() -> Vec<Option<Vote>> {
    let mut options = vec![None];
    for block in [2usize, 3] {
        for payload_present in [false, true] {
            options.push(Some(Vote {
                block,
                same_slot: false,
                payload_present,
            }));
        }
    }
    options
}

#[test]
fn differential_two_blocks_exhaustive() {
    let spec = spec();
    let mut count = 0u64;
    for_each_scenario(|scenario, current_slot| {
        check(scenario, &spec, current_slot);
        count += 1;
    });
    println!("checked {count} scenarios");
}

fn checkpoint_json(checkpoint: Checkpoint) -> serde_json::Value {
    serde_json::json!({
        "epoch": checkpoint.epoch.as_u64(),
        "root": format!("{:?}", checkpoint.root),
    })
}

/// Export a strided sample of scenarios plus the reference's answers as versioned JSONL, for
/// certification against the executable pyspec (`make certify-fork-choice`).
#[test]
#[ignore]
fn export_certification_scenarios() {
    const FORMAT_VERSION: u32 = 2;
    const STRIDE: u64 = 320;

    let spec = spec();
    let out_path =
        std::env::var("CERTIFY_OUT").unwrap_or_else(|_| "certify_scenarios.jsonl".to_string());
    let mut lines = vec![];
    let mut count = 0u64;

    for_each_scenario(|scenario, current_slot| {
        if is_known_equivocation_deviation(scenario) {
            return;
        }
        count += 1;
        if count % STRIDE != 0 {
            return;
        }

        let slots = block_slots(&scenario.blocks);
        let (head_root, payload_status) = run_reference(scenario, &spec, current_slot);
        let status_name = |status: u8| match status {
            0 => "empty",
            1 => "full",
            _ => "pending",
        };

        let blocks = scenario
            .blocks
            .iter()
            .enumerate()
            .map(|(i, block)| {
                let bid_parent = match (i, block.parent_status) {
                    (0, _) | (_, ParentStatus::Empty) => ExecutionBlockHash::zero(),
                    (_, ParentStatus::Full) => bid_hash(block.parent),
                };
                serde_json::json!({
                    "root": format!("{:?}", block_root(i)),
                    "slot": slots[i].as_u64(),
                    "parent_root": format!("{:?}", if i == 0 { Hash256::zero() } else { block_root(block.parent) }),
                    "bid_block_hash": format!("{:?}", bid_hash(i).into_root()),
                    "bid_parent_block_hash": format!("{:?}", bid_parent.into_root()),
                    "proposer_index": block.proposer,
                    "justified_checkpoint": checkpoint_json(scenario.checkpoints[i].0),
                    "unrealized_justified_checkpoint": checkpoint_json(scenario.checkpoints[i].1),
                })
            })
            .collect::<Vec<_>>();

        let revealed = scenario
            .revealed
            .iter()
            .enumerate()
            .filter(|(_, r)| **r)
            .map(|(i, _)| format!("{:?}", block_root(i)))
            .collect::<Vec<_>>();

        let ptc_votes_map = scenario
            .revealed
            .iter()
            .enumerate()
            .filter(|(_, r)| **r)
            .map(|(i, _)| (format!("{:?}", block_root(i)), ptc_votes(scenario.ptc)))
            .collect::<BTreeMap<_, _>>();

        let messages = scenario
            .votes
            .iter()
            .enumerate()
            .filter_map(|(validator, vote)| {
                vote.map(|vote| {
                    let slot = if vote.same_slot {
                        slots[vote.block]
                    } else {
                        slots[vote.block] + 1
                    };
                    (
                        validator.to_string(),
                        serde_json::json!({
                            "root": format!("{:?}", block_root(vote.block)),
                            "slot": slot.as_u64(),
                            "payload_present": vote.payload_present,
                        }),
                    )
                })
            })
            .collect::<BTreeMap<_, _>>();

        lines.push(
            serde_json::json!({
                "format_version": FORMAT_VERSION,
                "blocks": blocks,
                "revealed": revealed,
                "ptc_votes": ptc_votes_map,
                "latest_messages": messages,
                "balances": scenario.balances,
                "proposer_boost_root": format!("{:?}", scenario.boost.map_or(Hash256::zero(), block_root)),
                "equivocating_indices": scenario.equivocating,
                "store_justified_checkpoint": checkpoint_json(scenario.store_justified),
                "store_finalized_checkpoint": checkpoint_json(checkpoint()),
                "current_slot": current_slot.as_u64(),
                "expected_head_root": format!("{head_root:?}"),
                "expected_payload_status": status_name(payload_status),
            })
            .to_string(),
        );
    });

    let header = serde_json::json!({
        "format_version": FORMAT_VERSION,
        "spec_version": "v1.7.0-alpha.11",
        "preset": "minimal",
        "ptc_size": E::ptc_size(),
        "slots_per_epoch": E::slots_per_epoch(),
        "proposer_score_boost": spec.proposer_score_boost,
        "reorg_head_weight_threshold": spec.reorg_head_weight_threshold,
        "anchor_root": format!("{ANCHOR_ROOT:?}"),
        "stride": STRIDE,
        "total_scenarios": count,
        "exported": lines.len(),
    })
    .to_string();

    let output = std::iter::once(header)
        .chain(lines)
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&out_path, output).unwrap();
    println!("exported to {out_path}");
}

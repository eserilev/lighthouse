use crate::store::{Block, ForkChoiceNode, LatestMessage, PayloadStatus, Store};
use std::collections::BTreeSet;
use types::{Hash256, Slot};

const NO_VOTES: &[Option<bool>] = &[];

/// Spec: `get_parent_payload_status` (the bid block-hash comparison is replaced by the block's
/// `parent_payload_status` field).
fn get_parent_payload_status(_store: &Store, block: &Block) -> PayloadStatus {
    block.parent_payload_status
}

/// Spec: `is_parent_node_full`.
fn is_parent_node_full(store: &Store, block: &Block) -> bool {
    get_parent_payload_status(store, block) == PayloadStatus::Full
}

/// Spec: `get_ancestor`.
fn get_ancestor(store: &Store, node: ForkChoiceNode, slot: Slot) -> ForkChoiceNode {
    let block = &store.blocks[&node.root];
    if block.slot > slot {
        let parent = ForkChoiceNode {
            root: block.parent_root,
            payload_status: get_parent_payload_status(store, block),
        };
        get_ancestor(store, parent, slot)
    } else {
        node
    }
}

/// Spec: `is_ancestor`.
fn is_ancestor(store: &Store, node: ForkChoiceNode, ancestor: ForkChoiceNode) -> bool {
    let node_ancestor = get_ancestor(store, node, store.blocks[&ancestor.root].slot);
    if node_ancestor.root != ancestor.root {
        return false;
    }
    node_ancestor.payload_status == ancestor.payload_status
        || ancestor.payload_status == PayloadStatus::Pending
}

/// Spec: `get_supported_node`.
fn get_supported_node(store: &Store, message: &LatestMessage) -> ForkChoiceNode {
    let block = &store.blocks[&message.root];
    let payload_status = if block.slot < message.slot {
        if message.payload_present {
            PayloadStatus::Full
        } else {
            PayloadStatus::Empty
        }
    } else {
        PayloadStatus::Pending
    };
    ForkChoiceNode {
        root: message.root,
        payload_status,
    }
}

/// Spec: `is_previous_slot_payload_decision`.
fn is_previous_slot_payload_decision(store: &Store, node: ForkChoiceNode) -> bool {
    let is_previous_slot = store.blocks[&node.root].slot + 1 == store.current_slot;
    let is_payload_decision = matches!(
        node.payload_status,
        PayloadStatus::Empty | PayloadStatus::Full
    );
    is_previous_slot && is_payload_decision
}

/// Spec: `is_payload_verified`.
fn is_payload_verified(store: &Store, root: Hash256) -> bool {
    store.payload_revealed.contains(&root)
}

/// Spec: `payload_timeliness`.
fn payload_timeliness(store: &Store, root: Hash256, timely: bool) -> bool {
    if !is_payload_verified(store, root) {
        return !timely;
    }
    let votes = store
        .payload_timeliness_votes
        .get(&root)
        .map_or(NO_VOTES, Vec::as_slice);
    votes.iter().filter(|vote| **vote == Some(timely)).count() > store.ptc_threshold()
}

/// Spec: `payload_data_availability`.
fn payload_data_availability(store: &Store, root: Hash256, available: bool) -> bool {
    if !is_payload_verified(store, root) {
        return !available;
    }
    let votes = store
        .payload_data_availability_votes
        .get(&root)
        .map_or(NO_VOTES, Vec::as_slice);
    votes
        .iter()
        .filter(|vote| **vote == Some(available))
        .count()
        > store.ptc_threshold()
}

/// Spec: `should_extend_payload`.
fn should_extend_payload(store: &Store, root: Hash256) -> bool {
    assert!(store.blocks[&root].slot + 1 == store.current_slot);
    if !is_payload_verified(store, root) {
        return false;
    }
    let proposer_root = store.proposer_boost_root;
    let payload_is_timely = payload_timeliness(store, root, true);
    let payload_data_is_available = payload_data_availability(store, root, true);
    (payload_is_timely && payload_data_is_available)
        || proposer_root.is_zero()
        || store.blocks[&proposer_root].parent_root != root
        || is_parent_node_full(store, &store.blocks[&proposer_root])
}

/// Spec: `get_payload_status_tiebreaker`.
fn get_payload_status_tiebreaker(store: &Store, node: ForkChoiceNode) -> u8 {
    if is_previous_slot_payload_decision(store, node) {
        if node.payload_status == PayloadStatus::Empty {
            return 1;
        }
        if should_extend_payload(store, node.root) {
            return 2;
        }
        0
    } else {
        node.payload_status as u8
    }
}

/// Spec: `get_attestation_score` (phase0). `store.balances` stands in for the justified state's
/// active, unslashed effective balances.
fn get_attestation_score(store: &Store, node: ForkChoiceNode) -> u64 {
    store
        .balances
        .iter()
        .enumerate()
        .filter(|(i, _)| {
            let i = *i as u64;
            store.latest_messages.get(&i).is_some_and(|message| {
                !store.equivocating_indices.contains(&i)
                    && is_ancestor(store, get_supported_node(store, message), node)
            })
        })
        .map(|(_, balance)| balance)
        .sum()
}

/// Spec: `get_total_active_balance` / `get_total_balance` (floored at the increment).
fn total_active_balance(store: &Store) -> u64 {
    store
        .effective_balance_increment
        .max(store.balances.iter().sum())
}

/// Spec: `calculate_committee_fraction` (phase0).
fn calculate_committee_fraction(store: &Store, committee_percent: u64) -> u64 {
    let committee_weight = total_active_balance(store) / store.slots_per_epoch;
    (committee_weight * committee_percent) / 100
}

/// Spec: `get_proposer_score` / `compute_proposer_score` (phase0).
fn get_proposer_score(store: &Store) -> u64 {
    calculate_committee_fraction(store, store.proposer_score_boost)
}

/// Spec: `is_head_weak` (modified in gloas): attestation score plus equivocating weight, never
/// `get_weight` (which would recurse via `should_apply_proposer_boost`). The spec counts only
/// equivocators in the head slot's committees; this model has no committee structure, so all
/// equivocating validators are counted.
fn is_head_weak(store: &Store, head_root: Hash256) -> bool {
    let reorg_threshold = calculate_committee_fraction(store, store.reorg_head_weight_threshold);
    let head_node = ForkChoiceNode {
        root: head_root,
        payload_status: PayloadStatus::Pending,
    };
    let equivocating_weight: u64 = store
        .equivocating_indices
        .iter()
        .filter_map(|i| store.balances.get(*i as usize))
        .sum();
    let head_weight = get_attestation_score(store, head_node) + equivocating_weight;
    head_weight < reorg_threshold
}

/// Spec: `should_apply_proposer_boost`.
fn should_apply_proposer_boost(store: &Store) -> bool {
    if store.proposer_boost_root.is_zero() {
        return false;
    }

    let block = &store.blocks[&store.proposer_boost_root];
    let parent_root = block.parent_root;
    let parent = &store.blocks[&parent_root];
    let slot = block.slot;

    if parent.slot + 1 < slot {
        return true;
    }

    if !is_head_weak(store, parent_root) {
        return true;
    }

    let equivocations = store
        .blocks
        .iter()
        .filter(|(root, block)| {
            block.ptc_timely
                && block.proposer_index == parent.proposer_index
                && block.slot + 1 == slot
                && **root != parent_root
        })
        .count();

    equivocations == 0
}

/// Spec: `get_weight`.
fn get_weight(store: &Store, node: ForkChoiceNode) -> u64 {
    if is_previous_slot_payload_decision(store, node) {
        return 0;
    }

    let attestation_score = get_attestation_score(store, node);
    if !should_apply_proposer_boost(store) {
        return attestation_score;
    }

    let mut proposer_score = 0;
    let proposer_boost_node = ForkChoiceNode {
        root: store.proposer_boost_root,
        payload_status: PayloadStatus::Pending,
    };
    if is_ancestor(store, proposer_boost_node, node) {
        proposer_score = get_proposer_score(store);
    }

    attestation_score + proposer_score
}

/// Spec: `get_current_store_epoch`.
fn get_current_store_epoch(store: &Store) -> types::Epoch {
    store.epoch_at_slot(store.current_slot)
}

/// Spec: `get_voting_source`.
fn get_voting_source(store: &Store, block_root: Hash256) -> types::Checkpoint {
    let block = &store.blocks[&block_root];
    let current_epoch = get_current_store_epoch(store);
    let block_epoch = store.epoch_at_slot(block.slot);
    if current_epoch > block_epoch {
        block.unrealized_justified_checkpoint
    } else {
        block.justified_checkpoint
    }
}

/// Spec: `get_checkpoint_block` (gloas: via `get_ancestor` on a pending node).
fn get_checkpoint_block(store: &Store, root: Hash256, epoch: types::Epoch) -> Hash256 {
    let epoch_first_slot = epoch.start_slot(store.slots_per_epoch);
    let node = ForkChoiceNode {
        root,
        payload_status: PayloadStatus::Pending,
    };
    get_ancestor(store, node, epoch_first_slot).root
}

/// Spec: `filter_block_tree`, accumulating viable roots into `blocks`.
fn filter_block_tree(store: &Store, block_root: Hash256, blocks: &mut BTreeSet<Hash256>) -> bool {
    let children = store
        .blocks
        .iter()
        .filter(|(_, block)| block.parent_root == block_root)
        .map(|(root, _)| *root)
        .collect::<Vec<_>>();

    if !children.is_empty() {
        let results = children
            .into_iter()
            .map(|child| filter_block_tree(store, child, blocks))
            .collect::<Vec<_>>();
        if results.into_iter().any(|viable| viable) {
            blocks.insert(block_root);
            return true;
        }
        return false;
    }

    let current_epoch = get_current_store_epoch(store);
    let voting_source = get_voting_source(store, block_root);

    let correct_justified = store.justified_checkpoint.epoch == 0
        || voting_source.epoch == store.justified_checkpoint.epoch
        || voting_source.epoch + 2 >= current_epoch;

    let finalized_checkpoint_block =
        get_checkpoint_block(store, block_root, store.finalized_checkpoint.epoch);

    let correct_finalized = store.finalized_checkpoint.epoch == 0
        || store.finalized_checkpoint.root == finalized_checkpoint_block;

    if correct_justified && correct_finalized {
        blocks.insert(block_root);
        return true;
    }

    false
}

/// Spec: `get_filtered_block_tree`.
fn get_filtered_block_tree(store: &Store) -> BTreeSet<Hash256> {
    let mut blocks = BTreeSet::new();
    filter_block_tree(store, store.justified_checkpoint.root, &mut blocks);
    blocks
}

/// Spec: `get_node_children`, restricted to the filtered block tree.
fn get_node_children(
    store: &Store,
    blocks: &BTreeSet<Hash256>,
    node: ForkChoiceNode,
) -> Vec<ForkChoiceNode> {
    if node.payload_status == PayloadStatus::Pending {
        let mut children = vec![ForkChoiceNode {
            root: node.root,
            payload_status: PayloadStatus::Empty,
        }];
        if is_payload_verified(store, node.root) {
            children.push(ForkChoiceNode {
                root: node.root,
                payload_status: PayloadStatus::Full,
            });
        }
        children
    } else {
        blocks
            .iter()
            .filter(|root| {
                let block = &store.blocks[*root];
                block.parent_root == node.root
                    && node.payload_status == get_parent_payload_status(store, block)
            })
            .map(|root| ForkChoiceNode {
                root: *root,
                payload_status: PayloadStatus::Pending,
            })
            .collect()
    }
}

/// Spec: `is_head_late`.
fn is_head_late(store: &Store, head_root: Hash256) -> bool {
    !store.blocks[&head_root].attestation_timely
}

/// Spec: `is_epoch_boundary` (true when the proposal slot is NOT on a boundary).
fn is_epoch_boundary(store: &Store, slot: Slot) -> bool {
    slot % store.slots_per_epoch != 0
}

/// Spec: `is_ffg_competitive`.
fn is_ffg_competitive(store: &Store, head_root: Hash256, parent_root: Hash256) -> bool {
    store.blocks[&head_root].unrealized_justified_checkpoint
        == store.blocks[&parent_root].unrealized_justified_checkpoint
}

/// Spec: `is_finalization_ok`.
fn is_finalization_ok(store: &Store, slot: Slot) -> bool {
    let epochs_since_finalization = store
        .epoch_at_slot(slot)
        .saturating_sub(store.finalized_checkpoint.epoch);
    epochs_since_finalization.as_u64() <= store.reorg_max_epochs_since_finalization
}

/// Spec: `is_proposing_on_time`.
fn is_proposing_on_time(store: &Store) -> bool {
    store.time_into_slot_ms <= store.proposer_reorg_cutoff_ms
}

/// Spec: `is_parent_strong` (phase0, with the gloas `get_weight`).
fn is_parent_strong(store: &Store, head_root: Hash256) -> bool {
    let parent_threshold = calculate_committee_fraction(store, store.reorg_parent_weight_threshold);
    let parent_node = ForkChoiceNode {
        root: store.blocks[&head_root].parent_root,
        payload_status: PayloadStatus::Pending,
    };
    get_weight(store, parent_node) > parent_threshold
}

/// Spec: `is_proposer_equivocation`.
fn is_proposer_equivocation(store: &Store, root: Hash256) -> bool {
    let block = &store.blocks[&root];
    store
        .blocks
        .values()
        .filter(|other| other.proposer_index == block.proposer_index && other.slot == block.slot)
        .count()
        > 1
}

/// Spec: `get_proposer_head` (gloas: the parent node keeps its payload status).
pub fn get_proposer_head(store: &Store, head_node: ForkChoiceNode, slot: Slot) -> ForkChoiceNode {
    let head_block = &store.blocks[&head_node.root];
    let parent_root = head_block.parent_root;
    let parent_block = &store.blocks[&parent_root];
    let parent_node = ForkChoiceNode {
        root: parent_root,
        payload_status: get_parent_payload_status(store, head_block),
    };

    let head_late = is_head_late(store, head_node.root);
    let epoch_boundary = is_epoch_boundary(store, slot);
    let ffg_competitive = is_ffg_competitive(store, head_node.root, parent_root);
    let finalization_ok = is_finalization_ok(store, slot);
    let proposing_on_time = is_proposing_on_time(store);

    let parent_slot_ok = parent_block.slot + 1 == head_block.slot;
    let current_time_ok = head_block.slot + 1 == slot;
    let single_slot_reorg = parent_slot_ok && current_time_ok;

    assert!(store.proposer_boost_root != head_node.root);
    let head_weak = is_head_weak(store, head_node.root);
    let parent_strong = is_parent_strong(store, head_node.root);
    let proposer_equivocation = is_proposer_equivocation(store, head_node.root);

    if head_late
        && epoch_boundary
        && ffg_competitive
        && finalization_ok
        && proposing_on_time
        && single_slot_reorg
        && head_weak
        && parent_strong
    {
        parent_node
    } else if head_weak && current_time_ok && proposer_equivocation {
        parent_node
    } else {
        head_node
    }
}

/// Spec: `get_head`.
pub fn get_head(store: &Store) -> ForkChoiceNode {
    let blocks = get_filtered_block_tree(store);
    let mut head = ForkChoiceNode {
        root: store.justified_checkpoint.root,
        payload_status: PayloadStatus::Pending,
    };

    loop {
        let children = get_node_children(store, &blocks, head);
        let Some(best) = children.into_iter().max_by_key(|child| {
            (
                get_weight(store, *child),
                child.root,
                get_payload_status_tiebreaker(store, *child),
            )
        }) else {
            return head;
        };
        head = best;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    fn root(i: u8) -> Hash256 {
        Hash256::repeat_byte(i)
    }

    fn genesis_checkpoint() -> types::Checkpoint {
        types::Checkpoint {
            epoch: types::Epoch::new(0),
            root: root(1),
        }
    }

    fn block(slot: u64, parent_root: Hash256, parent_payload_status: PayloadStatus) -> Block {
        Block {
            slot: Slot::new(slot),
            parent_root,
            parent_payload_status,
            proposer_index: 0,
            ptc_timely: false,
            attestation_timely: true,
            justified_checkpoint: genesis_checkpoint(),
            unrealized_justified_checkpoint: genesis_checkpoint(),
        }
    }

    fn base_store() -> Store {
        let mut blocks = BTreeMap::new();
        blocks.insert(root(1), block(0, Hash256::ZERO, PayloadStatus::Empty));
        Store {
            current_slot: Slot::new(10),
            justified_checkpoint: genesis_checkpoint(),
            finalized_checkpoint: genesis_checkpoint(),
            blocks,
            payload_revealed: BTreeSet::new(),
            payload_timeliness_votes: BTreeMap::new(),
            payload_data_availability_votes: BTreeMap::new(),
            latest_messages: BTreeMap::new(),
            equivocating_indices: BTreeSet::new(),
            balances: vec![32; 8],
            proposer_boost_root: Hash256::ZERO,
            ptc_size: 4,
            slots_per_epoch: 32,
            proposer_score_boost: 40,
            reorg_head_weight_threshold: 20,
            effective_balance_increment: 1_000_000_000,
            reorg_parent_weight_threshold: 160,
            reorg_max_epochs_since_finalization: 2,
            time_into_slot_ms: 0,
            proposer_reorg_cutoff_ms: 1_000,
        }
    }

    #[test]
    fn head_is_empty_when_payload_not_revealed() {
        let store = base_store();
        assert_eq!(
            get_head(&store),
            ForkChoiceNode {
                root: root(1),
                payload_status: PayloadStatus::Empty,
            }
        );
    }

    #[test]
    fn full_beats_empty_on_status_tiebreak() {
        let mut store = base_store();
        store.payload_revealed.insert(root(1));
        assert_eq!(
            get_head(&store),
            ForkChoiceNode {
                root: root(1),
                payload_status: PayloadStatus::Full,
            }
        );
    }

    #[test]
    fn vote_weight_selects_fork() {
        let mut store = base_store();
        store
            .blocks
            .insert(root(2), block(2, root(1), PayloadStatus::Empty));
        store
            .blocks
            .insert(root(3), block(2, root(1), PayloadStatus::Empty));
        store.latest_messages.insert(
            0,
            LatestMessage {
                slot: Slot::new(2),
                root: root(2),
                payload_present: false,
            },
        );
        assert_eq!(get_head(&store).root, root(2));
    }

    #[test]
    fn lexicographic_root_tiebreak_on_equal_weight() {
        let mut store = base_store();
        store
            .blocks
            .insert(root(2), block(2, root(1), PayloadStatus::Empty));
        store
            .blocks
            .insert(root(3), block(2, root(1), PayloadStatus::Empty));
        assert_eq!(get_head(&store).root, root(3));
    }
}

use crate::Error;
use lean_types::{Hash256, Slot, State};
use safe_arith::SafeArith;
use tree_hash::TreeHash;

pub fn process_slots(state: &mut State, target_slot: Slot) -> Result<(), Error> {
    if state.slot >= target_slot {
        return Err(Error::BlockSlotNotInFuture {
            state_slot: state.slot,
            target_slot,
        });
    }

    while state.slot < target_slot {
        if state.latest_block_header.state_root == Hash256::ZERO {
            state.latest_block_header.state_root = state.tree_hash_root();
        }
        state.slot = Slot::new(state.slot.as_u64().safe_add(1)?);
    }

    Ok(())
}

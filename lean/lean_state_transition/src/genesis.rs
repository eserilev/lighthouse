use crate::Error;
use lean_types::{
    BlockBody, BlockHeader, Checkpoint, GenesisConfig, Hash256, JustificationValidators,
    JustifiedSlots, Slot, State, Validators,
};
use tree_hash::TreeHash;

pub fn generate_genesis(genesis_time: u64, validators: Validators) -> Result<State, Error> {
    Ok(State {
        config: GenesisConfig { genesis_time },
        slot: Slot::new(0),
        latest_block_header: BlockHeader {
            slot: Slot::new(0),
            proposer_index: 0,
            parent_root: Hash256::ZERO,
            state_root: Hash256::ZERO,
            body_root: BlockBody::default().tree_hash_root(),
        },
        latest_justified: Checkpoint::default(),
        latest_finalized: Checkpoint::default(),
        historical_block_hashes: <_>::default(),
        justified_slots: JustifiedSlots::with_capacity(0)?,
        validators,
        justifications_roots: <_>::default(),
        justifications_validators: JustificationValidators::with_capacity(0)?,
    })
}

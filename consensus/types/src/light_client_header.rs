use crate::{light_client_update::*, ChainSpec};
use crate::{
    test_utils::TestRandom, EthSpec, ExecutionPayloadHeader, FixedVector, Hash256,
    SignedBeaconBlock,
};
use crate::{BeaconBlockHeader, ExecutionPayload};
use merkle_proof::verify_merkle_proof;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use test_random_derive::TestRandom;
use tree_hash::TreeHash;

#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    Encode,
    Decode,
    TestRandom,
    arbitrary::Arbitrary,
)]
#[serde(bound = "E: EthSpec")]
#[arbitrary(bound = "E: EthSpec")]
pub struct LightClientHeader<E: EthSpec> {
    pub beacon: BeaconBlockHeader,
    #[test_random(default)]
    #[ssz(skip_serializing, skip_deserializing)]
    pub execution: Option<ExecutionPayloadHeader<E>>,
    #[test_random(default)]
    pub execution_branch: Option<FixedVector<Hash256, ExecutionPayloadProofLen>>,
}

impl<E: EthSpec> From<BeaconBlockHeader> for LightClientHeader<E> {
    fn from(beacon: BeaconBlockHeader) -> Self {
        LightClientHeader {
            beacon,
            execution: None,
            execution_branch: None,
        }
    }
}

impl<E: EthSpec> LightClientHeader<E> {
    fn new(chain_spec: ChainSpec, block: SignedBeaconBlock<E>) -> Result<Self, Error> {
        let current_epoch = block.slot().epoch(E::slots_per_epoch());

        if let Some(deneb_fork_epoch) = chain_spec.deneb_fork_epoch {
            if current_epoch >= deneb_fork_epoch {
                let payload: ExecutionPayload<E> = block
                    .message()
                    .execution_payload()?
                    .execution_payload_deneb()?
                    .to_owned()
                    .into();
                let header = ExecutionPayloadHeader::from(payload.to_ref());

                // TODO calculate execution branch, i.e. the merkle proof of the execution payload header
                return Ok(LightClientHeader {
                    beacon: block.message().block_header(),
                    execution: Some(header),
                    execution_branch: None,
                });
            }
        };

        if let Some(capella_fork_epoch) = chain_spec.capella_fork_epoch {
            if current_epoch >= capella_fork_epoch {
                let payload: ExecutionPayload<E> = block
                    .message()
                    .execution_payload()?
                    .execution_payload_capella()?
                    .to_owned()
                    .into();
                let header = ExecutionPayloadHeader::from(payload.to_ref());

                // TODO calculate execution branch, i.e. the merkle proof of the execution payload header
                return Ok(LightClientHeader {
                    beacon: block.message().block_header(),
                    execution: Some(header),
                    execution_branch: None,
                });
            }
        };

        Ok(LightClientHeader {
            beacon: block.message().block_header(),
            execution: None,
            execution_branch: None,
        })
    }

    fn get_lc_execution_root(&self, chain_spec: ChainSpec) -> Option<Hash256> {
        let current_epoch = self.beacon.slot.epoch(E::slots_per_epoch());

        if let Some(capella_fork_epoch) = chain_spec.capella_fork_epoch {
            if current_epoch >= capella_fork_epoch {
                if let Some(execution) = &self.execution {
                    return Some(execution.tree_hash_root());
                }
            }
        }

        None
    }

    fn is_valid_light_client_header(&self, chain_spec: ChainSpec) -> Result<bool, Error> {
        let current_epoch = self.beacon.slot.epoch(E::slots_per_epoch());

        if let Some(capella_fork_epoch) = chain_spec.capella_fork_epoch {
            if current_epoch < capella_fork_epoch {
                return Ok(self.execution == None && self.execution_branch == None);
            }
        }

        if let Some(deneb_fork_epoch) = chain_spec.deneb_fork_epoch {
            if current_epoch < deneb_fork_epoch {
                let Some(execution) = &self.execution else {
                    return Ok(false);
                };

                if execution.blob_gas_used()?.to_owned() != 0 as u64
                    || execution.excess_blob_gas()?.to_owned() != 0 as u64
                {
                    return Ok(false);
                }
            }
        }

        let Some(execution_root) = self.get_lc_execution_root(chain_spec) else {
            return Ok(false);
        };

        let Some(execution_branch) = &self.execution_branch else {
            return Ok(false);
        };

        Ok(verify_merkle_proof(
            execution_root,
            &execution_branch,
            EXECUTION_PAYLOAD_PROOF_LEN,
            get_subtree_index(EXECUTION_PAYLOAD_INDEX as u32) as usize,
            self.beacon.body_root,
        ))
    }
}

// TODO move to the relevant place
fn get_subtree_index(generalized_index: u32) -> u32 {
    return generalized_index % 2 * (log2_int(generalized_index));
}

// TODO move to the relevant place
fn log2_int(x: u32) -> u32 {
    if x == 0 {
        return 0;
    }
    31 - x.leading_zeros()
}
